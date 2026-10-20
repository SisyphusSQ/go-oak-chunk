# Controlled DELETE SDK

`NewControlledExecutor(ctx, conn, config, hooks)` is an opt-in API for embedding
applications which own approval, scheduling, credentials and database sessions.
It does not use the ordinary executor's connection discovery, retry, strategy
selection, progress goroutines or logging. Existing `NewExecutor` behavior is
unchanged.

## Caller responsibilities

- Supply one dedicated `*sql.Conn` with its default schema selected and session
  time zone explicitly set to `+00:00`. Keep a table advisory lock on this same
  physical connection until execution has stopped. Do not concurrently use it.
- Before constructing the executor, verify MySQL, InnoDB, a single integer `id`
  primary key, the datetime/timestamp column, resource identity and any required
  access plan. The controlled API does not certify metadata or approval.
- Perform bounded read-only prechecks separately. This API has no dry-run mode;
  calling `Run` is an actual DELETE operation. Ordinary executor dry-run behavior
  is a separate contract and must not replace an application precheck.
- Construct the typed plan from approved fields. Identifiers are restricted to
  ASCII identifiers of at most 64 characters. The cutoff must be a nonzero,
  whole-second UTC timestamp; batch size is 1–1000, total rows 1–1000000, rate
  1–10000 rows/sec, duration at most one hour, statement timeout at most five
  seconds. The time column cannot be `id`.

## Execution contract

The executor selects an ascending primary-key chunk on the borrowed connection,
closes the rows, synchronously calls `BeforeStatement`, and executes one
transaction with one DELETE restricted to those ids and the same cutoff. Values
and limits are bound parameters. Each statement's limit is the smaller of batch
size and remaining row budget. Signed BIGINT and unsigned BIGINT ids are
supported. Candidate and affected-row streams are validated.

After a successful COMMIT, the cumulative committed count is updated before
calling `AfterCommit` synchronously. An error in either hook stops the run; a
receipt error does not undo the committed count. Limits are enforced before
DML, not by treating a post-commit counter as a hard budget. The executor then
waits for rows/rate with cancellation enabled; the caller must independently
renew/check its execution lease during waits and queries. Hooks should finish
within their supplied context; approval checks happen at the statement boundary
and cannot revoke a transaction already committed.

No DML, COMMIT or ROLLBACK is automatically retried. Errors expose a fixed phase
and reason without raw driver errors, SQL, bound values or connection material.
Unconfirmed commit/rollback outcomes and unexpected panics carry
`OutcomeUnknown`. `CommittedRows` is the acknowledged count, not proof that an
unknown transaction had no effect.

`Run` returns `exhausted`, `budget_reached`, `guard_denied`, `canceled`, `timeout`,
`failed` or `outcome_unknown`. Budget exhaustion is not proof that no matching
rows remain. Each executor can run only once. Construct a new one for each run.

## Stop and resource ownership

`Stop()` requests cancellation. `Wait(ctx)` waits until the synchronous Run has
returned, including synchronous hooks. If a dependency ignores cancellation,
Wait can time out; this must not be reported as a completed stop. No background
producer or writer is created by this API.

The executor never closes the caller's connection or pool and never reconnects.
After local completion the caller must close/discard the physical writing
connection and verify server-side termination where required. Local Run/Wait
completion does not prove server termination after a transport failure. Retain
active ownership and block recovery when this proof is unavailable.

## Deterministic verification

`go test -race . -run TestControlled -count=1` uses a synthetic database/sql
driver; it opens no network connections. It covers strict remaining budgets,
bound SQL, signed/unsigned ids, synchronous guard/receipt errors, no retry after
execution/commit/rollback failures, UTC and input admission, replay rejection,
Stop/Wait and sanitized panic handling. Real MySQL acceptance is separate.

package oak

import (
	"context"
	"database/sql"
	"errors"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ControlledConfig is deliberately a typed DELETE plan, with no connection
// credentials, raw SQL, automatic discovery, retries, or strategy fallback.
type ControlledConfig struct {
	Schema, Table, TimeColumn     string
	CutoffUTC                     time.Time
	BatchSize                     int
	MaxRows, RowsPerSec           int64
	MaxDuration, StatementTimeout time.Duration
}
type ControlledBatch struct {
	Number        int
	Limit         int
	CommittedRows int64
}
type ControlledReceipt struct {
	Number                      int
	AffectedRows, CommittedRows int64
}
type ControlledHooks struct {
	BeforeStatement func(context.Context, ControlledBatch) error
	AfterCommit     func(context.Context, ControlledReceipt) error
}
type ControlledReason string

const (
	ControlledExhausted     ControlledReason = "exhausted"
	ControlledBudgetReached ControlledReason = "budget_reached"
	ControlledCanceled      ControlledReason = "canceled"
	ControlledTimeout       ControlledReason = "timeout"
	ControlledGuardDenied   ControlledReason = "guard_denied"
	ControlledFailed        ControlledReason = "failed"
	ControlledUnknown       ControlledReason = "outcome_unknown"
)

type ControlledResult struct {
	CommittedRows  int64
	Batches        int
	Reason         ControlledReason
	Phase          string
	OutcomeUnknown bool
}

// ControlledExecutor borrows ONE caller-owned connection. Run uses it serially
// and never reconnects, changes its session settings, or closes it. The caller
// must hold its target advisory lock on this connection for the entire run.
// Stop requests cancellation; Wait confirms local Run completion only. Neither
// proves that a server-side statement has stopped after a transport failure.
type ControlledExecutor struct {
	conn             *sql.Conn
	cfg              ControlledConfig
	hooks            ControlledHooks
	started, stopped atomic.Bool
	mu               sync.Mutex
	cancel           context.CancelFunc
	done             chan struct{}
}

var controlledIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)
var controlledInteger = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)

func NewControlledExecutor(ctx context.Context, conn *sql.Conn, cfg ControlledConfig, hooks ControlledHooks) (*ControlledExecutor, error) {
	if ctx == nil || conn == nil || hooks.BeforeStatement == nil || hooks.AfterCommit == nil {
		return nil, errors.New("controlled execution requires context, connection and synchronous hooks")
	}
	_, offset := cfg.CutoffUTC.Zone()
	if !controlledIdentifier.MatchString(cfg.Schema) || !controlledIdentifier.MatchString(cfg.Table) || !controlledIdentifier.MatchString(cfg.TimeColumn) || cfg.TimeColumn == "id" || cfg.CutoffUTC.IsZero() || offset != 0 || cfg.CutoffUTC.Nanosecond() != 0 || cfg.BatchSize < 1 || cfg.BatchSize > 1000 || cfg.MaxRows < 1 || cfg.MaxRows > 1000000 || cfg.RowsPerSec < 1 || cfg.RowsPerSec > 10000 || cfg.MaxDuration <= 0 || cfg.MaxDuration > time.Hour || cfg.StatementTimeout <= 0 || cfg.StatementTimeout > 5*time.Second {
		return nil, errors.New("invalid controlled execution plan")
	}
	op, cancel := context.WithTimeout(ctx, min(cfg.StatementTimeout, cfg.MaxDuration))
	defer cancel()
	var schema, zone string
	if err := conn.QueryRowContext(op, "SELECT DATABASE(), @@session.time_zone").Scan(&schema, &zone); err != nil || schema != cfg.Schema || zone != "+00:00" {
		return nil, errors.New("controlled session identity or UTC setting unavailable")
	}
	return &ControlledExecutor{conn: conn, cfg: cfg, hooks: hooks, done: make(chan struct{})}, nil
}

func (e *ControlledExecutor) Run(ctx context.Context) (result ControlledResult, err error) {
	if ctx == nil {
		return result, errors.New("controlled execution context required")
	}
	if !e.started.CompareAndSwap(false, true) {
		return result, ErrExecutorAlreadyRun
	}
	runCtx, cancel := context.WithTimeout(ctx, e.cfg.MaxDuration)
	e.mu.Lock()
	e.cancel = cancel
	e.mu.Unlock()
	defer func() {
		cancel()
		e.mu.Lock()
		e.cancel = nil
		e.mu.Unlock()
		if recover() != nil {
			result.Reason = ControlledUnknown
			result.Phase = "panic"
			result.OutcomeUnknown = true
			err = errors.New("controlled execution panic; reconciliation required")
		}
		close(e.done)
	}()
	if e.stopped.Load() {
		cancel()
	}
	lastID := ""
	for result.CommittedRows < e.cfg.MaxRows {
		if runCtx.Err() != nil {
			return e.contextFailure(result, runCtx)
		}
		limit := min(e.cfg.BatchSize, int(e.cfg.MaxRows-result.CommittedRows))
		op, stop := context.WithTimeout(runCtx, e.cfg.StatementTimeout)
		ids, readErr := e.selectIDs(op, lastID, limit)
		stop()
		if readErr != nil {
			if runCtx.Err() != nil {
				return e.contextFailure(result, runCtx)
			}
			return e.failure(result, ControlledFailed, "select", false)
		}
		if len(ids) == 0 {
			result.Reason = ControlledExhausted
			return result, nil
		}
		batch := ControlledBatch{Number: result.Batches + 1, Limit: limit, CommittedRows: result.CommittedRows}
		// Hooks are synchronous and cannot be skipped by a busy progress callback.
		op, stop = context.WithTimeout(runCtx, e.cfg.StatementTimeout)
		guardErr := e.hooks.BeforeStatement(op, batch)
		if guardErr != nil {
			stop()
			return e.failure(result, ControlledGuardDenied, "guard", false)
		}
		// Do not issue a new time budget after an authorization check. A paused
		// owner must not resume with a stale check but a fresh write deadline.
		if op.Err() != nil {
			failed, failure := e.contextFailure(result, op)
			stop()
			return failed, failure
		}
		affected, phase, unknown, writeErr := e.deleteIDs(op, ids, limit)
		stop()
		if writeErr != nil {
			reason := ControlledFailed
			if unknown {
				reason = ControlledUnknown
			}
			return e.failure(result, reason, phase, unknown)
		}
		result.CommittedRows += affected
		result.Batches++
		op, stop = context.WithTimeout(runCtx, e.cfg.StatementTimeout)
		receiptErr := e.hooks.AfterCommit(op, ControlledReceipt{Number: result.Batches, AffectedRows: affected, CommittedRows: result.CommittedRows})
		stop()
		if receiptErr != nil {
			return e.failure(result, ControlledFailed, "receipt", false)
		}
		lastID = ids[len(ids)-1]
		// The row budget is already satisfied. A rate-limit wait here can outlive
		// the run context and would report timeout or cancel for a finished delete.
		if result.CommittedRows >= e.cfg.MaxRows {
			break
		}
		delay := time.Duration(affected) * time.Second / time.Duration(e.cfg.RowsPerSec)
		timer := time.NewTimer(delay)
		select {
		case <-runCtx.Done():
			timer.Stop()
			return e.contextFailure(result, runCtx)
		case <-timer.C:
		}
	}
	result.Reason = ControlledBudgetReached
	return result, nil
}

// cutoffArg is the approved UTC wall time. Bind a datetime literal rather than
// time.Time: go-sql-driver reformats time.Time in the DSN loc before sending,
// which moves the comparison off the UTC cutoff when loc is not UTC.
func (e *ControlledExecutor) cutoffArg() string {
	return e.cfg.CutoffUTC.UTC().Format("2006-01-02 15:04:05")
}

func (e *ControlledExecutor) selectIDs(ctx context.Context, last string, limit int) ([]string, error) {
	q := "SELECT `id` FROM `" + e.cfg.Schema + "`.`" + e.cfg.Table + "` WHERE `" + e.cfg.TimeColumn + "` < ?"
	args := []any{e.cutoffArg()}
	if last != "" {
		q += " AND `id` > ?"
		args = append(args, e.numericID(last))
	}
	q += " ORDER BY `id` LIMIT ?"
	args = append(args, limit)
	rows, err := e.conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, errors.New("controlled candidate read failed")
	}
	defer rows.Close()
	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if rows.Scan(&id) != nil || !e.validID(id) || len(ids) >= limit {
			return nil, errors.New("controlled candidate identity invalid")
		}
		ids = append(ids, id)
	}
	if rows.Err() != nil {
		return nil, errors.New("controlled candidate read failed")
	}
	return ids, nil
}
func (e *ControlledExecutor) validID(id string) bool {
	if len(id) > 20 || !controlledInteger.MatchString(id) {
		return false
	}
	n, ok := new(big.Int).SetString(id, 10)
	if !ok {
		return false
	}
	minID := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 63))
	maxID := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(1))
	return n.Cmp(minID) >= 0 && n.Cmp(maxID) <= 0
}

// IDs are validated before reaching this conversion. Bind numeric values,
// including uint64's high bit, instead of relying on string/numeric coercion.
func (e *ControlledExecutor) numericID(id string) any {
	if value, err := strconv.ParseInt(id, 10, 64); err == nil {
		return value
	}
	value, _ := strconv.ParseUint(id, 10, 64)
	return value
}
func (e *ControlledExecutor) deleteIDs(ctx context.Context, ids []string, limit int) (int64, string, bool, error) {
	tx, err := e.conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, "begin", false, errors.New("controlled transaction unavailable")
	}
	defer func() { _ = tx.Rollback() }()
	args := []any{e.cutoffArg()}
	for _, id := range ids {
		args = append(args, e.numericID(id))
	}
	args = append(args, limit)
	q := "DELETE FROM `" + e.cfg.Schema + "`.`" + e.cfg.Table + "` WHERE `" + e.cfg.TimeColumn + "` < ? AND `id` IN (" + strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",") + ") LIMIT ?"
	res, err := tx.ExecContext(ctx, q, args...)
	if err != nil {
		rollbackErr := tx.Rollback()
		return 0, "execute", rollbackErr != nil, errors.New("controlled delete failed")
	}
	affected, err := res.RowsAffected()
	if err != nil || affected < 0 || affected > int64(limit) || affected > int64(len(ids)) {
		rollbackErr := tx.Rollback()
		return 0, "rows_affected", rollbackErr != nil, errors.New("controlled affected rows unavailable")
	}
	if err = tx.Commit(); err != nil {
		return 0, "commit", true, errors.New("controlled commit outcome unknown")
	}
	return affected, "commit", false, nil
}
func (e *ControlledExecutor) failure(r ControlledResult, reason ControlledReason, phase string, unknown bool) (ControlledResult, error) {
	r.Reason = reason
	r.Phase = phase
	r.OutcomeUnknown = unknown
	return r, errors.New("controlled execution stopped: " + string(reason) + " (" + phase + ")")
}
func (e *ControlledExecutor) contextFailure(r ControlledResult, ctx context.Context) (ControlledResult, error) {
	reason := ControlledCanceled
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		reason = ControlledTimeout
	}
	return e.failure(r, reason, "context", false)
}
func (e *ControlledExecutor) Stop() {
	e.stopped.Store(true)
	e.mu.Lock()
	cancel := e.cancel
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}
func (e *ControlledExecutor) Wait(ctx context.Context) error {
	if ctx == nil {
		return errors.New("controlled wait context required")
	}
	select {
	case <-e.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

package oak

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The driver exercises real database/sql connection/transaction boundaries;
// it never opens a socket or reads configuration or credentials.
type controlledDriver struct{ state *controlledState }
type controlledConn struct{ state *controlledState }
type controlledTx struct{ state *controlledState }
type controlledRows struct {
	cols   []string
	values [][]driver.Value
	index  int
}
type controlledResult struct {
	n   int64
	err error
}
type controlledState struct {
	mu                                       sync.Mutex
	ids                                      []string
	offset                                   int
	limits                                   []int64
	statements                               []string
	commits, rollbacks                       int
	execErr, commitErr, rollbackErr, rowsErr error
	zone                                     string
	boundIDs                                 []any
}

var controlledSequence atomic.Int64

func (d controlledDriver) Open(string) (driver.Conn, error)   { return &controlledConn{d.state}, nil }
func (c *controlledConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (c *controlledConn) Close() error                        { return nil }
func (c *controlledConn) Begin() (driver.Tx, error)           { return &controlledTx{c.state}, nil }
func (c *controlledConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return c.Begin()
}
func (c *controlledConn) CheckNamedValue(value *driver.NamedValue) error {
	if _, ok := value.Value.(uint64); ok {
		return nil
	}
	return driver.ErrSkip
}
func (c *controlledConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if strings.Contains(q, "DATABASE()") {
		return &controlledRows{cols: []string{"db", "zone"}, values: [][]driver.Value{{"app", c.state.zone}}}, nil
	}
	if !strings.HasPrefix(q, "SELECT `id` FROM `app`.`events` WHERE `ts` < ?") {
		return nil, fmt.Errorf("unexpected query")
	}
	limit := args[len(args)-1].Value.(int64)
	n := min(int(limit), len(c.state.ids)-c.state.offset)
	vals := make([][]driver.Value, n)
	for i := range vals {
		vals[i] = []driver.Value{c.state.ids[c.state.offset+i]}
	}
	c.state.offset += n
	return &controlledRows{cols: []string{"id"}, values: vals}, nil
}
func (c *controlledConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.statements = append(c.state.statements, q)
	limit := args[len(args)-1].Value.(int64)
	c.state.limits = append(c.state.limits, limit)
	for _, arg := range args[1 : len(args)-1] {
		c.state.boundIDs = append(c.state.boundIDs, arg.Value)
	}
	if c.state.execErr != nil {
		return nil, c.state.execErr
	}
	return controlledResult{int64(len(args) - 2), c.state.rowsErr}, nil
}
func (t *controlledTx) Commit() error {
	t.state.mu.Lock()
	defer t.state.mu.Unlock()
	t.state.commits++
	return t.state.commitErr
}
func (t *controlledTx) Rollback() error {
	t.state.mu.Lock()
	defer t.state.mu.Unlock()
	t.state.rollbacks++
	return t.state.rollbackErr
}
func (r *controlledRows) Columns() []string { return r.cols }
func (r *controlledRows) Close() error      { return nil }
func (r *controlledRows) Next(dest []driver.Value) error {
	if r.index == len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}
func (r controlledResult) LastInsertId() (int64, error) { return 0, nil }
func (r controlledResult) RowsAffected() (int64, error) { return r.n, r.err }
func controlledFixture(t *testing.T) (*sql.Conn, *controlledState, ControlledConfig, ControlledHooks) {
	t.Helper()
	s := &controlledState{ids: []string{"-2", "0", "1", "4", "18446744073709551615", "18446744073709551616"}, zone: "+00:00"}
	name := fmt.Sprintf("controlled_%d", controlledSequence.Add(1))
	sql.Register(name, controlledDriver{s})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	cfg := ControlledConfig{Schema: "app", Table: "events", TimeColumn: "ts", CutoffUTC: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), BatchSize: 3, MaxRows: 5, RowsPerSec: 10000, MaxDuration: time.Second, StatementTimeout: time.Second}
	hooks := ControlledHooks{BeforeStatement: func(context.Context, ControlledBatch) error { return nil }, AfterCommit: func(context.Context, ControlledReceipt) error { return nil }}
	return conn, s, cfg, hooks
}

func TestControlledStrictBudgetAndReceipts(t *testing.T) {
	conn, s, cfg, hooks := controlledFixture(t)
	var receipts []int64
	hooks.AfterCommit = func(_ context.Context, r ControlledReceipt) error {
		receipts = append(receipts, r.CommittedRows)
		return nil
	}
	e, err := NewControlledExecutor(context.Background(), conn, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	result, err := e.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.CommittedRows != 5 || result.Reason != ControlledBudgetReached || fmt.Sprint(s.limits) != "[3 2]" || fmt.Sprint(receipts) != "[3 5]" {
		t.Fatalf("result=%+v limits=%v receipts=%v", result, s.limits, receipts)
	}
	for _, q := range s.statements {
		if !strings.Contains(q, "`ts` < ? AND `id` IN (") || !strings.HasSuffix(q, "LIMIT ?") {
			t.Fatalf("unbounded statement: %s", q)
		}
	}
	if err = e.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Run(context.Background()); err == nil {
		t.Fatal("executor replay allowed")
	}
}
func TestControlledGuardAndReceiptFailuresStop(t *testing.T) {
	for _, phase := range []string{"guard", "receipt"} {
		t.Run(phase, func(t *testing.T) {
			conn, s, cfg, hooks := controlledFixture(t)
			if phase == "guard" {
				hooks.BeforeStatement = func(context.Context, ControlledBatch) error { return errors.New("secret must not escape") }
			} else {
				hooks.AfterCommit = func(context.Context, ControlledReceipt) error { return errors.New("secret must not escape") }
			}
			e, err := NewControlledExecutor(context.Background(), conn, cfg, hooks)
			if err != nil {
				t.Fatal(err)
			}
			result, err := e.Run(context.Background())
			if err == nil || strings.Contains(err.Error(), "secret") {
				t.Fatalf("unsafe result=%+v err=%v", result, err)
			}
			want := 0
			if phase == "receipt" {
				want = 1
			}
			if len(s.statements) != want || s.commits != want || result.CommittedRows != int64(want*3) {
				t.Fatalf("continued after failure: %+v commits=%d", result, s.commits)
			}
		})
	}
}
func TestControlledUnknownNeverRetries(t *testing.T) {
	for _, phase := range []string{"exec", "commit", "rollback", "rows"} {
		t.Run(phase, func(t *testing.T) {
			conn, s, cfg, hooks := controlledFixture(t)
			switch phase {
			case "exec":
				s.execErr = io.EOF
			case "commit":
				s.commitErr = io.EOF
			case "rollback":
				s.execErr = io.EOF
				s.rollbackErr = io.EOF
			case "rows":
				s.rowsErr = io.EOF
			}
			e, err := NewControlledExecutor(context.Background(), conn, cfg, hooks)
			if err != nil {
				t.Fatal(err)
			}
			result, err := e.Run(context.Background())
			if err == nil || len(s.statements) != 1 || s.commits > 1 || result.CommittedRows != 0 {
				t.Fatalf("replayed/incorrect result: %+v err=%v statements=%d", result, err, len(s.statements))
			}
			if (phase == "commit" || phase == "rollback") && !result.OutcomeUnknown {
				t.Fatalf("unknown lost: %+v", result)
			}
		})
	}
}
func TestControlledValidationAndCancellation(t *testing.T) {
	conn, s, cfg, hooks := controlledFixture(t)
	bad := cfg
	bad.Table = "events;DELETE"
	if _, err := NewControlledExecutor(context.Background(), conn, bad, hooks); err == nil {
		t.Fatal("invalid identifier")
	}
	if _, err := NewControlledExecutor(context.Background(), conn, cfg, ControlledHooks{}); err == nil {
		t.Fatal("missing hooks")
	}
	s.zone = "SYSTEM"
	if _, err := NewControlledExecutor(context.Background(), conn, cfg, hooks); err == nil {
		t.Fatal("non UTC session")
	}
	s.zone = "+00:00"
	cfg.RowsPerSec = 1
	ctx, cancel := context.WithCancel(context.Background())
	hooks.AfterCommit = func(context.Context, ControlledReceipt) error { cancel(); return nil }
	e, err := NewControlledExecutor(context.Background(), conn, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	result, err := e.Run(ctx)
	if err == nil || result.Reason != ControlledCanceled || result.CommittedRows != 3 || len(s.statements) != 1 {
		t.Fatalf("cancel=%+v err=%v", result, err)
	}
}

func TestControlledStopWaitDoesNotClaimBlockedHookIsStopped(t *testing.T) {
	conn, s, cfg, hooks := controlledFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	hooks.BeforeStatement = func(context.Context, ControlledBatch) error { close(entered); <-release; return nil }
	e, err := NewControlledExecutor(context.Background(), conn, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := e.Run(context.Background()); done <- err }()
	<-entered
	e.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if e.Wait(ctx) == nil {
		t.Fatal("blocked execution reported stopped")
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("canceled execution succeeded")
	}
	if err := e.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(s.statements) != 0 {
		t.Fatal("DELETE sent after cancellation")
	}
}

func TestControlledHookPanicIsContainedAndSanitized(t *testing.T) {
	conn, s, cfg, hooks := controlledFixture(t)
	hooks.BeforeStatement = func(context.Context, ControlledBatch) error { panic("private fixture") }
	e, err := NewControlledExecutor(context.Background(), conn, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	result, err := e.Run(context.Background())
	if err == nil || strings.Contains(err.Error(), "private") || len(s.statements) != 0 || !result.OutcomeUnknown {
		t.Fatalf("panic not contained: %+v %v", result, err)
	}
	if err = e.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestControlledGuardDeadlineIsNotResetBeforeDelete(t *testing.T) {
	conn, s, cfg, hooks := controlledFixture(t)
	cfg.StatementTimeout = 10 * time.Millisecond
	hooks.BeforeStatement = func(ctx context.Context, _ ControlledBatch) error { <-ctx.Done(); return nil }
	e, err := NewControlledExecutor(context.Background(), conn, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	result, err := e.Run(context.Background())
	if err == nil || len(s.statements) != 0 || result.Reason != ControlledTimeout {
		t.Fatalf("stale guard admitted write: %+v err=%v statements=%d", result, err, len(s.statements))
	}
}

func TestControlledIntegerIDsStayNumericAtDriverBoundary(t *testing.T) {
	conn, s, cfg, hooks := controlledFixture(t)
	s.ids = []string{"-9223372036854775808", "18446744073709551615"}
	cfg.BatchSize = 2
	cfg.MaxRows = 2
	e, err := NewControlledExecutor(context.Background(), conn, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(s.boundIDs) != 2 {
		t.Fatal(s.boundIDs)
	}
	if v, ok := s.boundIDs[0].(int64); !ok || v != -9223372036854775808 {
		t.Fatalf("signed id lost: %#v", s.boundIDs[0])
	}
	if v, ok := s.boundIDs[1].(uint64); !ok || v != 18446744073709551615 {
		t.Fatalf("unsigned id lost: %#v", s.boundIDs[1])
	}
}

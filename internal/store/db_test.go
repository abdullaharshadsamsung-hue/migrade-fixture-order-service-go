package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
)

// ---- fake driver recording executed statements ----

type recorder struct {
	mu       sync.Mutex
	execs    []string
	failOn   string // substring; exec containing it fails
	failAll  bool
	pingErr  error
	existing map[string]bool
	creates  int
}

func (r *recorder) record(q string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failAll || (r.failOn != "" && strings.Contains(q, r.failOn)) {
		return errors.New("connection refused")
	}
	r.execs = append(r.execs, q)
	up := strings.ToUpper(q)
	if strings.Contains(up, "ALTER") || strings.Contains(up, "DROP") {
		return errors.New("unexpected destructive statement")
	}
	if strings.Contains(up, "CREATE TABLE IF NOT EXISTS") {
		name := strings.Fields(strings.SplitN(up, "EXISTS", 2)[1])[0]
		if !r.existing[name] {
			r.existing[name] = true
			r.creates++
		}
	}
	return nil
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.execs...)
}

type fakeConnector struct{ r *recorder }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) { return &fakeConn{r: c.r}, nil }
func (c fakeConnector) Driver() driver.Driver                       { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return nil, errors.New("unused") }

type fakeConn struct{ r *recorder }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (c *fakeConn) Close() error                        { return nil }
func (c *fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }
func (c *fakeConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	if err := c.r.record(q); err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}
func (c *fakeConn) Ping(context.Context) error { return c.r.pingErr }

func newRec() *recorder { return &recorder{existing: map[string]bool{}} }

func newFakeDB(t *testing.T, r *recorder) *sql.DB {
	t.Helper()
	db := sql.OpenDB(fakeConnector{r: r})
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// ---- tests ----

func TestEnsureSchema(t *testing.T) {
	tests := []struct {
		name          string
		ordersFailAll bool
		legacyFailAll bool
		preExisting   []string
		wantErr       string
		wantOrders    int
		wantLegacy    int
	}{
		{name: "fresh databases", wantOrders: len(ordersSchema), wantLegacy: len(legacySchema)},
		{name: "some tables exist", preExisting: []string{"ORDERS"}, wantOrders: len(ordersSchema), wantLegacy: len(legacySchema)},
		{name: "orders unreachable", ordersFailAll: true, wantErr: "init orders schema", wantOrders: 0, wantLegacy: 0},
		{name: "legacy unreachable", legacyFailAll: true, wantErr: "init legacy schema", wantOrders: len(ordersSchema), wantLegacy: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			or, lr := newRec(), newRec()
			or.failAll, lr.failAll = tc.ordersFailAll, tc.legacyFailAll
			for _, n := range tc.preExisting {
				or.existing[n] = true
			}
			db := &DB{Orders: newFakeDB(t, or), Legacy: newFakeDB(t, lr)}
			err := db.EnsureSchema(context.Background())
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
			}
			if got := len(or.snapshot()); got != tc.wantOrders {
				t.Errorf("orders execs = %d, want %d", got, tc.wantOrders)
			}
			if got := len(lr.snapshot()); got != tc.wantLegacy {
				t.Errorf("legacy execs = %d, want %d", got, tc.wantLegacy)
			}
			if tc.name == "some tables exist" && or.creates != 0 {
				t.Errorf("existing orders table recreated")
			}
		})
	}
}

func TestEnsureSchemaIdempotent(t *testing.T) {
	or, lr := newRec(), newRec()
	db := &DB{Orders: newFakeDB(t, or), Legacy: newFakeDB(t, lr)}
	for i := 0; i < 3; i++ {
		if err := db.EnsureSchema(context.Background()); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}
	if or.creates != 1 || lr.creates != 1 {
		t.Errorf("creates orders=%d legacy=%d, want 1 each", or.creates, lr.creates)
	}
}

func TestEnsureSchemaOrdering(t *testing.T) {
	var mu sync.Mutex
	var order []string
	or, lr := newRec(), newRec()
	db := &DB{Orders: newFakeDB(t, or), Legacy: newFakeDB(t, lr)}
	if err := db.EnsureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	order = append(order, or.snapshot()...)
	order = append(order, lr.snapshot()...)
	mu.Unlock()
	want := append(append([]string{}, ordersSchema...), legacySchema...)
	if len(order) != len(want) {
		t.Fatalf("got %d stmts, want %d", len(order), len(want))
	}
	for i := range want {
		if order[i] != want[i] {
			t.Errorf("stmt %d mismatch", i)
		}
	}
}

func TestSchemaSeparationAndSafety(t *testing.T) {
	tests := []struct {
		name      string
		stmts     []string
		mustHave  string
		mustNotIn string
	}{
		{"orders only has orders", ordersSchema, "orders", "customer_credit"},
		{"legacy only has customer_credit", legacySchema, "customer_credit", "orders"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			joined := strings.Join(tc.stmts, "\n")
			if !strings.Contains(joined, tc.mustHave) {
				t.Errorf("missing %q", tc.mustHave)
			}
			if strings.Contains(joined, tc.mustNotIn) {
				t.Errorf("unexpected %q leaked", tc.mustNotIn)
			}
			for _, s := range tc.stmts {
				up := strings.ToUpper(s)
				if !strings.Contains(up, "IF NOT EXISTS") {
					t.Errorf("statement lacks IF NOT EXISTS: %s", s)
				}
				for _, bad := range []string{"DROP ", "ALTER ", "TRUNCATE", "DELETE "} {
					if strings.Contains(up, bad) {
						t.Errorf("destructive %q in %s", bad, s)
					}
				}
			}
		})
	}
}

func TestExecAllStopsAtFirstFailure(t *testing.T) {
	r := newRec()
	r.failOn = "second"
	db := newFakeDB(t, r)
	err := execAll(context.Background(), db, []string{"first", "second", "third"})
	if err == nil {
		t.Fatal("expected error")
	}
	if got := r.snapshot(); len(got) != 1 || got[0] != "first" {
		t.Errorf("execs = %v", got)
	}
}

func TestClose(t *testing.T) {
	tests := []struct {
		name string
		db   func(t *testing.T) *DB
	}{
		{"both nil", func(*testing.T) *DB { return &DB{} }},
		{"both set", func(t *testing.T) *DB {
			return &DB{Orders: sql.OpenDB(fakeConnector{newRec()}), Legacy: sql.OpenDB(fakeConnector{newRec()})}
		}},
		{"only orders", func(t *testing.T) *DB { return &DB{Orders: sql.OpenDB(fakeConnector{newRec()})} }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := tc.db(t)
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if db.Orders != nil {
				if err := db.Orders.Ping(); err == nil {
					t.Error("orders pool still open")
				}
			}
		})
	}
}

func TestConstants(t *testing.T) {
	if pingTimeout <= 0 || connMaxIdleTime <= 0 {
		t.Error("timeouts must be positive")
	}
}
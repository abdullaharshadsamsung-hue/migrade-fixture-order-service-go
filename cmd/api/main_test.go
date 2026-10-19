package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"migrated-app/internal/config"
)

// ---------------------------------------------------------------------------
// Fake database/sql driver (mocks Postgres / MySQL through driver interfaces)
// ---------------------------------------------------------------------------

type queryResult struct {
	cols []string
	rows [][]driver.Value
	err  error
}

type fakeDB struct {
	mu        sync.Mutex
	execs     []string
	execArgs  [][]driver.NamedValue
	queries   []string
	queryArgs [][]driver.NamedValue
	execFn    func(query string) error
	queryFn   func(query string, args []driver.NamedValue) queryResult
}

func (f *fakeDB) Connect(context.Context) (driver.Conn, error) { return &fakeConn{db: f}, nil }
func (f *fakeDB) Driver() driver.Driver                        { return fakeDriver{f} }

func (f *fakeDB) open(t *testing.T) *sql.DB {
	t.Helper()
	db := sql.OpenDB(f)
	t.Cleanup(func() { db.Close() })
	return db
}

func (f *fakeDB) execList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.execs...)
}

type fakeDriver struct{ db *fakeDB }

func (d fakeDriver) Open(string) (driver.Conn, error) { return &fakeConn{db: d.db}, nil }

type fakeConn struct{ db *fakeDB }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not supported")
}
func (c *fakeConn) Close() error              { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) { return nil, errors.New("tx not supported") }

func (c *fakeConn) ExecContext(_ context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	c.db.mu.Lock()
	c.db.execs = append(c.db.execs, q)
	c.db.execArgs = append(c.db.execArgs, args)
	fn := c.db.execFn
	c.db.mu.Unlock()
	if fn != nil {
		if err := fn(q); err != nil {
			return nil, err
		}
	}
	return driver.RowsAffected(1), nil
}

func (c *fakeConn) QueryContext(_ context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	c.db.mu.Lock()
	c.db.queries = append(c.db.queries, q)
	c.db.queryArgs = append(c.db.queryArgs, args)
	fn := c.db.queryFn
	c.db.mu.Unlock()
	if fn == nil {
		return &fakeRows{}, nil
	}
	res := fn(q, args)
	if res.err != nil {
		return nil, res.err
	}
	return &fakeRows{cols: res.cols, rows: res.rows}, nil
}

type fakeRows struct {
	cols []string
	rows [][]driver.Value
	i    int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.i])
	r.i++
	return nil
}

var orderCols = []string{"id", "customer_id", "product", "quantity", "total", "status"}

func orderRow(id, cust int64, product string, qty int64, total float64, status string) []driver.Value {
	return []driver.Value{id, cust, product, qty, total, status}
}

// ---------------------------------------------------------------------------
// initDB (create_app -> init_db)
// ---------------------------------------------------------------------------

func TestInitDB(t *testing.T) {
	boom := errors.New("database unreachable")
	tests := []struct {
		name         string
		ordersErr    error
		legacyErr    error
		wantErr      string
		wantOrders   int
		wantLegacy   int
		wantNoLegacy bool
	}{
		{name: "success creates all tables", wantOrders: len(ordersSchema), wantLegacy: len(legacySchema)},
		{name: "orders db failure propagates", ordersErr: boom, wantErr: "init orders schema", wantOrders: 1, wantNoLegacy: true},
		{name: "legacy db failure propagates", legacyErr: boom, wantErr: "init legacy schema", wantOrders: len(ordersSchema), wantLegacy: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := &fakeDB{execFn: func(string) error { return tc.ordersErr }}
			l := &fakeDB{execFn: func(string) error { return tc.legacyErr }}
			err := initDB(context.Background(), o.open(t), l.open(t))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
				}
				if !errors.Is(err, boom) {
					t.Fatalf("error does not wrap cause: %v", err)
				}
			}
			if got := len(o.execList()); got != tc.wantOrders {
				t.Errorf("orders execs = %d, want %d", got, tc.wantOrders)
			}
			if tc.wantNoLegacy {
				if got := len(l.execList()); got != 0 {
					t.Errorf("legacy execs = %d, want 0", got)
				}
			} else if got := len(l.execList()); got != tc.wantLegacy {
				t.Errorf("legacy execs = %d, want %d", got, tc.wantLegacy)
			}
		})
	}
}

func TestInitDBIdempotentRepeatedCalls(t *testing.T) {
	o, l := &fakeDB{}, &fakeDB{}
	odb, ldb := o.open(t), l.open(t)
	for i := 0; i < 3; i++ {
		if err := initDB(context.Background(), odb, ldb); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	for _, s := range append(o.execList(), l.execList()...) {
		if !strings.Contains(s, "IF NOT EXISTS") {
			t.Errorf("statement not idempotent: %s", s)
		}
	}
	if got, want := len(o.execList()), 3*len(ordersSchema); got != want {
		t.Errorf("orders execs = %d, want %d", got, want)
	}
}

func TestSchemaStatements(t *testing.T) {
	tests := []struct {
		name   string
		stmts  []string
		substr []string
	}{
		{"orders", ordersSchema, []string{"CREATE TABLE IF NOT EXISTS orders", "customer_id", "ix_orders_customer_id"}},
		{"legacy", legacySchema, []string{"CREATE TABLE IF NOT EXISTS customer_credit", "credit_limit", "used"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			all := strings.Join(tc.stmts, "\n")
			for _, s := range tc.substr {
				if !strings.Contains(all, s) {
					t.Errorf("schema missing %q", s)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// openDatabases error propagation (database unreachable)
// ---------------------------------------------------------------------------

func TestOpenDatabasesUnreachable(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.Config
	}{
		{"unreachable orders db", config.Config{
			DatabaseURL: "postgres://u:p@127.0.0.1:1/orders?sslmode=disable&connect_timeout=2",
			LegacyDBURL: "mysql://u:p@127.0.0.1:1/legacy",
		}},
		{"invalid legacy url", config.Config{
			DatabaseURL: "postgres://u:p@127.0.0.1:1/orders?sslmode=disable",
			LegacyDBURL: "::not a url::",
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o, l, err := openDatabases(context.Background(), tc.cfg)
			if err == nil {
				o.Close()
				l.Close()
				t.Fatal("expected error, got nil")
			}
			if o != nil || l != nil {
				t.Error("expected nil databases on error")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// newAPI / routes (create_app returns an app with blueprint registered)
// ---------------------------------------------------------------------------

func TestNewAPIReturnsIndependentInstances(t *testing.T) {
	o1, l1 := (&fakeDB{}).open(t), (&fakeDB{}).open(t)
	o2, l2 := (&fakeDB{}).open(t), (&fakeDB{}).open(t)
	a1, a2 := newAPI(o1, l1), newAPI(o2, l2)
	if a1 == a2 {
		t.Fatal("expected distinct api instances")
	}
	if a1.orders != o1 || a1.legacy != l1 || a2.orders != o2 || a2.legacy != l2 {
		t.Fatal("dependencies not wired correctly")
	}
	for i, h := range []http.Handler{a1.routes(), a2.routes()} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
		if rec.Code != http.StatusOK {
			t.Errorf("app %d /health = %d", i, rec.Code)
		}
	}
}

func TestRoutesRegistered(t *testing.T) {
	o := &fakeDB{queryFn: func(string, []driver.NamedValue) queryResult { return queryResult{cols: orderCols} }}
	l := &fakeDB{queryFn: func(string, []driver.NamedValue) queryResult { return queryResult{cols: []string{"a", "b"}} }}
	h := newAPI(o.open(t), l.open(t)).routes()
	tests := []struct {
		method, path string
	}{
		{http.MethodGet, "/health"},
		{http.MethodPost, "/customers/1/credit"},
		{http.MethodGet, "/customers/1/credit"},
		{http.MethodPost, "/orders"},
		{http.MethodGet, "/orders/1"},
		{http.MethodGet, "/orders"},
		{http.MethodGet, "/orders/1/audit"},
	}
	for _, tc := range tests {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, strings.NewReader("{}")))
			if rec.Code == http.StatusMethodNotAllowed {
				t.Fatalf("route not registered: %d", rec.Code)
			}
			if rec.Code == http.StatusNotFound && rec.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("route not registered (plain 404)")
			}
		})
	}
}

func TestHealthNoExternalDependencies(t *testing.T) {
	// Fresh app has no rate-limit state; /health works with no DB access.
	h := newAPI(nil, nil).routes()
	for i := 0; i < 5; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
		var body map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["status"] != "ok" {
			t.Fatalf("body = %s", rec.Body.String())
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("content-type = %q", ct)
		}
	}
}

// ---------------------------------------------------------------------------
// Credit endpoints
// ---------------------------------------------------------------------------

func TestSetCredit(t *testing.T) {
	dbErr := errors.New("mysql down")
	tests := []struct {
		name       string
		path       string
		body       string
		execErr    error
		query      queryResult
		wantStatus int
		wantErr    string
		wantResp   *creditResponse
		wantExec   bool
	}{
		{name: "valid", path: "/customers/7/credit", body: `{"credit_limit": 100.5}`,
			query:      queryResult{cols: []string{"credit_limit", "used"}, rows: [][]driver.Value{{100.5, 0.0}}},
			wantStatus: 200, wantResp: &creditResponse{CustomerID: 7, CreditLimit: 100.5}, wantExec: true},
		{name: "zero allowed", path: "/customers/1/credit", body: `{"credit_limit": 0}`,
			query:      queryResult{cols: []string{"credit_limit", "used"}, rows: [][]driver.Value{{"0.00", "0.00"}}},
			wantStatus: 200, wantResp: &creditResponse{CustomerID: 1}, wantExec: true},
		{name: "negative", path: "/customers/1/credit", body: `{"credit_limit": -1}`, wantStatus: 400, wantErr: "credit_limit must be a non-negative number"},
		{name: "string value", path: "/customers/1/credit", body: `{"credit_limit": "10"}`, wantStatus: 400, wantErr: "credit_limit must be a non-negative number"},
		{name: "missing field", path: "/customers/1/credit", body: `{}`, wantStatus: 400, wantErr: "credit_limit must be a non-negative number"},
		{name: "invalid json", path: "/customers/1/credit", body: `not json`, wantStatus: 400, wantErr: "credit_limit must be a non-negative number"},
		{name: "empty body", path: "/customers/1/credit", body: ``, wantStatus: 400, wantErr: "credit_limit must be a non-negative number"},
		{name: "non-int id", path: "/customers/abc/credit", body: `{"credit_limit": 1}`, wantStatus: 404},
		{name: "negative id", path: "/customers/-1/credit", body: `{"credit_limit": 1}`, wantStatus: 404},
		{name: "overflow id", path: "/customers/99999999999/credit", body: `{"credit_limit": 1}`, wantStatus: 404},
		{name: "exec error", path: "/customers/1/credit", body: `{"credit_limit": 1}`, execErr: dbErr, wantStatus: 500, wantErr: "internal server error", wantExec: true},
		{name: "readback missing", path: "/customers/1/credit", body: `{"credit_limit": 1}`,
			query: queryResult{cols: []string{"credit_limit", "used"}}, wantStatus: 404, wantErr: "customer not found", wantExec: true},
		{name: "readback error", path: "/customers/1/credit", body: `{"credit_limit": 1}`,
			query: queryResult{err: dbErr}, wantStatus: 500, wantErr: "internal server error", wantExec: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := &fakeDB{
				execFn:  func(string) error { return tc.execErr },
				queryFn: func(string, []driver.NamedValue) queryResult { return tc.query },
			}
			h := newAPI((&fakeDB{}).open(t), l.open(t)).routes()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body)))
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if got := len(l.execList()) > 0; got != tc.wantExec {
				t.Errorf("exec called = %v, want %v", got, tc.wantExec)
			}
			checkBody(t, rec, tc.wantErr, tc.wantResp)
		})
	}
}

func TestGetCredit(t *testing.T) {
	dbErr := errors.New("boom")
	tests := []struct {
		name       string
		path       string
		query      queryResult
		wantStatus int
		wantErr    string
		wantResp   *creditResponse
	}{
		{name: "found", path: "/customers/3/credit",
			query:      queryResult{cols: []string{"credit_limit", "used"}, rows: [][]driver.Value{{"250.00", "12.50"}}},
			wantStatus: 200, wantResp: &creditResponse{CustomerID: 3, CreditLimit: 250, Used: 12.5}},
		{name: "not found", path: "/customers/3/credit", query: queryResult{cols: []string{"credit_limit", "used"}}, wantStatus: 404, wantErr: "customer not found"},
		{name: "db error", path: "/customers/3/credit", query: queryResult{err: dbErr}, wantStatus: 500, wantErr: "internal server error"},
		{name: "bad id", path: "/customers/x1/credit", wantStatus: 404},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := &fakeDB{queryFn: func(string, []driver.NamedValue) queryResult { return tc.query }}
			h := newAPI((&fakeDB{}).open(t), l.open(t)).routes()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			checkBody(t, rec, tc.wantErr, tc.wantResp)
		})
	}
}

func checkBody(t *testing.T, rec *httptest.ResponseRecorder, wantErr string, wantResp *creditResponse) {
	t.Helper()
	if wantErr != "" {
		var e map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
			t.Fatalf("decode: %v (%s)", err, rec.Body.String())
		}
		if e["error"] != wantErr {
			t.Errorf("error = %q, want %q", e["error"], wantErr)
		}
	}
	if wantResp != nil {
		var got creditResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got != *wantResp {
			t.Errorf("resp = %+v, want %+v", got, *wantResp)
		}
	}
}

// ---------------------------------------------------------------------------
// Order endpoints
// ---------------------------------------------------------------------------

func TestGetOrder(t *testing.T) {
	dbErr := errors.New("pg down")
	tests := []struct {
		name       string
		path       string
		query      queryResult
		wantStatus int
		wantErr    string
		want       *orderResponse
	}{
		{name: "found", path: "/orders/5",
			query:      queryResult{cols: orderCols, rows: [][]driver.Value{orderRow(5, 2, "widget", 3, 29.97, "confirmed")}},
			wantStatus: 200, want: &orderResponse{ID: 5, CustomerID: 2, Product: "widget", Quantity: 3, Total: 29.97, Status: "confirmed"}},
		{name: "not found", path: "/orders/5", query: queryResult{cols: orderCols}, wantStatus: 404, wantErr: "order not found"},
		{name: "db error", path: "/orders/5", query: queryResult{err: dbErr}, wantStatus: 500, wantErr: "internal server error"},
		{name: "bad id", path: "/orders/abc", wantStatus: 404},
		{name: "overflow id", path: "/orders/3000000000", wantStatus: 404},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := &fakeDB{queryFn: func(string, []driver.NamedValue) queryResult { return tc.query }}
			h := newAPI(o.open(t), (&fakeDB{}).open(t)).routes()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			checkBody(t, rec, tc.wantErr, nil)
			if tc.want != nil {
				var got orderResponse
				if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
				if got != *tc.want {
					t.Errorf("got %+v, want %+v", got, *tc.want)
				}
			}
		})
	}
}

func TestListOrders(t *testing.T) {
	dbErr := errors.New("pg down")
	tests := []struct {
		name       string
		path       string
		query      queryResult
		wantStatus int
		wantWhere  bool
		wantArg    int64
		wantCount  int
	}{
		{name: "all", path: "/orders",
			query: queryResult{cols: orderCols, rows: [][]driver.Value{
				orderRow(1, 1, "a", 1, 1, "confirmed"), orderRow(2, 2, "b", 2, 4, "confirmed")}},
			wantStatus: 200, wantCount: 2},
		{name: "empty list is array", path: "/orders", query: queryResult{cols: orderCols}, wantStatus: 200},
		{name: "filter by customer", path: "/orders?customer_id=2",
			query:      queryResult{cols: orderCols, rows: [][]driver.Value{orderRow(2, 2, "b", 2, 4, "confirmed")}},
			wantStatus: 200, wantWhere: true, wantArg: 2, wantCount: 1},
		{name: "invalid filter ignored", path: "/orders?customer_id=abc", query: queryResult{cols: orderCols}, wantStatus: 200},
		{name: "db error", path: "/orders", query: queryResult{err: dbErr}, wantStatus: 500},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := &fakeDB{queryFn: func(string, []driver.NamedValue) queryResult { return tc.query }}
			h := newAPI(o.open(t), (&fakeDB{}).open(t)).routes()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			o.mu.Lock()
			q, args := o.queries[0], o.queryArgs[0]
			o.mu.Unlock()
			if got := strings.Contains(q, "WHERE customer_id"); got != tc.wantWhere {
				t.Errorf("WHERE present = %v, want %v (%s)", got, tc.wantWhere, q)
			}
			if !strings.HasSuffix(q, "ORDER BY id") {
				t.Errorf("query not ordered: %s", q)
			}
			if tc.wantWhere {
				if len(args) != 1 || args[0].Value != tc.wantArg {
					t.Errorf("args = %+v, want %d", args, tc.wantArg)
				}
			} else if len(args) != 0 {
				t.Errorf("unexpected args %+v", args)
			}
			if tc.wantStatus != 200 {
				return
			}
			var got []orderResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if got == nil || len(got) != tc.wantCount {
				t.Errorf("got %v, want %d orders (non-null)", got, tc.wantCount)
			}
		})
	}
}

func TestNotImplementedEndpoints(t *testing.T) {
	tests := []struct {
		name, method, path string
		wantStatus         int
	}{
		{"create order", http.MethodPost, "/orders", 501},
		{"audit", http.MethodGet, "/orders/1/audit", 501},
		{"audit bad id", http.MethodGet, "/orders/x/audit", 404},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newAPI((&fakeDB{}).open(t), (&fakeDB{}).open(t)).routes()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{}`)))
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if tc.wantStatus == 501 {
				checkBody(t, rec, "not implemented", nil)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func TestPathID(t *testing.T) {
	tests := []struct {
		raw    string
		want   int
		wantOK bool
	}{
		{"0", 0, true},
		{"42", 42, true},
		{"2147483647", 2147483647, true},
		{"2147483648", 0, false},
		{"", 0, false},
		{"-1", 0, false},
		{"+1", 0, false},
		{"1a", 0, false},
		{"1.5", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.raw, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.SetPathValue("id", tc.raw)
			got, ok := pathID(r, "id")
			if got != tc.want || ok != tc.wantOK {
				t.Errorf("pathID(%q) = %d,%v want %d,%v", tc.raw, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestDecodeBody(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantLen int
	}{
		{"object", `{"a":1,"b":"x"}`, 2},
		{"null", `null`, 0},
		{"array", `[1,2]`, 0},
		{"invalid", `{`, 0},
		{"empty", ``, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := decodeBody(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body)))
			if got == nil || len(got) != tc.wantLen {
				t.Errorf("decodeBody = %v, want len %d", got, tc.wantLen)
			}
		})
	}
	got := decodeBody(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"n": 1.25}`)))
	if _, ok := got["n"].(json.Number); !ok {
		t.Errorf("numbers should decode as json.Number, got %T", got["n"])
	}
}
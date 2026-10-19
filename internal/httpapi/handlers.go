// Package httpapi implements the order service HTTP API. It replaces the
// Flask blueprint in app/routes.py.
package httpapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/go-chi/chi/v5"

	"migrated-app/internal/audit"
	"migrated-app/internal/events"
	"migrated-app/internal/store"
)

// Error messages that are part of the public wire contract.
const (
	msgRateLimited      = "rate limit exceeded"
	msgBadCreditLimit   = "credit_limit must be a non-negative number"
	msgCustomerNotFound = "customer not found"
	msgOrderRequired    = "customer_id and product are required"
	msgBadQuantity      = "quantity must be a positive integer"
	msgCreditExceeded   = "credit limit exceeded"
	msgOrderNotFound    = "order not found"
	msgInternal         = "internal server error"
)

// maxBodyBytes bounds request bodies.
const maxBodyBytes = 1 << 20

// ErrCreditLimitExceeded is returned by CreditStore.ChargeCredit when the
// charge would push used above credit_limit.
var ErrCreditLimitExceeded = errors.New("httpapi: credit limit exceeded")

// OrderStore is the orders-database dependency of the API.
type OrderStore interface {
	InsertOrder(ctx context.Context, o store.Order) (store.Order, error)
	GetOrder(ctx context.Context, id int64) (store.Order, error)
	ListOrders(ctx context.Context, customerID *int64) ([]store.Order, error)
}

// CreditStore is the legacy credit-database dependency of the API.
type CreditStore interface {
	GetCustomerCredit(ctx context.Context, customerID int64) (store.CustomerCredit, error)
	UpsertCustomerCredit(ctx context.Context, customerID int64, creditLimit float64) (store.CustomerCredit, error)
	// ChargeCredit atomically adds amount to the customer's used credit.
	// It returns store.ErrNotFound for unknown customers and
	// ErrCreditLimitExceeded when used+amount > credit_limit.
	ChargeCredit(ctx context.Context, customerID int64, amount float64) error
}

// Catalog provides product prices and rate-limit decisions.
type Catalog interface {
	PriceOf(ctx context.Context, product string) (float64, bool, error)
	Allow(ctx context.Context, client string) (bool, error)
}

// AuditLog records and reads order audit events.
type AuditLog interface {
	Record(ctx context.Context, orderID int64, event string, detail map[string]any) error
	History(ctx context.Context, orderID int64) ([]audit.Entry, error)
}

// Deps bundles every dependency the handlers need.
type Deps struct {
	Orders  OrderStore
	Credits CreditStore
	Catalog Catalog
	Audit   AuditLog
	Events  events.Publisher
}

var deps atomic.Pointer[Deps]

// Configure installs the dependencies used by the handlers registered with
// RegisterRoutes. It must be called at startup before serving traffic.
//
// MIGRATION_NOTE: the Flask source used module-level globals (Session,
// LegacySession, catalog, audit, events). RegisterRoutes has a fixed
// signature, so dependencies are injected through Configure instead.
func Configure(d Deps) {
	deps.Store(&d)
}

func currentDeps() (*Deps, error) {
	d := deps.Load()
	if d == nil || d.Orders == nil || d.Credits == nil || d.Catalog == nil || d.Audit == nil || d.Events == nil {
		return nil, errors.New("httpapi: dependencies not configured")
	}
	return d, nil
}

// RegisterRoutes registers every API route on r.
func RegisterRoutes(r chi.Router) {
	r.Get("/health", health)
	r.Post("/customers/{customer_id}/credit", limited("customer_id", setCredit))
	r.Get("/customers/{customer_id}/credit", limited("customer_id", getCredit))
	r.Post("/orders", limited("", createOrder))
	r.Get("/orders", limited("", listOrders))
	r.Get("/orders/{order_id}", limited("order_id", getOrder))
	r.Get("/orders/{order_id}/audit", limited("order_id", orderAudit))
}

// idHandler is a handler that receives its validated integer path ID.
type idHandler func(w http.ResponseWriter, r *http.Request, d *Deps, id int64)

// limited validates the optional integer path parameter (emulating Flask's
// <int:...> converter: 404 before anything else runs) and then applies the
// per-client rate limit (the blueprint's before_request hook).
func limited(param string, h idHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var id int64
		if param != "" {
			v, ok := parsePathID(chi.URLParam(r, param))
			if !ok {
				http.NotFound(w, r)
				return
			}
			id = v
		}
		d, err := currentDeps()
		if err != nil {
			internalError(w, r, err)
			return
		}
		allowed, err := d.Catalog.Allow(r.Context(), clientKey(r))
		if err != nil {
			internalError(w, r, err)
			return
		}
		if !allowed {
			writeError(w, http.StatusTooManyRequests, msgRateLimited)
			return
		}
		h(w, r, d, id)
	}
}

// parsePathID accepts ASCII digits only. Values beyond int32 are rejected
// (404), since they cannot exist as INTEGER primary keys.
func parsePathID(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, false
	}
	return v, true
}

// clientKey mirrors request.remote_addr or "?".
func clientKey(r *http.Request) string {
	addr := r.RemoteAddr
	if host, _, err := net.SplitHostPort(addr); err == nil {
		addr = host
	}
	if addr == "" {
		return "?"
	}
	return addr
}

func health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// creditResponse fields are ordered alphabetically to match Flask's
// sorted-key jsonify output.
type creditResponse struct {
	CreditLimit float64 `json:"credit_limit"`
	CustomerID  int64   `json:"customer_id"`
	Used        float64 `json:"used"`
}

func setCredit(w http.ResponseWriter, r *http.Request, d *Deps, customerID int64) {
	body := readJSONObject(r)
	limit, ok := asNumber(body["credit_limit"])
	if !ok || limit < 0 {
		writeError(w, http.StatusBadRequest, msgBadCreditLimit)
		return
	}
	credit, err := d.Credits.UpsertCustomerCredit(r.Context(), customerID, limit)
	if err != nil {
		internalError(w, r, err)
		return
	}
	// The source echoes the submitted limit (float(credit.credit_limit)
	// before any DB rounding) and the stored used value.
	writeJSON(w, http.StatusOK, creditResponse{CreditLimit: limit, CustomerID: customerID, Used: credit.Used})
}

func getCredit(w http.ResponseWriter, r *http.Request, d *Deps, customerID int64) {
	credit, err := d.Credits.GetCustomerCredit(r.Context(), customerID)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, msgCustomerNotFound)
		return
	}
	if err != nil {
		internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, creditResponse{CreditLimit: credit.CreditLimit, CustomerID: customerID, Used: credit.Used})
}

func createOrder(w http.ResponseWriter, r *http.Request, d *Deps, _ int64) {
	ctx := r.Context()
	body := readJSONObject(r)

	customerID, okCustomer := asInt(body["customer_id"])
	product, okProduct := body["product"].(string)
	if !okCustomer || !okProduct {
		writeError(w, http.StatusBadRequest, msgOrderRequired)
		return
	}
	quantity := int64(1)
	if raw, present := body["quantity"]; present {
		q, ok := asInt(raw)
		if !ok {
			writeError(w, http.StatusBadRequest, msgBadQuantity)
			return
		}
		quantity = q
	}
	if quantity < 1 || quantity > math.MaxInt32 {
		writeError(w, http.StatusBadRequest, msgBadQuantity)
		return
	}

	price, known, err := d.Catalog.PriceOf(ctx, product)
	if err != nil {
		internalError(w, r, err)
		return
	}
	if !known {
		writeError(w, http.StatusBadRequest, "unknown product: "+product)
		return
	}
	// MIGRATION_NOTE: Python round() is banker's rounding on the binary
	// value; math.Round rounds half away from zero. Results can differ only
	// on exact half-cent float values.
	total := math.Round(price*float64(quantity)*100) / 100

	if err := d.Credits.ChargeCredit(ctx, customerID, total); err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			writeError(w, http.StatusNotFound, msgCustomerNotFound)
		case errors.Is(err, ErrCreditLimitExceeded):
			writeError(w, http.StatusPaymentRequired, msgCreditExceeded)
		default:
			internalError(w, r, err)
		}
		return
	}

	// MIGRATION_NOTE: as in the source, the legacy credit charge and the
	// order insert are separate transactions in separate databases. If any
	// step below fails the customer remains charged (no refund). Review.
	order, err := d.Orders.InsertOrder(ctx, store.NewOrder(customerID, product, int(quantity), total))
	if err != nil {
		internalError(w, r, err)
		return
	}
	if err := d.Audit.Record(ctx, order.ID, "created", map[string]any{"total": total}); err != nil {
		internalError(w, r, err)
		return
	}
	if err := d.Events.Publish(ctx, "order.created", map[string]any{"order_id": order.ID}); err != nil {
		internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, order.ToDict())
}

func getOrder(w http.ResponseWriter, r *http.Request, d *Deps, orderID int64) {
	order, err := d.Orders.GetOrder(r.Context(), orderID)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, msgOrderNotFound)
		return
	}
	if err != nil {
		internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, order.ToDict())
}

func listOrders(w http.ResponseWriter, r *http.Request, d *Deps, _ int64) {
	var filter *int64
	if raw, ok := r.URL.Query()["customer_id"]; ok && len(raw) > 0 {
		// request.args.get(type=int): unparseable values are ignored.
		if v, ok := parsePythonInt(raw[0]); ok {
			filter = &v
		}
	}
	orders, err := d.Orders.ListOrders(r.Context(), filter)
	if err != nil {
		internalError(w, r, err)
		return
	}
	out := make([]map[string]any, 0, len(orders))
	for _, o := range orders {
		out = append(out, o.ToDict())
	}
	writeJSON(w, http.StatusOK, out)
}

// auditEntry orders fields alphabetically (detail, event) like jsonify.
type auditEntry struct {
	Detail map[string]any `json:"detail"`
	Event  string         `json:"event"`
}

func orderAudit(w http.ResponseWriter, r *http.Request, d *Deps, orderID int64) {
	entries, err := d.Audit.History(r.Context(), orderID)
	if err != nil {
		internalError(w, r, err)
		return
	}
	out := make([]auditEntry, 0, len(entries))
	for _, e := range entries {
		detail := e.Detail
		if detail == nil {
			detail = map[string]any{}
		}
		out = append(out, auditEntry{Detail: detail, Event: e.Event})
	}
	writeJSON(w, http.StatusOK, out)
}

// readJSONObject emulates `request.get_json(silent=True) or {}`: a non-JSON
// content type, invalid JSON, or a non-object body yields an empty map.
//
// MIGRATION_NOTE: Python's json accepts NaN/Infinity; Go does not, so such
// bodies are treated as {} (and fail validation with 400).
func readJSONObject(r *http.Request) map[string]any {
	empty := map[string]any{}
	if !isJSONContentType(r.Header.Get("Content-Type")) || r.Body == nil {
		return empty
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return empty
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return empty
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return empty
	}
	return obj
}

func isJSONContentType(ct string) bool {
	if ct == "" {
		return false
	}
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}
	return mt == "application/json" || (strings.HasPrefix(mt, "application/") && strings.HasSuffix(mt, "+json"))
}

// asInt emulates isinstance(v, int): JSON integers and booleans (Python bool
// subclasses int) pass; floats such as 5.0 and nulls do not.
//
// MIGRATION_NOTE: integers beyond int64 are rejected, unlike Python.
func asInt(v any) (int64, bool) {
	switch t := v.(type) {
	case bool:
		if t {
			return 1, true
		}
		return 0, true
	case json.Number:
		s := t.String()
		if strings.ContainsAny(s, ".eE") {
			return 0, false
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
}

// asNumber emulates isinstance(v, (int, float)), including bools.
func asNumber(v any) (float64, bool) {
	switch t := v.(type) {
	case bool:
		if t {
			return 1, true
		}
		return 0, true
	case json.Number:
		f, err := strconv.ParseFloat(t.String(), 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

// parsePythonInt emulates Python's int(str): surrounding whitespace, an
// optional sign and single underscores between digits are accepted.
func parsePythonInt(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	sign := ""
	if strings.HasPrefix(s, "+") || strings.HasPrefix(s, "-") {
		if s[0] == '-' {
			sign = "-"
		}
		s = s[1:]
	}
	if s == "" || s[0] == '_' || s[len(s)-1] == '_' {
		return 0, false
	}
	var b strings.Builder
	prevUnderscore := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '_':
			if prevUnderscore {
				return 0, false
			}
			prevUnderscore = true
		case c >= '0' && c <= '9':
			prevUnderscore = false
			b.WriteByte(c)
		default:
			return 0, false
		}
	}
	n, err := strconv.ParseInt(sign+b.String(), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// SQLCreditStore adapts *store.DB to CreditStore, adding an atomic charge
// operation on the legacy MySQL database.
type SQLCreditStore struct {
	*store.DB
}

// NewSQLCreditStore wraps db as a CreditStore.
func NewSQLCreditStore(db *store.DB) *SQLCreditStore {
	return &SQLCreditStore{DB: db}
}

// ChargeCredit locks the customer's credit row, checks
// used + amount <= credit_limit (float comparison, as in the source) and
// stores the new used value in one transaction.
//
// MIGRATION_NOTE: the source read-modify-wrote without a lock (lost updates
// under concurrency). This is a flagged fix. MySQL placeholders are correct
// here because customer_credit lives in the legacy MySQL database.
func (s *SQLCreditStore) ChargeCredit(ctx context.Context, customerID int64, amount float64) (err error) {
	tx, err := s.Legacy.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("charge credit %d: begin: %w", customerID, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var limitStr, usedStr string
	err = tx.QueryRowContext(ctx,
		`SELECT credit_limit, used FROM customer_credit WHERE customer_id = ? FOR UPDATE`, customerID,
	).Scan(&limitStr, &usedStr)
	if errors.Is(err, sql.ErrNoRows) {
		return store.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("charge credit %d: select: %w", customerID, err)
	}
	limit, err := strconv.ParseFloat(limitStr, 64)
	if err != nil {
		return fmt.Errorf("charge credit %d: parse credit_limit: %w", customerID, err)
	}
	used, err := strconv.ParseFloat(usedStr, 64)
	if err != nil {
		return fmt.Errorf("charge credit %d: parse used: %w", customerID, err)
	}
	if used+amount > limit {
		err = ErrCreditLimitExceeded
		return err
	}
	newUsed := strconv.FormatFloat(used+amount, 'f', 2, 64)
	if _, err = tx.ExecContext(ctx,
		`UPDATE customer_credit SET used = ? WHERE customer_id = ?`, newUsed, customerID,
	); err != nil {
		return fmt.Errorf("charge credit %d: update: %w", customerID, err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("charge credit %d: commit: %w", customerID, err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("httpapi: write response", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func internalError(w http.ResponseWriter, r *http.Request, err error) {
	slog.Error("httpapi: unhandled error", "method", r.Method, "path", r.URL.Path, "error", err)
	writeError(w, http.StatusInternalServerError, msgInternal)
}

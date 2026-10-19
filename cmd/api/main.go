// Command api is the Order Service HTTP API: orders in Postgres, credit
// limits in a legacy MySQL database, an audit log in MongoDB, a Redis cache
// and rate limit, and order events through RabbitMQ to a background worker.
//
// It replaces the Flask application factory (app.create_app): it loads the
// configuration, initialises the database layer (creating the schema exactly
// like SQLAlchemy's metadata.create_all did), builds the HTTP handler with
// every route of the original "api" blueprint, and serves it with graceful
// shutdown.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rs/zerolog/log"

	"migrated-app/internal/config"
)

func main() {
	if err := run(); err != nil {
		log.Fatal().Err(err).Msg("order service api exited")
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ordersDB, legacyDB, err := openDatabases(ctx, cfg)
	if err != nil {
		return err
	}
	defer ordersDB.Close()
	defer legacyDB.Close()

	// Equivalent of app.db.init_db(): runs before the server exists.
	if err := initDB(ctx, ordersDB, legacyDB); err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              cfg.Addr(),
		Handler:           newAPI(ordersDB, legacyDB).routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info().Str("addr", srv.Addr).Msg("order service api listening")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err, ok := <-errCh:
		if ok {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	return nil
}

// openDatabases opens the Postgres orders database and the legacy MySQL
// billing database and verifies connectivity (pool_pre_ping equivalent).
func openDatabases(ctx context.Context, cfg config.Config) (*sql.DB, *sql.DB, error) {
	ordersDB, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("open orders db: %w", err)
	}
	dsn, err := config.MySQLDSN(cfg.LegacyDBURL)
	if err != nil {
		ordersDB.Close()
		return nil, nil, fmt.Errorf("legacy db url: %w", err)
	}
	legacyDB, err := sql.Open("mysql", dsn)
	if err != nil {
		ordersDB.Close()
		return nil, nil, fmt.Errorf("open legacy db: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := ordersDB.PingContext(pingCtx); err != nil {
		ordersDB.Close()
		legacyDB.Close()
		return nil, nil, fmt.Errorf("ping orders db: %w", err)
	}
	if err := legacyDB.PingContext(pingCtx); err != nil {
		ordersDB.Close()
		legacyDB.Close()
		return nil, nil, fmt.Errorf("ping legacy db: %w", err)
	}
	return ordersDB, legacyDB, nil
}

// ordersSchema mirrors app.models.Order (Postgres).
var ordersSchema = []string{
	`CREATE TABLE IF NOT EXISTS orders (
		id          SERIAL PRIMARY KEY,
		customer_id INTEGER NOT NULL,
		product     VARCHAR(100) NOT NULL,
		quantity    INTEGER NOT NULL,
		total       NUMERIC(10, 2) NOT NULL,
		status      VARCHAR(20) NOT NULL,
		created_at  TIMESTAMPTZ NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS ix_orders_customer_id ON orders (customer_id)`,
}

// legacySchema mirrors app.models.CustomerCredit (legacy MySQL).
var legacySchema = []string{
	`CREATE TABLE IF NOT EXISTS customer_credit (
		customer_id  INTEGER NOT NULL AUTO_INCREMENT,
		credit_limit DECIMAL(10, 2) NOT NULL,
		used         DECIMAL(10, 2) NOT NULL,
		PRIMARY KEY (customer_id)
	)`,
}

// initDB creates every table the service owns if it does not yet exist.
func initDB(ctx context.Context, ordersDB, legacyDB *sql.DB) error {
	for _, stmt := range ordersSchema {
		if _, err := ordersDB.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("init orders schema: %w", err)
		}
	}
	for _, stmt := range legacySchema {
		if _, err := legacyDB.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("init legacy schema: %w", err)
		}
	}
	return nil
}

// api holds the dependencies of the HTTP handlers.
type api struct {
	orders *sql.DB
	legacy *sql.DB
}

func newAPI(orders, legacy *sql.DB) *api {
	return &api{orders: orders, legacy: legacy}
}

// routes registers every route of the original Flask "api" blueprint
// (no url_prefix) at its exact path.
func (a *api) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", a.health)
	mux.HandleFunc("POST /customers/{customer_id}/credit", a.setCredit)
	mux.HandleFunc("GET /customers/{customer_id}/credit", a.getCredit)
	mux.HandleFunc("POST /orders", a.createOrder)
	mux.HandleFunc("GET /orders/{order_id}", a.getOrder)
	mux.HandleFunc("GET /orders", a.listOrders)
	mux.HandleFunc("GET /orders/{order_id}/audit", a.orderAudit)
	// MIGRATION_NOTE: the blueprint's before_request rate limiter
	// (catalog.allow_request, Redis-backed, 429 "rate limit exceeded",
	// /health exempt) must wrap this mux once internal/catalog is migrated.
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Error().Err(err).Msg("write json response")
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// pathID mimics Flask's <int:...> converter: non-negative decimal digits
// only. Anything else (including values overflowing int32) yields 404.
func pathID(r *http.Request, name string) (int, bool) {
	raw := r.PathValue(name)
	if raw == "" {
		return 0, false
	}
	for _, c := range raw {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, false
	}
	return int(n), true
}

// decodeBody mimics request.get_json(silent=True) or {}.
func decodeBody(r *http.Request) map[string]any {
	var body map[string]any
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(&body); err != nil || body == nil {
		return map[string]any{}
	}
	return body
}

func (a *api) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type creditResponse struct {
	CustomerID  int     `json:"customer_id"`
	CreditLimit float64 `json:"credit_limit"`
	Used        float64 `json:"used"`
}

func (a *api) setCredit(w http.ResponseWriter, r *http.Request) {
	customerID, ok := pathID(r, "customer_id")
	if !ok {
		http.NotFound(w, r)
		return
	}
	num, isNum := decodeBody(r)["credit_limit"].(json.Number)
	limit, err := num.Float64()
	if !isNum || err != nil || limit < 0 {
		writeError(w, http.StatusBadRequest, "credit_limit must be a non-negative number")
		return
	}
	ctx := r.Context()
	if _, err := a.legacy.ExecContext(ctx,
		`INSERT INTO customer_credit (customer_id, credit_limit, used) VALUES (?, ?, 0)
		 ON DUPLICATE KEY UPDATE credit_limit = VALUES(credit_limit)`,
		customerID, limit); err != nil {
		log.Error().Err(err).Msg("set credit")
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	a.respondCredit(w, r, customerID)
}

func (a *api) getCredit(w http.ResponseWriter, r *http.Request) {
	customerID, ok := pathID(r, "customer_id")
	if !ok {
		http.NotFound(w, r)
		return
	}
	a.respondCredit(w, r, customerID)
}

func (a *api) respondCredit(w http.ResponseWriter, r *http.Request, customerID int) {
	resp := creditResponse{CustomerID: customerID}
	err := a.legacy.QueryRowContext(r.Context(),
		`SELECT credit_limit, used FROM customer_credit WHERE customer_id = ?`, customerID).
		Scan(&resp.CreditLimit, &resp.Used)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "customer not found")
		return
	}
	if err != nil {
		log.Error().Err(err).Msg("get credit")
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

type orderResponse struct {
	ID         int     `json:"id"`
	CustomerID int     `json:"customer_id"`
	Product    string  `json:"product"`
	Quantity   int     `json:"quantity"`
	Total      float64 `json:"total"`
	Status     string  `json:"status"`
}

func (a *api) createOrder(w http.ResponseWriter, _ *http.Request) {
	// MIGRATION_NOTE: POST /orders depends on catalog.price_of (Redis cache),
	// audit.record (MongoDB) and events.publish (RabbitMQ), none of which are
	// migrated yet. Replace with the internal/httpapi implementation (atomic
	// credit charge, Postgres INSERT ... RETURNING id, 201 + order JSON).
	writeError(w, http.StatusNotImplemented, "not implemented")
}

func (a *api) getOrder(w http.ResponseWriter, r *http.Request) {
	orderID, ok := pathID(r, "order_id")
	if !ok {
		http.NotFound(w, r)
		return
	}
	var o orderResponse
	err := a.orders.QueryRowContext(r.Context(),
		`SELECT id, customer_id, product, quantity, total, status FROM orders WHERE id = $1`, orderID).
		Scan(&o.ID, &o.CustomerID, &o.Product, &o.Quantity, &o.Total, &o.Status)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "order not found")
		return
	}
	if err != nil {
		log.Error().Err(err).Msg("get order")
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, o)
}

func (a *api) listOrders(w http.ResponseWriter, r *http.Request) {
	query := `SELECT id, customer_id, product, quantity, total, status FROM orders`
	var args []any
	// Flask's args.get(type=int) silently ignores unparsable values.
	if raw := r.URL.Query().Get("customer_id"); raw != "" {
		if id, err := strconv.Atoi(raw); err == nil {
			query += ` WHERE customer_id = $1`
			args = append(args, id)
		}
	}
	query += ` ORDER BY id`

	rows, err := a.orders.QueryContext(r.Context(), query, args...)
	if err != nil {
		log.Error().Err(err).Msg("list orders")
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	defer rows.Close()

	out := []orderResponse{}
	for rows.Next() {
		var o orderResponse
		if err := rows.Scan(&o.ID, &o.CustomerID, &o.Product, &o.Quantity, &o.Total, &o.Status); err != nil {
			log.Error().Err(err).Msg("scan order")
			writeError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		log.Error().Err(err).Msg("iterate orders")
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *api) orderAudit(w http.ResponseWriter, r *http.Request) {
	if _, ok := pathID(r, "order_id"); !ok {
		http.NotFound(w, r)
		return
	}
	// MIGRATION_NOTE: depends on audit.history (MongoDB), not migrated yet.
	writeError(w, http.StatusNotImplemented, "not implemented")
}

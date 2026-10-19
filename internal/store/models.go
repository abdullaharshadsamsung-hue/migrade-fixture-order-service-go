package store

// This file replaces app/models.py: the Order model (primary Postgres
// database) and the CustomerCredit model (legacy MySQL database), plus the
// data-access functions the HTTP API and worker use instead of SQLAlchemy
// sessions. The DDL for both tables lives in db.go (EnsureSchema).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// Order statuses. StatusPending is the default for new orders, mirroring
// the SQLAlchemy column default="pending".
const (
	StatusPending   = "pending"
	StatusConfirmed = "confirmed"
)

// ErrNotFound is returned when a requested row does not exist.
var ErrNotFound = errors.New("store: not found")

// Order is a customer order stored in the Postgres "orders" table.
type Order struct {
	ID         int64     `json:"id"`
	CustomerID int64     `json:"customer_id"`
	Product    string    `json:"product"`
	Quantity   int       `json:"quantity"`
	Total      float64   `json:"total"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"-"` // excluded from serialization, as in to_dict()
}

// NewOrder builds an unsaved order with the source model's Python-side
// defaults applied: status "pending" and created_at = now (UTC).
func NewOrder(customerID int64, product string, quantity int, total float64) Order {
	return Order{
		CustomerID: customerID,
		Product:    product,
		Quantity:   quantity,
		Total:      total,
		Status:     StatusPending,
		CreatedAt:  time.Now().UTC(),
	}
}

// ToDict returns the order's public representation, identical to the
// source's to_dict(): id, customer_id, product, quantity, total (float) and
// status. created_at is intentionally omitted.
func (o Order) ToDict() map[string]any {
	return map[string]any{
		"id":          o.ID,
		"customer_id": o.CustomerID,
		"product":     o.Product,
		"quantity":    o.Quantity,
		"total":       o.Total,
		"status":      o.Status,
	}
}

// CustomerCredit is a customer's credit record stored in the legacy MySQL
// "customer_credit" table.
type CustomerCredit struct {
	CustomerID  int64   `json:"customer_id"`
	CreditLimit float64 `json:"credit_limit"`
	Used        float64 `json:"used"`
}

// NewCustomerCredit builds an unsaved credit record with used defaulting to
// 0, mirroring the SQLAlchemy column default=0.
func NewCustomerCredit(customerID int64, creditLimit float64) CustomerCredit {
	return CustomerCredit{CustomerID: customerID, CreditLimit: creditLimit, Used: 0}
}

// parseNumeric converts a NUMERIC column scanned as text into a float64,
// equivalent to Python's float(Decimal(...)).
func parseNumeric(col, s string) (float64, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s %q: %w", col, s, err)
	}
	return f, nil
}

// formatNumeric renders a float for a NUMERIC(10,2) parameter.
func formatNumeric(f float64) string {
	return strconv.FormatFloat(f, 'f', 2, 64)
}

const orderColumns = `id, customer_id, product, quantity, total, status, created_at`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanOrder(r rowScanner) (Order, error) {
	var (
		o     Order
		total string
	)
	if err := r.Scan(&o.ID, &o.CustomerID, &o.Product, &o.Quantity, &total, &o.Status, &o.CreatedAt); err != nil {
		return Order{}, err
	}
	t, err := parseNumeric("total", total)
	if err != nil {
		return Order{}, err
	}
	o.Total = t
	o.CreatedAt = o.CreatedAt.UTC()
	return o, nil
}

// InsertOrder persists o in the orders database and returns it with its
// generated ID. Defaults are applied if the caller left them empty.
func (db *DB) InsertOrder(ctx context.Context, o Order) (Order, error) {
	if o.Status == "" {
		o.Status = StatusPending
	}
	if o.CreatedAt.IsZero() {
		o.CreatedAt = time.Now().UTC()
	}
	err := db.Orders.QueryRowContext(ctx,
		`INSERT INTO orders (customer_id, product, quantity, total, status, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		o.CustomerID, o.Product, o.Quantity, formatNumeric(o.Total), o.Status, o.CreatedAt,
	).Scan(&o.ID)
	if err != nil {
		return Order{}, fmt.Errorf("insert order: %w", err)
	}
	return o, nil
}

// GetOrder loads the order with the given ID, or returns ErrNotFound.
func (db *DB) GetOrder(ctx context.Context, id int64) (Order, error) {
	row := db.Orders.QueryRowContext(ctx,
		`SELECT `+orderColumns+` FROM orders WHERE id = $1`, id)
	o, err := scanOrder(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	if err != nil {
		return Order{}, fmt.Errorf("get order %d: %w", id, err)
	}
	return o, nil
}

// ListOrders returns all orders ordered by ID. When customerID is non-nil
// only that customer's orders are returned (uses ix_orders_customer_id).
func (db *DB) ListOrders(ctx context.Context, customerID *int64) ([]Order, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if customerID != nil {
		rows, err = db.Orders.QueryContext(ctx,
			`SELECT `+orderColumns+` FROM orders WHERE customer_id = $1 ORDER BY id`, *customerID)
	} else {
		rows, err = db.Orders.QueryContext(ctx,
			`SELECT `+orderColumns+` FROM orders ORDER BY id`)
	}
	if err != nil {
		return nil, fmt.Errorf("list orders: %w", err)
	}
	defer rows.Close()

	orders := make([]Order, 0)
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			return nil, fmt.Errorf("scan order: %w", err)
		}
		orders = append(orders, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list orders: %w", err)
	}
	return orders, nil
}

// GetCustomerCredit loads a customer's credit record from the legacy MySQL
// database, or returns ErrNotFound.
//
// NOTE: this table lives in MySQL, so "?" placeholders are correct here.
func (db *DB) GetCustomerCredit(ctx context.Context, customerID int64) (CustomerCredit, error) {
	var limit, used string
	err := db.Legacy.QueryRowContext(ctx,
		`SELECT credit_limit, used FROM customer_credit WHERE customer_id = ?`, customerID,
	).Scan(&limit, &used)
	if errors.Is(err, sql.ErrNoRows) {
		return CustomerCredit{}, ErrNotFound
	}
	if err != nil {
		return CustomerCredit{}, fmt.Errorf("get credit %d: %w", customerID, err)
	}
	c := CustomerCredit{CustomerID: customerID}
	if c.CreditLimit, err = parseNumeric("credit_limit", limit); err != nil {
		return CustomerCredit{}, err
	}
	if c.Used, err = parseNumeric("used", used); err != nil {
		return CustomerCredit{}, err
	}
	return c, nil
}

// UpsertCustomerCredit sets a customer's credit limit, creating the record
// with used = 0 if it does not exist, and returns the stored record. This
// mirrors set_credit: get-or-create, then overwrite credit_limit only.
func (db *DB) UpsertCustomerCredit(ctx context.Context, customerID int64, creditLimit float64) (CustomerCredit, error) {
	_, err := db.Legacy.ExecContext(ctx,
		`INSERT INTO customer_credit (customer_id, credit_limit, used) VALUES (?, ?, ?)
		 ON DUPLICATE KEY UPDATE credit_limit = VALUES(credit_limit)`,
		customerID, formatNumeric(creditLimit), formatNumeric(0),
	)
	if err != nil {
		return CustomerCredit{}, fmt.Errorf("upsert credit %d: %w", customerID, err)
	}
	return db.GetCustomerCredit(ctx, customerID)
}

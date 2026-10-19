// Package store owns the two relational databases used by the order
// service: the primary orders database (Postgres) and the legacy billing
// database (MySQL). It opens both connection pools and bootstraps their
// schemas, replacing the SQLAlchemy engines, session factories and
// metadata.create_all calls of app/db.py.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql" // registers the "mysql" driver
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" driver

	"migrated-app/internal/config"
)

// pingTimeout bounds the initial connectivity check of each database.
const pingTimeout = 15 * time.Second

// connMaxIdleTime recycles idle connections so stale ones (e.g. dropped by
// a firewall or a database restart) are not handed out. Together with
// database/sql's automatic retry on driver.ErrBadConn this stands in for
// SQLAlchemy's pool_pre_ping=True.
const connMaxIdleTime = 5 * time.Minute

// DB bundles the connection pools of both databases. Each field is an
// independent pool, mirroring the separate engine / legacy_engine pair.
type DB struct {
	// Orders is the primary orders database (Postgres).
	Orders *sql.DB
	// Legacy is the legacy billing database (MySQL).
	Legacy *sql.DB
}

// Open opens both connection pools and verifies that each database is
// reachable. On any error every pool opened so far is closed.
func Open(ctx context.Context, cfg config.Config) (*DB, error) {
	orders, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("open orders db: %w", err)
	}
	orders.SetConnMaxIdleTime(connMaxIdleTime)

	dsn, err := config.MySQLDSN(cfg.LegacyDBURL)
	if err != nil {
		_ = orders.Close()
		return nil, fmt.Errorf("legacy db url: %w", err)
	}
	legacy, err := sql.Open("mysql", dsn)
	if err != nil {
		_ = orders.Close()
		return nil, fmt.Errorf("open legacy db: %w", err)
	}
	legacy.SetConnMaxIdleTime(connMaxIdleTime)

	db := &DB{Orders: orders, Legacy: legacy}

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := orders.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping orders db: %w", err)
	}
	if err := legacy.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping legacy db: %w", err)
	}
	return db, nil
}

// Close closes both connection pools, returning any errors joined.
func (db *DB) Close() error {
	var errs []error
	if db.Orders != nil {
		if err := db.Orders.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close orders db: %w", err))
		}
	}
	if db.Legacy != nil {
		if err := db.Legacy.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close legacy db: %w", err))
		}
	}
	return errors.Join(errs...)
}

// ordersSchema is the DDL for the tables registered on Base (app.models
// Order), executed against the Postgres orders database. Columns match the
// SQLAlchemy model exactly; defaults (status 'pending', created_at now UTC)
// were Python-side in the source and are set explicitly by Go on insert,
// so no server defaults are declared.
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

// legacySchema is the DDL for the tables registered on LegacyBase
// (app.models CustomerCredit), executed against the legacy MySQL database.
// MySQL syntax is intentional here: this database is MySQL, not Postgres.
var legacySchema = []string{
	`CREATE TABLE IF NOT EXISTS customer_credit (
		customer_id  INTEGER NOT NULL,
		credit_limit NUMERIC(10, 2) NOT NULL,
		used         NUMERIC(10, 2) NOT NULL,
		PRIMARY KEY (customer_id)
	)`,
}

// EnsureSchema creates every missing table on both databases, the
// equivalent of init_db()'s Base.metadata.create_all(engine) followed by
// LegacyBase.metadata.create_all(legacy_engine). Like create_all it only
// creates what is missing; it never alters existing tables.
//
// MIGRATION_NOTE: there is no versioned migration system, exactly as in the
// source. Schema changes to existing tables need a real migration tool.
func (db *DB) EnsureSchema(ctx context.Context) error {
	if err := execAll(ctx, db.Orders, ordersSchema); err != nil {
		return fmt.Errorf("init orders schema: %w", err)
	}
	if err := execAll(ctx, db.Legacy, legacySchema); err != nil {
		return fmt.Errorf("init legacy schema: %w", err)
	}
	return nil
}

// execAll runs each DDL statement in order, stopping at the first failure.
func execAll(ctx context.Context, conn *sql.DB, stmts []string) error {
	for _, stmt := range stmts {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

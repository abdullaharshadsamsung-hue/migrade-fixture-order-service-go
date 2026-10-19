// Command worker is the background order-events consumer. It replaces
// `python -m app.worker`: it consumes the durable order_events RabbitMQ
// queue (the same queue the HTTP server publishes to) and runs the migrated
// worker.Handler on every message. It never starts the HTTP server.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"migrated-app/internal/audit"
	"migrated-app/internal/config"
	"migrated-app/internal/store"
	"migrated-app/internal/worker"
)

// startupRetryDelay is the wait between attempts to reach the databases at
// startup (compose may start the worker before they accept connections).
const startupRetryDelay = 2 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("load config")
	}

	db := openStore(ctx, cfg)
	if db == nil {
		return // cancelled during startup
	}
	defer func() { _ = db.Close() }()

	// Idempotent (CREATE TABLE IF NOT EXISTS); ensures the orders table
	// exists even if the worker starts before the API.
	if err := db.EnsureSchema(ctx); err != nil {
		log.Error().Err(err).Msg("ensure schema")
	}

	auditLog, err := audit.New(ctx, cfg.MongoDBURI)
	if err != nil {
		log.Fatal().Err(err).Msg("audit")
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = auditLog.Close(closeCtx)
	}()

	h := worker.NewHandler(worker.NewSQLOrders(db), auditLog)

	log.Info().Str("queue", config.OrderEventsQueue).Msg("worker starting")
	if err := worker.Run(ctx, cfg.AMQPURL, h); err != nil {
		log.Error().Err(err).Msg("worker stopped with error")
		return
	}
	log.Info().Msg("worker stopped")
}

// openStore opens both database pools, retrying until success or until ctx
// is cancelled (in which case it returns nil).
func openStore(ctx context.Context, cfg config.Config) *store.DB {
	for {
		db, err := store.Open(ctx, cfg)
		if err == nil {
			return db
		}
		log.Error().Err(err).Msg("open databases; retrying")
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(startupRetryDelay):
		}
	}
}

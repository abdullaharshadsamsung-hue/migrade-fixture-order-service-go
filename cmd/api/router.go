package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog/log"

	"migrated-app/internal/audit"
	"migrated-app/internal/catalog"
	"migrated-app/internal/config"
	"migrated-app/internal/events"
	"migrated-app/internal/httpapi"
	"migrated-app/internal/store"
)

// buildRouter creates the chi router, installs middleware, keeps /healthz,
// configures the API dependencies and mounts every API route.
func buildRouter(ctx context.Context, cfg config.Config) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	if err := configureAPI(ctx, cfg); err != nil {
		// Routes stay mounted; handlers answer 500 until dependencies exist.
		log.Error().Err(err).Msg("failed to configure API dependencies")
	}

	httpapi.RegisterRoutes(r)
	return r
}

// configureAPI builds every concrete dependency from cfg and installs them
// with httpapi.Configure.
func configureAPI(ctx context.Context, cfg config.Config) error {
	db, err := store.Open(ctx, cfg)
	if err != nil {
		return fmt.Errorf("open databases: %w", err)
	}
	if err := db.EnsureSchema(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("ensure schema: %w", err)
	}

	auditLog, err := audit.New(ctx, cfg.MongoDBURI)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("audit: %w", err)
	}

	pub, err := events.NewAMQPPublisher(cfg.AMQPURL)
	if err != nil {
		_ = auditLog.Close(ctx)
		_ = db.Close()
		return fmt.Errorf("events: %w", err)
	}

	cat, err := catalog.NewFromURL(cfg.RedisURL, cfg.RateLimitPerMinute)
	if err != nil {
		_ = auditLog.Close(ctx)
		_ = db.Close()
		return fmt.Errorf("catalog: %w", err)
	}

	httpapi.Configure(httpapi.Deps{
		Orders:  db,
		Credits: httpapi.NewSQLCreditStore(db),
		Catalog: cat,
		Audit:   auditLog,
		Events:  pub,
	})
	return nil
}

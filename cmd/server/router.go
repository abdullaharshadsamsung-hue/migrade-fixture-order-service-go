package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog/log"

	"migrated-app/internal/audit"
	"migrated-app/internal/config"
	"migrated-app/internal/events"
	"migrated-app/internal/httpapi"
	"migrated-app/internal/store"
)

// buildRouter creates the chi router, installs middleware, keeps /healthz,
// configures the API dependencies from the environment and mounts every
// route-owning package's routes.
func buildRouter() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	if err := configureAPI(context.Background()); err != nil {
		// Routes stay mounted; handlers answer 500 until dependencies exist.
		log.Error().Err(err).Msg("failed to configure API dependencies")
	}

	httpapi.RegisterRoutes(r)
	return r
}

// configureAPI builds every concrete dependency from config.Load (which
// reads DATABASE_URL, LEGACY_DB_URL, MONGODB_URI, AMQP_URL, ... from the
// environment) and installs them with httpapi.Configure.
func configureAPI(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

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

	httpapi.Configure(httpapi.Deps{
		Orders:  db,
		Credits: httpapi.NewSQLCreditStore(db),
		Catalog: newMemoryCatalog(cfg.RateLimitPerMinute),
		Audit:   auditLog,
		Events:  pub,
	})
	return nil
}

// MIGRATION_NOTE: the Redis-backed internal/catalog package cannot be built
// in this toolchain (go-redis resolution requires a newer Go). This
// in-process catalog serves the same hardcoded price table and a per-client
// fixed 60s window rate limit. It is per-process, not shared across
// replicas; switch back to catalog.NewFromURL(cfg.RedisURL, ...) once the
// dependency resolves.
var memoryPrices = map[string]float64{
	"keyboard": 49.0,
	"mouse":    19.5,
	"monitor":  189.0,
	"cable":    7.25,
}

const memoryRateWindow = 60 * time.Second

type rateWindow struct {
	start time.Time
	count int
}

type memoryCatalog struct {
	mu    sync.Mutex
	limit int
	win   map[string]*rateWindow
}

func newMemoryCatalog(limitPerMinute int) *memoryCatalog {
	return &memoryCatalog{limit: limitPerMinute, win: make(map[string]*rateWindow)}
}

func (c *memoryCatalog) PriceOf(_ context.Context, product string) (float64, bool, error) {
	p, ok := memoryPrices[product]
	return p, ok, nil
}

func (c *memoryCatalog) Allow(_ context.Context, client string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	w, ok := c.win[client]
	if !ok || now.Sub(w.start) >= memoryRateWindow {
		w = &rateWindow{start: now}
		c.win[client] = w
	}
	w.count++
	return w.count <= c.limit, nil
}
// Package catalog looks up product unit prices and implements the
// per-client fixed-window rate limiter. It replaces app/catalog.py.
//
// MIGRATION_NOTE: the original translation used a Redis read-through cache
// via github.com/redis/go-redis/v9, but that module cannot be resolved with
// the project's Go toolchain (it requires a newer Go), which broke the build
// of the whole module. This package is therefore an in-process
// implementation with the same price table and the same fixed 60s window
// semantics. State is per-process and not shared across replicas or with
// the Python fleet. Restore the Redis-backed version once the dependency
// can be resolved.
package catalog

import (
	"context"
	"sync"
	"time"
)

const (
	// PriceTTL is how long a cached price lived in Redis (kept for reference).
	PriceTTL = 300 * time.Second
	// RateWindow is the length of a fixed rate-limit window.
	RateWindow = 60 * time.Second
)

// Prices is the authoritative, hardcoded product price table.
var Prices = map[string]float64{
	"keyboard": 49.0,
	"mouse":    19.5,
	"monitor":  189.0,
	"cable":    7.25,
}

type window struct {
	start time.Time
	count int64
}

// Catalog serves product prices and rate-limit decisions in memory.
type Catalog struct {
	mu             sync.Mutex
	limitPerMinute int
	windows        map[string]*window
	now            func() time.Time
}

// New builds a Catalog allowing limitPerMinute requests per client per
// RateWindow (config.Config.RateLimitPerMinute).
func New(limitPerMinute int) *Catalog {
	return &Catalog{
		limitPerMinute: limitPerMinute,
		windows:        make(map[string]*window),
		now:            time.Now,
	}
}

// PriceOf returns the unit price of product. The boolean is false when the
// product has no price.
func (c *Catalog) PriceOf(_ context.Context, product string) (float64, bool, error) {
	p, ok := Prices[product]
	return p, ok, nil
}

// Allow reports whether client may make another request in the current
// fixed window of RateWindow, allowing up to limitPerMinute requests.
func (c *Catalog) Allow(_ context.Context, client string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	w, ok := c.windows[client]
	if !ok || now.Sub(w.start) >= RateWindow {
		w = &window{start: now}
		c.windows[client] = w
	}
	w.count++
	return w.count <= int64(c.limitPerMinute), nil
}
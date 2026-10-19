// Package catalog looks up product unit prices through a Redis read-through
// cache and implements the per-client fixed-window rate limiter. It replaces
// app/catalog.py.
package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis key prefixes and TTLs. These are part of the wire contract shared
// with the Python service during a mixed-fleet cutover; do not change them.
const (
	priceKeyPrefix = "price:"
	rateKeyPrefix  = "rate:"

	// PriceTTL is how long a cached price lives in Redis.
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

// Catalog serves product prices and rate-limit decisions backed by Redis.
type Catalog struct {
	rdb            *redis.Client
	limitPerMinute int
}

// New builds a Catalog on an existing Redis client. limitPerMinute is the
// number of requests each client may make per 60s window
// (config.Config.RateLimitPerMinute).
func New(rdb *redis.Client, limitPerMinute int) *Catalog {
	return &Catalog{rdb: rdb, limitPerMinute: limitPerMinute}
}

// NewFromURL parses redisURL (config.Config.RedisURL) and builds a Catalog
// with its own Redis client. The caller owns the client and should Close it
// via Catalog.Redis().Close() on shutdown.
//
// MIGRATION_NOTE: the Python module created its client at import time with
// decode_responses=True. go-redis always returns strings from string
// commands, so no equivalent option is needed; the client is created
// explicitly here instead of as a package-level singleton.
func NewFromURL(redisURL string, limitPerMinute int) (*Catalog, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("catalog: parse redis url: %w", err)
	}
	return New(redis.NewClient(opts), limitPerMinute), nil
}

// Redis returns the underlying Redis client (the equivalent of
// redis_client() in the Python module).
func (c *Catalog) Redis() *redis.Client {
	return c.rdb
}

// PriceOf returns the unit price of product. It reads through the Redis key
// "price:<product>"; on a miss it falls back to Prices and caches known
// products for PriceTTL. Unknown products are never cached. The boolean is
// false when the product has no price. Redis errors are returned wrapped.
func (c *Catalog) PriceOf(ctx context.Context, product string) (float64, bool, error) {
	key := priceKeyPrefix + product

	cached, err := c.rdb.Get(ctx, key).Result()
	switch {
	case err == nil:
		var price *float64
		if err := json.Unmarshal([]byte(cached), &price); err != nil {
			return 0, false, fmt.Errorf("catalog: decode cached price %q: %w", key, err)
		}
		if price == nil {
			// json.loads("null") returned None in Python.
			return 0, false, nil
		}
		return *price, true, nil
	case errors.Is(err, redis.Nil):
		// cache miss, fall through
	default:
		return 0, false, fmt.Errorf("catalog: get %q: %w", key, err)
	}

	price, ok := Prices[product]
	if !ok {
		return 0, false, nil
	}
	if err := c.rdb.Set(ctx, key, encodePrice(price), PriceTTL).Err(); err != nil {
		return 0, false, fmt.Errorf("catalog: set %q: %w", key, err)
	}
	return price, true, nil
}

// Allow reports whether client may make another request in the current
// fixed window of RateWindow, allowing up to limitPerMinute requests.
//
// MIGRATION_NOTE: INCR and EXPIRE are issued as two separate commands, as in
// the Python source, so key layout and behaviour stay identical across the
// mixed fleet. If the process dies between them the key has no TTL; consider
// a MULTI/EXEC or Lua script once the Python service is retired.
func (c *Catalog) Allow(ctx context.Context, client string) (bool, error) {
	key := rateKeyPrefix + client
	count, err := c.rdb.Incr(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("catalog: incr %q: %w", key, err)
	}
	if count == 1 {
		if err := c.rdb.Expire(ctx, key, RateWindow).Err(); err != nil {
			return false, fmt.Errorf("catalog: expire %q: %w", key, err)
		}
	}
	return count <= int64(c.limitPerMinute), nil
}

// encodePrice renders price the way Python's json.dumps renders a float
// (e.g. 49.0 -> "49.0", 7.25 -> "7.25"), so cached values written by Go and
// Python are byte-identical.
func encodePrice(price float64) string {
	s := strconv.FormatFloat(price, 'f', -1, 64)
	if price == math.Trunc(price) && !math.IsInf(price, 0) && !math.IsNaN(price) {
		s += ".0"
	}
	return s
}

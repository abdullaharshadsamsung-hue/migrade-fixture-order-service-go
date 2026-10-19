// Package config loads every runtime setting from the environment, with
// local-development defaults, and provides helpers for turning database
// URLs into driver-specific connection strings.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

// OrderEventsQueue is the RabbitMQ queue that carries order events between
// the API and the worker. It is part of the wire contract and is not
// configurable.
const OrderEventsQueue = "order_events"

// Environment variable names and their local-dev defaults.
const (
	envDatabaseURL        = "DATABASE_URL"
	envLegacyDBURL        = "LEGACY_DB_URL"
	envMongoDBURI         = "MONGODB_URI"
	envRedisURL           = "REDIS_URL"
	envAMQPURL            = "AMQP_URL"
	envRateLimitPerMinute = "RATE_LIMIT_PER_MINUTE"
	envPort               = "PORT"

	defaultDatabaseURL        = "postgresql://app:app@localhost:5432/app"
	defaultLegacyDBURL        = "mysql://app:app@localhost:3306/legacy"
	defaultMongoDBURI         = "mongodb://localhost:27017/app"
	defaultRedisURL           = "redis://localhost:6379/0"
	defaultAMQPURL            = "amqp://app:app@localhost:5672/"
	defaultRateLimitPerMinute = "600"
	defaultPort               = "8080"
)

// Config holds every setting the API and worker need.
type Config struct {
	// DatabaseURL is the primary PostgreSQL URL (postgres:// or postgresql://).
	DatabaseURL string
	// LegacyDBURL is the legacy MySQL URL (mysql://...). Use MySQLDSN to
	// convert it for github.com/go-sql-driver/mysql.
	LegacyDBURL string
	// MongoDBURI is the MongoDB connection URI; its path names the database.
	MongoDBURI string
	// RedisURL is the Redis connection URL.
	RedisURL string
	// AMQPURL is the RabbitMQ connection URL.
	AMQPURL string
	// RateLimitPerMinute is the per-client request budget per minute.
	RateLimitPerMinute int
	// Port is the TCP port the HTTP server listens on.
	Port int
}

// Load reads the configuration from the environment, applying local-dev
// defaults for unset variables. It returns an error when a numeric setting
// cannot be parsed, so misconfiguration fails fast at startup.
func Load() (Config, error) {
	v := viper.New()
	defaults := map[string]string{
		envDatabaseURL:        defaultDatabaseURL,
		envLegacyDBURL:        defaultLegacyDBURL,
		envMongoDBURI:         defaultMongoDBURI,
		envRedisURL:           defaultRedisURL,
		envAMQPURL:            defaultAMQPURL,
		envRateLimitPerMinute: defaultRateLimitPerMinute,
		envPort:               defaultPort,
	}
	for key, def := range defaults {
		// SetDefault registers the key so the env binding is honoured;
		// BindEnv makes the exact env var name authoritative.
		v.SetDefault(key, def)
		if err := v.BindEnv(key); err != nil {
			return Config{}, fmt.Errorf("config: bind %s: %w", key, err)
		}
	}

	rateLimit, err := parseInt(v, envRateLimitPerMinute)
	if err != nil {
		return Config{}, err
	}
	port, err := parseInt(v, envPort)
	if err != nil {
		return Config{}, err
	}
	if port <= 0 || port > 65535 {
		return Config{}, fmt.Errorf("config: %s=%d is out of range 1-65535", envPort, port)
	}

	return Config{
		DatabaseURL:        v.GetString(envDatabaseURL),
		LegacyDBURL:        v.GetString(envLegacyDBURL),
		MongoDBURI:         v.GetString(envMongoDBURI),
		RedisURL:           v.GetString(envRedisURL),
		AMQPURL:            v.GetString(envAMQPURL),
		RateLimitPerMinute: rateLimit,
		Port:               port,
	}, nil
}

// Addr returns the listen address for the HTTP server, e.g. ":8080".
func (c Config) Addr() string {
	return ":" + strconv.Itoa(c.Port)
}

func parseInt(v *viper.Viper, key string) (int, error) {
	raw := strings.TrimSpace(v.GetString(key))
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q is not a valid integer: %w", key, raw, err)
	}
	return n, nil
}

// SQLAlchemyURL rewrites a database URL so its scheme names the driver the
// original Python service used: postgres:// and postgresql:// become
// postgresql+psycopg://, mysql:// becomes mysql+pymysql://. Any other URL
// (including one with no scheme) is returned unchanged.
//
// MIGRATION_NOTE: Go drivers do not need this (pgx accepts postgres:// and
// postgresql:// directly; MySQL needs MySQLDSN). It is kept for parity with
// the Python fleet, e.g. when handing URLs to Python tooling during cutover.
func SQLAlchemyURL(rawURL string) string {
	scheme, rest, found := strings.Cut(rawURL, "://")
	if !found {
		// Python's str.partition yields scheme=url, rest="" here; no
		// branch matches unless the whole string equals a scheme name.
		rest = ""
	}
	switch scheme {
	case "postgres", "postgresql":
		return "postgresql+psycopg://" + rest
	case "mysql":
		return "mysql+pymysql://" + rest
	default:
		return rawURL
	}
}

// ErrNotMySQLURL is returned by MySQLDSN when the URL scheme is not mysql.
var ErrNotMySQLURL = errors.New("config: not a mysql:// URL")

// MySQLDSN converts a mysql://user:pass@host:port/db URL into the DSN format
// expected by github.com/go-sql-driver/mysql:
// user:pass@tcp(host:port)/db?parseTime=true. Percent-escaped credentials
// are decoded. The port defaults to 3306 when omitted. Existing query
// parameters are preserved.
func MySQLDSN(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("config: parse mysql url: %w", err)
	}
	if u.Scheme != "mysql" {
		return "", fmt.Errorf("%w: scheme %q", ErrNotMySQLURL, u.Scheme)
	}
	if u.Hostname() == "" {
		return "", errors.New("config: mysql url has no host")
	}

	var b strings.Builder
	if u.User != nil {
		b.WriteString(u.User.Username())
		if pw, ok := u.User.Password(); ok {
			b.WriteByte(':')
			b.WriteString(pw)
		}
		b.WriteByte('@')
	}

	port := u.Port()
	if port == "" {
		port = "3306"
	}
	fmt.Fprintf(&b, "tcp(%s)/", joinHostPort(u.Hostname(), port))
	b.WriteString(strings.TrimPrefix(u.Path, "/"))

	q := u.Query()
	if q.Get("parseTime") == "" {
		q.Set("parseTime", "true")
	}
	b.WriteByte('?')
	b.WriteString(q.Encode())
	return b.String(), nil
}

func joinHostPort(host, port string) string {
	if strings.Contains(host, ":") {
		return "[" + host + "]:" + port
	}
	return host + ":" + port
}

package config

import (
	"errors"
	"os"
	"strings"
	"testing"
)

var allEnvKeys = []string{
	envDatabaseURL,
	envLegacyDBURL,
	envMongoDBURI,
	envRedisURL,
	envAMQPURL,
	envRateLimitPerMinute,
	envPort,
}

// clearEnv unsets every config variable for the duration of the test,
// restoring original values afterwards.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range allEnvKeys {
		t.Setenv(k, "") // registers restore of original value
		if err := os.Unsetenv(k); err != nil {
			t.Fatalf("unsetenv %s: %v", k, err)
		}
	}
}

func TestOrderEventsQueueConstant(t *testing.T) {
	if OrderEventsQueue != "order_events" {
		t.Fatalf("OrderEventsQueue = %q, want %q", OrderEventsQueue, "order_events")
	}
}

func TestOrderEventsQueueNotConfigurable(t *testing.T) {
	clearEnv(t)
	t.Setenv("ORDER_EVENTS_QUEUE", "something_else")
	if _, err := Load(); err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if OrderEventsQueue != "order_events" {
		t.Fatalf("OrderEventsQueue changed to %q", OrderEventsQueue)
	}
}

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	want := Config{
		DatabaseURL:        "postgresql://app:app@localhost:5432/app",
		LegacyDBURL:        "mysql://app:app@localhost:3306/legacy",
		MongoDBURI:         "mongodb://localhost:27017/app",
		RedisURL:           "redis://localhost:6379/0",
		AMQPURL:            "amqp://app:app@localhost:5672/",
		RateLimitPerMinute: 600,
		Port:               8080,
	}
	if cfg != want {
		t.Fatalf("Load() = %+v, want %+v", cfg, want)
	}
	if got := cfg.Addr(); got != ":8080" {
		t.Fatalf("Addr() = %q, want %q", got, ":8080")
	}
}

func TestLoadFromEnv(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
		check func(Config) (string, string)
	}{
		{"database url", envDatabaseURL, "postgres://u:p@db:5432/x",
			func(c Config) (string, string) { return c.DatabaseURL, "postgres://u:p@db:5432/x" }},
		{"legacy db url", envLegacyDBURL, "mysql://u:p@legacy:3306/y",
			func(c Config) (string, string) { return c.LegacyDBURL, "mysql://u:p@legacy:3306/y" }},
		{"mongodb uri", envMongoDBURI, "mongodb://mongo:27017/z",
			func(c Config) (string, string) { return c.MongoDBURI, "mongodb://mongo:27017/z" }},
		{"redis url", envRedisURL, "redis://cache:6379/2",
			func(c Config) (string, string) { return c.RedisURL, "redis://cache:6379/2" }},
		{"amqp url", envAMQPURL, "amqp://g:g@rabbit:5672/vh",
			func(c Config) (string, string) { return c.AMQPURL, "amqp://g:g@rabbit:5672/vh" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t)
			t.Setenv(tt.key, tt.value)
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			got, want := tt.check(cfg)
			if got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
		})
	}
}

func TestLoadEmptyStringUsedVerbatim(t *testing.T) {
	tests := []struct {
		key string
		get func(Config) string
	}{
		{envDatabaseURL, func(c Config) string { return c.DatabaseURL }},
		{envLegacyDBURL, func(c Config) string { return c.LegacyDBURL }},
		{envMongoDBURI, func(c Config) string { return c.MongoDBURI }},
		{envRedisURL, func(c Config) string { return c.RedisURL }},
		{envAMQPURL, func(c Config) string { return c.AMQPURL }},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			clearEnv(t)
			t.Setenv(tt.key, "")
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			if got := tt.get(cfg); got != "" {
				t.Fatalf("%s set to empty string: got %q, want empty string verbatim", tt.key, got)
			}
		})
	}
}

func TestLoadRateLimit(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    int
		wantErr bool
	}{
		{"integer", "120", 120, false},
		{"zero", "0", 0, false},
		{"surrounding spaces", " 42 ", 42, false},
		{"non integer", "abc", 0, true},
		{"float", "1.5", 0, true},
		{"empty string is invalid integer", "", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t)
			t.Setenv(envRateLimitPerMinute, tt.value)
			cfg, err := Load()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Load() expected error for %q, got cfg %+v", tt.value, cfg)
				}
				if !strings.Contains(err.Error(), envRateLimitPerMinute) {
					t.Fatalf("error %q does not mention %s", err, envRateLimitPerMinute)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			if cfg.RateLimitPerMinute != tt.want {
				t.Fatalf("RateLimitPerMinute = %d, want %d", cfg.RateLimitPerMinute, tt.want)
			}
		})
	}
}

func TestLoadPort(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		want     int
		wantAddr string
		wantErr  bool
	}{
		{"valid", "9090", 9090, ":9090", false},
		{"min", "1", 1, ":1", false},
		{"max", "65535", 65535, ":65535", false},
		{"zero", "0", 0, "", true},
		{"negative", "-1", 0, "", true},
		{"too large", "65536", 0, "", true},
		{"non integer", "http", 0, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t)
			t.Setenv(envPort, tt.value)
			cfg, err := Load()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Load() expected error for PORT=%q", tt.value)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			if cfg.Port != tt.want {
				t.Fatalf("Port = %d, want %d", cfg.Port, tt.want)
			}
			if cfg.Addr() != tt.wantAddr {
				t.Fatalf("Addr() = %q, want %q", cfg.Addr(), tt.wantAddr)
			}
		})
	}
}

func TestLoadSnapshotNotAffectedByLaterEnvChanges(t *testing.T) {
	clearEnv(t)
	t.Setenv(envDatabaseURL, "postgres://first/db")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	t.Setenv(envDatabaseURL, "postgres://second/db")
	if cfg.DatabaseURL != "postgres://first/db" {
		t.Fatalf("loaded config changed after env change: %q", cfg.DatabaseURL)
	}
}

func TestSQLAlchemyURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"postgresql", "postgresql://app:app@localhost:5432/app", "postgresql+psycopg://app:app@localhost:5432/app"},
		{"postgres normalized", "postgres://u:p@h:5432/db", "postgresql+psycopg://u:p@h:5432/db"},
		{"mysql", "mysql://app:app@localhost:3306/legacy", "mysql+pymysql://app:app@localhost:3306/legacy"},
		{"already psycopg", "postgresql+psycopg://h/db", "postgresql+psycopg://h/db"},
		{"already pymysql", "mysql+pymysql://h/db", "mysql+pymysql://h/db"},
		{"sqlite", "sqlite:///file.db", "sqlite:///file.db"},
		{"mongodb", "mongodb://localhost:27017/app", "mongodb://localhost:27017/app"},
		{"redis", "redis://localhost:6379/0", "redis://localhost:6379/0"},
		{"uppercase postgresql", "POSTGRESQL://h/db", "POSTGRESQL://h/db"},
		{"mixed case mysql", "MySQL://h/db", "MySQL://h/db"},
		{"bare postgresql", "postgresql", "postgresql+psycopg://"},
		{"bare postgres", "postgres", "postgresql+psycopg://"},
		{"bare mysql", "mysql", "mysql+pymysql://"},
		{"no separator", "not a url", "not a url"},
		{"empty", "", ""},
		{"nested separator", "mysql://h/db?x=a://b", "mysql+pymysql://h/db?x=a://b"},
		{"empty rest postgres", "postgres://", "postgresql+psycopg://"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SQLAlchemyURL(tt.in)
			if got != tt.want {
				t.Fatalf("SQLAlchemyURL(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestSQLAlchemyURLInvariants(t *testing.T) {
	inputs := []string{
		"postgresql://app:app@localhost:5432/app",
		"postgres://u:p@h:5432/db",
		"mysql://app:app@localhost:3306/legacy",
		"mysql://h/db?x=a://b",
		"sqlite:///file.db",
		"POSTGRESQL://h/db",
		"postgresql",
		"mysql",
		"not a url",
		"",
		"postgres://ü%20ñ@h/δ",
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			once := SQLAlchemyURL(in)
			if again := SQLAlchemyURL(in); again != once {
				t.Fatalf("non-deterministic: %q vs %q", once, again)
			}
			if twice := SQLAlchemyURL(once); twice != once {
				t.Fatalf("not idempotent: once=%q twice=%q", once, twice)
			}
			if _, rest, ok := strings.Cut(in, "://"); ok {
				_, outRest, outOK := strings.Cut(once, "://")
				if !outOK || outRest != rest {
					t.Fatalf("rest not preserved: in=%q out=%q", in, once)
				}
			}
			scheme, _, _ := strings.Cut(in, "://")
			switch scheme {
			case "postgres", "postgresql", "mysql":
			default:
				if once != in {
					t.Fatalf("non-matching scheme rewritten: %q -> %q", in, once)
				}
			}
		})
	}
}

func TestMySQLDSN(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
		errIs   error
	}{
		{name: "default legacy", in: "mysql://app:app@localhost:3306/legacy",
			want: "app:app@tcp(localhost:3306)/legacy?parseTime=true"},
		{name: "port defaults", in: "mysql://u:p@db/x",
			want: "u:p@tcp(db:3306)/x?parseTime=true"},
		{name: "escaped credentials", in: "mysql://us%40r:p%40ss@db:3306/x",
			want: "us@r:p@ss@tcp(db:3306)/x?parseTime=true"},
		{name: "user without password", in: "mysql://u@db:3307/x",
			want: "u@tcp(db:3307)/x?parseTime=true"},
		{name: "no user", in: "mysql://db:3306/x",
			want: "tcp(db:3306)/x?parseTime=true"},
		{name: "query preserved", in: "mysql://u:p@db/x?charset=utf8mb4",
			want: "u:p@tcp(db:3306)/x?charset=utf8mb4&parseTime=true"},
		{name: "parseTime kept", in: "mysql://u:p@db/x?parseTime=false",
			want: "u:p@tcp(db:3306)/x?parseTime=false"},
		{name: "ipv6", in: "mysql://u@[::1]:3307/db",
			want: "u@tcp([::1]:3307)/db?parseTime=true"},
		{name: "wrong scheme", in: "postgres://u:p@h/db", wantErr: true, errIs: ErrNotMySQLURL},
		{name: "pymysql scheme", in: "mysql+pymysql://u:p@h/db", wantErr: true, errIs: ErrNotMySQLURL},
		{name: "no host", in: "mysql:///db", wantErr: true},
		{name: "parse error", in: "mysql://%zz/db", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MySQLDSN(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("MySQLDSN(%q) expected error, got %q", tt.in, got)
				}
				if tt.errIs != nil && !errors.Is(err, tt.errIs) {
					t.Fatalf("error %v is not %v", err, tt.errIs)
				}
				return
			}
			if err != nil {
				t.Fatalf("MySQLDSN(%q) error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("MySQLDSN(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
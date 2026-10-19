package config

import "github.com/spf13/viper"

type Config struct {
	DatabaseURL string `mapstructure:"DATABASE_URL"`
	Port        string `mapstructure:"PORT"`
	JWTSecret   string `mapstructure:"JWT_SECRET"`
}

func Load() (*Config, error) {
	viper.AutomaticEnv()
	// AutomaticEnv() alone does NOT make viper.Unmarshal() populate a
	// mapstructure-tagged field from a real env var unless viper already
	// "knows about" that key first (via SetDefault or an explicit BindEnv)
	// — it only makes viper.Get(key) work directly. Without these, Unmarshal
	// silently leaves DatabaseURL/JWTSecret as empty strings regardless of
	// what's actually set in the real environment, and callers reading
	// cfg.DatabaseURL fall back to whatever hardcoded default they have —
	// PORT "worked" here only because SetDefault registers a key as a side
	// effect, DatabaseURL/JWTSecret had no such registration. Real incident:
	// a migrated app's DATABASE_URL was genuinely set to the correct
	// per-migration Postgres sidecar DSN, but cfg.DatabaseURL still came
	// back "" every time, so the app's own hardcoded main.go fallback DSN
	// ran instead — wrong credentials, connection refused on every attempt.
	viper.SetDefault("PORT", "8080")
	_ = viper.BindEnv("DATABASE_URL")
	_ = viper.BindEnv("JWT_SECRET")
	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

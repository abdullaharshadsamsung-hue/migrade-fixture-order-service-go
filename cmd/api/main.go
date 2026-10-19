package main

// This file carries the translation of main.py, the entry-point module that
// started the HTTP API on host 0.0.0.0 and port $PORT (default 8080).
//
// The application factory (create_app) and the process lifecycle (main/run,
// graceful shutdown) already live in main.go. This file adds only the parts
// main.py owned: the exact listen address and the HTTP server settings.
//
// MIGRATION_NOTE: main.py exposed a module-level WSGI object (main:app) for
// servers such as gunicorn, and ran Werkzeug's development server only under
// `if __name__ == "__main__"`. A Go binary does not separate being imported
// from being run. The compiled binary always runs the production-grade
// net/http server, so docker-compose's `command: python main.py` must change
// to run the built cmd/api binary.
//
// MIGRATION_NOTE (manual review): main.go currently builds its http.Server
// with Addr: cfg.Addr() (":<port>", every interface including IPv6) and only
// a ReadHeaderTimeout. To match app.run(host="0.0.0.0") exactly and get the
// full timeout set, replace that literal in run() with
// newHTTPServer(cfg, handler).

import (
	"net"
	"net/http"
	"strconv"
	"time"

	"migrated-app/internal/config"
)

// listenHost mirrors app.run(host="0.0.0.0", ...): listen on every IPv4
// interface.
const listenHost = "0.0.0.0"

// HTTP server timeouts. Werkzeug's dev server had none; these protect the
// production server against slow or idle clients.
const (
	serverReadHeaderTimeout = 10 * time.Second
	serverReadTimeout       = 30 * time.Second
	serverWriteTimeout      = 30 * time.Second
	serverIdleTimeout       = 120 * time.Second
)

// listenAddr returns "0.0.0.0:<port>". The port comes from cfg.Port, which
// config.Load reads from the PORT environment variable (default 8080).
func listenAddr(cfg config.Config) string {
	return net.JoinHostPort(listenHost, strconv.Itoa(cfg.Port))
}

// newHTTPServer builds the server that replaces app.run(): it serves h on
// listenAddr(cfg) with production timeouts.
func newHTTPServer(cfg config.Config, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              listenAddr(cfg),
		Handler:           h,
		ReadHeaderTimeout: serverReadHeaderTimeout,
		ReadTimeout:       serverReadTimeout,
		WriteTimeout:      serverWriteTimeout,
		IdleTimeout:       serverIdleTimeout,
	}
}

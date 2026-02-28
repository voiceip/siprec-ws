package main

import (
	"context"
	"embed"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/sirupsen/logrus"

	"siprec-server/pkg/metrics"
)

//go:embed static/dashboard.html
var dashboardFS embed.FS

// runHTTPServer starts the debug/metrics HTTP server. It registers /metrics
// (Prometheus), /health, /debug/calls, and / (dashboard). Runs until ctx is cancelled.
func runHTTPServer(ctx context.Context, port int, pool *WSForwarderPool, logger *logrus.Logger) {
	mux := http.NewServeMux()

	metrics.RegisterHandler(mux)

	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("/debug/calls", func(w http.ResponseWriter, _ *http.Request) {
		calls := pool.ActiveCalls()
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(calls); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	dashboardHTML, _ := dashboardFS.ReadFile("static/dashboard.html")
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(dashboardHTML)
	})

	srv := &http.Server{Addr: ":" + strconv.Itoa(port), Handler: mux}
	go func() {
		logger.WithField("http_port", port).Info("HTTP server starting")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.WithError(err).Error("HTTP server failed")
		}
	}()
	<-ctx.Done()
	_ = srv.Shutdown(context.Background())
	logger.Info("HTTP server stopped")
}

package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// listen binds the admin listener; tests swap it to capture the bound address
// of 127.0.0.1:0 or to inject a listener whose Accept fails. Same
// package-state caveat as notifyContext: no t.Parallel in tests that touch it.
var listen = net.Listen

const (
	// adminShutdownTimeout bounds how long shutdown waits for in-flight admin
	// requests before closing them.
	adminShutdownTimeout = 5 * time.Second
	// adminReadHeaderTimeout and adminMaxHeaderBytes bound what a client can
	// hold open or send before a handler runs.
	adminReadHeaderTimeout = 5 * time.Second
	adminMaxHeaderBytes    = 8 << 10
	// metricsScrapeTimeout and metricsMaxInFlight bound /metrics: a gather
	// past the timeout gets a 503, and so does a scrape beyond the in-flight
	// limit. No server WriteTimeout: it would race promhttp's own timeout.
	metricsScrapeTimeout = 10 * time.Second
	metricsMaxInFlight   = 2
)

// bindAdmin binds the admin listener. addr was checked with net.SplitHostPort
// at flag time; a bind failure (port in use, permission) is a runtime error.
func bindAdmin(addr string) (net.Listener, error) { return listen("tcp", addr) }

// readyState is /readyz: not ready until Run starts, ready while it runs, not
// ready from the first shutdown signal on.
type readyState struct{ ok atomic.Bool }

func (s *readyState) set(v bool) { s.ok.Store(v) }

func (s *readyState) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	if s.ok.Load() {
		writeText(w, http.StatusOK, "ok\n")
		return
	}
	writeText(w, http.StatusServiceUnavailable, "not ready\n")
}

// writeText writes a short text/plain response. A failed write means the
// client went away; there is nothing to do about it.
func writeText(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

// newAdminMux builds a fresh mux (never http.DefaultServeMux, so nothing a
// dependency registers there is exposed). GET patterns also match HEAD; other
// methods get 405 and other paths 404.
func newAdminMux(g prometheus.Gatherer, ready *readyState) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(g, promhttp.HandlerOpts{
		MaxRequestsInFlight: metricsMaxInFlight,
		Timeout:             metricsScrapeTimeout,
	}))
	// /healthz is liveness: 200 whenever the server answers at all.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeText(w, http.StatusOK, "ok\n")
	})
	mux.Handle("GET /readyz", ready)
	return mux
}

// adminServer is the running admin HTTP server.
type adminServer struct {
	srv  *http.Server
	done chan struct{} // closed when Serve returns
	err  error         // Serve's error unless http.ErrServerClosed; read after done
}

// serveAdmin serves h on ln in a goroutine. A Serve error other than
// http.ErrServerClosed is kept and cancel is called, so the run shuts down
// instead of probing on with no metrics or probes.
func serveAdmin(ln net.Listener, h http.Handler, cancel context.CancelFunc) *adminServer {
	a := &adminServer{
		srv: &http.Server{
			Handler:           h,
			ReadHeaderTimeout: adminReadHeaderTimeout,
			MaxHeaderBytes:    adminMaxHeaderBytes,
		},
		done: make(chan struct{}),
	}
	slog.Default().Info("admin server listening", "addr", ln.Addr().String())
	go func() {
		defer close(a.done)
		if err := a.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.err = err
			cancel()
		}
	}()
	return a
}

// shutdown stops accepting, waits up to timeout for in-flight requests (then
// closes them), waits for Serve to return and returns Serve's error. Running
// out of time is logged, not returned: the admin plane is all that is cut off.
func (a *adminServer) shutdown(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := a.srv.Shutdown(ctx); err != nil {
		_ = a.srv.Close()
		slog.Default().Warn("admin server shutdown", "err", err)
	}
	<-a.done
	return a.err
}

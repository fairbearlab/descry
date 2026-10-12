package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
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
	// hold open or send before a handler runs; adminReadTimeout bounds reading
	// the whole request. Keep-alives are off (see serveAdmin), so
	// adminIdleTimeout is only a backstop.
	adminReadHeaderTimeout = 5 * time.Second
	adminReadTimeout       = 10 * time.Second
	adminIdleTimeout       = 60 * time.Second
	adminMaxHeaderBytes    = 8 << 10
	// adminWriteTimeout bounds a client that sends GET /metrics and then stops
	// reading: promhttp's timeout handler copies the buffered body out after
	// its in-flight slot and its own timeout are released, so neither bounds
	// that write. It sits well above metricsScrapeTimeout so a slow gather
	// still gets its 503 rather than a cut connection.
	adminWriteTimeout = 30 * time.Second
	// adminMaxConns caps open admin connections. One more waits in the
	// kernel's accept backlog, holding no descriptor in this process, until
	// one closes.
	adminMaxConns = 64
	// metricsScrapeTimeout and metricsMaxInFlight bound /metrics: a gather
	// past the timeout gets a 503, and so does a scrape beyond the in-flight
	// limit (enforced by limitInFlight around the whole handler, and by
	// promhttp around the gather).
	metricsScrapeTimeout = 10 * time.Second
	metricsMaxInFlight   = 2
)

// bindAdmin binds the admin listener. addr was checked at flag time (host:port
// with a numeric port); a bind failure (port in use, permission) is a runtime error.
func bindAdmin(addr string) (net.Listener, error) { return listen("tcp", addr) }

// limitListener lets at most cap(sem) accepted connections be open at once.
// Accept takes a slot before calling the inner Accept, so a connection over
// the limit is not accepted at all until a slot frees up. A connection gives
// its slot back on its first Close, and an Accept error gives it back at once.
// Closing the listener wakes an Accept waiting for a slot, so Shutdown is not
// held up by a full server.
type limitListener struct {
	net.Listener
	sem       chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

func newLimitListener(ln net.Listener, n int) *limitListener {
	return &limitListener{Listener: ln, sem: make(chan struct{}, n), done: make(chan struct{})}
}

func (l *limitListener) Accept() (net.Conn, error) {
	select {
	case l.sem <- struct{}{}:
	case <-l.done:
		return nil, net.ErrClosed
	}
	c, err := l.Listener.Accept()
	if err != nil {
		<-l.sem
		return nil, err
	}
	return &limitConn{Conn: c, release: func() { <-l.sem }}, nil
}

func (l *limitListener) Close() error {
	err := l.Listener.Close()
	l.closeOnce.Do(func() { close(l.done) })
	return err
}

// limitConn returns its listener slot on its first Close only.
type limitConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *limitConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

// CloseWrite forwards the TCP half-close net/http uses before closing a
// connection it answered with an error (an oversized header, say), so the
// client reads that response and a clean EOF instead of a reset. It does not
// give the slot back; only Close does.
func (c *limitConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}

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
	// promhttp's Timeout buffers the body and writes it after its own
	// in-flight slot is freed, so this outer slot is what bounds held bodies.
	mux.Handle("GET /metrics", limitInFlight(promhttp.HandlerFor(g, promhttp.HandlerOpts{
		MaxRequestsInFlight: metricsMaxInFlight,
		Timeout:             metricsScrapeTimeout,
	}), metricsMaxInFlight))
	// /healthz is liveness: 200 whenever the server answers at all.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeText(w, http.StatusOK, "ok\n")
	})
	mux.Handle("GET /readyz", ready)
	return mux
}

// limitInFlight lets at most n requests run h at once, holding each slot until
// h returns (for promhttp, after the body is written); the rest get 503 with
// promhttp's own message.
func limitInFlight(h http.Handler, n int) http.Handler {
	sem := make(chan struct{}, n)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
		default:
			http.Error(w, fmt.Sprintf("Limit of concurrent requests reached (%d), try again later.", n),
				http.StatusServiceUnavailable)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// adminServer is the running admin HTTP server.
type adminServer struct {
	srv  *http.Server
	done chan struct{} // closed when Serve returns
	err  error         // Serve's error unless http.ErrServerClosed; read after done
}

// serveAdmin serves h on ln, capped at adminMaxConns open connections, in a
// goroutine. A Serve error other than
// http.ErrServerClosed is kept and passed to fail at once; run's fail reports
// it and cancels the run, so the run shuts down instead of probing on with no
// metrics or probes.
func serveAdmin(ln net.Listener, h http.Handler, fail func(error)) *adminServer {
	a := &adminServer{
		srv: &http.Server{
			Handler:           h,
			ReadHeaderTimeout: adminReadHeaderTimeout,
			ReadTimeout:       adminReadTimeout,
			IdleTimeout:       adminIdleTimeout,
			WriteTimeout:      adminWriteTimeout,
			MaxHeaderBytes:    adminMaxHeaderBytes,
		},
		done: make(chan struct{}),
	}
	// With the connection cap, an idle keep-alive would hold a slot; one
	// request per connection bounds slot time by the read and write timeouts.
	a.srv.SetKeepAlivesEnabled(false)
	slog.Default().Info("admin server listening", "addr", ln.Addr().String())
	go func() {
		defer close(a.done)
		if err := a.srv.Serve(newLimitListener(ln, adminMaxConns)); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.err = err
			fail(err)
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

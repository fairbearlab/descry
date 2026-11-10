package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	nethttp "net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/fairbearlab/descry/check"
	"github.com/fairbearlab/descry/metrics"
)

// TestVerdictMatrix is the fault table for the HTTP check: for each way a
// target or this host can fail, the verdict (status), class and kind of
// evidence a check produces, in the default fresh-connection mode and, where
// it behaves differently, in connection-reuse mode with its own expectation.
// A transport change needs its fault case here; see the standing rule in
// CONTRIBUTING.md#transport-and-check-changes.
//
// Every case runs the real newSafeTransport(reuse, timeout). Only its dialer is
// replaced, because the SSRF dialer refuses loopback (and some cases stub
// name resolution or inject dial errors), and only its trust roots are added.
//
// Evidence kinds:
//   - target: a completed Observation, recorded by the runner and metrics as a
//     verdict on the target.
//   - not run: Run returned an error wrapping check.ErrNotRun; there is no
//     verdict and metrics count it under descry_checks_not_run_total.
//   - dropped: the run's ctx was cancelled; whatever Run returns, the runner
//     drops it and metrics record nothing.
//
// The reuse-mode silent-death case on HTTP/2 waits out the real ping timeouts;
// that is TestDeadHTTP2ConnectionIsReplaced and TestHealthyIdleHTTP2ConnectionIsKept,
// which already run in reuse mode, so it is not repeated here.
//
// The test itself is not parallel, so its serial leak case runs while every
// other test in the package is paused; its other cases run in parallel with
// each other.
func TestVerdictMatrix(t *testing.T) {
	if os.Getenv(rlimitChildEnv) != "" {
		t.Run("local_resource_exhaustion", func(t *testing.T) {
			t.Run("RLIMIT_NOFILE_lookup", matrixRlimitLookup)
		})
		return
	}
	// Serial first: measures goroutines, so nothing else may run alongside.
	t.Run("stalled_handshake_releases_resources", matrixStalledHandshake)

	t.Run("silent_connection_death", matrixSilentDeath)
	t.Run("ping_responsive_stall", matrixPingResponsiveStall)
	t.Run("stream_limit", matrixStreamLimit)
	t.Run("dns_starts_failing", matrixDNSStartsFailing)
	t.Run("certificate_replaced_or_expired", matrixCertificateChange)
	t.Run("cut_midway", matrixCutMidway)
	t.Run("handshake_longer_than_10s", matrixLongHandshake)
	t.Run("cancelled_at_shutdown", matrixCancelledAtShutdown)
	t.Run("local_resource_exhaustion", matrixLocalExhaustion)
	t.Run("fresh_connection_per_check_and_hop", matrixFreshConnections)
	t.Run("handshake_finishes_after_deadline", matrixLateHandshake)
	t.Run("conn_reused_key", matrixConnReusedKey)
	t.Run("malformed_response_text", matrixMalformedResponse)
}

// matrixMalformedResponse: a target that answers with a malformed status line
// or header line is down/unknown, whatever words it puts in them. net/http
// quotes those bytes in its error, so without this a target could pick its own
// class (here tls_error, timeout or dns_failure).
func matrixMalformedResponse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, reply string }{
		{"status line", "XTTP/1.1 tls certificate timeout\r\n\r\n"},
		{"status code", "HTTP/1.1 tls 200 OK\r\n\r\n"},
		{"header line", "HTTP/1.1 200 OK\r\nno such host timeout tls\r\n\r\n"},
	} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
				_, _ = conn.Read(make([]byte, 4096))
				_, _ = io.WriteString(conn, tc.reply)
				_ = conn.Close()
			}
		}()
		target := "http://" + ln.Addr().String() + "/"
		for _, reuse := range []bool{false, true} {
			t.Run(tc.name+"/"+modeName(reuse), func(t *testing.T) {
				t.Parallel()
				c, _ := newMatrixCheck(t, reuse, 2*time.Second, nil, nil)
				obs, err := runCheck(c, target)
				expect(t, tc.name, obs, err, down(reusedOr(reuse, keyFalse), check.ErrUnknown))
			})
		}
	}
}

// --- expectations ---------------------------------------------------------

type evidence string

const (
	evTarget  evidence = "target"
	evNotRun  evidence = "not run"
	evDropped evidence = "dropped"
)

// reusedKey is the expectation for Extra["conn_reused"].
type reusedKey int

const (
	keyAbsent reusedKey = iota // the key is not set
	keyFalse                   // the last hop opened a new connection
	keyTrue                    // the last hop ran on a pooled connection
	keyAny                     // not asserted
)

type outcome struct {
	status  check.Status
	classes []check.ErrorClass // the accepted classes; nil means ErrNone
	ev      evidence
	reused  reusedKey
}

func up(reused reusedKey) outcome {
	return outcome{status: check.StatusUp, ev: evTarget, reused: reused}
}

func down(reused reusedKey, classes ...check.ErrorClass) outcome {
	return outcome{status: check.StatusDown, classes: classes, ev: evTarget, reused: reused}
}

// reusedOr returns keyAbsent in fresh mode, where the key is never set, and k in
// reuse mode.
func reusedOr(reuse bool, k reusedKey) reusedKey {
	if !reuse {
		return keyAbsent
	}
	return k
}

func modeName(reuse bool) string {
	if reuse {
		return "reuse"
	}
	return "fresh"
}

// expect asserts one Run result against want. label names the step.
func expect(t *testing.T, label string, obs check.Observation, err error, want outcome) {
	t.Helper()
	if want.ev == evNotRun {
		if !errors.Is(err, check.ErrNotRun) {
			t.Errorf("%s: err = %v, want it to wrap check.ErrNotRun (evidence: not run)", label, err)
		}
		if obs.Status != "" {
			t.Errorf("%s: status %q with a not-run error, want no verdict", label, obs.Status)
		}
		return
	}
	if err != nil {
		t.Errorf("%s: Run returned error %v, want an Observation (evidence: %s)", label, err, want.ev)
		return
	}
	classes := want.classes
	if classes == nil {
		classes = []check.ErrorClass{check.ErrNone}
	}
	if obs.Status != want.status || !slices.Contains(classes, obs.ErrorClass) {
		t.Errorf("%s: got %s/%q, want %s/%v (evidence: %s)", label, obs.Status, obs.ErrorClass, want.status, classes, want.ev)
	}
	got, present := obs.Extra["conn_reused"]
	switch want.reused {
	case keyAbsent:
		if present {
			t.Errorf("%s: conn_reused = %v, want the key absent", label, got)
		}
	case keyFalse, keyTrue:
		if w := want.reused == keyTrue; !present || got != w {
			t.Errorf("%s: conn_reused = %v (present %v), want %v", label, got, present, w)
		}
	}
}

// --- fixtures -------------------------------------------------------------

// stubResolver stands in for DNS: it maps host names to loopback addresses and
// can be switched to fail every lookup with the *net.DNSError shape a real
// net.Dialer returns. IP-literal addresses are dialed as they are.
type stubResolver struct {
	timeout time.Duration
	mu      sync.Mutex
	hosts   map[string]string // host -> "127.0.0.1:port"
	failing bool
	dials   atomic.Int64
}

func newStubResolver(timeout time.Duration) *stubResolver {
	return &stubResolver{timeout: timeout, hosts: map[string]string{}}
}

func (r *stubResolver) set(host, addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hosts[host] = addr
}

func (r *stubResolver) fail() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failing = true
}

func (r *stubResolver) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	r.dials.Add(1)
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if net.ParseIP(host) == nil {
		r.mu.Lock()
		target, ok := r.hosts[host]
		failing := r.failing
		r.mu.Unlock()
		if failing || !ok {
			return nil, &net.OpError{Op: "dial", Net: network, Err: &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}}
		}
		addr = target
	}
	d := newSafeDialer(r.timeout)
	d.ControlContext = nil // loopback targets
	return d.DialContext(ctx, network, addr)
}

// newMatrixCheck returns a test Check (Layer 1 skipped) over the real
// newSafeTransport(reuse, timeout), with dial as its DialContext and roots
// added to its TLS config. Unlike newLoopbackSafeTransport, it keeps every
// other field of the production TLS config, so a session cache added there
// would show up as DidResume in matrixFreshConnections.
func newMatrixCheck(t *testing.T, reuse bool, timeout time.Duration, roots *x509.CertPool,
	dial func(context.Context, string, string) (net.Conn, error),
) (*Check, *nethttp.Transport) {
	t.Helper()
	tr := newSafeTransport(reuse, timeout)
	if dial == nil {
		d := newSafeDialer(timeout)
		d.ControlContext = nil
		dial = d.DialContext
	}
	tr.DialContext = dial
	if roots != nil {
		if tr.TLSClientConfig == nil {
			tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		} else {
			tr.TLSClientConfig = tr.TLSClientConfig.Clone()
		}
		tr.TLSClientConfig.RootCAs = roots
	}
	var opts []Option
	if reuse {
		opts = append(opts, WithConnectionReuse())
	}
	t.Cleanup(tr.CloseIdleConnections)
	return newCheckWithTransport(timeout, tr, opts...), tr
}

func runCheck(c *Check, url string) (check.Observation, error) {
	return c.Run(context.Background(), check.Target{URL: url})
}

// connCounter counts the TCP connections a server accepts, and tags each
// request's context with its connection's number.
type connCounter struct{ n atomic.Int64 }

type connIDKey struct{}

func (cc *connCounter) install(srv *httptest.Server) {
	srv.Config.ConnContext = func(ctx context.Context, _ net.Conn) context.Context {
		return context.WithValue(ctx, connIDKey{}, cc.n.Add(1))
	}
}

func connID(r *nethttp.Request) int64 {
	id, _ := r.Context().Value(connIDKey{}).(int64)
	return id
}

// hang blocks until the request is cancelled (the client reset the stream or
// closed the connection) or the test ends.
func hang(r *nethttp.Request, release <-chan struct{}) {
	select {
	case <-r.Context().Done():
	case <-release:
	}
}

// discardLog silences a test server's error log (deliberate handler aborts).
func discardLog() *log.Logger { return log.New(io.Discard, "", 0) }

func releaseOnCleanup(t *testing.T) <-chan struct{} {
	ch := make(chan struct{})
	t.Cleanup(func() { close(ch) })
	return ch
}

// --- cases ----------------------------------------------------------------

// matrixSilentDeath: the connection dies silently (no FIN, no RST) between
// checks. fresh: zero false failures, the next check dials anew. reuse: a
// bounded number of down/timeout checks, then recovery on a new connection;
// HTTP/1.1 drops the timed-out connection after one failure (shown here), and
// HTTP/2's ping-bounded recovery is TestDeadHTTP2ConnectionIsReplaced.
func matrixSilentDeath(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		h2    bool
		proto int64
	}{{"HTTP2", true, 2}, {"HTTP1", false, 1}} {
		t.Run(tc.name+"/fresh", func(t *testing.T) {
			t.Parallel()
			f := newDeadConnFixture(t, tc.h2, false)
			f.warmUp(t, tc.proto)
			f.proxy.blackhole()
			f.check.timeout = deadConnCheckTimeout
			obs, err := f.check.Run(context.Background(), f.target)
			expect(t, "check after the silent death", obs, err, up(keyAbsent))
			if got := f.proxy.accepted.Load(); got != 2 {
				t.Errorf("proxy accepted %d connections, want 2 (one per check)", got)
			}
		})
	}
	t.Run("HTTP1/reuse", func(t *testing.T) {
		t.Parallel()
		f := newDeadConnFixture(t, false, true)
		f.warmUp(t, 1)
		f.proxy.blackhole()
		f.check.timeout = deadConnCheckTimeout
		obs, err := f.check.Run(context.Background(), f.target)
		expect(t, "check on the dead pooled connection", obs, err, down(keyTrue, check.ErrTimeout))
		obs, err = f.check.Run(context.Background(), f.target)
		expect(t, "next check", obs, err, up(keyFalse))
	})
}

// matrixPingResponsiveStall: the server keeps answering HTTP/2 PINGs on a
// connection but never answers requests on it (a wedged backend behind a
// healthy front end); new connections are served normally. fresh: the stalled
// check is down/timeout and the next check, on a new connection, is up (no
// carry-over). reuse: the ping health check never fires, so down/timeout
// repeats on the pooled connection; this is the documented cost of opting in.
func matrixPingResponsiveStall(t *testing.T) {
	t.Parallel()
	for _, reuse := range []bool{false, true} {
		t.Run(modeName(reuse), func(t *testing.T) {
			t.Parallel()
			release := releaseOnCleanup(t)
			var conns connCounter
			srv := httptest.NewUnstartedServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
				if connID(r) == 1 { // the first connection is the wedged one
					hang(r, release)
					return
				}
				w.WriteHeader(nethttp.StatusOK)
			}))
			conns.install(srv)
			srv.EnableHTTP2 = true
			srv.StartTLS()
			t.Cleanup(srv.Close)

			c, _ := newMatrixCheck(t, reuse, 500*time.Millisecond, serverRoots(srv), nil)
			obs, err := runCheck(c, srv.URL)
			expect(t, "check 1 on the wedged connection", obs, err, down(reusedOr(reuse, keyFalse), check.ErrTimeout))
			if !reuse {
				obs, err = runCheck(c, srv.URL)
				expect(t, "check 2", obs, err, up(keyAbsent))
				if got := conns.n.Load(); got != 2 {
					t.Errorf("server accepted %d connections, want 2", got)
				}
				return
			}
			for i := 2; i <= 3; i++ {
				obs, err = runCheck(c, srv.URL)
				expect(t, fmt.Sprintf("check %d on the same pooled connection", i), obs, err, down(keyTrue, check.ErrTimeout))
			}
			if got := conns.n.Load(); got != 1 {
				t.Errorf("server accepted %d connections, want 1 (the stalled one kept)", got)
			}
		})
	}
}

// matrixStreamLimit: the server allows one concurrent stream per connection
// and a hung request holds it on the connection the check would use. fresh:
// the check opens its own connection with its own stream budget, so it is up.
// reuse: Go's HTTP/2 client does not queue behind the limit (it does not set
// StrictMaxConcurrentRequests); it opens another connection, so the check is
// also up, with conn_reused false. The plan expected repeated down/timeout
// here; see the PR notes.
func matrixStreamLimit(t *testing.T) {
	t.Parallel()
	for _, reuse := range []bool{false, true} {
		t.Run(modeName(reuse), func(t *testing.T) {
			t.Parallel()
			release := releaseOnCleanup(t)
			held := make(chan struct{}, 1)
			var conns connCounter
			srv := httptest.NewUnstartedServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
				if r.URL.Path == "/hang" {
					held <- struct{}{}
					hang(r, release)
					return
				}
				w.WriteHeader(nethttp.StatusOK)
			}))
			conns.install(srv)
			srv.EnableHTTP2 = true
			srv.Config.HTTP2 = &nethttp.HTTP2Config{MaxConcurrentStreams: 1}
			srv.StartTLS()
			t.Cleanup(srv.Close)

			c, tr := newMatrixCheck(t, reuse, 2*time.Second, serverRoots(srv), nil)
			obs, err := runCheck(c, srv.URL)
			expect(t, "warm-up", obs, err, up(reusedOr(reuse, keyFalse)))

			// Hold the only stream with a request on the same transport, which
			// in reuse mode lands on the pooled connection.
			holdCtx, stopHold := context.WithCancel(context.Background())
			t.Cleanup(stopHold)
			go func() {
				req, _ := nethttp.NewRequestWithContext(holdCtx, nethttp.MethodGet, srv.URL+"/hang", nil)
				if resp, err := tr.RoundTrip(req); err == nil {
					_ = resp.Body.Close()
				}
			}()
			select {
			case <-held:
			case <-time.After(5 * time.Second):
				t.Fatal("the holding request never reached the server")
			}
			before := conns.n.Load()

			for i := 1; i <= 2; i++ {
				obs, err = runCheck(c, srv.URL)
				// In reuse mode the first check opens a second connection and the
				// next one reuses it.
				want := up(reusedOr(reuse, map[bool]reusedKey{true: keyFalse, false: keyTrue}[i == 1]))
				expect(t, "check while the stream is held", obs, err, want)
			}
			if got := conns.n.Load() - before; reuse && got != 1 {
				t.Errorf("reuse: %d new connections while the stream was held, want 1", got)
			} else if !reuse && got != 2 {
				t.Errorf("fresh: %d new connections for 2 checks, want 2", got)
			}
		})
	}
}

// matrixDNSStartsFailing: the name stops resolving after a check passed.
// fresh: the next check resolves again and is down/dns_failure. reuse: not
// seen while the pooled connection lives (up, conn_reused true); the first
// check that needs a new connection is down/dns_failure.
func matrixDNSStartsFailing(t *testing.T) {
	t.Parallel()
	for _, reuse := range []bool{false, true} {
		t.Run(modeName(reuse), func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewUnstartedServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
				w.WriteHeader(nethttp.StatusOK)
			}))
			srv.EnableHTTP2 = true
			srv.StartTLS()
			t.Cleanup(srv.Close)
			_, port, _ := net.SplitHostPort(srv.Listener.Addr().String())
			res := newStubResolver(2 * time.Second)
			res.set("example.com", srv.Listener.Addr().String()) // httptest's certificate covers example.com
			url := "https://example.com:" + port + "/"

			c, tr := newMatrixCheck(t, reuse, 2*time.Second, serverRoots(srv), res.DialContext)
			obs, err := runCheck(c, url)
			expect(t, "check before the DNS failure", obs, err, up(reusedOr(reuse, keyFalse)))
			res.fail()
			obs, err = runCheck(c, url)
			if !reuse {
				expect(t, "check after the DNS failure", obs, err, down(keyAbsent, check.ErrDNSFailure))
				return
			}
			expect(t, "check on the pooled connection", obs, err, up(keyTrue))
			tr.CloseIdleConnections()
			obs, err = runCheck(c, url)
			expect(t, "check that needs a new connection", obs, err, down(keyAbsent, check.ErrDNSFailure))
		})
	}
}

// matrixCertificateChange: the server's certificate is replaced, then
// replaced by an expired one. fresh: every check handshakes, so TLSExpiry
// follows the new certificate and the expired one is down/tls_error. reuse:
// neither change is seen while the pooled connection lives; the first new
// connection sees the expired certificate.
func matrixCertificateChange(t *testing.T) {
	t.Parallel()
	for _, reuse := range []bool{false, true} {
		t.Run(modeName(reuse), func(t *testing.T) {
			t.Parallel()
			s := newCertSwapServer(t, true, nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
				w.WriteHeader(nethttp.StatusOK)
			}))
			first := s.cert.Load().Leaf.NotAfter
			res := newStubResolver(2 * time.Second)
			res.set(certSwapHost, s.srv.Listener.Addr().String())
			_, port, _ := net.SplitHostPort(s.srv.Listener.Addr().String())
			url := "https://" + certSwapHost + ":" + port + "/"
			c, tr := newMatrixCheck(t, reuse, 2*time.Second, s.roots, res.DialContext)

			expiry := func(label string, obs check.Observation, want time.Time) {
				t.Helper()
				if obs.TLSExpiry == nil || !obs.TLSExpiry.Equal(want) {
					t.Errorf("%s: TLSExpiry = %v, want %v", label, obs.TLSExpiry, want)
				}
			}
			obs, err := runCheck(c, url)
			expect(t, "check 1", obs, err, up(reusedOr(reuse, keyFalse)))
			expiry("check 1", obs, first)

			renewed := s.issue(t, time.Now().Add(96*time.Hour))
			s.swap(renewed)
			obs, err = runCheck(c, url)
			if !reuse {
				expect(t, "check after the replacement", obs, err, up(keyAbsent))
				expiry("check after the replacement", obs, renewed.Leaf.NotAfter)
			} else {
				expect(t, "check after the replacement", obs, err, up(keyTrue))
				expiry("check after the replacement (pooled, not seen)", obs, first)
			}

			s.swap(s.issue(t, time.Now().Add(-time.Hour)))
			obs, err = runCheck(c, url)
			if !reuse {
				expect(t, "check after expiry", obs, err, down(keyAbsent, check.ErrTLSError))
				return
			}
			expect(t, "check after expiry (pooled, not seen)", obs, err, up(keyTrue))
			tr.CloseIdleConnections()
			obs, err = runCheck(c, url)
			expect(t, "check on a new connection", obs, err, down(keyAbsent, check.ErrTLSError))
		})
	}
}

// matrixCutMidway: the connection is cut at each stage of one check. With no
// connection pooled beforehand, fresh and reuse agree except for conn_reused.
//   - refused before TLS: down/connection_refused.
//   - cut during the TLS handshake: down/tls_error or down/unknown.
//   - cut before response headers: down/unknown (no EOF mapping, by decision).
//   - cut after response headers: up; the check's verdict rests on headers only.
func matrixCutMidway(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refusedURL := "https://" + ln.Addr().String() + "/"
	_ = ln.Close()

	// Accepts, reads the ClientHello, and closes.
	tlsCut, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tlsCut.Close() })
	go func() {
		for {
			conn, err := tlsCut.Accept()
			if err != nil {
				return
			}
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			_, _ = conn.Read(make([]byte, 64*1024))
			_ = conn.Close()
		}
	}()
	tlsCutURL := "https://" + tlsCut.Addr().String() + "/"

	for _, proto := range []struct {
		name string
		h2   bool
	}{{"HTTP2", true}, {"HTTP1", false}} {
		srv := httptest.NewUnstartedServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
			if r.URL.Path == "/after-headers" {
				w.Header().Set("Content-Length", "1000")
				w.WriteHeader(nethttp.StatusOK)
				_, _ = w.Write([]byte("partial"))
				w.(nethttp.Flusher).Flush()
			}
			panic(nethttp.ErrAbortHandler) // HTTP/1.1 closes the connection; HTTP/2 resets the stream
		}))
		srv.EnableHTTP2 = proto.h2
		srv.Config.ErrorLog = discardLog()
		srv.StartTLS()
		t.Cleanup(srv.Close)
		roots := serverRoots(srv)

		for _, reuse := range []bool{false, true} {
			t.Run(proto.name+"/"+modeName(reuse), func(t *testing.T) {
				t.Parallel()
				for _, step := range []struct {
					name string
					url  string
					want outcome
				}{
					{"refused before TLS", refusedURL, down(keyAbsent, check.ErrConnectionRefused)},
					{"cut during TLS", tlsCutURL, down(keyAbsent, check.ErrTLSError, check.ErrUnknown)},
					{"cut before headers", srv.URL + "/before-headers", down(reusedOr(reuse, keyFalse), check.ErrUnknown)},
					{"cut after headers", srv.URL + "/after-headers", up(reusedOr(reuse, keyFalse))},
				} {
					// A new Check per step, so no step runs on a connection another
					// step pooled.
					c, _ := newMatrixCheck(t, reuse, 2*time.Second, roots, nil)
					obs, err := runCheck(c, step.url)
					expect(t, step.name, obs, err, step.want)
				}
			})
		}
	}
}

// matrixStalledHandshake (E1): a handshake that never completes is down/timeout,
// and the dial and handshake it started end with the check: the client closes
// its socket and the transport's goroutines return to baseline within the
// check timeout plus a margin well under net/http's own 10s default handshake
// timeout. Without the TLSHandshakeTimeout cap they would outlive the check.
// Serial: it counts goroutines.
func matrixStalledHandshake(t *testing.T) {
	const (
		timeout = 500 * time.Millisecond
		margin  = 2500 * time.Millisecond
		checks  = 5
	)
	for _, reuse := range []bool{false, true} {
		t.Run(modeName(reuse), func(t *testing.T) {
			srv := httptest.NewUnstartedServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
				w.WriteHeader(nethttp.StatusOK)
			}))
			srv.EnableHTTP2 = true
			srv.StartTLS()
			proxy := newBlackholeProxy(t, srv.Listener.Addr().String())
			t.Cleanup(func() { proxy.close(); srv.Close() })
			proxy.setHandshakeDelay(time.Hour)
			url := "https://" + proxy.ln.Addr().String() + "/"

			c, _ := newMatrixCheck(t, reuse, timeout, serverRoots(srv), nil)
			baseline := clientGoroutines()
			start := time.Now()
			var wg sync.WaitGroup
			results := make([]check.Observation, checks)
			errs := make([]error, checks)
			for i := range checks {
				wg.Go(func() { results[i], errs[i] = runCheck(c, url) })
			}
			wg.Wait()
			for i := range checks {
				expect(t, "stalled check", results[i], errs[i], down(reusedOr(reuse, keyAbsent), check.ErrTimeout))
			}

			deadline := start.Add(timeout + margin)
			for {
				gaveUp, g := proxy.clientGaveUp.Load(), clientGoroutines()
				if gaveUp == checks && g <= baseline {
					t.Logf("released after %v: %d sockets closed, client goroutines %d (baseline %d)", time.Since(start).Round(time.Millisecond), gaveUp, g, baseline)
					return
				}
				if time.Now().After(deadline) {
					t.Fatalf("after %v: client closed %d of %d stalled sockets, client transport goroutines %d, baseline %d; the dial or handshake outlives the check (TLSHandshakeTimeout or Dialer.Timeout not capped at the check timeout?)",
						time.Since(start).Round(time.Millisecond), gaveUp, checks, g, baseline)
				}
				time.Sleep(20 * time.Millisecond)
			}
		})
	}
}

// clientGoroutines counts goroutines running net/http client transport or TLS
// client handshake code; server and proxy goroutines are not counted.
func clientGoroutines() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	count := 0
	for _, g := range bytes.Split(buf, []byte("\n\n")) {
		for _, marker := range []string{
			"net/http.(*Transport).",
			"net/http.(*persistConn).",
			"net/http.(*http2ClientConn).",
			"net/http.(*http2clientConnReadLoop).",
			"crypto/tls.(*Conn).clientHandshake",
		} {
			if bytes.Contains(g, []byte(marker)) {
				count++
				break
			}
		}
	}
	return count
}

// matrixLongHandshake (E1): a TLS handshake that takes longer than net/http's
// default 10s handshake timeout succeeds when the check timeout is longer; the
// handshake cap is the check timeout, not the stdlib default.
func matrixLongHandshake(t *testing.T) {
	t.Parallel()
	const (
		delay   = 10*time.Second + 500*time.Millisecond
		timeout = 15 * time.Second
	)
	for _, reuse := range []bool{false, true} {
		t.Run(modeName(reuse), func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewUnstartedServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
				w.WriteHeader(nethttp.StatusOK)
			}))
			srv.EnableHTTP2 = true
			srv.StartTLS()
			proxy := newBlackholeProxy(t, srv.Listener.Addr().String())
			t.Cleanup(func() { proxy.close(); srv.Close() })
			proxy.setHandshakeDelay(delay)

			c, _ := newMatrixCheck(t, reuse, timeout, serverRoots(srv), nil)
			obs, err := runCheck(c, "https://"+proxy.ln.Addr().String()+"/")
			expect(t, "slow handshake", obs, err, up(reusedOr(reuse, keyFalse)))
			if obs.LatencyMs < int(delay/time.Millisecond) {
				t.Errorf("latency %dms, want at least the %v handshake delay; the delay did not apply", obs.LatencyMs, delay)
			}
		})
	}
}

// matrixCancelledAtShutdown: the run's ctx is cancelled mid-handshake, as the
// runner's in-flight runs are at shutdown. Run returns promptly with
// down/unknown and a nil error: the check does not tell cancellation apart
// from a failure. That Observation is not evidence, because the caller drops
// it: metrics record nothing (asserted here through the Collector decorator)
// and the runner drops any result once the run ctx is done
// (runner.TestShutdown_InFlightRunDropped).
func matrixCancelledAtShutdown(t *testing.T) {
	t.Parallel()
	for _, reuse := range []bool{false, true} {
		t.Run(modeName(reuse), func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewUnstartedServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
				w.WriteHeader(nethttp.StatusOK)
			}))
			srv.EnableHTTP2 = true
			srv.StartTLS()
			proxy := newBlackholeProxy(t, srv.Listener.Addr().String())
			t.Cleanup(func() { proxy.close(); srv.Close() })
			proxy.setHandshakeDelay(time.Hour)

			col, err := metrics.New(metrics.Options{})
			if err != nil {
				t.Fatal(err)
			}
			c, _ := newMatrixCheck(t, reuse, 5*time.Second, serverRoots(srv), nil)
			instrumented := col.Check(c)

			ctx, cancel := context.WithCancel(context.Background())
			time.AfterFunc(200*time.Millisecond, cancel)
			start := time.Now()
			obs, err := instrumented.Run(ctx, check.Target{URL: "https://" + proxy.ln.Addr().String() + "/"})
			if elapsed := time.Since(start); elapsed > 2*time.Second {
				t.Errorf("Run returned %v after start, want promptly after the cancel at 200ms", elapsed)
			}
			// What the check itself returns; evidence kind is dropped, below.
			expect(t, "cancelled run (check level)", obs, err, down(reusedOr(reuse, keyAbsent), check.ErrUnknown))

			families, err := col.Registry().Gather()
			if err != nil {
				t.Fatal(err)
			}
			for _, mf := range families {
				switch mf.GetName() {
				case "descry_check_runs_total", "descry_check_up", "descry_check_last_run_timestamp_seconds":
					if len(mf.GetMetric()) > 0 {
						t.Errorf("%s has %d series after a cancelled run, want none (evidence: %s)", mf.GetName(), len(mf.GetMetric()), evDropped)
					}
				case "descry_check_errors_total", "descry_checks_not_run_total":
					var v float64
					for _, m := range mf.GetMetric() {
						v += m.GetCounter().GetValue()
					}
					if v != 0 {
						t.Errorf("%s = %v after a cancelled run, want 0 (evidence: %s)", mf.GetName(), v, evDropped)
					}
				}
			}
		})
	}
}

// matrixLocalExhaustion (E2): this host, not the target, is out of a resource.
// Every mode: not run (err wraps check.ErrNotRun and the errno, no verdict),
// and the error text carries no part of the target URL.
func matrixLocalExhaustion(t *testing.T) {
	t.Parallel()
	for _, reuse := range []bool{false, true} {
		for _, tc := range localErrnoReasons {
			t.Run(modeName(reuse)+"/dial_"+tc.reason, func(t *testing.T) {
				t.Parallel()
				c, _ := newMatrixCheck(t, reuse, 2*time.Second, nil, func(context.Context, string, string) (net.Conn, error) {
					return nil, &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "socket", Err: tc.errno}}
				})
				obs, err := runCheck(c, secretURL)
				expect(t, "dial "+tc.reason, obs, err, outcome{ev: evNotRun})
				assertNotRun(t, err, tc.errno, tc.reason, "check not run: dial: "+tc.errno.Error())
			})
		}
		t.Run(modeName(reuse)+"/lookup_DNSError_text", func(t *testing.T) {
			t.Parallel()
			// A real *net.DNSError: the pure Go resolver's own socket fails, and
			// net flattens the errno into DNSError.Err text.
			d := newSafeDialer(2 * time.Second)
			d.Resolver = &net.Resolver{
				PreferGo: true,
				Dial: func(context.Context, string, string) (net.Conn, error) {
					return nil, &os.SyscallError{Syscall: "socket", Err: syscall.EMFILE}
				},
			}
			c, _ := newMatrixCheck(t, reuse, 2*time.Second, nil, d.DialContext)
			obs, err := runCheck(c, secretURL)
			expect(t, "lookup EMFILE", obs, err, outcome{ev: evNotRun})
			assertNotRun(t, err, syscall.EMFILE, metrics.NotRunEMFILE, "check not run: lookup: "+syscall.EMFILE.Error())
		})
	}
	t.Run("RLIMIT_NOFILE_lookup", func(t *testing.T) {
		t.Parallel()
		if !rlimitSupported {
			t.Skip("RLIMIT_NOFILE subprocess case needs a unix setrlimit")
		}
		// Re-exec this test binary with a child-mode env var: the child lowers
		// its own RLIMIT_NOFILE, so lowering it cannot starve this process.
		cmd := exec.Command(os.Args[0], "-test.run=^TestVerdictMatrix$/^local_resource_exhaustion$/^RLIMIT_NOFILE_lookup$", "-test.v", "-test.count=1") // #nosec G204 G702 -- re-executes this test binary with fixed arguments
		cmd.Env = append(os.Environ(), rlimitChildEnv+"=1", "GODEBUG=netdns=go")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("child failed: %v\n%s", err, out)
		}
		if !strings.Contains(string(out), rlimitChildMarker) {
			t.Fatalf("child did not run the lookup case:\n%s", out)
		}
	})
}

const (
	rlimitChildEnv    = "DESCRY_VERDICT_MATRIX_RLIMIT_CHILD"
	rlimitChildMarker = "rlimit child: lookup under RLIMIT_NOFILE is not run"
)

// matrixRlimitLookup runs in the child process only. It drives the production
// check (New, both SSRF layers, the default pure Go resolver forced by
// GODEBUG=netdns=go) with RLIMIT_NOFILE at the number of open descriptors, so
// the resolver's socket fails with EMFILE before any packet is sent.
func matrixRlimitLookup(t *testing.T) {
	// Warm up what would otherwise open descriptors lazily: the network poller,
	// and the resolver's config (it reads /etc/resolv.conf and /etc/hosts once).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = ln.Close()
	_, _ = net.DefaultResolver.LookupHost(context.Background(), "localhost")

	restore, err := lowerNoFileLimit()
	if err != nil {
		t.Skipf("cannot lower RLIMIT_NOFILE: %v", err)
	}
	obs, err := New(2*time.Second).Run(context.Background(), check.Target{URL: secretURL})
	if rerr := restore(); rerr != nil {
		t.Fatalf("restore RLIMIT_NOFILE: %v", rerr)
	}
	expect(t, "lookup under RLIMIT_NOFILE", obs, err, outcome{ev: evNotRun})
	assertNotRun(t, err, syscall.EMFILE, metrics.NotRunEMFILE, "check not run: lookup: "+syscall.EMFILE.Error())
	if !t.Failed() {
		t.Log(rlimitChildMarker)
	}
}

// matrixFreshConnections (C6): in fresh mode, every check and every redirect
// hop within it opens a new connection with a full TLS handshake (DidResume
// false), on HTTP/2 and HTTP/1.1. In reuse mode the same checks share one
// connection.
func matrixFreshConnections(t *testing.T) {
	t.Parallel()
	const runs, hops = 3, 3 // /a -> /b -> /end
	for _, proto := range []struct {
		name  string
		h2    bool
		major int
	}{{"HTTP2", true, 2}, {"HTTP1", false, 1}} {
		for _, reuse := range []bool{false, true} {
			t.Run(proto.name+"/"+modeName(reuse), func(t *testing.T) {
				t.Parallel()
				var (
					conns    connCounter
					requests atomic.Int64
					resumed  atomic.Int64
					badProto atomic.Int64
				)
				srv := httptest.NewUnstartedServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
					requests.Add(1)
					if r.TLS == nil || r.TLS.DidResume {
						resumed.Add(1)
					}
					if r.ProtoMajor != proto.major {
						badProto.Add(1)
					}
					switch r.URL.Path {
					case "/a":
						nethttp.Redirect(w, r, "/b", nethttp.StatusFound)
					case "/b":
						nethttp.Redirect(w, r, "/end", nethttp.StatusFound)
					default:
						w.WriteHeader(nethttp.StatusOK)
					}
				}))
				conns.install(srv)
				srv.EnableHTTP2 = proto.h2
				srv.StartTLS()
				t.Cleanup(srv.Close)

				c, _ := newMatrixCheck(t, reuse, 2*time.Second, serverRoots(srv), nil)
				for i := range runs {
					// Reuse: the first hop of the first check opens the connection, so
					// the last hop of every check runs on a pooled one.
					obs, err := runCheck(c, srv.URL+"/a")
					expect(t, fmt.Sprintf("redirecting check %d", i+1), obs, err, up(reusedOr(reuse, keyTrue)))
				}
				if got := requests.Load(); got != runs*hops {
					t.Fatalf("server saw %d requests, want %d", got, runs*hops)
				}
				wantConns := int64(runs * hops)
				if reuse {
					wantConns = 1
				}
				if got := conns.n.Load(); got != wantConns {
					t.Errorf("server accepted %d connections for %d checks of %d hops, want %d", got, runs, hops, wantConns)
				}
				if got := resumed.Load(); got != 0 {
					t.Errorf("%d requests ran on a resumed (or non-TLS) session, want every handshake full", got)
				}
				if got := badProto.Load(); got != 0 {
					t.Errorf("%d requests did not use HTTP/%d", got, proto.major)
				}
			})
		}
	}
}

// matrixLateHandshake: check A's deadline fires while its TLS handshake is
// still running, and the handshake then completes anyway, because net/http
// dials on a context detached from the request. On HTTP/2 that connection
// lands in the pool unused. fresh: check B must not run on it; B opens its
// own connection. reuse: B may run on it, and must
// report conn_reused=true, since B made no connection or handshake itself.
func matrixLateHandshake(t *testing.T) {
	t.Parallel()
	const (
		timeout   = time.Second
		dialDelay = 400 * time.Millisecond // handshake starts at ~400ms
		hsDelay   = 800 * time.Millisecond // and ends at ~1.2s: after the 1s deadline, before the 1.4s handshake cap
	)
	for _, proto := range []struct {
		name string
		h2   bool
	}{{"HTTP2", true}, {"HTTP1", false}} {
		for _, reuse := range []bool{false, true} {
			t.Run(proto.name+"/"+modeName(reuse), func(t *testing.T) {
				t.Parallel()
				var conns connCounter
				srv := httptest.NewUnstartedServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
					w.WriteHeader(nethttp.StatusOK)
				}))
				conns.install(srv)
				srv.EnableHTTP2 = proto.h2
				srv.StartTLS()
				proxy := newBlackholeProxy(t, srv.Listener.Addr().String())
				t.Cleanup(func() { proxy.close(); srv.Close() })
				proxy.setHandshakeDelay(hsDelay)

				var dials atomic.Int64
				d := newSafeDialer(timeout)
				d.ControlContext = nil
				dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
					if dials.Add(1) == 1 {
						time.Sleep(dialDelay)
					}
					return d.DialContext(ctx, network, addr)
				}
				c, _ := newMatrixCheck(t, reuse, timeout, serverRoots(srv), dial)
				url := "https://" + proxy.ln.Addr().String() + "/"

				obs, err := runCheck(c, url)
				expect(t, "check A, deadline mid-handshake", obs, err, down(keyAbsent, check.ErrTimeout))

				// Give A's detached handshake time to finish and its connection to
				// be pooled (reuse) or discarded. In fresh mode Run closes its own
				// transport on return, which also cancels A's dial, so that
				// connection may never reach the server at all.
				time.Sleep(hsDelay)
				proxy.setHandshakeDelay(0)
				before := conns.n.Load()
				if reuse && before != 1 {
					t.Fatalf("server accepted %d connections after check A, want its late one", before)
				}

				obs, err = runCheck(c, url)
				expect(t, "check B", obs, err, up(reusedOr(reuse, keyTrue)))
				want := before + 1 // fresh: B dials its own
				if reuse {
					want = before // reuse: B runs on A's late connection
				}
				if got := conns.n.Load(); got != want {
					t.Errorf("server accepted %d connections in all, want %d", got, want)
				}
			})
		}
	}
}

// matrixConnReusedKey: Extra["conn_reused"] per check. fresh: never set.
// reuse: false on the check that opened the connection, true on the next one,
// and absent when the check obtained no connection at all.
func matrixConnReusedKey(t *testing.T) {
	t.Parallel()
	for _, proto := range []struct {
		name string
		h2   bool
	}{{"HTTP2", true}, {"HTTP1", false}} {
		for _, reuse := range []bool{false, true} {
			t.Run(proto.name+"/"+modeName(reuse), func(t *testing.T) {
				t.Parallel()
				srv := httptest.NewUnstartedServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
					if r.URL.Path == "/fail" {
						w.WriteHeader(nethttp.StatusInternalServerError)
						return
					}
					w.WriteHeader(nethttp.StatusOK)
				}))
				srv.EnableHTTP2 = proto.h2
				srv.StartTLS()
				t.Cleanup(srv.Close)
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				refused := "https://" + ln.Addr().String() + "/"
				_ = ln.Close()

				c, _ := newMatrixCheck(t, reuse, 2*time.Second, serverRoots(srv), nil)
				for _, step := range []struct {
					name string
					url  string
					want outcome
				}{
					{"first check", srv.URL + "/", up(reusedOr(reuse, keyFalse))},
					{"second check", srv.URL + "/", up(reusedOr(reuse, keyTrue))},
					{"down check on the pooled connection", srv.URL + "/fail", down(reusedOr(reuse, keyTrue), check.ErrHTTPError)},
					{"no connection obtained", refused, down(keyAbsent, check.ErrConnectionRefused)},
				} {
					obs, err := runCheck(c, step.url)
					expect(t, step.name, obs, err, step.want)
				}
			})
		}
	}
}

// Package http implements an HTTP uptime check.Check with a two-layer SSRF
// guard (parse-time scheme/port/literal-IP checks, plus a dial-time control
// hook on the resolved socket address — see ssrf.go for the threat model).
package http

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	nethttp "net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fairbearlab/descry/check"
)

const (
	defaultTimeout = 10 * time.Second
	maxRedirects   = 5
	maxBodyBytes   = 4096
	// drainLimit caps how many leftover body bytes we discard before Close. In
	// reuse mode this lets the keep-alive connection return to the pool; in both
	// modes it reads the response to its end, so the target does not log a
	// client abort. Bodies larger than this are abandoned (the transport closes
	// the connection), which is the safe default.
	drainLimit = 1 << 16 // 64 KiB
)

// Check is an HTTP uptime check that implements check.Check.
type Check struct {
	client    *nethttp.Client
	timeout   time.Duration
	userAgent string // when "", net/http's default User-Agent is used
	skipSSRF  bool   // disable both SSRF guard layers (tests only)
	reuse     bool   // pool connections across checks; set by WithConnectionReuse
}

// Option configures a Check at construction time.
type Option func(*Check)

// WithUserAgent sets the User-Agent header sent on every request. When unset
// (empty string), Go's net/http default User-Agent applies — no impersonation.
func WithUserAgent(ua string) Option {
	return func(c *Check) { c.userAgent = ua }
}

// WithConnectionReuse makes the Check pool connections across checks instead
// of opening a fresh connection for every check. Pooled HTTP/2 connections
// receive a PING after 15s without a received frame, so one that silently died
// is replaced within 30s; see docs/OPERATIONS.md for what that means for false
// failures.
//
// Reuse weakens what up proves: a check can succeed on a connection opened by
// an earlier check, so up no longer shows that a new TCP connection and TLS
// handshake to the target work from this host. Each result then carries
// Extra["conn_reused"] (bool), true when the last attempted request hop ran
// on a pooled connection; the key is omitted when that hop obtained no
// connection at all.
func WithConnectionReuse() Option {
	return func(c *Check) { c.reuse = true }
}

// New creates a new HTTP Check with the given timeout. If timeout <= 0,
// defaultTimeout (10s) is used. Options are applied after the defaults, and
// the transport is built after the options.
//
// By default every check, and every redirect hop within it, opens a fresh
// connection (a new TCP connect and TLS handshake), so up shows that a new
// connection to the target works from this host. The TCP dial and the TLS
// handshake are each capped at timeout. WithConnectionReuse opts into
// connection pooling, which weakens that claim.
func New(timeout time.Duration, opts ...Option) *Check {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	c := &Check{timeout: timeout}
	for _, opt := range opts {
		opt(c)
	}
	c.client = &nethttp.Client{
		Transport: newSafeTransport(c.reuse, timeout),
		CheckRedirect: func(req *nethttp.Request, via []*nethttp.Request) error {
			if len(via) >= maxRedirects {
				return errors.New("too many redirects")
			}
			// Re-run Layer 1 on each redirect hop: the dial-time guard only checks
			// resolved IPs, so without this a redirect could bypass the scheme/port
			// policy (e.g. hop to a public host on :8080).
			return assertSafeURL(req.URL.String())
		},
	}
	return c
}

// newForTest creates a Check that uses a plain (non-SSRF-guarded) transport,
// for use in package-internal tests against httptest servers. Like New, it
// disables keep-alives unless WithConnectionReuse is passed.
func newForTest(timeout time.Duration, opts ...Option) *Check {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	c := &Check{timeout: timeout, skipSSRF: true}
	for _, opt := range opts {
		opt(c)
	}
	c.client = &nethttp.Client{
		Transport: &nethttp.Transport{DisableKeepAlives: !c.reuse},
		CheckRedirect: func(_ *nethttp.Request, via []*nethttp.Request) error {
			if len(via) >= maxRedirects {
				return errors.New("too many redirects")
			}
			return nil
		},
	}
	return c
}

// Name identifies this Check implementation as "http".
func (c *Check) Name() string { return "http" }

// Run performs a single GET against t.URL and maps the outcome to an
// Observation. It never returns a non-nil error for ordinary probe failures
// (timeouts, refused connections, non-2xx responses, SSRF blocks, etc.) —
// those are reported via Observation.Status/ErrorClass instead, since a
// failed probe is a successful check of a down target.
//
// When the request fails because this host ran out of a local resource
// (EMFILE, ENFILE, EADDRNOTAVAIL, ENOBUFS or ENOMEM, including during DNS
// resolution), Run returns a zero Observation and a non-nil error wrapping
// check.ErrNotRun and the errno, so the failure is not read as a verdict on
// the target. That error names only the operation and the errno, never the
// target URL.
func (c *Check) Run(ctx context.Context, t check.Target) (check.Observation, error) {
	obs := check.Observation{
		ObservedAt: time.Now().UTC(),
		Labels:     t.Labels,
		Extra:      map[string]any{},
	}

	// Layer 1 guard (skipped in test mode)
	if !c.skipSSRF {
		if err := assertSafeURL(t.URL); err != nil {
			obs.Status = check.StatusDown
			obs.ErrorClass = check.ErrSSRFBlocked
			obs.ObservedAt = time.Now().UTC()
			return obs, nil // best-effort produce: report, don't error the run
		}
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodGet, t.URL, nil)
	if err != nil {
		obs.Status = check.StatusDown
		obs.ErrorClass = check.ErrUnknown
		return obs, nil
	}
	if c.userAgent != "" {
		req.Header.Set("User-Agent", c.userAgent)
	}
	var conn *connTrace
	if c.reuse {
		conn = &connTrace{}
		req = req.WithContext(httptrace.WithClientTrace(ctx, conn.clientTrace()))
	}

	client := c.client
	if !c.reuse {
		// Fresh mode runs each check on its own clone of the transport, closed
		// when Run returns. A shared transport is not enough even with
		// DisableKeepAlives: net/http dials on a context detached from the
		// request, so an HTTP/2 connection whose handshake finished after an
		// earlier check gave up lands in the pool unused, and the next check (or
		// a concurrent one waiting on its own dial) would run on it without a
		// connect or handshake of its own.
		if base, ok := c.client.Transport.(*nethttp.Transport); ok {
			tr := base.Clone()
			defer tr.CloseIdleConnections()
			perRun := *c.client
			perRun.Transport = tr
			client = &perRun
		}
	}

	start := time.Now()
	resp, err := client.Do(req)
	obs.LatencyMs = int(time.Since(start).Milliseconds())
	obs.ObservedAt = time.Now().UTC()

	if err != nil {
		if nr := localFailure(err); nr != nil {
			return check.Observation{}, nr
		}
	}
	if conn != nil {
		conn.record(obs.Extra)
	}
	if err != nil {
		obs.Status = check.StatusDown
		obs.ErrorClass = classifyError(err)
		return obs, nil
	}
	// Drain leftover body bytes (bounded) before close, in both modes: in reuse
	// mode the keep-alive connection returns to the pool, and in both the target
	// sees the response read to its end rather than a client abort. The
	// down-path read below consumes the first maxBodyBytes; this drains whatever
	// remains within drainLimit.
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, drainLimit))
		_ = resp.Body.Close()
	}()

	obs.StatusCode = resp.StatusCode
	obs.FinalURL = resp.Request.URL.String()

	// TLS expiry from connection state
	if resp.TLS != nil && len(resp.TLS.PeerCertificates) > 0 {
		exp := resp.TLS.PeerCertificates[0].NotAfter
		obs.TLSExpiry = &exp
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		obs.Status = check.StatusUp
	} else {
		obs.Status = check.StatusDown
		obs.ErrorClass = check.ErrHTTPError
	}

	// capture body only on down, capped at maxBodyBytes. ToValidUTF8 drops any
	// partial multibyte rune left dangling at the byte boundary.
	if obs.Status == check.StatusDown {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
		obs.Extra["body"] = strings.ToValidUTF8(string(body), "")
	}
	return obs, nil
}

// connTrace records, for one Run in reuse mode, whether the last attempted
// request hop obtained a connection and whether it was a pooled one. GetConn
// starts each hop (redirects included) and resets the state; GotConn sets it.
// Atomic because the client trace hooks carry no goroutine guarantee.
//
// GotConnInfo.Reused alone is not enough on HTTP/2: a connection dialed for an
// earlier check that gave up before the handshake finished is pooled unused,
// and the next request to take it sees Reused false (and no GetConn) although
// it made no connection itself. So a hop counts as fresh only when it was not
// Reused and a TCP connect or TLS handshake started under this request's trace
// since its GetConn (or since the previous hop's GotConn).
type connTrace struct {
	state  atomic.Int32 // connNone, connFresh or connReused
	dialed atomic.Bool  // this hop began a connect or handshake
}

const (
	connNone int32 = iota
	connFresh
	connReused
)

func (ct *connTrace) clientTrace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GetConn: func(string) {
			ct.state.Store(connNone)
			ct.dialed.Store(false)
		},
		ConnectStart:      func(string, string) { ct.dialed.Store(true) },
		TLSHandshakeStart: func() { ct.dialed.Store(true) },
		GotConn: func(info httptrace.GotConnInfo) {
			if dialed := ct.dialed.Swap(false); info.Reused || !dialed {
				ct.state.Store(connReused)
			} else {
				ct.state.Store(connFresh)
			}
		},
	}
}

// record sets extra["conn_reused"] from the last hop, or leaves it unset when
// that hop obtained no connection.
func (ct *connTrace) record(extra map[string]any) {
	switch ct.state.Load() {
	case connFresh:
		extra["conn_reused"] = false
	case connReused:
		extra["conn_reused"] = true
	}
}

// localErrnos are the errnos that mean this host, not the target, ran out of
// a resource. metrics.NotRunReason maps the same set to its reason label.
var localErrnos = [...]syscall.Errno{
	syscall.EMFILE,
	syscall.ENFILE,
	syscall.EADDRNOTAVAIL,
	syscall.ENOBUFS,
	syscall.ENOMEM,
}

// localFailure reports whether err, from client.Do, was caused by local
// resource exhaustion rather than by the target. If so it returns a new error
// wrapping check.ErrNotRun and the errno, of the form "check not run: <op>:
// <errno text>"; otherwise nil. The result never wraps the *url.Error, so it
// cannot carry the target URL or its credentials.
//
// The errno is found with errors.Is, or, for a *net.DNSError, by matching its
// Err text: net flattens a resolver's errno into that string, so without this
// a lookup that failed for want of a file descriptor would read as
// dns_failure. The errno text must end DNSError.Err, as it does in net's
// "<op>: <errno text>" wrapping; the host name is never matched.
func localFailure(err error) error {
	for _, errno := range localErrnos {
		if errors.Is(err, errno) {
			return fmt.Errorf("%w: %s: %w", check.ErrNotRun, opName(err), errno)
		}
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		for _, errno := range localErrnos {
			// A suffix, not a substring: EMFILE's text is a prefix of ENFILE's.
			if strings.HasSuffix(dnsErr.Err, errno.Error()) {
				return fmt.Errorf("%w: lookup: %w", check.ErrNotRun, errno)
			}
		}
	}
	return nil
}

// opName names the operation that failed, from a fixed vocabulary: the Op of a
// *net.OpError (such as "dial", "read" or "write"), "lookup" for a
// *net.DNSError, or "request".
func opName(err error) string {
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op != "" {
		return opErr.Op
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "lookup"
	}
	return "request"
}

// targetTextErrors are lower-case fragments of the net/http errors whose text
// includes bytes the target sent.
var targetTextErrors = [...]string{
	"malformed http",        // status line, status code or response
	"malformed mime header", // header line
	"goaway",                // HTTP/2 GOAWAY with debug data
}

// classifyError maps a transport error to the engine's closed ErrorClass enum.
// It prefers typed-error unwrapping (errors.Is/As) over matching on error text,
// since stdlib/OS wording is not a stable contract. Substring matching remains a
// last-resort fallback for cases without a usable concrete type (e.g. the
// "too many redirects" error from CheckRedirect).
func classifyError(err error) check.ErrorClass {
	if errors.Is(err, ErrSSRFBlocked) {
		return check.ErrSSRFBlocked
	}

	// Timeout: context deadline, or any net.Error that reports a timeout.
	var netErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()) {
		return check.ErrTimeout
	}

	// Connection refused / reset, surfaced as syscall errnos.
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) {
		return check.ErrConnectionRefused
	}

	// DNS resolution failure.
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return check.ErrDNSFailure
	}

	// TLS / certificate verification failures.
	var (
		x509Unknown  x509.UnknownAuthorityError
		x509Invalid  x509.CertificateInvalidError
		x509Hostname x509.HostnameError
	)
	if errors.As(err, &x509Unknown) || errors.As(err, &x509Invalid) || errors.As(err, &x509Hostname) {
		return check.ErrTLSError
	}

	// Fallback: substring matching for errors without a usable concrete type.
	// Match only the cause, not a *url.Error's own text, which contains the
	// request URL: a target named ".../timeout" must not steer the class.
	var urlErr *url.Error
	for errors.As(err, &urlErr) {
		err = urlErr.Err
		if err == nil {
			return check.ErrUnknown
		}
	}
	msg := strings.ToLower(err.Error())
	// A malformed response or an HTTP/2 GOAWAY carries bytes the target sent
	// (its status or header line, its GOAWAY debug data). Matching words in
	// them would let the target pick its own class, so these stay unknown.
	for _, s := range targetTextErrors {
		if strings.Contains(msg, s) {
			return check.ErrUnknown
		}
	}
	switch {
	case strings.Contains(msg, "timeout"):
		return check.ErrTimeout
	case strings.Contains(msg, "too many redirects"):
		return check.ErrHTTPError
	case strings.Contains(msg, "connection refused"), strings.Contains(msg, "connection reset"):
		return check.ErrConnectionRefused
	case strings.Contains(msg, "no such host"), strings.Contains(msg, "server misbehaving"):
		return check.ErrDNSFailure
	case strings.Contains(msg, "certificate"), strings.Contains(msg, "tls"), strings.Contains(msg, "x509"):
		return check.ErrTLSError
	default:
		return check.ErrUnknown
	}
}

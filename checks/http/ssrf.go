package http

import (
	"context"
	"errors"
	"fmt"
	"net"
	nethttp "net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"
)

// ErrSSRFBlocked is returned (or surfaced as ErrorClass ssrf_blocked) when a
// target resolves to a blocked address. Best-effort guard, NOT a security
// boundary — see README Security section.
var ErrSSRFBlocked = errors.New("ssrf blocked")

var allowedSchemes = map[string]bool{"http": true, "https": true}
var allowedPorts = map[string]bool{"": true, "80": true, "443": true}

// extraBlocked covers ranges net/netip predicates do NOT classify.
var extraBlocked = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),       // "this" network
	netip.MustParsePrefix("100.64.0.0/10"),   // CGNAT (RFC 6598)
	netip.MustParsePrefix("192.0.0.0/24"),    // IETF protocol assignments (RFC 6890)
	netip.MustParsePrefix("192.0.2.0/24"),    // TEST-NET-1
	netip.MustParsePrefix("198.18.0.0/15"),   // benchmarking (RFC 2544)
	netip.MustParsePrefix("198.51.100.0/24"), // TEST-NET-2
	netip.MustParsePrefix("203.0.113.0/24"),  // TEST-NET-3
	netip.MustParsePrefix("240.0.0.0/4"),     // reserved
}

// isBlockedIP is shared by Layer 1 (literal hosts) and Layer 2 (resolved dials).
func isBlockedIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
		return true
	}
	for _, p := range extraBlocked {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// assertSafeURL is Layer 1: parse-time, string-only. Mirrors the consumer-side
// URL safety allowlist.
func assertSafeURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%w: not parseable", ErrSSRFBlocked)
	}
	if !allowedSchemes[u.Scheme] {
		return fmt.Errorf("%w: scheme %q not allowed", ErrSSRFBlocked, u.Scheme)
	}
	if !allowedPorts[u.Port()] {
		return fmt.Errorf("%w: port %q not allowed", ErrSSRFBlocked, u.Port())
	}
	host := strings.ToLower(u.Hostname())
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || host == "0.0.0.0" {
		return fmt.Errorf("%w: host %q is reserved", ErrSSRFBlocked, host)
	}
	if ip, err := netip.ParseAddr(host); err == nil && isBlockedIP(ip) {
		return fmt.Errorf("%w: host %q is private or reserved", ErrSSRFBlocked, host)
	}
	return nil // non-IP host: assume public (DNS not resolved here)
}

// controlContext is Layer 2: runs on the already-resolved socket address before
// connect, on every dial attempt (incl. Happy-Eyeballs + each redirect hop).
// No TOCTOU window.
func controlContext(_ context.Context, _, address string, _ syscall.RawConn) error {
	ap, err := netip.ParseAddrPort(address)
	if err != nil {
		return fmt.Errorf("%w: unparseable dial address %q", ErrSSRFBlocked, address)
	}
	if isBlockedIP(ap.Addr()) {
		return fmt.Errorf("%w: resolved to blocked address %s", ErrSSRFBlocked, ap.Addr())
	}
	return nil
}

// HTTP/2 health checks, used in connection-reuse mode only (see
// WithConnectionReuse). In the default fresh-connection mode no connection
// outlives a check, so there is nothing to health-check and HTTP2 is nil.
//
// In reuse mode, a check's timeout resets only its own stream, so without
// these a connection that silently stopped delivering stays pooled and every
// later check of that host times out on it until the kernel gives up.
//
// A connection is pinged after h2SendPingTimeout without a received frame and
// closed if the ping goes unanswered for h2PingTimeout, so a dead connection
// leaves the pool at most 30s after it dies. Counting a check already in flight
// at that moment, at most about ceil((30s + timeout) / interval) checks fail per
// connection, given that a new connection works: one at a 60s interval with a
// 10s timeout.
//
// h2PingTimeout is the stdlib default. It meets that bound while tolerating a
// slow path, which matters because an unanswered ping also fails any check in
// flight on the connection.
const (
	h2SendPingTimeout = 15 * time.Second
	h2PingTimeout     = 15 * time.Second
)

// dialKeepAlive is the TCP keep-alive period of the dialer. It only matters in
// reuse mode, where a connection can outlive a check.
const dialKeepAlive = 30 * time.Second

// newSafeDialer returns the dialer newSafeTransport uses: its ControlContext
// enforces the Layer 2 SSRF guard, and its Timeout caps the TCP connect at the
// check's timeout. The cap matters because net/http dials on a context
// detached from the request's deadline, so an uncapped dial to a stalled
// target outlives the check that started it, holding a file descriptor and
// goroutines.
func newSafeDialer(timeout time.Duration) *net.Dialer {
	return &net.Dialer{
		Timeout:        timeout,
		KeepAlive:      dialKeepAlive,
		ControlContext: controlContext,
	}
}

// newSafeTransport clones http.DefaultTransport and swaps in newSafeDialer,
// whose ControlContext enforces the Layer 2 SSRF guard. ForceAttemptHTTP2 (set
// on DefaultTransport) keeps HTTP/2 working despite the custom DialContext.
//
// With reuse false (the default for New), DisableKeepAlives is set and Run
// gives each check its own clone of this transport, closed when the check
// returns, so every check, and every redirect hop within it, dials and
// handshakes a new connection and never takes one dialed for another check;
// no HTTP/2 health-check config is needed. With reuse true,
// connections are pooled and HTTP/2 connections are health-checked with PINGs
// (h2SendPingTimeout, h2PingTimeout).
//
// In both modes the TCP dial and the TLS handshake are each capped at timeout,
// because net/http runs them on a context detached from the request deadline;
// the request context still fires first, so a stalled handshake is classified
// as a timeout.
//
// Proxy support is explicitly disabled: a proxy resolves the target host itself,
// so our dial-time guard would only see the proxy's address and the Layer 2 guard
// would be bypassed (a private rebinding result never reaches this process).
//
// TestSafeTransportWiring asserts these fields by name; see the standing rule in
// CONTRIBUTING.md#transport-and-check-changes before changing any of them.
func newSafeTransport(reuse bool, timeout time.Duration) *nethttp.Transport {
	t := nethttp.DefaultTransport.(*nethttp.Transport).Clone()
	t.Proxy = nil
	t.TLSHandshakeTimeout = timeout
	if reuse {
		t.DisableKeepAlives = false
		t.HTTP2 = &nethttp.HTTP2Config{
			SendPingTimeout: h2SendPingTimeout,
			PingTimeout:     h2PingTimeout,
		}
	} else {
		t.DisableKeepAlives = true
		t.HTTP2 = nil
	}
	t.DialContext = newSafeDialer(timeout).DialContext
	return t
}

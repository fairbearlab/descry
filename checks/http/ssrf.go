package http

import (
	"context"
	"errors"
	"fmt"
	nethttp "net/http"
	"net"
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

// assertSafeURL is Layer 1: parse-time, string-only. Mirrors urlSafety.ts.
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

// newSafeTransport clones http.DefaultTransport (preserving its timeouts,
// connection pooling, and HTTP/2 settings) and swaps in a dialer whose
// ControlContext enforces the Layer 2 SSRF guard. ForceAttemptHTTP2 (set on
// DefaultTransport) keeps HTTP/2 working despite the custom DialContext.
//
// Proxy support is explicitly disabled: a proxy resolves the target host itself,
// so our dial-time guard would only see the proxy's address and the Layer 2 guard
// would be bypassed (a private rebinding result never reaches this process).
func newSafeTransport() *nethttp.Transport {
	t := nethttp.DefaultTransport.(*nethttp.Transport).Clone()
	t.Proxy = nil
	d := &net.Dialer{
		Timeout:        30 * time.Second,
		KeepAlive:      30 * time.Second,
		ControlContext: controlContext,
	}
	t.DialContext = d.DialContext
	return t
}

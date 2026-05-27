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
	netip.MustParsePrefix("192.0.2.0/24"),    // TEST-NET-1
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

// newSafeTransport builds an http.Transport whose dialer enforces Layer 2.
func newSafeTransport() *nethttp.Transport {
	d := &net.Dialer{ControlContext: controlContext}
	return &nethttp.Transport{DialContext: d.DialContext}
}

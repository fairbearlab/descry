package http

import (
	"context"
	"errors"
	"net/netip"
	"net/url"
	"testing"
)

// The SSRF guard is a best-effort guard, not a security boundary (see ssrf.go).
// Fuzzing it does not change that. What it does buy: the guard is the one place
// in descry that consumes fully untrusted strings, so it is where a panic or a
// disagreement between the two layers would actually hurt. These targets hunt
// for exactly those two things — crashes, and Layer 1 / Layer 2 divergence —
// not for a proof that the guard is complete.
//
// The seed corpora run as ordinary unit tests in the regular CI test job; the
// perf workflow additionally runs 60s of coverage-guided fuzzing on both SSRF
// targets on every PR. On demand:
// go test ./checks/http -run '^$' -fuzz FuzzAssertSafeURL -fuzztime 60s

// FuzzAssertSafeURL checks two invariants of Layer 1:
//
//  1. Every error it returns wraps ErrSSRFBlocked. Callers classify failures with
//     errors.Is; a bare error would be silently misclassified as a non-SSRF fault.
//  2. Cross-layer agreement: if Layer 1 allows a URL whose host is a literal IP,
//     Layer 2 must allow that same IP at dial time. A disagreement means one layer
//     extracts a different address from the same input than the other does, which
//     is the shape a real bypass would take.
func FuzzAssertSafeURL(f *testing.F) {
	// Seeds lifted from the TestAssertSafeURL tables so the fuzzer starts on
	// both sides of every existing decision boundary.
	seeds := []string{
		"http://127.0.0.1", "http://10.0.0.1", "http://172.16.0.1",
		"http://192.168.1.1", "http://169.254.169.254", "http://0.0.0.0",
		"http://100.64.0.1", "http://192.0.0.1", "http://192.0.2.1",
		"http://198.18.0.1", "http://198.19.255.254", "http://198.51.100.1",
		"http://203.0.113.1", "http://240.0.0.1", "http://localhost",
		"http://foo.localhost", "http://[::1]", "http://[::]",
		"http://[fc00::1]", "http://[fe80::1]", "http://[::ffff:127.0.0.1]",
		"ftp://example.com", "http://example.com:8080", "http://example.com:22",
		"http://172.15.0.1", "http://172.32.0.1", "http://8.8.8.8",
		"https://example.com", "http://example.com:80", "https://example.com:443",
		// Parser-disagreement bait: alternate IP spellings, case, userinfo,
		// zone identifiers, and hosts that url.Parse and netip.ParseAddr may
		// read differently.
		"http://0177.0.0.1", "http://2130706433", "http://0x7f.1",
		"http://LOCALHOST", "http://LocalHost.", "http://127.0.0.1.",
		"http://user:pw@127.0.0.1", "http://example.com@127.0.0.1",
		"http://[::ffff:10.0.0.1]", "http://[fe80::1%25eth0]",
		"http://[0:0:0:0:0:ffff:127.0.0.1]", "http:///path", "http://",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		err := assertSafeURL(raw)

		if err != nil {
			if !errors.Is(err, ErrSSRFBlocked) {
				t.Fatalf("assertSafeURL(%q) returned %v, which does not wrap ErrSSRFBlocked", raw, err)
			}
			return
		}

		// Layer 1 allowed it. If the host is a literal IP, Layer 2 must agree.
		u, perr := url.Parse(raw)
		if perr != nil {
			t.Fatalf("assertSafeURL(%q) returned nil but url.Parse fails: %v", raw, perr)
		}
		ip, aerr := netip.ParseAddr(u.Hostname())
		if aerr != nil {
			return // hostname is a DNS name; Layer 2 judges it after resolution
		}
		// Port is irrelevant to controlContext, which only inspects the address.
		addr := netip.AddrPortFrom(ip, 443).String()
		if cerr := controlContext(context.Background(), "tcp", addr, nil); cerr != nil {
			t.Fatalf("layer disagreement for %q: Layer 1 allowed it, Layer 2 rejected %s: %v", raw, addr, cerr)
		}
	})
}

// FuzzIsBlockedIP targets the shared predicate both layers depend on. The
// invariant that matters is 4-in-6 consistency: ::ffff:127.0.0.1 must be blocked
// exactly when 127.0.0.1 is. That equivalence rests entirely on the single
// Unmap call at the top of isBlockedIP, and dropping it is a textbook SSRF
// bypass, so it is worth a target of its own.
func FuzzIsBlockedIP(f *testing.F) {
	seeds := []string{
		// One address from each extraBlocked range, plus the boundaries.
		"0.0.0.0", "0.255.255.255", "100.64.0.0", "100.127.255.255",
		"192.0.0.0", "192.0.0.255", "192.0.2.1", "198.18.0.0",
		"198.19.255.255", "198.51.100.1", "203.0.113.1", "240.0.0.0",
		"255.255.255.255",
		// Just outside those ranges — must stay allowed.
		"1.0.0.0", "100.63.255.255", "100.128.0.0", "192.0.1.0",
		"198.17.255.255", "198.20.0.0", "239.255.255.255",
		// netip predicate territory and 4-in-6 forms.
		"127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.1.1",
		"169.254.169.254", "8.8.8.8", "::1", "::", "fc00::1", "fe80::1",
		"ff02::1", "2001:db8::1", "::ffff:127.0.0.1", "::ffff:8.8.8.8",
		"64:ff9b::8.8.8.8",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, s string) {
		ip, err := netip.ParseAddr(s)
		if err != nil {
			return
		}

		got := isBlockedIP(ip)

		// An IPv4 address and its IPv4-mapped IPv6 form are the same host and
		// must be judged the same way.
		if ip.Is4() {
			mapped := netip.AddrFrom16(ip.As16())
			if isBlockedIP(mapped) != got {
				t.Fatalf("4-in-6 divergence: isBlockedIP(%s)=%v but isBlockedIP(%s)=%v",
					ip, got, mapped, !got)
			}
		}
		if ip.Is4In6() {
			if isBlockedIP(ip.Unmap()) != got {
				t.Fatalf("4-in-6 divergence: isBlockedIP(%s)=%v but isBlockedIP(%s)=%v",
					ip, got, ip.Unmap(), !got)
			}
		}

		// Every address inside an extraBlocked prefix must be blocked. Prefix
		// membership is checked on the unmapped, unzoned form because
		// netip.Prefix.Contains reports false for any address carrying a zone.
		bare := ip.Unmap().WithZone("")
		for _, p := range extraBlocked {
			if p.Contains(bare) && !got {
				t.Fatalf("isBlockedIP(%s)=false but it is inside %s", ip, p)
			}
		}
	})
}

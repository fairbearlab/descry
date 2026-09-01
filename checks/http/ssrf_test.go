package http

import (
	"context"
	"errors"
	"testing"
)

func TestAssertSafeURL(t *testing.T) {
	blocked := []string{
		"http://127.0.0.1", "http://10.0.0.1", "http://172.16.0.1",
		"http://192.168.1.1", "http://169.254.169.254", "http://0.0.0.0",
		"http://100.64.0.1", "http://192.0.0.1", "http://192.0.2.1",
		"http://198.18.0.1", "http://198.19.255.254", "http://198.51.100.1",
		"http://203.0.113.1", "http://240.0.0.1", "http://localhost",
		"http://foo.localhost", "http://[::1]", "http://[::]",
		"http://[fc00::1]", "http://[fe80::1]", "http://[::ffff:127.0.0.1]",
		"ftp://example.com", "http://example.com:8080", "http://example.com:22",
	}
	for _, u := range blocked {
		if err := assertSafeURL(u); !errors.Is(err, ErrSSRFBlocked) {
			t.Errorf("assertSafeURL(%q) = %v, want ErrSSRFBlocked", u, err)
		}
	}
	allowed := []string{
		"http://172.15.0.1", "http://172.32.0.1", "http://8.8.8.8",
		"https://example.com", "http://example.com:80", "https://example.com:443",
	}
	for _, u := range allowed {
		if err := assertSafeURL(u); err != nil {
			t.Errorf("assertSafeURL(%q) = %v, want nil", u, err)
		}
	}
}

// TestControlContext exercises Layer 2 (the dial-time guard that runs on the
// already-resolved socket address). This is the production guard — Layer 1 only
// catches literal-IP and reserved hostnames, so a DNS name resolving into a
// private range is stopped solely here.
func TestControlContext(t *testing.T) {
	blocked := []string{
		"127.0.0.1:80", "[::1]:443", "10.0.0.1:80", "192.168.1.1:443",
		"169.254.169.254:80", "[fc00::1]:443", "[fe80::1]:80", "0.0.0.0:80",
	}
	for _, a := range blocked {
		if err := controlContext(context.Background(), "tcp", a, nil); !errors.Is(err, ErrSSRFBlocked) {
			t.Errorf("controlContext(%q) = %v, want ErrSSRFBlocked", a, err)
		}
	}
	allowed := []string{"8.8.8.8:443", "1.1.1.1:80", "172.15.0.1:80"}
	for _, a := range allowed {
		if err := controlContext(context.Background(), "tcp", a, nil); err != nil {
			t.Errorf("controlContext(%q) = %v, want nil", a, err)
		}
	}
	if err := controlContext(context.Background(), "tcp", "garbage", nil); !errors.Is(err, ErrSSRFBlocked) {
		t.Errorf("unparseable address: got %v, want ErrSSRFBlocked", err)
	}
}

// TestAssertSafeURL_Allocs guards the Layer 1 pass path: parsing and
// classifying a safe URL costs the url.Parse allocations and nothing more.
// Bound measured 2026-09-01 on go1.26.6 darwin/arm64; a Go toolchain bump may
// require re-measuring (bounds are "<=" the measured value).
func TestAssertSafeURL_Allocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are unreliable under -race")
	}
	got := testing.AllocsPerRun(500, func() {
		if err := assertSafeURL("https://example.com/healthz"); err != nil {
			t.Fatal(err)
		}
	})
	if got > 2 {
		t.Errorf("assertSafeURL allocs/op = %v, want <= 2", got)
	}
}

// TestControlContext_Allocs guards the Layer 2 pass path, which runs on every
// dial attempt: netip.ParseAddrPort and isBlockedIP must stay allocation-free.
// Bound measured 2026-09-01 on go1.26.6 darwin/arm64; a Go toolchain bump may
// require re-measuring (bounds are "<=" the measured value).
func TestControlContext_Allocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are unreliable under -race")
	}
	ctx := context.Background()
	got := testing.AllocsPerRun(500, func() {
		if err := controlContext(ctx, "tcp", "8.8.8.8:443", nil); err != nil {
			t.Fatal(err)
		}
	})
	if got > 0 {
		t.Errorf("controlContext allocs/op = %v, want 0", got)
	}
}

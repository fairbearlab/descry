package http

import (
	"errors"
	"testing"
)

func TestAssertSafeURL(t *testing.T) {
	blocked := []string{
		"http://127.0.0.1", "http://10.0.0.1", "http://172.16.0.1",
		"http://192.168.1.1", "http://169.254.169.254", "http://0.0.0.0",
		"http://100.64.0.1", "http://192.0.2.1", "http://198.51.100.1",
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

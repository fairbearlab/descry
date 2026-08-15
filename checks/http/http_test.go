package http

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	nethttp "net/http"
	"net/http/httptest"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/fairbearlab/descry/check"
)

func TestRun_200Up(t *testing.T) {
	srv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()
	obs, _ := newForTest(2*time.Second).Run(context.Background(), check.Target{URL: srv.URL})
	if obs.Status != check.StatusUp {
		t.Fatalf("status = %v, want up", obs.Status)
	}
}

func TestRun_500HTTPError(t *testing.T) {
	srv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()
	obs, _ := newForTest(2*time.Second).Run(context.Background(), check.Target{URL: srv.URL})
	if obs.Status != check.StatusDown || obs.ErrorClass != check.ErrHTTPError {
		t.Fatalf("got %v/%v, want down/http_error", obs.Status, obs.ErrorClass)
	}
}

func TestRun_BodyCappedTo4096(t *testing.T) {
	srv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte(strings.Repeat("x", 8000)))
	}))
	defer srv.Close()
	obs, _ := newForTest(2*time.Second).Run(context.Background(), check.Target{URL: srv.URL})
	if got := obs.Extra["body"].(string); len(got) != 4096 {
		t.Fatalf("body len = %d, want 4096", len(got))
	}
}

func TestRun_Timeout(t *testing.T) {
	srv := httptest.NewServer(nethttp.HandlerFunc(func(_ nethttp.ResponseWriter, r *nethttp.Request) {
		// Block until client disconnects
		<-r.Context().Done()
	}))
	defer srv.Close()
	// Use a very short timeout so the test runs quickly
	obs, _ := newForTest(50*time.Millisecond).Run(context.Background(), check.Target{URL: srv.URL})
	if obs.Status != check.StatusDown {
		t.Fatalf("status = %v, want down", obs.Status)
	}
	if obs.ErrorClass != check.ErrTimeout {
		t.Fatalf("error_class = %v, want timeout", obs.ErrorClass)
	}
}

func TestRun_FinalURLAfterRedirect(t *testing.T) {
	var srvURL string
	srv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if r.URL.Path == "/start" {
			nethttp.Redirect(w, r, srvURL+"/end", nethttp.StatusFound)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()
	srvURL = srv.URL

	obs, _ := newForTest(2*time.Second).Run(context.Background(), check.Target{URL: srv.URL + "/start"})
	if obs.Status != check.StatusUp {
		t.Fatalf("status = %v, want up", obs.Status)
	}
	if !strings.HasSuffix(obs.FinalURL, "/end") {
		t.Fatalf("final_url = %q, want suffix /end", obs.FinalURL)
	}
}

func TestRun_RedirectLoop(t *testing.T) {
	var srvURL string
	srv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		nethttp.Redirect(w, r, srvURL+"/loop", nethttp.StatusFound)
	}))
	defer srv.Close()
	srvURL = srv.URL

	obs, _ := newForTest(2*time.Second).Run(context.Background(), check.Target{URL: srv.URL + "/loop"})
	if obs.Status != check.StatusDown {
		t.Fatalf("status = %v, want down", obs.Status)
	}
	if obs.ErrorClass != check.ErrHTTPError {
		t.Fatalf("error_class = %v, want http_error", obs.ErrorClass)
	}
}

// TestRun_SSRFBlockedProducesObservation drives the real (SSRF-guarded)
// constructor against a literal loopback URL and asserts the best-effort produce
// contract: a down/ssrf_blocked observation with a nil error.
func TestRun_SSRFBlockedProducesObservation(t *testing.T) {
	obs, err := New(2*time.Second).Run(context.Background(), check.Target{URL: "http://127.0.0.1"})
	if err != nil {
		t.Fatalf("err = %v, want nil (best-effort produce)", err)
	}
	if obs.Status != check.StatusDown {
		t.Fatalf("status = %v, want down", obs.Status)
	}
	if obs.ErrorClass != check.ErrSSRFBlocked {
		t.Fatalf("error_class = %v, want ssrf_blocked", obs.ErrorClass)
	}
}

// TestClassifyError locks in the typed-error mappings and the substring
// fallback so a Go upgrade that changes error wording fails loudly here.
func TestClassifyError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want check.ErrorClass
	}{
		{"ssrf", ErrSSRFBlocked, check.ErrSSRFBlocked},
		{"deadline", context.DeadlineExceeded, check.ErrTimeout},
		{"net.Error timeout (not deadline)", &net.OpError{Op: "dial", Err: os.ErrDeadlineExceeded}, check.ErrTimeout},
		{"wrapped deadline", fmt.Errorf("Get: %w", context.DeadlineExceeded), check.ErrTimeout},
		{"econnrefused", syscall.ECONNREFUSED, check.ErrConnectionRefused},
		{"econnreset", syscall.ECONNRESET, check.ErrConnectionRefused},
		{"wrapped econnrefused", &net.OpError{Op: "dial", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}, check.ErrConnectionRefused},
		{"dns", &net.DNSError{Err: "no such host", Name: "x"}, check.ErrDNSFailure},
		{"x509 unknown authority", x509.UnknownAuthorityError{}, check.ErrTLSError},
		{"x509 certificate invalid", x509.CertificateInvalidError{Reason: x509.Expired}, check.ErrTLSError},
		{"x509 hostname mismatch", x509.HostnameError{Host: "example.com"}, check.ErrTLSError},
		{"wrapped x509", fmt.Errorf("tls: %w", x509.HostnameError{Host: "x"}), check.ErrTLSError},
		{"redirects (fallback)", errors.New("too many redirects"), check.ErrHTTPError},
		{"timeout (fallback)", errors.New("i/o Timeout waiting"), check.ErrTimeout},
		{"connection refused (fallback)", errors.New("dial: Connection Refused"), check.ErrConnectionRefused},
		{"connection reset (fallback)", errors.New("read: connection reset by peer"), check.ErrConnectionRefused},
		{"no such host (fallback)", errors.New("lookup x: no such host"), check.ErrDNSFailure},
		{"server misbehaving (fallback)", errors.New("lookup x: server misbehaving"), check.ErrDNSFailure},
		{"certificate (fallback)", errors.New("bad Certificate"), check.ErrTLSError},
		{"tls (fallback)", errors.New("remote error: tls: handshake failure"), check.ErrTLSError},
		{"unknown", errors.New("something weird"), check.ErrUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := classifyError(c.err); got != c.want {
				t.Errorf("classifyError(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

func TestRun_TLSExpiryCaptured(t *testing.T) {
	srv := httptest.NewTLSServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()

	// Use the TLS server's client which trusts the test cert, with skipSSRF
	c := newForTest(2 * time.Second)
	c.client.Transport = srv.Client().Transport

	obs, _ := c.Run(context.Background(), check.Target{URL: srv.URL})
	if obs.Status != check.StatusUp {
		t.Fatalf("status = %v, want up", obs.Status)
	}
	if obs.TLSExpiry == nil {
		t.Fatalf("tls_expiry is nil, want non-nil")
	}
}

func TestRun_WithUserAgentSetsHeader(t *testing.T) {
	const wantUA = "MyMonitor/1.0 (+https://example.com)"
	var gotUA string
	srv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		gotUA = r.UserAgent()
		w.WriteHeader(200)
	}))
	defer srv.Close()

	obs, _ := newForTest(2*time.Second, WithUserAgent(wantUA)).
		Run(context.Background(), check.Target{URL: srv.URL})
	if obs.Status != check.StatusUp {
		t.Fatalf("status = %v, want up", obs.Status)
	}
	if gotUA != wantUA {
		t.Fatalf("User-Agent = %q, want %q", gotUA, wantUA)
	}
}

func TestRun_DefaultUserAgentIsStdlib(t *testing.T) {
	var gotUA string
	srv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		gotUA = r.UserAgent()
		w.WriteHeader(200)
	}))
	defer srv.Close()

	// No WithUserAgent option → net/http's default UA ("Go-http-client/...").
	_, _ = newForTest(2*time.Second).Run(context.Background(), check.Target{URL: srv.URL})
	if !strings.HasPrefix(gotUA, "Go-http-client/") {
		t.Fatalf("default User-Agent = %q, want stdlib Go-http-client/ prefix", gotUA)
	}
}

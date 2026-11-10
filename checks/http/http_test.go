package http

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	nethttp "net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/fairbearlab/descry/check"
	"github.com/fairbearlab/descry/metrics"
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
		// The *url.Error's own text holds the request URL, which must not steer
		// the class: only its cause is matched.
		{"misleading URL: timeout", &url.Error{Op: "Get", URL: "https://tls-gateway.example/api/timeout", Err: errors.New("something weird")}, check.ErrUnknown},
		{"misleading URL: refused", &url.Error{Op: "Get", URL: "https://connection-refused.example/", Err: errors.New("something weird")}, check.ErrUnknown},
		{"misleading URL: dns", &url.Error{Op: "Get", URL: "https://example.com/no-such-host", Err: errors.New("something weird")}, check.ErrUnknown},
		{"misleading URL: certificate", &url.Error{Op: "Get", URL: "https://certificate.example/x509", Err: errors.New("something weird")}, check.ErrUnknown},
		{"misleading URL: EOF before headers", &url.Error{Op: "Get", URL: "https://example.com/timeout", Err: io.EOF}, check.ErrUnknown},
		{"url.Error nil cause", &url.Error{Op: "Get", URL: "https://example.com/timeout"}, check.ErrUnknown},
		{"url.Error cause still matched", &url.Error{Op: "Get", URL: "https://example.com/", Err: errors.New("too many redirects")}, check.ErrHTTPError},
		{"nested url.Error", &url.Error{Op: "Get", URL: "https://timeout.example/", Err: &url.Error{Op: "Get", URL: "https://tls.example/", Err: errors.New("remote error: tls: handshake failure")}}, check.ErrTLSError},
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

func TestWithConnectionReuseWiring(t *testing.T) {
	for _, tc := range []struct {
		name  string
		opts  []Option
		reuse bool
	}{
		{"default", nil, false},
		{"WithConnectionReuse", []Option{WithConnectionReuse()}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := New(3*time.Second, tc.opts...)
			if c.reuse != tc.reuse {
				t.Fatalf("reuse = %v, want %v", c.reuse, tc.reuse)
			}
			tr, ok := c.client.Transport.(*nethttp.Transport)
			if !ok {
				t.Fatalf("transport is %T, want *http.Transport", c.client.Transport)
			}
			if tr.DisableKeepAlives == tc.reuse {
				t.Errorf("DisableKeepAlives = %v, want %v", tr.DisableKeepAlives, !tc.reuse)
			}
			if (tr.HTTP2 != nil) != tc.reuse {
				t.Errorf("HTTP2 config set = %v, want %v", tr.HTTP2 != nil, tc.reuse)
			}
			if tr.TLSHandshakeTimeout != 3*time.Second {
				t.Errorf("TLSHandshakeTimeout = %v, want the check timeout", tr.TLSHandshakeTimeout)
			}
		})
	}
	if got := New(0).client.Transport.(*nethttp.Transport).TLSHandshakeTimeout; got != defaultTimeout {
		t.Errorf("New(0) TLSHandshakeTimeout = %v, want defaultTimeout %v", got, defaultTimeout)
	}
}

func TestRun_ConnReused(t *testing.T) {
	srv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if r.URL.Path == "/fail" {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	t.Run("fresh omits the key", func(t *testing.T) {
		c := newForTest(2 * time.Second)
		for _, path := range []string{"/", "/", "/fail"} {
			obs, err := c.Run(context.Background(), check.Target{URL: srv.URL + path})
			if err != nil {
				t.Fatal(err)
			}
			if v, ok := obs.Extra["conn_reused"]; ok {
				t.Errorf("%s: conn_reused = %v in fresh mode, want the key omitted", path, v)
			}
		}
	})
	t.Run("reuse sets it on up and down", func(t *testing.T) {
		c := newForTest(2*time.Second, WithConnectionReuse())
		for i, tc := range []struct {
			path   string
			status check.Status
			want   bool
		}{
			{"/", check.StatusUp, false},
			{"/", check.StatusUp, true},
			{"/fail", check.StatusDown, true},
		} {
			obs, err := c.Run(context.Background(), check.Target{URL: srv.URL + tc.path})
			if err != nil {
				t.Fatal(err)
			}
			if obs.Status != tc.status {
				t.Fatalf("run %d: status %v, want %v", i, obs.Status, tc.status)
			}
			if got, ok := obs.Extra["conn_reused"]; !ok || got != tc.want {
				t.Errorf("run %d: conn_reused = %v (present %v), want %v", i, got, ok, tc.want)
			}
		}
	})
}

// TestRun_ConnReusedFollowsLastHop: the key reflects the last attempted hop,
// so a redirect whose second hop is refused leaves it omitted even though the
// first hop connected.
func TestRun_ConnReusedFollowsLastHop(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := "http://" + ln.Addr().String() + "/"
	_ = ln.Close()

	second := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		w.WriteHeader(200)
	}))
	defer second.Close()
	first := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if r.URL.Path == "/to-refused" {
			nethttp.Redirect(w, r, refused, nethttp.StatusFound)
			return
		}
		nethttp.Redirect(w, r, second.URL+"/", nethttp.StatusFound)
	}))
	defer first.Close()

	c := newForTest(2*time.Second, WithConnectionReuse())
	obs, err := c.Run(context.Background(), check.Target{URL: first.URL + "/to-refused"})
	if err != nil {
		t.Fatal(err)
	}
	if obs.Status != check.StatusDown || obs.ErrorClass != check.ErrConnectionRefused {
		t.Fatalf("got %v/%v, want down/connection_refused", obs.Status, obs.ErrorClass)
	}
	if v, ok := obs.Extra["conn_reused"]; ok {
		t.Errorf("conn_reused = %v, want the key omitted when the last hop got no connection", v)
	}

	// Control: a redirect to a working second host records that hop, which is
	// a new connection to a host not yet pooled.
	obs, err = c.Run(context.Background(), check.Target{URL: first.URL + "/to-second"})
	if err != nil {
		t.Fatal(err)
	}
	if obs.Status != check.StatusUp {
		t.Fatalf("control: status %v, class %v, want up", obs.Status, obs.ErrorClass)
	}
	if got, ok := obs.Extra["conn_reused"]; !ok || got != false {
		t.Errorf("control: conn_reused = %v (present %v), want false for the new second-hop connection", got, ok)
	}
}

// secretURL carries credentials in userinfo and the query; none of it may
// reach a not-run error.
const secretURL = "http://alice:hunter2@secret-host.example/health?token=s3cr3t-token" // #nosec G101 -- fake test credentials, asserted absent from errors

var secretParts = []string{"alice", "hunter2", "secret-host", "s3cr3t-token", "token=", "health"}

func assertNoSecrets(t *testing.T, msg string) {
	t.Helper()
	for _, p := range secretParts {
		if strings.Contains(msg, p) {
			t.Errorf("error %q contains %q from the target URL", msg, p)
		}
	}
}

var localErrnoReasons = []struct {
	errno  syscall.Errno
	reason string
}{
	{syscall.EMFILE, metrics.NotRunEMFILE},
	{syscall.ENFILE, metrics.NotRunENFILE},
	{syscall.EADDRNOTAVAIL, metrics.NotRunEADDRNOTAVAIL},
	{syscall.ENOBUFS, metrics.NotRunENOBUFS},
	{syscall.ENOMEM, metrics.NotRunENOMEM},
}

func assertNotRun(t *testing.T, err error, errno syscall.Errno, reason, wantMsg string) {
	t.Helper()
	if !errors.Is(err, check.ErrNotRun) {
		t.Fatalf("error %v does not wrap check.ErrNotRun", err)
	}
	if !errors.Is(err, errno) {
		t.Errorf("error %v does not wrap %v", err, errno)
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		t.Errorf("error wraps a *url.Error: %v", urlErr)
	}
	if got := metrics.NotRunReason(err); got != reason {
		t.Errorf("metrics.NotRunReason = %q, want %q", got, reason)
	}
	if got := err.Error(); got != wantMsg {
		t.Errorf("error text = %q, want %q", got, wantMsg)
	}
	assertNoSecrets(t, err.Error())
}

func TestLocalFailure(t *testing.T) {
	for _, tc := range localErrnoReasons {
		t.Run(tc.reason, func(t *testing.T) {
			t.Run("OpError", func(t *testing.T) {
				err := &url.Error{Op: "Get", URL: secretURL, Err: &net.OpError{
					Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "socket", Err: tc.errno},
				}}
				assertNotRun(t, localFailure(err), tc.errno, tc.reason, "check not run: dial: "+tc.errno.Error())
			})
			t.Run("bare errno", func(t *testing.T) {
				err := &url.Error{Op: "Get", URL: secretURL, Err: tc.errno}
				assertNotRun(t, localFailure(err), tc.errno, tc.reason, "check not run: request: "+tc.errno.Error())
			})
			t.Run("DNSError text", func(t *testing.T) {
				// net flattens the resolver's errno into DNSError.Err.
				err := &url.Error{Op: "Get", URL: secretURL, Err: &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{
					Err:    "dial udp 192.0.2.53:53: socket: " + tc.errno.Error(),
					Name:   "secret-host.example",
					Server: "192.0.2.53:53",
				}}}
				assertNotRun(t, localFailure(err), tc.errno, tc.reason, "check not run: lookup: "+tc.errno.Error())
			})
		})
	}
	for _, err := range []error{
		&net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}},
		&net.DNSError{Err: "no such host", Name: "too many open files", IsNotFound: true},
		context.DeadlineExceeded,
		errors.New("too many open files"), // text alone, outside a DNSError, is not trusted
	} {
		if got := localFailure(err); got != nil {
			t.Errorf("localFailure(%v) = %v, want nil", err, got)
		}
	}
}

// TestRun_LocalFailureIsNotRun drives each errno through the real transport
// with a stubbed DialContext, in both modes.
func TestRun_LocalFailureIsNotRun(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		for _, tc := range localErrnoReasons {
			t.Run(fmt.Sprintf("reuse=%v/%s", reuse, tc.reason), func(t *testing.T) {
				tr := newLoopbackSafeTransport(reuse, 2*time.Second, nil)
				tr.DialContext = func(context.Context, string, string) (net.Conn, error) {
					return nil, &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "socket", Err: tc.errno}}
				}
				var opts []Option
				if reuse {
					opts = append(opts, WithConnectionReuse())
				}
				obs, err := newCheckWithTransport(2*time.Second, tr, opts...).Run(context.Background(), check.Target{URL: secretURL})
				assertNotRun(t, err, tc.errno, tc.reason, "check not run: dial: "+tc.errno.Error())
				if obs.Status != "" || obs.ErrorClass != "" || obs.Extra != nil {
					t.Errorf("Observation = %+v, want the zero Observation", obs)
				}
			})
		}
	}
}

// TestRun_LocalFailureDuringLookup uses a real *net.DNSError: the pure Go
// resolver's own dial to its server fails with EMFILE, and net flattens that
// errno into DNSError.Err text.
func TestRun_LocalFailureDuringLookup(t *testing.T) {
	r := &net.Resolver{
		PreferGo: true,
		Dial: func(context.Context, string, string) (net.Conn, error) {
			return nil, &os.SyscallError{Syscall: "socket", Err: syscall.EMFILE}
		},
	}
	d := newSafeDialer(2 * time.Second)
	d.Resolver = r
	tr := newSafeTransport(false, 2*time.Second)
	tr.DialContext = d.DialContext

	var dnsErr *net.DNSError
	if _, err := r.LookupHost(context.Background(), "secret-host.example"); !errors.As(err, &dnsErr) {
		t.Fatalf("lookup error %v is not a *net.DNSError", err)
	} else if errors.Is(err, syscall.EMFILE) {
		t.Logf("net now keeps the errno in the DNSError chain: %v", err)
	}

	obs, err := newCheckWithTransport(2*time.Second, tr).Run(context.Background(), check.Target{URL: secretURL})
	assertNotRun(t, err, syscall.EMFILE, metrics.NotRunEMFILE, "check not run: lookup: "+syscall.EMFILE.Error())
	if obs.Status != "" {
		t.Errorf("status = %v, want no verdict", obs.Status)
	}
}

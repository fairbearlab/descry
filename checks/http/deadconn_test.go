package http

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	nethttp "net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fairbearlab/descry/check"
)

// The behavior tests in this file wait out the shipped ping timeouts instead of
// scaling them down, so that removing the HTTP/2 config from reuse mode of
// newSafeTransport fails them directly. They run in parallel and take about 32s.
//
// Dead pooled connections exist only in reuse mode (WithConnectionReuse), so
// the replacement tests run there. In the default fresh-connection mode a
// connection never outlives its check; TestFreshModeNeverUsesDeadConnection
// shows a check after a blackhole simply opens a new connection.

const (
	deadConnSetupTimeout = 10 * time.Second
	deadConnCheckTimeout = 2 * time.Second
	deadConnCheckGap     = 500 * time.Millisecond
	deadConnMargin       = 15 * time.Second
)

// blackholeProxy forwards TCP to target until blackhole is called. After that,
// every connection open at that moment stays open and keeps being read, but
// nothing more is forwarded, so neither end sees a close or a reset. This is
// what a silent mid-path drop looks like. Connections accepted later forward
// normally.
//
//	client <--TCP--> blackholeProxy <--TCP--> httptest server
type blackholeProxy struct {
	ln             net.Listener
	target         string
	accepted       atomic.Int64
	serverToClient atomic.Int64 // bytes forwarded from the server

	// Handshake-delay mode (setHandshakeDelay): a connection accepted while
	// handshakeDelay is non-zero has its client bytes held for that long before
	// the target is dialed, so a TLS handshake through it takes at least that
	// long. clientGaveUp counts held connections the client closed first.
	handshakeDelay atomic.Int64 // a time.Duration
	clientGaveUp   atomic.Int64

	mu    sync.Mutex
	gen   int // bumped by blackhole; a pipe from an older generation stops forwarding
	conns []net.Conn
}

func newBlackholeProxy(t *testing.T, target string) *blackholeProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &blackholeProxy{ln: ln, target: target}
	go p.serve()
	return p
}

func (p *blackholeProxy) serve() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		if d := time.Duration(p.handshakeDelay.Load()); d > 0 {
			p.accepted.Add(1)
			p.mu.Lock()
			p.conns = append(p.conns, client)
			p.mu.Unlock()
			go p.holdThenForward(client, d)
			continue
		}
		server, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.accepted.Add(1)
		p.mu.Lock()
		gen := p.gen
		p.conns = append(p.conns, client, server)
		p.mu.Unlock()
		go p.pipe(client, server, gen, nil)
		go p.pipe(server, client, gen, &p.serverToClient)
	}
}

func (p *blackholeProxy) forwarding(gen int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.gen == gen
}

func (p *blackholeProxy) pipe(src, dst net.Conn, gen int, count *atomic.Int64) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 && p.forwarding(gen) {
			if count != nil {
				count.Add(int64(n))
			}
			_, _ = dst.Write(buf[:n])
		}
		if err != nil {
			if p.forwarding(gen) {
				_ = dst.Close()
			}
			return
		}
	}
}

// setHandshakeDelay turns handshake-delay mode on for connections accepted
// from now on (d > 0) or off (d == 0). Use a d longer than the test to model a
// handshake that stalls forever; close releases held connections.
func (p *blackholeProxy) setHandshakeDelay(d time.Duration) { p.handshakeDelay.Store(int64(d)) }

// holdThenForward reads and holds the client's bytes for d, then dials the
// target, replays them and forwards both ways. A client that closes during the
// hold is counted in clientGaveUp, which shows that it released its socket.
func (p *blackholeProxy) holdThenForward(client net.Conn, d time.Duration) {
	chunks := make(chan []byte, 16)
	go func() {
		defer close(chunks)
		for {
			buf := make([]byte, 32*1024)
			n, err := client.Read(buf)
			if n > 0 {
				chunks <- buf[:n]
			}
			if err != nil {
				return
			}
		}
	}()
	drain := func() { // discard until the reader exits, so it never blocks on a send
		for b := range chunks {
			_ = b
		}
	}

	timer := time.NewTimer(d)
	defer timer.Stop()
	var held [][]byte
	for waiting := true; waiting; {
		select {
		case b, ok := <-chunks:
			if !ok {
				p.clientGaveUp.Add(1)
				_ = client.Close()
				return
			}
			held = append(held, b)
		case <-timer.C:
			waiting = false
		}
	}

	server, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = client.Close()
		drain()
		return
	}
	p.mu.Lock()
	gen := p.gen
	p.conns = append(p.conns, server)
	p.mu.Unlock()
	go p.pipe(server, client, gen, &p.serverToClient)
	for _, b := range held {
		_, _ = server.Write(b)
	}
	for b := range chunks {
		if p.forwarding(gen) {
			_, _ = server.Write(b)
		}
	}
	if p.forwarding(gen) {
		_ = server.Close()
	}
}

func (p *blackholeProxy) blackhole() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gen++
}

func (p *blackholeProxy) close() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
}

type deadConnFixture struct {
	check     *Check
	proxy     *blackholeProxy
	target    check.Target
	transport *nethttp.Transport
	lastProto *atomic.Int64
}

// newLoopbackSafeTransport returns the real newSafeTransport(reuse, timeout)
// with only the dialer and, when roots is non-nil, the trust roots replaced:
// the SSRF dialer refuses loopback by design, and httptest certificates are
// test ones. The replacement dialer keeps the production Timeout and
// KeepAlive. Tests that need to inject dial faults set tr.DialContext again
// afterwards.
func newLoopbackSafeTransport(reuse bool, timeout time.Duration, roots *x509.CertPool) *nethttp.Transport {
	tr := newSafeTransport(reuse, timeout)
	d := newSafeDialer(timeout)
	d.ControlContext = nil
	tr.DialContext = d.DialContext
	if roots != nil {
		tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	}
	return tr
}

// newCheckWithTransport returns a test Check (Layer 1 SSRF guard skipped) whose
// client uses tr. Pass WithConnectionReuse() when tr is a reuse-mode transport,
// so Run also records Extra["conn_reused"].
func newCheckWithTransport(timeout time.Duration, tr nethttp.RoundTripper, opts ...Option) *Check {
	c := newForTest(timeout, opts...)
	c.client.Transport = tr
	return c
}

// serverRoots returns the trust roots that verify srv's test certificate.
func serverRoots(srv *httptest.Server) *x509.CertPool {
	return srv.Client().Transport.(*nethttp.Transport).TLSClientConfig.RootCAs
}

// newDeadConnFixture wires the real newSafeTransport, in reuse or fresh mode, to
// a TLS server behind a blackholeProxy, via newLoopbackSafeTransport.
func newDeadConnFixture(t *testing.T, enableHTTP2, reuse bool) *deadConnFixture {
	t.Helper()
	var lastProto atomic.Int64
	srv := httptest.NewUnstartedServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		lastProto.Store(int64(r.ProtoMajor))
		w.WriteHeader(nethttp.StatusOK)
	}))
	srv.EnableHTTP2 = enableHTTP2
	srv.StartTLS()
	proxy := newBlackholeProxy(t, srv.Listener.Addr().String())

	tr := newLoopbackSafeTransport(reuse, deadConnSetupTimeout, serverRoots(srv))
	var opts []Option
	if reuse {
		opts = append(opts, WithConnectionReuse())
	}
	c := newCheckWithTransport(deadConnSetupTimeout, tr, opts...)

	// Proxy connections go first: a deliberately hung connection must not be
	// able to stall httptest.Server.Close.
	t.Cleanup(func() {
		proxy.close()
		tr.CloseIdleConnections()
		srv.Close()
	})
	return &deadConnFixture{
		check:     c,
		proxy:     proxy,
		target:    check.Target{URL: "https://" + proxy.ln.Addr().String() + "/"},
		transport: tr,
		lastProto: &lastProto,
	}
}

func (f *deadConnFixture) run(t *testing.T) check.Observation {
	t.Helper()
	obs, err := f.check.Run(context.Background(), f.target)
	if err != nil {
		t.Fatalf("Run returned an error: %v", err)
	}
	return obs
}

// warmUp opens the connection under test. A failure here says nothing about the
// mechanism, so it reports as setup rather than as a reproduction result.
func (f *deadConnFixture) warmUp(t *testing.T, wantProto int64) {
	t.Helper()
	if obs := f.run(t); obs.Status != check.StatusUp {
		t.Fatalf("setup check failed: status %v, class %v", obs.Status, obs.ErrorClass)
	}
	if got := f.lastProto.Load(); got != wantProto {
		t.Fatalf("setup negotiated HTTP/%d, want HTTP/%d", got, wantProto)
	}
	if got := f.proxy.accepted.Load(); got != 1 {
		t.Fatalf("setup opened %d connections, want 1", got)
	}
}

// probeFreshConnection reports whether a brand-new connection through the same
// proxy works. It runs only on the failure path, so it cannot satisfy the
// replacement assertion by raising the accepted count.
func (f *deadConnFixture) probeFreshConnection() error {
	tr := f.transport.Clone()
	defer tr.CloseIdleConnections()
	client := &nethttp.Client{Transport: tr, Timeout: deadConnSetupTimeout}
	resp, err := client.Get(f.target.URL)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != nethttp.StatusOK {
		return errors.New(resp.Status)
	}
	return nil
}

func TestDeadHTTP2ConnectionIsReplaced(t *testing.T) {
	t.Parallel()
	f := newDeadConnFixture(t, true, true)
	f.warmUp(t, 2)

	f.proxy.blackhole()
	f.check.timeout = deadConnCheckTimeout
	deadline := time.Now().Add(h2SendPingTimeout + h2PingTimeout + deadConnMargin)

	var classes []check.ErrorClass
	for time.Now().Before(deadline) {
		obs := f.run(t)
		if obs.Status == check.StatusUp {
			if len(classes) == 0 {
				t.Fatalf("first check after the blackhole succeeded; the proxy did not blackhole the connection")
			}
			if got := f.proxy.accepted.Load(); got < 2 {
				t.Fatalf("check succeeded on %d accepted connection(s), want a new one", got)
			}
			if got := f.lastProto.Load(); got != 2 {
				t.Fatalf("recovered on HTTP/%d, want HTTP/2", got)
			}
			t.Logf("recovered after %d failed checks %v; the cadence here is compressed, so this count is not the production bound", len(classes), classes)
			return
		}
		if len(classes) == 0 {
			if got := f.proxy.accepted.Load(); got != 1 {
				t.Fatalf("first failed check saw %d accepted connections, want it on the original one", got)
			}
		}
		classes = append(classes, obs.ErrorClass)
		time.Sleep(deadConnCheckGap)
	}

	accepted := f.proxy.accepted.Load()
	t.Fatalf("connection never replaced: %d checks failed, classes %v, proxy accepted %d connection(s), fresh-connection probe error: %v",
		len(classes), classes, accepted, f.probeFreshConnection())
}

func TestHealthyIdleHTTP2ConnectionIsKept(t *testing.T) {
	t.Parallel()
	f := newDeadConnFixture(t, true, true)
	f.warmUp(t, 2)

	time.Sleep(time.Second) // let trailing frames from the first check settle
	baseline := f.proxy.serverToClient.Load()
	time.Sleep(h2SendPingTimeout + h2PingTimeout + 2*time.Second)
	afterIdle := f.proxy.serverToClient.Load() // before the next check adds its own bytes

	obs := f.run(t)
	if obs.Status != check.StatusUp {
		t.Fatalf("check after idle: status %v, class %v", obs.Status, obs.ErrorClass)
	}
	if got, ok := obs.Extra["conn_reused"]; got != true {
		t.Errorf("conn_reused = %v (present %v), want true on the kept connection", got, ok)
	}
	if got := f.proxy.accepted.Load(); got != 1 {
		t.Fatalf("proxy accepted %d connections, want the healthy one kept", got)
	}
	if afterIdle <= baseline {
		t.Fatalf("no server bytes during the idle window (%d then %d): health-check pings are not being sent", baseline, afterIdle)
	}
}

// TestDeadHTTP1ConnectionIsReplacedAfterOneFailure runs in reuse mode, the only
// mode with a pooled connection to go dead: HTTP/1.1 drops a connection whose
// request timed out, so exactly one check fails.
func TestDeadHTTP1ConnectionIsReplacedAfterOneFailure(t *testing.T) {
	t.Parallel()
	f := newDeadConnFixture(t, false, true)
	f.warmUp(t, 1)

	f.proxy.blackhole()
	f.check.timeout = deadConnCheckTimeout
	if obs := f.run(t); obs.Status != check.StatusDown || obs.ErrorClass != check.ErrTimeout {
		t.Fatalf("check on the dead connection: status %v, class %v, want down/timeout", obs.Status, obs.ErrorClass)
	}

	f.check.timeout = deadConnSetupTimeout
	if obs := f.run(t); obs.Status != check.StatusUp {
		t.Fatalf("check after the failure: status %v, class %v, want up", obs.Status, obs.ErrorClass)
	}
	if got := f.proxy.accepted.Load(); got != 2 {
		t.Fatalf("proxy accepted %d connections, want 2", got)
	}
	if got := f.lastProto.Load(); got != 1 {
		t.Fatalf("control ran on HTTP/%d, want HTTP/1.1", got)
	}
}

// TestFreshModeNeverUsesDeadConnection is the default-mode counterpart of the
// replacement tests: with a fresh connection per check, the check right after
// a blackhole dials a new connection and succeeds, on HTTP/2 and HTTP/1.1.
func TestFreshModeNeverUsesDeadConnection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		h2    bool
		proto int64
	}{{"HTTP2", true, 2}, {"HTTP1", false, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newDeadConnFixture(t, tc.h2, false)
			f.warmUp(t, tc.proto)
			if _, ok := f.run(t).Extra["conn_reused"]; ok {
				t.Error("conn_reused present in fresh mode")
			}

			f.proxy.blackhole()
			f.check.timeout = deadConnCheckTimeout
			if obs := f.run(t); obs.Status != check.StatusUp {
				t.Fatalf("check after the blackhole: status %v, class %v, want up on a new connection", obs.Status, obs.ErrorClass)
			}
			if got := f.proxy.accepted.Load(); got != 3 {
				t.Fatalf("proxy accepted %d connections over 3 checks, want 3 (one per check)", got)
			}
			if got := f.lastProto.Load(); got != tc.proto {
				t.Fatalf("ran on HTTP/%d, want HTTP/%d", got, tc.proto)
			}
		})
	}
}

// certSwapServer is a TLS httptest server whose certificate can be replaced
// between checks. It serves the certificate set by swap through
// GetCertificate, which crypto/tls consults only when the client sends SNI, so
// reach it by host name (certSwapHost), not by IP.
type certSwapServer struct {
	srv   *httptest.Server
	roots *x509.CertPool // trusts every certificate issue returns

	caCert *x509.Certificate
	caKey  *ecdsa.PrivateKey
	serial atomic.Int64
	cert   atomic.Pointer[tls.Certificate]
}

const certSwapHost = "swap.test"

// newCertSwapServer starts the server with a certificate valid for 48 hours.
func newCertSwapServer(t *testing.T, enableHTTP2 bool, h nethttp.Handler) *certSwapServer {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Second)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "descry test CA"},
		NotBefore:             now.Add(-72 * time.Hour),
		NotAfter:              now.Add(72 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	s := &certSwapServer{roots: x509.NewCertPool(), caCert: caCert, caKey: caKey}
	s.roots.AddCert(caCert)
	s.serial.Store(1)
	s.swap(s.issue(t, now.Add(48*time.Hour)))

	s.srv = httptest.NewUnstartedServer(h)
	s.srv.EnableHTTP2 = enableHTTP2
	s.srv.TLS = &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return s.cert.Load(), nil
		},
	}
	s.srv.StartTLS()
	t.Cleanup(s.srv.Close)
	return s
}

// issue returns a leaf certificate for certSwapHost, signed by the test CA, that
// expires at notAfter (which may be in the past).
func (s *certSwapServer) issue(t *testing.T, notAfter time.Time) *tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	notAfter = notAfter.Truncate(time.Second)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(s.serial.Add(1)),
		Subject:      pkix.Name{CommonName: certSwapHost},
		DNSNames:     []string{certSwapHost},
		NotBefore:    notAfter.Add(-96 * time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, s.caCert, &key.PublicKey, s.caKey)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

// swap makes new handshakes present c. Established connections keep the
// certificate they were opened with.
func (s *certSwapServer) swap(c *tls.Certificate) { s.cert.Store(c) }

// wiringRule is appended to every TestSafeTransportWiring failure.
const wiringRule = "; see the standing rule in CONTRIBUTING.md#transport-and-check-changes: a transport change needs its fault case in verdict_matrix_test.go"

// TestSafeTransportWiring asserts the transport fields the verdict contract
// depends on, by name and in both modes. It does not compare whole structs, so
// a new stdlib field does not break it.
func TestSafeTransportWiring(t *testing.T) {
	const timeout = 7 * time.Second
	for _, reuse := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh", true: "reuse"}[reuse], func(t *testing.T) {
			tr := newSafeTransport(reuse, timeout)
			if tr.DisableKeepAlives == reuse {
				t.Errorf("DisableKeepAlives = %v, want %v%s", tr.DisableKeepAlives, !reuse, wiringRule)
			}
			if tr.Proxy != nil {
				t.Errorf("Proxy is set; a proxy would bypass the dial-time SSRF guard%s", wiringRule)
			}
			if tr.TLSHandshakeTimeout != timeout {
				t.Errorf("TLSHandshakeTimeout = %v, want the check timeout %v%s", tr.TLSHandshakeTimeout, timeout, wiringRule)
			}
			if tr.TLSClientConfig != nil && tr.TLSClientConfig.ClientSessionCache != nil {
				t.Errorf("TLSClientConfig has a ClientSessionCache; resumed sessions would skip the full handshake%s", wiringRule)
			}
			switch {
			case !reuse && tr.HTTP2 != nil:
				t.Errorf("HTTP2 = %+v, want nil in fresh mode%s", *tr.HTTP2, wiringRule)
			case reuse && tr.HTTP2 == nil:
				t.Errorf("HTTP2 config is nil in reuse mode; a dead pooled connection would never be detected%s", wiringRule)
			case reuse:
				if tr.HTTP2.SendPingTimeout != h2SendPingTimeout {
					t.Errorf("SendPingTimeout = %v, want %v%s", tr.HTTP2.SendPingTimeout, h2SendPingTimeout, wiringRule)
				}
				if tr.HTTP2.PingTimeout != h2PingTimeout {
					t.Errorf("PingTimeout = %v, want %v%s", tr.HTTP2.PingTimeout, h2PingTimeout, wiringRule)
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, err := tr.DialContext(ctx, "tcp", "127.0.0.1:1")
			if err == nil {
				_ = conn.Close()
			}
			if !errors.Is(err, ErrSSRFBlocked) {
				t.Errorf("loopback dial error = %v, want ErrSSRFBlocked%s", err, wiringRule)
			}
		})
	}

	// The Transport does not expose its dialer, so assert the one newSafeTransport
	// builds through the same constructor.
	d := newSafeDialer(timeout)
	if d.Timeout != timeout {
		t.Errorf("dialer Timeout = %v, want the check timeout %v%s", d.Timeout, timeout, wiringRule)
	}
	if d.KeepAlive != dialKeepAlive {
		t.Errorf("dialer KeepAlive = %v, want %v%s", d.KeepAlive, dialKeepAlive, wiringRule)
	}
	if d.ControlContext == nil {
		t.Errorf("dialer ControlContext is nil; the dial-time SSRF guard is off%s", wiringRule)
	}
}

package http

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
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
// scaling them down, so that removing the HTTP/2 config from newSafeTransport
// fails them directly. They run in parallel and take about 32s.

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

// newDeadConnFixture wires the real newSafeTransport to a TLS server behind a
// blackholeProxy. Only the dialer and the trust roots are replaced, because the
// SSRF dialer refuses loopback by design and the server's certificate is a test
// one.
func newDeadConnFixture(t *testing.T, enableHTTP2 bool) *deadConnFixture {
	t.Helper()
	var lastProto atomic.Int64
	srv := httptest.NewUnstartedServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		lastProto.Store(int64(r.ProtoMajor))
		w.WriteHeader(nethttp.StatusOK)
	}))
	srv.EnableHTTP2 = enableHTTP2
	srv.StartTLS()
	proxy := newBlackholeProxy(t, srv.Listener.Addr().String())

	tr := newSafeTransport()
	tr.DialContext = (&net.Dialer{}).DialContext
	tr.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    srv.Client().Transport.(*nethttp.Transport).TLSClientConfig.RootCAs,
	}
	c := newForTest(deadConnSetupTimeout)
	c.client.Transport = tr

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
	f := newDeadConnFixture(t, true)
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
	f := newDeadConnFixture(t, true)
	f.warmUp(t, 2)

	time.Sleep(time.Second) // let trailing frames from the first check settle
	baseline := f.proxy.serverToClient.Load()
	time.Sleep(h2SendPingTimeout + h2PingTimeout + 2*time.Second)
	afterIdle := f.proxy.serverToClient.Load() // before the next check adds its own bytes

	if obs := f.run(t); obs.Status != check.StatusUp {
		t.Fatalf("check after idle: status %v, class %v", obs.Status, obs.ErrorClass)
	}
	if got := f.proxy.accepted.Load(); got != 1 {
		t.Fatalf("proxy accepted %d connections, want the healthy one kept", got)
	}
	if afterIdle <= baseline {
		t.Fatalf("no server bytes during the idle window (%d then %d): health-check pings are not being sent", baseline, afterIdle)
	}
}

func TestDeadHTTP1ConnectionIsReplacedAfterOneFailure(t *testing.T) {
	t.Parallel()
	f := newDeadConnFixture(t, false)
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

func TestSafeTransportWiring(t *testing.T) {
	tr := newSafeTransport()
	if tr.Proxy != nil {
		t.Error("Proxy is set; a proxy would bypass the dial-time SSRF guard")
	}
	if tr.HTTP2 == nil {
		t.Fatal("HTTP2 config is nil; a dead pooled connection would never be detected")
	}
	if tr.HTTP2.SendPingTimeout != h2SendPingTimeout {
		t.Errorf("SendPingTimeout = %v, want %v", tr.HTTP2.SendPingTimeout, h2SendPingTimeout)
	}
	if tr.HTTP2.PingTimeout != h2PingTimeout {
		t.Errorf("PingTimeout = %v, want %v", tr.HTTP2.PingTimeout, h2PingTimeout)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := tr.DialContext(ctx, "tcp", "127.0.0.1:1")
	if err == nil {
		_ = conn.Close()
	}
	if !errors.Is(err, ErrSSRFBlocked) {
		t.Errorf("loopback dial error = %v, want ErrSSRFBlocked", err)
	}
}

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/fairbearlab/descry/check"
	"github.com/fairbearlab/descry/metrics"
	"github.com/fairbearlab/descry/runner"
	"github.com/fairbearlab/descry/sink"
)

// adminYAML is one loopback target: the SSRF guard refuses it without any
// network I/O (port 1 is not on the allowlist), so every run yields an
// ssrf_blocked observation and an event within one interval.
const adminYAML = "source: https://example.com/descry\ninterval: 1s\ntimeout: 500ms\ntargets:\n  - url: http://127.0.0.1:1/\n"

// scrape GETs path on the admin server at addr.
func scrape(t *testing.T, addr, path string) (int, string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return resp.StatusCode, string(b)
}

// metricValue returns the value of the sample whose name and labels (the line
// up to its last space) equal series exactly, as in
// `descry_check_errors_total{class="ssrf_blocked"}`.
func metricValue(body, series string) (float64, bool) {
	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		line := sc.Text()
		i := strings.LastIndexByte(line, ' ')
		if i < 0 || strings.HasPrefix(line, "#") || line[:i] != series {
			continue
		}
		v, err := strconv.ParseFloat(line[i+1:], 64)
		return v, err == nil
	}
	return 0, false
}

// hasFamily reports whether body has a sample or a TYPE line for name.
func hasFamily(body, name string) bool {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+"{") || strings.HasPrefix(line, name+" ") || strings.HasPrefix(line, "# TYPE "+name+" ") {
			return true
		}
	}
	return false
}

// scrapeValue scrapes /metrics and returns series' value, or -1 when the
// series is absent or the scrape fails.
func scrapeValue(addr, series string) float64 {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + "/metrics")
	if err != nil {
		return -1
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return -1
	}
	v, ok := metricValue(string(b), series)
	if !ok {
		return -1
	}
	return v
}

// adminRun is a run() started with --listen 127.0.0.1:0 in the background.
type adminRun struct {
	addr   string
	cancel func()     // the "signal"
	wait   func() int // run's exit code; safe to call more than once
	stderr *lockedBuffer
}

// startAdminRun writes yaml to a config file and starts run with --config,
// --listen 127.0.0.1:0 and extra. notifyContext is swapped for a context the
// returned cancel ends, and listen is wrapped (around whatever the test set)
// to capture the bound address. It returns once the listener is bound;
// cleanup cancels the run, waits for it and restores both seams.
func startAdminRun(t *testing.T, yaml string, extra ...string) *adminRun {
	t.Helper()
	cfgPath := filepath.Join(t.TempDir(), "descry.yaml")
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	origNotify, prevListen := notifyContext, listen
	notifyContext = func(context.Context, ...os.Signal) (context.Context, context.CancelFunc) {
		return ctx, func() {}
	}
	addrs := make(chan string, 1)
	listen = func(network, addr string) (net.Listener, error) {
		ln, err := prevListen(network, addr)
		if err == nil {
			addrs <- ln.Addr().String()
		}
		return ln, err
	}

	ar := &adminRun{cancel: cancel, stderr: &lockedBuffer{}}
	done := make(chan int, 1)
	args := append([]string{"--config", cfgPath, "--listen", "127.0.0.1:0"}, extra...)
	go func() { done <- run(args, io.Discard, ar.stderr) }()

	var once sync.Once
	code := -1
	ar.wait = func() int {
		once.Do(func() {
			select {
			case code = <-done:
			case <-time.After(15 * time.Second):
				t.Errorf("run did not return within 15s; stderr:\n%s", ar.stderr.String())
			}
		})
		return code
	}
	t.Cleanup(func() {
		cancel()
		ar.wait()
		notifyContext, listen = origNotify, prevListen
	})

	select {
	case ar.addr = <-addrs:
	case c := <-done:
		done <- c // keep it for wait
		t.Fatalf("run exited with %d before binding; stderr:\n%s", c, ar.stderr.String())
	case <-time.After(5 * time.Second):
		t.Fatalf("run did not bind within 5s; stderr:\n%s", ar.stderr.String())
	}
	return ar
}

// waitExit waits for the run to exit and fails unless it exits with want.
func (ar *adminRun) waitExit(t *testing.T, want int) {
	t.Helper()
	if code := ar.wait(); code != want {
		t.Fatalf("exit code = %d, want %d; stderr:\n%s", code, want, ar.stderr.String())
	}
}

// orderLog records shutdown steps in the order they happen.
type orderLog struct {
	mu    sync.Mutex
	steps []string
}

func (l *orderLog) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.steps = append(l.steps, s)
}

func (l *orderLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.steps...)
}

// loggingListener logs its first Close.
type loggingListener struct {
	net.Listener
	once sync.Once
	log  *orderLog
}

func (l *loggingListener) Close() error {
	l.once.Do(func() { l.log.add("admin listener closed") })
	return l.Listener.Close()
}

// loggingCloseSink accepts every event and logs its Close.
type loggingCloseSink struct{ log *orderLog }

func (loggingCloseSink) Publish(context.Context, cloudevents.Event) error { return nil }
func (s loggingCloseSink) Close() error {
	s.log.add("sink closed")
	return nil
}

// publishFailSink fails every Publish.
type publishFailSink struct{}

func (publishFailSink) Publish(context.Context, cloudevents.Event) error {
	return errors.New("publish refused")
}
func (publishFailSink) Close() error { return nil }

// swapFileSink points newFileSink at s for the rest of the test.
func swapFileSink(t *testing.T, s interface {
	sink.EventSink
	io.Closer
}) {
	t.Helper()
	orig := newFileSink
	t.Cleanup(func() { newFileSink = orig })
	newFileSink = func(string) (interface {
		sink.EventSink
		io.Closer
	}, error) {
		return s, nil
	}
}

// Value: protects=the probe contract: /readyz follows the ready flag (503, 200, 503), /healthz is 200 regardless, only GET/HEAD are routed and unknown paths 404
// Value: fails_when=readyz ignores the flag, healthz is tied to readiness, the mux accepts POST, or it falls through to DefaultServeMux
// Value: why_new=the admin mux is new; seam=none
func TestAdminMux_HealthAndReadiness(t *testing.T) {
	ready := &readyState{}
	mux := newAdminMux(prometheus.NewRegistry(), ready)
	do := func(method, path string) (int, string) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
		return rec.Code, rec.Body.String()
	}
	for i, step := range []struct {
		set       *bool
		wantReady int
	}{
		{nil, http.StatusServiceUnavailable},
		{ptr(true), http.StatusOK},
		{ptr(false), http.StatusServiceUnavailable},
	} {
		if step.set != nil {
			ready.set(*step.set)
		}
		if code, body := do(http.MethodGet, "/readyz"); code != step.wantReady {
			t.Errorf("step %d: /readyz = %d %q, want %d", i, code, body, step.wantReady)
		}
		if code, body := do(http.MethodGet, "/healthz"); code != http.StatusOK || body != "ok\n" {
			t.Errorf("step %d: /healthz = %d %q, want 200 ok", i, code, body)
		}
	}
	if code, _ := do(http.MethodHead, "/healthz"); code != http.StatusOK {
		t.Errorf("HEAD /healthz = %d, want 200", code)
	}
	if code, _ := do(http.MethodPost, "/healthz"); code != http.StatusMethodNotAllowed {
		t.Errorf("POST /healthz = %d, want 405", code)
	}
	if code, _ := do(http.MethodGet, "/nope"); code != http.StatusNotFound {
		t.Errorf("GET /nope = %d, want 404", code)
	}
	if code, _ := do(http.MethodGet, "/debug/pprof/"); code != http.StatusNotFound {
		t.Errorf("GET /debug/pprof/ = %d, want 404 (DefaultServeMux must not be exposed)", code)
	}
}

func ptr[T any](v T) *T { return &v }

// blockingCollector holds every Collect until release is closed, after
// signalling entered. Its empty Describe makes it an unchecked collector.
type blockingCollector struct {
	entered chan struct{}
	release chan struct{}
}

func (blockingCollector) Describe(chan<- *prometheus.Desc) {}
func (c blockingCollector) Collect(chan<- prometheus.Metric) {
	c.entered <- struct{}{}
	<-c.release
}

// Value: protects=the /metrics in-flight limit: a third concurrent scrape is refused with 503 instead of queueing another full gather
// Value: fails_when=MaxRequestsInFlight is dropped or raised, or the limit answers with something other than 503
// Value: why_new=the scrape handler is new; seam=a collector that blocks until released
func TestAdminMux_MetricsInFlightLimit(t *testing.T) {
	bc := blockingCollector{entered: make(chan struct{}, 4), release: make(chan struct{})}
	reg := prometheus.NewRegistry()
	reg.MustRegister(bc)
	srv := httptest.NewServer(newAdminMux(reg, &readyState{}))
	defer srv.Close()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(bc.release) }) }
	defer release()

	get := func() int {
		resp, err := srv.Client().Get(srv.URL + "/metrics")
		if err != nil {
			t.Errorf("GET /metrics: %v", err)
			return 0
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	codes := make(chan int, 2)
	for range 2 {
		go func() { codes <- get() }()
	}
	for i := range 2 {
		select {
		case <-bc.entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d scrapes reached Collect", i)
		}
	}
	if code := get(); code != http.StatusServiceUnavailable {
		t.Errorf("third concurrent scrape = %d, want 503", code)
	}
	release()
	for range 2 {
		if code := <-codes; code != http.StatusOK {
			t.Errorf("held scrape = %d, want 200", code)
		}
	}
}

// Value: protects=bind failure is a runtime error (exit 1) caught before the sink opens, so a port clash leaves no events file
// Value: fails_when=the bind moves after the sink is opened, maps to 2 or 0, or the message stops naming --listen
// Value: why_new=--listen is new; seam=none (a real listener holds the port)
func TestRun_ListenPortInUseExitsOneBeforeSink(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "descry.yaml")
	if err := os.WriteFile(cfgPath, []byte(adminYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	events := filepath.Join(dir, "events.jsonl")

	var stderr lockedBuffer
	code := run([]string{"--config", cfgPath, "--listen", held.Addr().String(), "--sink", "file", "--file", events}, io.Discard, &stderr)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "error: --listen") {
		t.Errorf("stderr does not name --listen: %q", stderr.String())
	}
	if _, err := os.Stat(events); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("events file created despite the bind failure (stat err = %v)", err)
	}
}

// Value: protects=v0.4.0 behaviour without --listen: no listener is bound and the run is otherwise unchanged
// Value: fails_when=the admin server or collector starts unconditionally
// Value: why_new=--listen is new; seam=listen counts calls, notifyContext is already cancelled
func TestRun_NoListenStartsNoServer(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "descry.yaml")
	if err := os.WriteFile(cfgPath, []byte(adminYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	origListen, origNotify := listen, notifyContext
	t.Cleanup(func() { listen, notifyContext = origListen, origNotify })
	listen = func(network, addr string) (net.Listener, error) {
		calls.Add(1)
		return net.Listen(network, addr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	notifyContext = func(context.Context, ...os.Signal) (context.Context, context.CancelFunc) {
		return ctx, func() {}
	}

	var stderr lockedBuffer
	if code := run([]string{"--config", cfgPath}, io.Discard, &stderr); code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr:\n%s", code, stderr.String())
	}
	if n := calls.Load(); n != 0 {
		t.Errorf("listen called %d times without --listen", n)
	}
	if strings.Contains(stderr.String(), "error:") {
		t.Errorf("stderr has an error: %q", stderr.String())
	}
}

// Value: protects=the served run end to end: readiness after start, liveness, a scrape with build info and the target count, then a clean signal shutdown that exits 0 and closes the port
// Value: fails_when=the server is not started, readiness never turns 200, the collector is not wired, ErrServerClosed is treated as a failure, or the listener outlives run
// Value: why_new=--listen is new; seam=notifyContext and listen (wrapped only to learn the bound port)
func TestRun_ListenServesAndExitsZeroOnSignal(t *testing.T) {
	ar := startAdminRun(t, adminYAML)
	waitFor(t, func() bool {
		code, _ := scrape(t, ar.addr, "/readyz")
		return code == http.StatusOK
	})
	if code, body := scrape(t, ar.addr, "/healthz"); code != http.StatusOK || body != "ok\n" {
		t.Errorf("/healthz = %d %q, want 200 ok", code, body)
	}
	code, body := scrape(t, ar.addr, "/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", code)
	}
	if !strings.Contains(body, "descry_build_info{") {
		t.Errorf("/metrics lacks descry_build_info:\n%s", body)
	}
	if v, ok := metricValue(body, "descry_runner_targets"); !ok || v != 1 {
		t.Errorf("descry_runner_targets = %v (found %v), want 1", v, ok)
	}

	ar.cancel()
	ar.waitExit(t, 0)
	if strings.Contains(ar.stderr.String(), "error:") {
		t.Errorf("clean shutdown printed an error: %q", ar.stderr.String())
	}
	if c, err := net.DialTimeout("tcp", ar.addr, time.Second); err == nil {
		_ = c.Close()
		t.Errorf("admin port %s still accepts connections after run returned", ar.addr)
	}
}

// failingListener's Accept fails with a non-temporary error, so Serve
// returns it at once.
type failingListener struct{ closed atomic.Bool }

func (*failingListener) Accept() (net.Conn, error) { return nil, errors.New("accept boom") }
func (l *failingListener) Close() error {
	l.closed.Store(true)
	return nil
}
func (*failingListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9} }

// Value: protects=an admin server failure is not silent: it cancels the run, prints the cause exactly once and exits 1, even though the cancellation itself would map to 0
// Value: fails_when=serveAdmin drops the Serve error, does not cancel the run (run would never return here), the code stays 0, or the cause is printed both when it happens and again at exit
// Value: why_new=--listen is new; seam=listen returns a listener whose Accept fails; the test never cancels the signal context
func TestRun_AdminServeErrorExitsOne(t *testing.T) {
	origListen := listen
	t.Cleanup(func() { listen = origListen })
	fl := &failingListener{}
	listen = func(string, string) (net.Listener, error) { return fl, nil }

	ar := startAdminRun(t, adminYAML)
	ar.waitExit(t, 1)
	out := ar.stderr.String()
	if n := strings.Count(out, "error: admin server:"); n != 1 || !strings.Contains(out, "accept boom") {
		t.Errorf("stderr reports the server failure %d times, want once with its cause: %q", n, out)
	}
	if strings.Contains(out, "context canceled") {
		t.Errorf("the run's own cancellation was reported as an error: %q", out)
	}
	if !fl.closed.Load() {
		t.Error("listener was never closed")
	}
}

// gatedFailListener's Accept blocks until fail is closed and then fails with
// a non-temporary error, so the test picks when Serve returns. Close unblocks
// a pending Accept.
type gatedFailListener struct {
	fail      chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func newGatedFailListener() *gatedFailListener {
	return &gatedFailListener{fail: make(chan struct{}), closed: make(chan struct{})}
}

func (l *gatedFailListener) Accept() (net.Conn, error) {
	select {
	case <-l.fail:
		return nil, errors.New("accept boom")
	case <-l.closed:
		return nil, net.ErrClosed
	}
}
func (l *gatedFailListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}
func (*gatedFailListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}
}

// Value: protects=an admin server failure is on stderr while the run is still shutting down, not only once it exits, so a second signal that kills a slow shutdown does not lose the cause
// Value: fails_when=the cause is printed only after Run returns (as the exit-path summary), or is not printed until the drain finishes
// Value: why_new=the early report is new; seam=listen returns a listener whose Accept fails when the test says, newFileSink returns a sink whose Publish blocks until released, so Run waits on its worker after the cancel
func TestRun_AdminServeErrorPrintedBeforeExit(t *testing.T) {
	origListen := listen
	t.Cleanup(func() { listen = origListen })
	gl := newGatedFailListener()
	listen = func(string, string) (net.Listener, error) { return gl, nil }
	bs := newBlockingSink()
	swapFileSink(t, bs)

	ar := startAdminRun(t, adminYAML, "--sink", "file", "--file", filepath.Join(t.TempDir(), "events.jsonl"))
	t.Cleanup(bs.unblock) // runs before startAdminRun's cleanup waits for the run
	select {
	case <-bs.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("no Publish within 10s")
	}
	// Run cannot return while the publish is held, so the line seen here was
	// printed before the run finished.
	close(gl.fail)
	waitFor(t, func() bool { return strings.Contains(ar.stderr.String(), "error: admin server: accept boom") })
	bs.unblock()
	ar.waitExit(t, 1)
	if n := strings.Count(ar.stderr.String(), "error: admin server:"); n != 1 {
		t.Errorf("the server failure was printed %d times, want once: %q", n, ar.stderr.String())
	}
}

// Value: protects=the wiring of the collector around the check the runner actually runs: a guarded probe shows up in errors_total and the per-target up gauge
// Value: fails_when=c.Check is not applied, is applied to a check the runner does not use, or SetTargetCount is skipped (SetRunner: TestRun_SkippedSlotsReachMetrics)
// Value: why_new=--listen is new; seam=notifyContext and listen
func TestRun_MetricsCountSSRFBlocked(t *testing.T) {
	// Two targets, so each gets its own per-target series.
	ar := startAdminRun(t, adminYAML+"  - url: http://127.0.0.1:2/x\n")
	// The port allowlist rejects :1 and :2 before the IP layer, so each run
	// is an ssrf_blocked observation without any network I/O.
	waitFor(t, func() bool { return scrapeValue(ar.addr, `descry_check_errors_total{class="ssrf_blocked"}`) >= 2 })
	waitFor(t, func() bool { return scrapeValue(ar.addr, `descry_check_up{url="http://127.0.0.1:2/x"}`) == 0 })
	_, body := scrape(t, ar.addr, "/metrics")
	if v, ok := metricValue(body, `descry_check_up{url="http://127.0.0.1:1/"}`); !ok || v != 0 {
		t.Errorf("descry_check_up for the first target = %v (found %v), want 0:\n%s", v, ok, body)
	}
	if v, ok := metricValue(body, "descry_runner_targets"); !ok || v != 2 {
		t.Errorf("descry_runner_targets = %v (found %v), want 2", v, ok)
	}
	ar.cancel()
	ar.waitExit(t, 0)
}

// blockingSink holds every Publish until release is closed, ignoring the
// context, so the worker that publishes stays in flight: the target's next
// slots are skipped, and a cancelled Run waits for it. entered is signalled
// on each Publish when someone is waiting for it.
type blockingSink struct {
	entered chan struct{}
	release chan struct{}
	once    *sync.Once
}

func newBlockingSink() blockingSink {
	return blockingSink{entered: make(chan struct{}), release: make(chan struct{}), once: &sync.Once{}}
}

func (s blockingSink) Publish(context.Context, cloudevents.Event) error {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-s.release
	return nil
}
func (blockingSink) Close() error { return nil }
func (s blockingSink) unblock()   { s.once.Do(func() { close(s.release) }) }

// Value: protects=SetRunner wiring: skipped_slots_total reads the runner the CLI built, so a slot skipped behind a stuck publish is counted
// Value: fails_when=SetRunner is skipped or given another runner (the series is registered either way and reads 0 without a runner)
// Value: why_new=--listen is new; seam=newFileSink returns a sink whose Publish blocks until released
func TestRun_SkippedSlotsReachMetrics(t *testing.T) {
	bs := newBlockingSink()
	swapFileSink(t, bs)
	ar := startAdminRun(t, adminYAML, "--sink", "file", "--file", filepath.Join(t.TempDir(), "events.jsonl"))
	t.Cleanup(bs.unblock) // runs before startAdminRun's cleanup waits for the run
	// The first run's publish holds its worker, so the next 1s slot is skipped.
	waitFor(t, func() bool { return scrapeValue(ar.addr, "descry_runner_skipped_slots_total") >= 1 })
	bs.unblock()
	ar.cancel()
	ar.waitExit(t, 0)
}

// Value: protects=/readyz turns 503 at the first signal and stays 503 while the run drains, while /healthz stays 200 until the server stops
// Value: fails_when=the signal path stops clearing the ready flag (load balancers would keep routing to a probe that is shutting down), or /healthz follows readiness
// Value: why_new=readiness was only tested on the mux, never through run's shutdown; seam=newFileSink returns a sink whose Publish blocks until released, so Run waits on its worker after cancel
func TestRun_ReadyzTurns503OnSignal(t *testing.T) {
	bs := newBlockingSink()
	swapFileSink(t, bs)
	ar := startAdminRun(t, adminYAML, "--sink", "file", "--file", filepath.Join(t.TempDir(), "events.jsonl"))
	t.Cleanup(bs.unblock) // runs before startAdminRun's cleanup waits for the run
	select {
	case <-bs.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("no Publish within 10s")
	}
	if code, body := scrape(t, ar.addr, "/readyz"); code != http.StatusOK {
		t.Fatalf("/readyz while running = %d %q, want 200", code, body)
	}

	ar.cancel()
	waitFor(t, func() bool {
		code, _ := scrape(t, ar.addr, "/readyz")
		return code == http.StatusServiceUnavailable
	})
	if code, body := scrape(t, ar.addr, "/healthz"); code != http.StatusOK || body != "ok\n" {
		t.Errorf("/healthz while draining = %d %q, want 200 ok", code, body)
	}
	bs.unblock()
	ar.waitExit(t, 0)
}

// Value: protects=result_errors_total reads the drain's live failure count, so a publish failure shows up while the run is still going, not only after shutdown
// Value: fails_when=ResultErrors is not wired, or is fed from a value stored only when the drain returns
// Value: why_new=--listen is new; seam=newFileSink returns a sink whose Publish always fails
func TestRun_ResultErrorsVisibleBeforeCancel(t *testing.T) {
	swapFileSink(t, publishFailSink{})
	ar := startAdminRun(t, adminYAML, "--sink", "file", "--file", filepath.Join(t.TempDir(), "events.jsonl"))
	waitFor(t, func() bool { return scrapeValue(ar.addr, "descry_runner_result_errors_total") >= 1 })
	ar.cancel()
	ar.waitExit(t, 0)
}

// Value: protects=shutdown order: the admin server is shut down (its listener closed) before the file sink closes, so the last scrape cannot outlive the sink or race its Close
// Value: fails_when=the admin shutdown moves into a defer that runs after the sink's Close, or is dropped and left to the listener defer
// Value: why_new=--listen is new; seam=listen wraps the real listener to log its Close, newFileSink returns a sink that logs its Close
func TestRun_AdminShutdownBeforeSinkClose(t *testing.T) {
	log := &orderLog{}
	origListen := listen
	t.Cleanup(func() { listen = origListen })
	listen = func(network, addr string) (net.Listener, error) {
		ln, err := origListen(network, addr)
		if err != nil {
			return nil, err
		}
		return &loggingListener{Listener: ln, log: log}, nil
	}
	swapFileSink(t, loggingCloseSink{log: log})

	ar := startAdminRun(t, adminYAML, "--sink", "file", "--file", filepath.Join(t.TempDir(), "events.jsonl"))
	waitFor(t, func() bool {
		code, _ := scrape(t, ar.addr, "/readyz")
		return code == http.StatusOK
	})
	ar.cancel()
	ar.waitExit(t, 0)
	if got, want := log.get(), []string{"admin listener closed", "sink closed"}; !reflect.DeepEqual(got, want) {
		t.Errorf("shutdown order = %q, want %q", got, want)
	}
}

// Value: protects=--metrics-per-target=false: no series carries a url label, while the fleet series stay
// Value: fails_when=the flag is not passed to DisablePerTarget, or is inverted
// Value: why_new=the flag is new; seam=notifyContext and listen
func TestRun_MetricsPerTargetOff(t *testing.T) {
	perTarget := []string{
		"descry_check_up", "descry_check_status_code", "descry_check_latency_seconds",
		"descry_check_last_run_timestamp_seconds", "descry_check_tls_expiry_timestamp_seconds", "descry_check_runs_total",
	}
	const blocked = `descry_check_errors_total{class="ssrf_blocked"}`

	ar := startAdminRun(t, adminYAML, "--metrics-per-target=false")
	waitFor(t, func() bool { return scrapeValue(ar.addr, blocked) >= 1 })
	_, body := scrape(t, ar.addr, "/metrics")
	for _, name := range perTarget {
		if hasFamily(body, name) {
			t.Errorf("per-target family %s served with --metrics-per-target=false", name)
		}
	}
	if strings.Contains(body, `url="`) {
		t.Errorf("a url label is served with --metrics-per-target=false:\n%s", body)
	}
	for _, name := range []string{"descry_check_duration_seconds_count", blocked, "descry_runner_targets", "descry_build_info{"} {
		if !strings.Contains(body, name) {
			t.Errorf("fleet series %s missing:\n%s", name, body)
		}
	}
	ar.cancel()
	ar.waitExit(t, 0)

	// Control: the default serves the per-target series for the same target.
	ar = startAdminRun(t, adminYAML)
	const up = `descry_check_up{url="http://127.0.0.1:1/"}`
	waitFor(t, func() bool { return scrapeValue(ar.addr, up) == 0 })
	ar.cancel()
	ar.waitExit(t, 0)
}

// Value: protects=skips_observed_total counts every skip by kind, before the stderr rate limit drops lines
// Value: fails_when=observeSkip moves after the rate limit, the kinds are swapped (ErrSkippedQueued tested after ErrSkipped), or skips count as failures
// Value: why_new=the drain's skip observer is new; seam=none (a real Collector scraped through the admin mux)
func TestDrainResults_ObservesSkipsBeforeRateLimit(t *testing.T) {
	c, err := metrics.New(metrics.Options{})
	if err != nil {
		t.Fatal(err)
	}
	a := check.Target{URL: "https://a.example/"}
	b := check.Target{URL: "https://b.example/"}
	ch := make(chan runner.Result, 4)
	ch <- runner.Result{Target: b, Err: runner.ErrSkippedQueued}
	ch <- runner.Result{Target: a, Err: runner.ErrSkipped}
	ch <- runner.Result{Target: a, Err: runner.ErrSkipped} // same target, same window: line suppressed
	close(ch)
	clock := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	var out lockedBuffer
	var failures atomic.Int64
	drainResults(ch, &out, 30*time.Second, func() time.Time { return clock }, &failures, c.ObserveSkip)

	rec := httptest.NewRecorder()
	newAdminMux(c.Registry(), &readyState{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for series, want := range map[string]float64{
		`descry_runner_skips_observed_total{kind="queued"}`:    1,
		`descry_runner_skips_observed_total{kind="in_flight"}`: 2,
	} {
		if v, ok := metricValue(body, series); !ok || v != want {
			t.Errorf("%s = %v (found %v), want %v", series, v, ok, want)
		}
	}
	if n := strings.Count(out.String(), "prior run still running"); n != 1 {
		t.Errorf("in-flight skip lines = %d, want 1 (the second is rate-limited):\n%s", n, out.String())
	}
	if got := failures.Load(); got != 0 {
		t.Errorf("failures = %d, want 0 (skips are not failures)", got)
	}
}

// stalledWriter is a ResponseWriter whose Write blocks until release is
// closed, after signalling writing: a client that sent GET /metrics and then
// stopped reading.
type stalledWriter struct {
	header  http.Header
	writing chan struct{}
	release chan struct{}
}

func (w *stalledWriter) Header() http.Header { return w.header }
func (*stalledWriter) WriteHeader(int)       {}
func (w *stalledWriter) Write(p []byte) (int, error) {
	w.writing <- struct{}{}
	<-w.release
	return len(p), nil
}

// Value: protects=a /metrics response stuck in its write keeps its in-flight slot, so at most metricsMaxInFlight buffered bodies are held at once and a further scrape gets 503 even though every gather has finished
// Value: fails_when=the outer limiter is dropped or moved inside promhttp's timeout handler (whose own in-flight slot is freed before the buffered body is written), or answers with something other than 503
// Value: why_new=the outer limiter is new; seam=mux.ServeHTTP with a ResponseWriter whose Write blocks
func TestAdminMux_MetricsSlotHeldThroughWrite(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{Name: "descry_test_gauge", Help: "x"}))
	mux := newAdminMux(reg, &readyState{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	var wg sync.WaitGroup
	// One cleanup so the release always runs before the wait (cleanups are
	// LIFO); a failure with scrapes still stuck then fails instead of hanging.
	t.Cleanup(func() { releaseAll(); wg.Wait() })
	for i := range metricsMaxInFlight {
		w := &stalledWriter{header: http.Header{}, writing: make(chan struct{}, 1), release: release}
		wg.Add(1)
		go func() {
			defer wg.Done()
			mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		}()
		select {
		case <-w.writing: // gathered; the body write is stuck
		case <-time.After(5 * time.Second):
			t.Fatalf("scrape %d never reached its write", i+1)
		}
	}

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("scrape while %d writes are stuck = %d, want 503", metricsMaxInFlight, rec.Code)
	}
	releaseAll()
	wg.Wait()
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("scrape after the stuck writes finished = %d, want 200", rec.Code)
	}
}

// Value: protects=a stuck admin request cannot hold shutdown past its timeout: the server is closed, the stuck client is cut off, and shutdown reports no error (the exit code is not changed by it)
// Value: fails_when=the Close fallback is dropped (the stuck connection stays open after shutdown returns), or a Shutdown timeout is returned as a Serve error
// Value: why_new=the admin server is new; seam=a handler that blocks until the test releases it
func TestAdminServer_ShutdownTimeoutForcesClose(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var cancelled atomic.Bool
	a := serveAdmin(ln, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	}), func(error) { cancelled.Store(true) })

	clientErr := make(chan error, 1)
	go func() {
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Get("http://" + ln.Addr().String() + "/")
		if err == nil {
			_ = resp.Body.Close()
		}
		clientErr <- err
	}()
	<-entered

	start := time.Now()
	if err := a.shutdown(50 * time.Millisecond); err != nil {
		t.Errorf("shutdown = %v, want nil (a timeout is logged, not returned)", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("shutdown took %v with a 50ms timeout", d)
	}
	// Shutdown returns promptly either way (it closes the listener, so Serve
	// returns); only Close cuts the stuck connection. Wait well under the
	// client's own timeout, so a client that gives up by itself is not
	// mistaken for one that was cut off.
	select {
	case err := <-clientErr:
		if err == nil {
			t.Error("the stuck request completed; want its connection closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the stuck request was not cut off by shutdown")
	}
	if cancelled.Load() {
		t.Error("a clean shutdown cancelled the run")
	}
}

// Value: protects=the admin server's documented hardening: header read 5s, whole-request read 10s, response write 30s (well above the 10s scrape timeout), idle backstop 60s, about 8 KiB of headers, and keep-alives off (every response closes its connection, so an idle client cannot hold a connection slot)
// Value: fails_when=a timeout or the header cap is dropped or changed from the documented value, or keep-alives are left on
// Value: why_new=no test asserted the server's limits; seam=none (the returned adminServer's srv fields, and one real request)
func TestServeAdmin_ServerLimits(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := serveAdmin(ln, http.NotFoundHandler(), func(error) {})
	t.Cleanup(func() { _ = a.shutdown(time.Second) })
	for _, c := range []struct {
		name      string
		got, want time.Duration
	}{
		{"ReadHeaderTimeout", a.srv.ReadHeaderTimeout, 5 * time.Second},
		{"ReadTimeout", a.srv.ReadTimeout, 10 * time.Second},
		{"IdleTimeout", a.srv.IdleTimeout, 60 * time.Second},
		{"WriteTimeout", a.srv.WriteTimeout, 30 * time.Second},
	} {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if a.srv.MaxHeaderBytes != 8<<10 {
		t.Errorf("MaxHeaderBytes = %d, want %d", a.srv.MaxHeaderBytes, 8<<10)
	}

	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get("http://" + ln.Addr().String() + "/")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if !resp.Close {
		t.Error("the response kept the connection open; want Connection: close (keep-alives off)")
	}
}

// acceptLoop accepts from l until it fails, sending each conn on the returned
// channel.
func acceptLoop(l net.Listener) <-chan net.Conn {
	conns := make(chan net.Conn)
	go func() {
		defer close(conns)
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			conns <- c
		}
	}()
	return conns
}

// countingListener counts calls to Accept, made or in progress.
type countingListener struct {
	net.Listener
	calls atomic.Int32
}

func (l *countingListener) Accept() (net.Conn, error) {
	l.calls.Add(1)
	return l.Listener.Accept()
}

// Value: protects=the admin connection cap: a connection over the limit is not accepted from the kernel (it waits in the backlog, holding no descriptor here) until an accepted one closes, and an Accept error gives its slot back
// Value: fails_when=Accept takes no slot or takes it after the inner Accept returns (the inner Accept is then called while the only slot is taken), a closed conn does not give its slot back, or a failed Accept keeps its slot (the server would stall after a few transient errors)
// Value: why_new=the cap is new; seam=newLimitListener with a cap of 1 (production uses adminMaxConns) over a listener that counts inner Accept calls; the not-yet-accepted check waits 200ms
func TestLimitListener_CapsOpenConns(t *testing.T) {
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	inner := &countingListener{Listener: tcp}
	l := newLimitListener(inner, 1)
	t.Cleanup(func() { _ = l.Close() })
	conns := acceptLoop(l)

	dial := func() {
		c, err := net.DialTimeout("tcp", inner.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
	}
	dial()
	var first net.Conn
	select {
	case first = <-conns:
	case <-time.After(5 * time.Second):
		t.Fatal("the first connection was not accepted")
	}
	dial() // completes the TCP handshake in the kernel backlog
	select {
	case c := <-conns:
		_ = c.Close()
		t.Fatal("a second connection was accepted while the only slot was taken")
	case <-time.After(200 * time.Millisecond):
	}
	// The accept loop is waiting for a slot; it must not have taken the
	// pending connection from the kernel meanwhile.
	if n := inner.calls.Load(); n != 1 {
		t.Errorf("inner Accept calls while the only slot was taken = %d, want 1", n)
	}
	_ = first.Close()
	select {
	case c := <-conns:
		_ = c.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("the second connection was not accepted after the first closed")
	}

	// A failed Accept gives its slot back: with a cap of 1, a second Accept
	// would otherwise wait forever.
	fl := newLimitListener(&failingListener{}, 1)
	for i := range 2 {
		errc := make(chan error, 1)
		go func() {
			_, err := fl.Accept()
			errc <- err
		}()
		select {
		case err := <-errc:
			if err == nil {
				t.Fatalf("Accept %d on a failing listener returned no error", i+1)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Accept %d blocked: an earlier failed Accept kept its slot", i+1)
		}
	}
}

// Value: protects=a connection gives its slot back once, however often it is closed (net/http may close a conn more than once)
// Value: fails_when=Close releases on every call, so a double Close frees another live connection's slot and the cap is exceeded
// Value: why_new=the cap is new; seam=newLimitListener with a cap of 2, reading the semaphore's fill
func TestLimitListener_DoubleCloseReleasesOnce(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := newLimitListener(inner, 2)
	t.Cleanup(func() { _ = l.Close() })
	// Accepted one at a time here, with no Accept left waiting to take a
	// freed slot.
	var accepted []net.Conn
	for range 2 {
		c, err := net.DialTimeout("tcp", inner.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		sc, err := l.Accept()
		if err != nil {
			t.Fatal(err)
		}
		accepted = append(accepted, sc)
	}
	// Closed underneath its wrapper: if the double Close below freed this
	// conn's slot too, a wrapper Close here would wait on an empty semaphore.
	t.Cleanup(func() { _ = accepted[1].(*limitConn).Conn.Close() })
	_ = accepted[0].Close()
	_ = accepted[0].Close()
	if n := len(l.sem); n != 1 {
		t.Errorf("slots held after closing one of two conns twice = %d, want 1", n)
	}
}

// Value: protects=Shutdown is not held up by a full server: closing the listener wakes an Accept waiting for a slot
// Value: fails_when=Accept waits only on the semaphore, so with every slot taken Serve never returns and shutdown hangs
// Value: why_new=the cap is new; seam=newLimitListener with a cap of 1 and its only slot held
func TestLimitListener_CloseUnblocksAccept(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := newLimitListener(inner, 1)
	c, err := net.DialTimeout("tcp", inner.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	held, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close() })

	errc := make(chan error, 1)
	go func() {
		_, err := l.Accept() // waits for the held slot
		errc <- err
	}()
	_ = l.Close()
	select {
	case err := <-errc:
		if !errors.Is(err, net.ErrClosed) {
			t.Errorf("Accept after Close = %v, want net.ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not unblock an Accept waiting for a slot")
	}
}

// Value: protects=the admin server serves at most adminMaxConns connections at once: one more is not served until a served one closes
// Value: fails_when=serveAdmin stops wrapping ln in newLimitListener(ln, adminMaxConns), or wraps it with another cap
// Value: why_new=the limit listener was only tested on its own; seam=none (the handler passed to serveAdmin holds each request until released); the not-yet-served check waits 200ms
func TestServeAdmin_CapsOpenConns(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, adminMaxConns+1) // never blocks a handler
	release := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
		// Close after responding, so a released request frees its slot
		// (keep-alives are off anyway; this keeps the test about the cap).
		w.Header().Set("Connection", "close")
	})
	a := serveAdmin(ln, h, func(error) {})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		releaseAll()
		_ = a.shutdown(time.Second)
	})

	// Each connection holds one request in the handler, so its slot stays
	// taken.
	send := func() {
		c, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		if _, err := io.WriteString(c, "GET / HTTP/1.1\r\nHost: admin\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
	}
	waitEntered := func(what string) {
		t.Helper()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s was not served within 5s", what)
		}
	}
	for range adminMaxConns {
		send()
	}
	for i := range adminMaxConns {
		waitEntered(fmt.Sprintf("connection %d", i+1))
	}

	send() // one over the cap: waits in the kernel backlog
	select {
	case <-entered:
		t.Fatalf("connection %d was served while %d were open", adminMaxConns+1, adminMaxConns)
	case <-time.After(200 * time.Millisecond):
	}
	release <- struct{}{} // finishes one held request; its connection closes
	waitEntered(fmt.Sprintf("connection %d, after one closed,", adminMaxConns+1))
	releaseAll()
}

// noCloseWriteConn is a net.Conn with no CloseWrite method.
type noCloseWriteConn struct{ net.Conn }

// Value: protects=limitConn forwards net/http's TCP half-close, so a client sent an error response (an oversized header, say) reads it and a clean EOF rather than a reset, while the connection stays open for reading; over a conn with no half-close it returns without panicking
// Value: fails_when=CloseWrite is dropped from limitConn (net/http's type check then misses it and skips the half-close) or stops forwarding, or it panics on a conn without CloseWrite
// Value: why_new=CloseWrite on the wrapper is new; seam=a limitConn built over a real TCP conn and over one with the method hidden
func TestLimitConn_CloseWriteForwardsHalfClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	client, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	server, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	var c net.Conn = &limitConn{Conn: server, release: func() {}}
	cw, ok := c.(interface{ CloseWrite() error })
	if !ok {
		t.Fatal("limitConn has no CloseWrite method; net/http would skip the half-close")
	}
	if err := cw.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite = %v", err)
	}
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if n, err := client.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Errorf("client read after CloseWrite = %d, %v; want 0, EOF", n, err)
	}
	// Only the write side is closed: the server still reads.
	if _, err := client.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
	if n, err := c.Read(make([]byte, 1)); n != 1 || err != nil {
		t.Errorf("server read after CloseWrite = %d, %v; want 1, nil", n, err)
	}

	// Over a conn without CloseWrite there is nothing to forward.
	p1, p2 := net.Pipe()
	t.Cleanup(func() { _ = p1.Close(); _ = p2.Close() })
	nc := &limitConn{Conn: noCloseWriteConn{p1}, release: func() {}}
	if err := nc.CloseWrite(); err != nil {
		t.Errorf("CloseWrite over a conn without it = %v, want nil", err)
	}
}

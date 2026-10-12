package main

import (
	"bufio"
	"context"
	"errors"
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

// Value: protects=an admin server failure is not silent: it cancels the run, prints the cause and exits 1, even though the cancellation itself would map to 0
// Value: fails_when=serveAdmin drops the Serve error, does not cancel the run (run would never return here), or the code stays 0
// Value: why_new=--listen is new; seam=listen returns a listener whose Accept fails; the test never cancels the signal context
func TestRun_AdminServeErrorExitsOne(t *testing.T) {
	origListen := listen
	t.Cleanup(func() { listen = origListen })
	fl := &failingListener{}
	listen = func(string, string) (net.Listener, error) { return fl, nil }

	ar := startAdminRun(t, adminYAML)
	ar.waitExit(t, 1)
	out := ar.stderr.String()
	if !strings.Contains(out, "error: admin server:") || !strings.Contains(out, "accept boom") {
		t.Errorf("stderr does not report the server failure: %q", out)
	}
	if strings.Contains(out, "context canceled") {
		t.Errorf("the run's own cancellation was reported as an error: %q", out)
	}
	if !fl.closed.Load() {
		t.Error("listener was never closed")
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
	}), func() { cancelled.Store(true) })

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

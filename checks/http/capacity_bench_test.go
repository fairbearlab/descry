//go:build unix

package http

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	nethttp "net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/fairbearlab/descry/check"
	"github.com/fairbearlab/descry/event"
	"github.com/fairbearlab/descry/runner"
)

// Cold-connection capacity harness (spec E9). Two things live here, both kept
// out of the default `go test` run:
//
//   - TestColdConnectionCapacity: a Test gated by DESCRY_CAPACITY=1. It is a
//     Test and not a Benchmark because it asserts a pass bar, runs for a couple
//     of minutes in real time, and is a one-shot sizing check; a Benchmark's
//     b.N calibration loop would rerun it and has no way to fail on a bar.
//   - BenchmarkCheckRun: a real Benchmark (runs only with -bench) comparing
//     fresh and reuse mode per check: ns/op, allocs/op, cpu-ns/op and
//     conns/op, which is the connection churn.
//
// Run:
//
//	DESCRY_CAPACITY=1 go test -run TestColdConnectionCapacity -v -timeout 15m ./checks/http/
//	go test -run '^$' -bench BenchmarkCheckRun -benchmem ./checks/http/
//
// What it models. Fresh mode (the default) pays a TCP connect, a TLS handshake
// and the request on every check, so a check's duration is its cold-connection
// cost. The harness uses the real runner.New and the real newSafeTransport(false,
// timeout) against a loopback TLS HTTP/2 server, and replaces only the dialer:
// every dial first waits a fixed 50ms or 200ms, standing in for the network
// round trips of a cold connection to a distant target. The real local TLS
// handshake and request still run on top, so the measured p99 is the injected
// latency plus a few real milliseconds. Only the dial is delayed (the handshake
// and request round trips are not each delayed), so the injected figure is the
// whole cold-connection cost, not a per-round-trip one.
//
// Pass bar, stated before the first run: for each injected latency, concurrency
// is taken from the sizing formula in docs/OPERATIONS.md using the cold p99
// measured by a calibration pass of the same check (8 concurrent checks, 400
// samples):
//
//	concurrency = ceil(2 * N * p99 / interval)      (2x headroom, at least 1)
//
// and then, with that concurrency, a real-time run must show
//
//   - zero skipped slots (Runner.Skipped() == 0, both kinds) and Dropped() == 0,
//   - zero not-run results and zero other result errors,
//   - zero down verdicts (the server is healthy),
//   - zero requests on a connection the check did not dial itself
//     (GotConnInfo.Reused), and
//   - no leaked goroutines or descriptors once the runner has stopped,
//
// at 44 targets on a 60s interval (run for 2 intervals) and at 500 targets on a
// 10s interval (run for 3 intervals). Nothing is time-compressed: intervals,
// latencies and run lengths are real, and the four cases run in parallel.
//
// The fresh-mode contract is one new connection per check. Run gives each
// check its own clone of the transport, because on a shared one net/http
// hands a freshly handshaken HTTP/2 connection to any request waiting on its
// own dial, even with DisableKeepAlives: before that change, at 500
// concurrent targets about 1% of requests ran on another check's connection
// and dials per check was about 1.13. Measured since, in all four cases:
// reused=0 and dials per check 1.000. The harness logs got_conn, reused and
// dials, and fails on any reused request. TIME_WAIT sockets for the server
// port are sampled from netstat every 2s (best effort; the peak is reported).

const capacityEnv = "DESCRY_CAPACITY"

type capacityCase struct {
	name     string
	targets  int
	interval time.Duration
	runFor   time.Duration
	latency  time.Duration
}

func skipUnlessCapacity(t *testing.T) {
	t.Helper()
	if os.Getenv(capacityEnv) != "1" {
		t.Skipf("capacity harness: set %s=1 to run (real time, about 2 minutes)", capacityEnv)
	}
	if testing.Short() {
		t.Skip("capacity harness: skipped under -short")
	}
	if raceEnabled {
		t.Skip("capacity harness: skipped under the race detector")
	}
}

// capacityServer is a loopback TLS HTTP/2 server that counts accepted
// connections.
type capacityServer struct {
	srv      *httptest.Server
	accepted atomic.Int64
	port     string
}

func newCapacityServer(t *testing.T) *capacityServer {
	t.Helper()
	cs := &capacityServer{}
	cs.srv = httptest.NewUnstartedServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		w.WriteHeader(nethttp.StatusOK)
	}))
	cs.srv.EnableHTTP2 = true
	cs.srv.Config.ConnState = func(_ net.Conn, s nethttp.ConnState) {
		if s == nethttp.StateNew {
			cs.accepted.Add(1)
		}
	}
	cs.srv.StartTLS()
	t.Cleanup(cs.srv.Close)
	_, cs.port, _ = net.SplitHostPort(cs.srv.Listener.Addr().String())
	return cs
}

// coldCheck is the real HTTP check on the real fresh-mode transport, with the
// dialer replaced by one that waits latency before connecting.
func newColdCheck(cs *capacityServer, timeout, latency time.Duration) (*Check, *nethttp.Transport) {
	tr := newLoopbackSafeTransport(false, timeout, serverRoots(cs.srv))
	inner := tr.DialContext
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		select {
		case <-time.After(latency):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return inner(ctx, network, addr)
	}
	return newCheckWithTransport(timeout, tr), tr
}

// recorder wraps a Check and records every completed run's duration and verdict.
type recorder struct {
	inner check.Check
	mu    sync.Mutex
	durs  []time.Duration
	down  atomic.Int64
	runs  atomic.Int64

	// From net/http/httptrace, per request attempt (a redirect hop is one).
	gotConn atomic.Int64
	reused  atomic.Int64 // GotConn with Reused: must stay 0 in fresh mode
	dials   atomic.Int64 // ConnectStart: TCP dials begun
}

func (r *recorder) Name() string { return r.inner.Name() }

func (r *recorder) Run(ctx context.Context, t check.Target) (check.Observation, error) {
	start := time.Now()
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(i httptrace.GotConnInfo) {
			r.gotConn.Add(1)
			if i.Reused {
				r.reused.Add(1)
			}
		},
		ConnectStart: func(_, _ string) { r.dials.Add(1) },
	})
	obs, err := r.inner.Run(ctx, t)
	d := time.Since(start)
	if ctx.Err() != nil {
		return obs, err // cut short at shutdown: the runner drops it, so do not count it
	}
	r.runs.Add(1)
	if err == nil && obs.Status != check.StatusUp {
		r.down.Add(1)
	}
	r.mu.Lock()
	r.durs = append(r.durs, d)
	r.mu.Unlock()
	return obs, err
}

func (r *recorder) percentiles() (p50, p99, maxD time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return pct(r.durs, 0.50), pct(r.durs, 0.99), pct(r.durs, 1)
}

func pct(ds []time.Duration, q float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	i := int(math.Ceil(q*float64(len(s)))) - 1
	return s[max(i, 0)]
}

type nopEventSink struct{}

func (nopEventSink) Publish(context.Context, cloudevents.Event) error { return nil }

func capacityTargets(cs *capacityServer, n int) []check.Target {
	ts := make([]check.Target, n)
	for i := range ts {
		ts[i] = check.Target{URL: fmt.Sprintf("https://127.0.0.1:%s/t%d", cs.port, i)}
	}
	return ts
}

// calibrate measures the cold-check p99 with calibN checks, 8 at a time.
func calibrate(cs *capacityServer, c *Check, calibN int) (p50, p99 time.Duration) {
	rec := &recorder{inner: c}
	var wg sync.WaitGroup
	var next atomic.Int64
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for next.Add(1) <= int64(calibN) {
				_, _ = rec.Run(context.Background(), check.Target{URL: fmt.Sprintf("https://127.0.0.1:%s/cal", cs.port)})
			}
		}()
	}
	wg.Wait()
	p50, p99, _ = rec.percentiles()
	return p50, p99
}

func openFDs() int {
	f, err := os.Open("/dev/fd")
	if err != nil {
		return -1
	}
	defer func() { _ = f.Close() }()
	names, err := f.Readdirnames(-1)
	if err != nil {
		return -1
	}
	return len(names) - 1 // minus the descriptor that listing itself holds
}

var timeWaitLine = regexp.MustCompile(`TIME_WAIT`)

// timeWaitFor counts TIME_WAIT sockets whose local or foreign address is port.
// Best effort: returns -1 when netstat is unavailable.
func timeWaitFor(port string) int {
	out, err := exec.Command("netstat", "-an").Output()
	if err != nil {
		return -1
	}
	re := regexp.MustCompile(`[.:]` + regexp.QuoteMeta(port) + `\s`)
	n := 0
	for _, line := range regexp.MustCompile(`\n`).Split(string(out), -1) {
		if timeWaitLine.MatchString(line) && re.MatchString(line) {
			n++
		}
	}
	return n
}

func settleGoroutines(base int) int {
	var g int
	for range 40 {
		runtime.GC()
		g = runtime.NumGoroutine()
		if g <= base {
			return g
		}
		time.Sleep(100 * time.Millisecond)
	}
	return g
}

func TestColdConnectionCapacity(t *testing.T) {
	skipUnlessCapacity(t)
	var cases []capacityCase
	for _, lat := range []time.Duration{50 * time.Millisecond, 200 * time.Millisecond} {
		cases = append(cases,
			capacityCase{"44targets_60s", 44, 60 * time.Second, 125 * time.Second, lat},
			capacityCase{"500targets_10s", 500, 10 * time.Second, 32 * time.Second, lat},
		)
	}
	baseGoroutines := runtime.NumGoroutine()
	baseFDs := openFDs()
	// The group's t.Run returns only after every parallel case and its
	// cleanups have finished, so the leak check below sees a quiet process.
	t.Run("cases", func(t *testing.T) {
		for _, cc := range cases {
			t.Run(fmt.Sprintf("%s/latency_%dms", cc.name, cc.latency.Milliseconds()), func(t *testing.T) {
				t.Parallel()
				runCapacityCase(t, cc)
			})
		}
	})
	nethttp.DefaultTransport.(*nethttp.Transport).CloseIdleConnections()
	endG := settleGoroutines(baseGoroutines + 2)
	endFDs := openFDs()
	t.Logf("end of run: goroutines %d (base %d), open fds %d (base %d)", endG, baseGoroutines, endFDs, baseFDs)
	if endG > baseGoroutines+2 {
		t.Errorf("goroutines after all cases = %d, base %d: leak", endG, baseGoroutines)
	}
	if endFDs >= 0 && endFDs > baseFDs+2 {
		t.Errorf("open fds after all cases = %d, base %d: leak", endFDs, baseFDs)
	}
}

func runCapacityCase(t *testing.T, cc capacityCase) {
	cs := newCapacityServer(t)
	const timeout = 10 * time.Second
	chk, tr := newColdCheck(cs, timeout, cc.latency)

	calP50, calP99 := calibrate(cs, chk, 400)
	load := float64(cc.targets) * calP99.Seconds() / cc.interval.Seconds()
	conc := max(int(math.Ceil(2*load)), 1)
	cs.accepted.Store(0)

	rec := &recorder{inner: chk}
	r := runner.New(rec, nopEventSink{}, event.Config{Source: "capacity"}, capacityTargets(cs, cc.targets), cc.interval, conc)

	var skippedQ, skippedF, notRun, other int64
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for res := range r.Results() {
			switch {
			case errors.Is(res.Err, runner.ErrSkippedQueued):
				skippedQ++
			case errors.Is(res.Err, runner.ErrSkipped):
				skippedF++
			case errors.Is(res.Err, check.ErrNotRun):
				notRun++
			case res.Err != nil:
				other++
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), cc.runFor)
	defer cancel()
	peakG, peakTW := 0, 0
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		tk := time.NewTicker(50 * time.Millisecond)
		defer tk.Stop()
		for tick := 0; ; tick++ {
			select {
			case <-stop:
				return
			case <-tk.C:
				peakG = max(peakG, runtime.NumGoroutine())
				if tick%40 == 39 { // every 2s: netstat is comparatively slow
					peakTW = max(peakTW, timeWaitFor(cs.port))
				}
			}
		}
	}()
	if err := r.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Run: %v", err)
	}
	<-drained
	close(stop)
	<-sampled

	runs := rec.runs.Load()
	p50, p99, maxD := rec.percentiles()
	conns := cs.accepted.Load()
	tr.CloseIdleConnections()

	t.Logf("latency=%v targets=%d interval=%v runFor=%v | calibration p50=%v p99=%v -> load=%.2f concurrency=%d",
		cc.latency, cc.targets, cc.interval, cc.runFor, calP50.Round(time.Millisecond), calP99.Round(time.Millisecond), load, conc)
	t.Logf("checks=%d down=%d skipped(queued=%d in_flight=%d, Skipped()=%d) dropped=%d not_run=%d other_err=%d",
		runs, rec.down.Load(), skippedQ, skippedF, r.Skipped(), r.Dropped(), notRun, other)
	t.Logf("httptrace: got_conn=%d reused=%d dials=%d (dials/check=%.3f)",
		rec.gotConn.Load(), rec.reused.Load(), rec.dials.Load(), float64(rec.dials.Load())/math.Max(float64(runs), 1))
	t.Logf("check duration p50=%v p99=%v max=%v | conns_accepted=%d conns/check=%.3f peak time_wait(server port, sampled every 2s)=%d | peak goroutines=%d (process-wide, parallel cases share it)",
		p50.Round(time.Millisecond), p99.Round(time.Millisecond), maxD.Round(time.Millisecond),
		conns, float64(conns)/math.Max(float64(runs), 1), peakTW, peakG)

	if r.Skipped() != 0 || skippedQ != 0 || skippedF != 0 {
		t.Errorf("skipped slots: Skipped()=%d queued=%d in_flight=%d, want 0", r.Skipped(), skippedQ, skippedF)
	}
	if r.Dropped() != 0 {
		t.Errorf("Dropped() = %d, want 0", r.Dropped())
	}
	if notRun != 0 || other != 0 {
		t.Errorf("not_run=%d other_err=%d, want 0", notRun, other)
	}
	if n := rec.reused.Load(); n != 0 {
		t.Errorf("%d requests ran on a connection the check did not dial, want 0 in fresh mode", n)
	}
	if rec.down.Load() != 0 {
		t.Errorf("down verdicts = %d on a healthy server, want 0", rec.down.Load())
	}
	if want := int64(cc.targets); runs < want {
		t.Errorf("only %d checks ran, want at least one per target (%d)", runs, want)
	}
}

// BenchmarkCheckRun compares fresh and reuse mode for one check against a
// loopback TLS HTTP/2 server with no injected latency, so it isolates the local
// CPU and allocation cost of a cold connection. cpu-ns/op is process CPU time
// (user+system) per check and includes the in-process server's side of the
// handshake, so read it as the fresh/reuse ratio, not as descry's own cost.
// conns/op is the connection churn: 1.0 in fresh mode, about 0 in reuse mode.
func BenchmarkCheckRun(b *testing.B) {
	for _, mode := range []struct {
		name  string
		reuse bool
	}{{"fresh", false}, {"reuse", true}} {
		b.Run(mode.name, func(b *testing.B) {
			var accepted atomic.Int64
			srv := httptest.NewUnstartedServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
				w.WriteHeader(nethttp.StatusOK)
			}))
			srv.EnableHTTP2 = true
			srv.Config.ConnState = func(_ net.Conn, s nethttp.ConnState) {
				if s == nethttp.StateNew {
					accepted.Add(1)
				}
			}
			srv.StartTLS()
			defer srv.Close()

			const timeout = 10 * time.Second
			tr := newLoopbackSafeTransport(mode.reuse, timeout, serverRoots(srv))
			defer tr.CloseIdleConnections()
			var opts []Option
			if mode.reuse {
				opts = append(opts, WithConnectionReuse())
			}
			c := newCheckWithTransport(timeout, tr, opts...)
			tgt := check.Target{URL: srv.URL}
			ctx := context.Background()
			if obs, err := c.Run(ctx, tgt); err != nil || obs.Status != check.StatusUp {
				b.Fatalf("warm-up run: %v %v", obs.Status, err)
			}
			accepted.Store(0)

			cpu0 := cpuTime()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := c.Run(ctx, tgt); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(cpuTime()-cpu0)/float64(b.N), "cpu-ns/op")
			b.ReportMetric(float64(accepted.Load())/float64(b.N), "conns/op")
		})
	}
}

// cpuTime returns the process's user+system CPU time in nanoseconds.
func cpuTime() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return ru.Utime.Nano() + ru.Stime.Nano()
}

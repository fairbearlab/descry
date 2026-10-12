package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/fairbearlab/descry/check"
)

var (
	t0      = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	fakeNow = t0.Add(time.Minute)
	expiry  = time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC)
)

// perTargetSeries is the per-target series count the package doc and
// Options.DisablePerTarget state: five gauges (tls included, for an HTTPS
// target) plus runs_total with both status values.
const perTargetSeries = 7

// TestCheck_Decorator walks one target through up, down, a Run error and a
// third-party error class, checking every series the decorator writes.
func TestCheck_Decorator(t *testing.T) {
	c := newTestCollector(t, Options{}, fakeNow)
	f := &fakeCheck{obs: check.Observation{
		Status: check.StatusUp, StatusCode: 200, LatencyMs: 150,
		ObservedAt: t0, TLSExpiry: &expiry,
	}}
	chk := c.Check(f)
	if chk.Name() != "fake" {
		t.Errorf("Name() = %q, want the inner check's name", chk.Name())
	}
	const u = `{url="https://a.example/health"}`
	tgt := check.Target{URL: "https://a.example/health"}

	// Up: every per-target gauge, runs_total{up}, the histogram.
	obs, err := chk.Run(context.Background(), tgt)
	if err != nil || obs.StatusCode != 200 || obs.TLSExpiry != &expiry {
		t.Fatalf("Run = (%+v, %v); want the inner Observation unchanged", obs, err)
	}
	got := samples(t, c)
	want(t, got, "descry_check_up"+u, 1)
	want(t, got, "descry_check_status_code"+u, 200)
	want(t, got, "descry_check_latency_seconds"+u, 0.15)
	want(t, got, "descry_check_last_run_timestamp_seconds"+u, float64(t0.Unix()))
	want(t, got, "descry_check_tls_expiry_timestamp_seconds"+u, float64(expiry.Unix()))
	want(t, got, `descry_check_runs_total{status="up",url="https://a.example/health"}`, 1)
	want(t, got, `descry_check_runs_total{status="down",url="https://a.example/health"}`, 0)
	want(t, got, "descry_check_duration_seconds_count", 1)
	want(t, got, "descry_check_duration_seconds_sum", 0.15)
	want(t, got, `descry_check_errors_total{class="http_error"}`, 0)

	// Down with no ObservedAt and no TLSExpiry: the clock fills last_run and
	// the tls gauge keeps its last value.
	f.obs = check.Observation{Status: check.StatusDown, StatusCode: 503, LatencyMs: 20, ErrorClass: check.ErrHTTPError}
	if _, err := chk.Run(context.Background(), tgt); err != nil {
		t.Fatal(err)
	}
	got = samples(t, c)
	want(t, got, "descry_check_up"+u, 0)
	want(t, got, "descry_check_status_code"+u, 503)
	want(t, got, "descry_check_latency_seconds"+u, 0.02)
	want(t, got, "descry_check_last_run_timestamp_seconds"+u, float64(fakeNow.Unix()))
	want(t, got, "descry_check_tls_expiry_timestamp_seconds"+u, float64(expiry.Unix()))
	want(t, got, `descry_check_runs_total{status="down",url="https://a.example/health"}`, 1)
	want(t, got, "descry_check_duration_seconds_count", 2)
	want(t, got, `descry_check_errors_total{class="http_error"}`, 1)

	// Run error: returned unchanged; up and status_code 0, runs down, class
	// unknown; latency, the histogram and the tls gauge untouched.
	boom := errors.New("boom")
	// A half-second clock also checks that unixSeconds keeps the sub-second part.
	c.now = func() time.Time { return fakeNow.Add(1500 * time.Millisecond) }
	f.obs, f.err = check.Observation{}, boom
	if _, err := chk.Run(context.Background(), tgt); !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want the inner error", err)
	}
	got = samples(t, c)
	want(t, got, "descry_check_up"+u, 0)
	want(t, got, "descry_check_status_code"+u, 0)
	want(t, got, "descry_check_latency_seconds"+u, 0.02)
	want(t, got, "descry_check_last_run_timestamp_seconds"+u, float64(fakeNow.Unix())+1.5)
	want(t, got, `descry_check_runs_total{status="down",url="https://a.example/health"}`, 2)
	want(t, got, `descry_check_errors_total{class="unknown"}`, 1)
	want(t, got, "descry_check_duration_seconds_count", 2)
	want(t, got, "descry_check_tls_expiry_timestamp_seconds"+u, float64(expiry.Unix()))

	// An ErrorClass outside the engine's set maps to unknown, keeping the
	// class label a closed set.
	f.obs, f.err = check.Observation{Status: check.StatusDown, ErrorClass: "vendor_specific"}, nil
	if _, err := chk.Run(context.Background(), tgt); err != nil {
		t.Fatal(err)
	}
	got = samples(t, c)
	want(t, got, `descry_check_errors_total{class="unknown"}`, 2)
	absent(t, got, `descry_check_errors_total{class="vendor_specific"}`)

	// A target that never reports a TLSExpiry exports no tls series.
	f.obs = check.Observation{Status: check.StatusUp, StatusCode: 200, ObservedAt: t0}
	if _, err := chk.Run(context.Background(), check.Target{URL: "http://b.example"}); err != nil {
		t.Fatal(err)
	}
	got = samples(t, c)
	want(t, got, `descry_check_up{url="http://b.example/"}`, 1)
	absent(t, got, `descry_check_tls_expiry_timestamp_seconds{url="http://b.example/"}`)
}

// TestCheck_NoSecretsInLabels proves neither URL credentials, query, fragment
// nor Target/Observation labels reach any exported label.
func TestCheck_NoSecretsInLabels(t *testing.T) {
	c := newTestCollector(t, Options{}, fakeNow)
	pw := "secret" // joined at runtime: no literal basic-auth URL in source
	f := &fakeCheck{obs: check.Observation{
		Status: check.StatusDown, StatusCode: 500, ErrorClass: check.ErrHTTPError, TLSExpiry: &expiry,
		Labels: map[string]string{"owner": "observation-label-value"},
	}}
	tgt := check.Target{
		URL:    "https://user:" + pw + "@host/p?token=abc#frag",
		Labels: map[string]string{"team": "target-label-value"},
	}
	if _, err := c.Check(f).Run(context.Background(), tgt); err != nil {
		t.Fatal(err)
	}
	text := labelText(t, c)
	if !slices.Contains(text, "https://host/p") {
		t.Fatalf("labels %q lack the rendered url https://host/p", text)
	}
	for _, s := range text {
		for _, bad := range []string{"secret", "token", "abc", "frag", "user", "team", "target-label", "owner", "observation-label"} {
			if strings.Contains(s, bad) {
				t.Errorf("label text %q contains %q", s, bad)
			}
		}
	}
}

// TestCheck_Collision proves targets that differ only in dropped URL parts
// share one series set: counters sum, gauges are last-writer-wins.
func TestCheck_Collision(t *testing.T) {
	c := newTestCollector(t, Options{}, fakeNow)
	f := &fakeCheck{}
	chk := c.Check(f)
	pw := "secret"

	f.obs = check.Observation{Status: check.StatusUp, StatusCode: 200, LatencyMs: 100, ObservedAt: t0}
	if _, err := chk.Run(context.Background(), check.Target{URL: "https://user:" + pw + "@host/p?a=1"}); err != nil {
		t.Fatal(err)
	}
	f.obs = check.Observation{Status: check.StatusDown, StatusCode: 500, LatencyMs: 300, ObservedAt: t0.Add(time.Second), ErrorClass: check.ErrHTTPError}
	if _, err := chk.Run(context.Background(), check.Target{URL: "https://host/p?b=2#x"}); err != nil {
		t.Fatal(err)
	}

	got := samples(t, c)
	const u = `{url="https://host/p"}`
	want(t, got, `descry_check_runs_total{status="up",url="https://host/p"}`, 1)
	want(t, got, `descry_check_runs_total{status="down",url="https://host/p"}`, 1)
	want(t, got, "descry_check_up"+u, 0)
	want(t, got, "descry_check_status_code"+u, 500)
	want(t, got, "descry_check_latency_seconds"+u, 0.3)
	want(t, got, "descry_check_last_run_timestamp_seconds"+u, float64(t0.Unix()+1))
	urls := 0
	for k := range got {
		if strings.HasPrefix(k, "descry_check_up{") {
			urls++
		}
	}
	if urls != 1 {
		t.Errorf("descry_check_up has %d url series, want 1", urls)
	}
}

// TestCheck_Concurrent runs the decorator from many goroutines over shared
// and distinct targets; with -race it is the decorator's race test.
func TestCheck_Concurrent(t *testing.T) {
	c := newTestCollector(t, Options{}, fakeNow)
	chk := c.Check(&fakeCheck{obs: check.Observation{Status: check.StatusUp, StatusCode: 200, ObservedAt: t0, TLSExpiry: &expiry}})
	const workers, runs, urls = 8, 200, 25
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range runs {
				tgt := check.Target{URL: fmt.Sprintf("https://t%d.example/", (w+i)%urls)}
				if _, err := chk.Run(context.Background(), tgt); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	got := samples(t, c)
	var total float64
	for k, v := range got {
		if strings.HasPrefix(k, `descry_check_runs_total{status="up"`) {
			total += v
		}
	}
	if total != workers*runs {
		t.Errorf("sum(runs_total{status=up}) = %v, want %d", total, workers*runs)
	}
	want(t, got, "descry_check_duration_seconds_count", workers*runs)
}

// TestCheck_RunAllocs guards the steady-state decorator: once a target's
// children are cached, recording a run (TLS gauge included) allocates
// nothing around an inner check that allocates nothing.
func TestCheck_RunAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are unreliable under -race")
	}
	c := newTestCollector(t, Options{}, fakeNow)
	chk := c.Check(&fakeCheck{obs: check.Observation{
		Status: check.StatusDown, StatusCode: 503, LatencyMs: 40, ErrorClass: check.ErrHTTPError,
		ObservedAt: t0, TLSExpiry: &expiry,
	}})
	ctx := context.Background()
	tgt := check.Target{URL: "https://a.example/health"}
	if _, err := chk.Run(ctx, tgt); err != nil { // resolve and cache the children
		t.Fatal(err)
	}
	got := testing.AllocsPerRun(1000, func() {
		if _, err := chk.Run(ctx, tgt); err != nil {
			t.Fatal(err)
		}
	})
	if got != 0 {
		t.Errorf("allocs/op = %v, want 0", got)
	}
}

// TestNew_Registration covers New's contract: its own registry, no conflict
// between collectors, every series named in the package doc, and the
// non-decorator setters.
func TestNew_Registration(t *testing.T) {
	stats := &fakeStats{}
	stats.skipped.Store(3)
	stats.dropped.Store(4)
	c, err := New(Options{Version: "v1.2.3", Commit: "0123456"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := samples(t, c)["descry_runner_skipped_slots_total"]; got != 0 {
		t.Errorf("skipped_slots_total before SetRunner = %v, want 0", got)
	}
	// A nil SetRunner after a real one is ignored: stats stays attached, so
	// the gathers below still read 3 and 4 instead of calling through a nil
	// RunnerStats.
	// Value: protects=SetRunner(nil) is ignored and keeps the attached runner;
	// fails_when=the nil guard is dropped, so a gather calls Skipped on a nil
	// RunnerStats and panics; why_new=no other test calls SetRunner(nil) after
	// a real runner is attached; seam=none; no_claim=a typed-nil
	// *runner.Runner (not covered; fakeStats stands in for the runner).
	c.SetRunner(stats)
	c.SetRunner(nil)
	if _, err := New(Options{}); err != nil {
		t.Fatalf("second New: %v", err)
	}
	if c.Registry() == nil || c.Registry() == prometheus.DefaultRegisterer {
		t.Fatal("Registry() is not the collector's own registry")
	}
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if strings.HasPrefix(mf.GetName(), "descry_") {
			t.Errorf("DefaultGatherer has %s", mf.GetName())
		}
	}

	// Before any observation every error class and skip kind already exists
	// at 0, and the per-target family is empty.
	got := samples(t, c)
	for _, ec := range errorClasses {
		want(t, got, fmt.Sprintf(`descry_check_errors_total{class=%q}`, ec), 0)
	}
	want(t, got, `descry_runner_skips_observed_total{kind="in_flight"}`, 0)
	want(t, got, `descry_runner_skips_observed_total{kind="queued"}`, 0)
	want(t, got, "descry_runner_result_errors_total", 0) // nil ResultErrors exports 0
	want(t, got, "descry_runner_skipped_slots_total", 3)
	want(t, got, "descry_runner_dropped_results_total", 4)
	want(t, got, `descry_build_info{commit="0123456",version="v1.2.3"}`, 1)

	c.SetTargetCount(42)
	c.ObserveSkip(SkipQueued)
	c.ObserveSkip(SkipQueued)
	c.ObserveSkip(SkipInFlight)
	c.ObserveSkip("bogus")
	if _, err := c.Check(&fakeCheck{obs: check.Observation{Status: check.StatusUp, StatusCode: 200, TLSExpiry: &expiry}}).
		Run(context.Background(), check.Target{URL: "https://a.example/"}); err != nil {
		t.Fatal(err)
	}
	got = samples(t, c)
	want(t, got, "descry_runner_targets", 42)
	want(t, got, `descry_runner_skips_observed_total{kind="queued"}`, 2)
	want(t, got, `descry_runner_skips_observed_total{kind="in_flight"}`, 1)
	absent(t, got, `descry_runner_skips_observed_total{kind="bogus"}`)

	all := []string{
		"descry_build_info",
		"descry_check_duration_seconds",
		"descry_check_errors_total",
		"descry_check_last_run_timestamp_seconds",
		"descry_check_latency_seconds",
		"descry_check_runs_total",
		"descry_check_status_code",
		"descry_check_tls_expiry_timestamp_seconds",
		"descry_check_up",
		"descry_runner_dropped_results_total",
		"descry_runner_result_errors_total",
		"descry_runner_skipped_slots_total",
		"descry_runner_skips_observed_total",
		"descry_runner_targets",
	}
	if names := familyNames(t, c); !slices.Equal(names, all) {
		t.Errorf("families = %q\nwant      %q", names, all)
	}
}

// TestNew_DisablePerTarget proves the option drops only the per-target family:
// the fleet histogram, the errors series, the runner series and build_info
// all keep working.
func TestNew_DisablePerTarget(t *testing.T) {
	stats := &fakeStats{}
	stats.skipped.Store(3)
	stats.dropped.Store(4)
	c, err := New(Options{
		DisablePerTarget: true, Version: "v", Commit: "c",
		ResultErrors: func() int64 { return 5 },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.SetRunner(stats)
	c.now = func() time.Time { return fakeNow }
	c.SetTargetCount(7)
	c.ObserveSkip(SkipQueued)
	f := &fakeCheck{obs: check.Observation{Status: check.StatusDown, LatencyMs: 10, ErrorClass: check.ErrTimeout, TLSExpiry: &expiry}}
	chk := c.Check(f)
	if _, err := chk.Run(context.Background(), check.Target{URL: "https://a.example/"}); err != nil {
		t.Fatal(err)
	}
	// A Run error with no per-target family still counts class unknown and
	// leaves the histogram alone.
	// Value: protects=with DisablePerTarget a Run error counts
	// errors_total{class="unknown"} and touches no per-target state;
	// fails_when=the error path's per-target nil guard is dropped (nil deref) or
	// it returns before counting; why_new=no other test runs the error path
	// with DisablePerTarget; seam=none; no_claim=none beyond protects (fakeCheck
	// stands in for a check that errors).
	boom := errors.New("boom")
	f.obs, f.err = check.Observation{}, boom
	if _, err := chk.Run(context.Background(), check.Target{URL: "https://a.example/"}); !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want the inner error", err)
	}
	got := samples(t, c)
	want(t, got, `descry_check_errors_total{class="unknown"}`, 1)
	for k := range got {
		if strings.Contains(k, "url=") {
			t.Errorf("per-target series %s exported with DisablePerTarget", k)
		}
	}
	want(t, got, "descry_check_duration_seconds_count", 1)
	want(t, got, `descry_check_errors_total{class="timeout"}`, 1)
	want(t, got, "descry_runner_skipped_slots_total", 3)
	want(t, got, "descry_runner_dropped_results_total", 4)
	want(t, got, "descry_runner_result_errors_total", 5)
	want(t, got, "descry_runner_targets", 7)
	want(t, got, `descry_runner_skips_observed_total{kind="queued"}`, 1)
	want(t, got, `descry_build_info{commit="c",version="v"}`, 1)
}

// TestCheck_TenThousandTargets scrapes a 10,000-target fleet end to end
// through promhttp: the per-target series count is what the docs state, and
// DisablePerTarget keeps the scrape O(1).
func TestCheck_TenThousandTargets(t *testing.T) {
	const targets = 10_000
	for _, tc := range []struct {
		name     string
		disabled bool
	}{{"per-target", false}, {"disabled", true}} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestCollector(t, Options{DisablePerTarget: tc.disabled}, fakeNow)
			chk := c.Check(&fakeCheck{obs: check.Observation{Status: check.StatusUp, StatusCode: 200, LatencyMs: 5, ObservedAt: t0, TLSExpiry: &expiry}})
			ctx := context.Background()
			for i := range targets {
				if _, err := chk.Run(ctx, check.Target{URL: fmt.Sprintf("https://t%05d.example.com/health", i)}); err != nil {
					t.Fatal(err)
				}
			}

			rec := httptest.NewRecorder()
			promhttp.HandlerFor(c.Registry(), promhttp.HandlerOpts{}).
				ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("scrape status = %d, want 200", rec.Code)
			}
			body := rec.Body.String()
			perTarget, total := 0, 0
			for line := range strings.Lines(body) {
				if strings.HasPrefix(line, "#") {
					continue
				}
				total++
				if strings.Contains(line, `url="`) {
					perTarget++
				}
			}
			if tc.disabled {
				if strings.Contains(body, "url=") {
					t.Error("scrape body has a url= label with DisablePerTarget")
				}
				if total > 50 {
					t.Errorf("scrape has %d series with DisablePerTarget, want O(1)", total)
				}
				return
			}
			if perTarget != perTargetSeries*targets {
				t.Errorf("per-target series = %d, want %d (%d per target)", perTarget, perTargetSeries*targets, perTargetSeries)
			}
		})
	}
}

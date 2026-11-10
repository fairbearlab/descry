package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/fairbearlab/descry/check"
)

// notRunErr is the shape a check returns when it could not run: the sentinel,
// the operation and the errno, with no URL.
func notRunErr(errno syscall.Errno) error {
	return fmt.Errorf("%w: dial: %w", check.ErrNotRun, errno)
}

// perTargetOf returns every per-target sample of got for label u, keyed as
// in samples.
func perTargetOf(got map[string]float64, url string) map[string]float64 {
	out := map[string]float64{}
	for k, v := range got {
		if strings.Contains(k, `url="`+url+`"`) {
			out[k] = v
		}
	}
	return out
}

// TestCheck_NotRun walks a target through not-runs: no verdict series moves,
// the not-run series count, descry_check_up is removed on the third in a row
// and recreated by the next completed run, and a completed run resets the
// streak.
func TestCheck_NotRun(t *testing.T) {
	c := newTestCollector(t, Options{}, fakeNow)
	f := &fakeCheck{obs: check.Observation{
		Status: check.StatusUp, StatusCode: 200, LatencyMs: 150, ObservedAt: t0, TLSExpiry: &expiry,
	}}
	chk := c.Check(f)
	const url = "https://a.example/health"
	const up = `descry_check_up{url="` + url + `"}`
	tgt := check.Target{URL: url}
	ctx := context.Background()

	if _, err := chk.Run(ctx, tgt); err != nil {
		t.Fatal(err)
	}
	before := samples(t, c)
	beforeTarget := perTargetOf(before, url)

	notRun := func(errno syscall.Errno) {
		t.Helper()
		f.obs, f.err = check.Observation{}, notRunErr(errno)
		if _, err := chk.Run(ctx, tgt); !errors.Is(err, check.ErrNotRun) {
			t.Fatalf("Run error = %v, want the inner not-run error unchanged", err)
		}
	}

	// Two not-runs: every per-target series, up included, and every verdict
	// fleet series keep their values; only the not-run series move.
	c.now = func() time.Time { return fakeNow.Add(time.Minute) }
	notRun(syscall.EMFILE)
	notRun(syscall.EADDRNOTAVAIL)
	got := samples(t, c)
	for k, v := range beforeTarget {
		want(t, got, k, v)
	}
	for _, k := range []string{
		"descry_check_duration_seconds_count", "descry_check_duration_seconds_sum",
		`descry_check_errors_total{class="unknown"}`, "descry_runner_result_errors_total",
	} {
		want(t, got, k, before[k])
	}
	want(t, got, `descry_checks_not_run_total{reason="EMFILE"}`, 1)
	want(t, got, `descry_checks_not_run_total{reason="EADDRNOTAVAIL"}`, 1)
	want(t, got, `descry_checks_not_run_total{reason="other"}`, 0)
	want(t, got, "descry_checks_not_run_last_timestamp_seconds", float64(fakeNow.Add(time.Minute).Unix()))

	// The third in a row removes up and nothing else; a fourth keeps it gone.
	notRun(syscall.ENOMEM)
	got = samples(t, c)
	absent(t, got, up)
	for k, v := range beforeTarget {
		if k != up {
			want(t, got, k, v)
		}
	}
	notRun(syscall.ENOMEM)
	absent(t, samples(t, c), up)
	want(t, samples(t, c), `descry_checks_not_run_total{reason="ENOMEM"}`, 2)

	// A completed run recreates up through the cached handle path.
	f.obs, f.err = check.Observation{Status: check.StatusDown, StatusCode: 503, ErrorClass: check.ErrHTTPError, ObservedAt: t0}, nil
	if _, err := chk.Run(ctx, tgt); err != nil {
		t.Fatal(err)
	}
	want(t, samples(t, c), up, 0)

	// The completed run reset the streak: two more not-runs keep up.
	notRun(syscall.EMFILE)
	notRun(syscall.EMFILE)
	want(t, samples(t, c), up, 0)
	notRun(syscall.EMFILE)
	absent(t, samples(t, c), up)

	// A Run error that is not a not-run still records a failed run, and
	// recreates up at 0.
	f.obs, f.err = check.Observation{}, errors.New("boom")
	if _, err := chk.Run(ctx, tgt); err == nil {
		t.Fatal("want the inner error")
	}
	got = samples(t, c)
	want(t, got, up, 0)
	want(t, got, `descry_check_runs_total{status="down",url="`+url+`"}`, 2)
}

// TestCheck_NotRunCreatesNoSeries: a not-run on a target that has never
// completed a run exports no per-target series, which would read up=0.
func TestCheck_NotRunCreatesNoSeries(t *testing.T) {
	c := newTestCollector(t, Options{}, fakeNow)
	chk := c.Check(&fakeCheck{err: notRunErr(syscall.EMFILE)})
	for range 5 {
		if _, err := chk.Run(context.Background(), check.Target{URL: "https://new.example/"}); err == nil {
			t.Fatal("want the inner error")
		}
	}
	got := samples(t, c)
	for k := range got {
		if strings.Contains(k, "url=") {
			t.Errorf("not-run created per-target series %s", k)
		}
	}
	want(t, got, `descry_checks_not_run_total{reason="EMFILE"}`, 5)
}

// TestCheck_NotRunDisablePerTarget: the not-run series are fleet-wide, so
// they work without the per-target family.
func TestCheck_NotRunDisablePerTarget(t *testing.T) {
	c := newTestCollector(t, Options{DisablePerTarget: true}, fakeNow)
	chk := c.Check(&fakeCheck{err: fmt.Errorf("custom: %w", check.ErrNotRun)})
	if _, err := chk.Run(context.Background(), check.Target{URL: "https://a.example/"}); err == nil {
		t.Fatal("want the inner error")
	}
	got := samples(t, c)
	want(t, got, `descry_checks_not_run_total{reason="other"}`, 1)
	want(t, got, "descry_checks_not_run_last_timestamp_seconds", float64(fakeNow.Unix()))
	want(t, got, `descry_check_errors_total{class="unknown"}`, 0)
}

// TestCheck_NotRunSharedLabel: raw URLs that share a url label share its up
// series, so a not-run streak on one removes it and a completed run of the
// other recreates it; neither writes to an orphaned handle.
func TestCheck_NotRunSharedLabel(t *testing.T) {
	c := newTestCollector(t, Options{}, fakeNow)
	f := &fakeCheck{obs: check.Observation{Status: check.StatusUp, StatusCode: 200, ObservedAt: t0}}
	chk := c.Check(f)
	ctx := context.Background()
	a, b := check.Target{URL: "https://h/p?a=1"}, check.Target{URL: "https://h/p?b=2"}
	const up = `descry_check_up{url="https://h/p"}`
	for _, tgt := range []check.Target{a, b} {
		if _, err := chk.Run(ctx, tgt); err != nil {
			t.Fatal(err)
		}
	}
	f.obs, f.err = check.Observation{}, notRunErr(syscall.EMFILE)
	for range 3 {
		_, _ = chk.Run(ctx, a)
	}
	absent(t, samples(t, c), up)
	f.obs, f.err = check.Observation{Status: check.StatusUp, StatusCode: 200, ObservedAt: t0}, nil
	if _, err := chk.Run(ctx, b); err != nil {
		t.Fatal(err)
	}
	want(t, samples(t, c), up, 1)
}

// TestCheck_CancelledRecordsNothing: a run whose ctx was cancelled by the
// time the inner check returns records nothing, whatever it returned; a run
// that hit a deadline is recorded.
func TestCheck_CancelledRecordsNothing(t *testing.T) {
	for name, inner := range map[string]*fakeCheck{
		"down observation": {obs: check.Observation{Status: check.StatusDown, ErrorClass: check.ErrUnknown, LatencyMs: 3}},
		"ctx error":        {err: context.Canceled},
		"not-run error":    {err: notRunErr(syscall.EMFILE)},
	} {
		t.Run(name, func(t *testing.T) {
			c := newTestCollector(t, Options{}, fakeNow)
			before := samples(t, c)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, _ = c.Check(inner).Run(ctx, check.Target{URL: "https://a.example/"})
			got := samples(t, c)
			if len(got) != len(before) {
				t.Errorf("a cancelled run changed the exported series: %d before, %d after", len(before), len(got))
			}
			for k, v := range before {
				want(t, got, k, v)
			}
		})
	}

	c := newTestCollector(t, Options{}, fakeNow)
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer cancel()
	chk := c.Check(&fakeCheck{obs: check.Observation{Status: check.StatusDown, ErrorClass: check.ErrTimeout}})
	_, _ = chk.Run(ctx, check.Target{URL: "https://a.example/"})
	got := samples(t, c)
	want(t, got, `descry_check_up{url="https://a.example/"}`, 0)
	want(t, got, `descry_check_errors_total{class="timeout"}`, 1)
}

// TestNotRunReason pins the reason derivation: errors.Is through any wrapping
// the net package uses, and "other" for anything else.
func TestNotRunReason(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{notRunErr(syscall.EMFILE), "EMFILE"},
		{notRunErr(syscall.ENFILE), "ENFILE"},
		{notRunErr(syscall.EADDRNOTAVAIL), "EADDRNOTAVAIL"},
		{notRunErr(syscall.ENOBUFS), "ENOBUFS"},
		{notRunErr(syscall.ENOMEM), "ENOMEM"},
		{fmt.Errorf("%w: %w", check.ErrNotRun, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.EMFILE}), "EMFILE"},
		{check.ErrNotRun, "other"},
		{notRunErr(syscall.ECONNREFUSED), "other"},
		{nil, "other"},
	} {
		if got := NotRunReason(tc.err); got != tc.want {
			t.Errorf("NotRunReason(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}

// TestCheck_NotRunConcurrentScrapes repeatedly removes and recovers up
// series through the cached path while scrapes run; with -race it is the
// delete/recreate race test. Every target ends on a completed run, so every
// up series must be back.
func TestCheck_NotRunConcurrentScrapes(t *testing.T) {
	c := newTestCollector(t, Options{}, fakeNow)
	up := &fakeCheck{obs: check.Observation{Status: check.StatusUp, StatusCode: 200, ObservedAt: t0}}
	nr := &fakeCheck{err: notRunErr(syscall.EMFILE)}
	upChk, nrChk := c.Check(up), c.Check(nr)
	const targets, cycles = 8, 200
	ctx := context.Background()

	stop := make(chan struct{})
	var scrapes sync.WaitGroup
	for range 2 {
		scrapes.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := c.Registry().Gather(); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	var wg sync.WaitGroup
	for i := range targets {
		wg.Go(func() {
			tgt := check.Target{URL: fmt.Sprintf("https://t%d.example/", i)}
			for range cycles {
				if _, err := upChk.Run(ctx, tgt); err != nil {
					t.Error(err)
					return
				}
				for range notRunsBeforeRemoval {
					_, _ = nrChk.Run(ctx, tgt)
				}
			}
			_, _ = upChk.Run(ctx, tgt)
		})
	}
	wg.Wait()
	close(stop)
	scrapes.Wait()

	got := samples(t, c)
	for i := range targets {
		want(t, got, fmt.Sprintf(`descry_check_up{url="https://t%d.example/"}`, i), 1)
		want(t, got, fmt.Sprintf(`descry_check_runs_total{status="up",url="https://t%d.example/"}`, i), cycles+1)
	}
	want(t, got, `descry_checks_not_run_total{reason="EMFILE"}`, targets*cycles*notRunsBeforeRemoval)
}

// TestCheck_NotRunUncompletedAlias: a raw URL that has never completed, but
// shares its label with one that has, still counts toward the shared not-run
// streak, so the old verdict is removed rather than outliving the not-runs.
// It creates no series of its own.
//
// Value: protects=not-runs on a never-completed URL variant remove a shared
// label's stale descry_check_up; fails_when=recordNotRun returns early for
// any uncached raw URL; why_new=TestCheck_NotRunSharedLabel completes both
// URLs first, so the uncached path is never taken; seam=none;
// no_claim=label collisions the CLI refuses (library only)
func TestCheck_NotRunUncompletedAlias(t *testing.T) {
	c := newTestCollector(t, Options{}, fakeNow)
	f := &fakeCheck{obs: check.Observation{Status: check.StatusUp, StatusCode: 200, ObservedAt: t0}}
	chk := c.Check(f)
	ctx := context.Background()
	const up = `descry_check_up{url="https://h/p"}`
	if _, err := chk.Run(ctx, check.Target{URL: "https://h/p?a=1"}); err != nil {
		t.Fatal(err)
	}
	f.obs, f.err = check.Observation{}, notRunErr(syscall.EMFILE)
	for range 2 {
		_, _ = chk.Run(ctx, check.Target{URL: "https://h/p?b=2"})
	}
	want(t, samples(t, c), up, 1)
	_, _ = chk.Run(ctx, check.Target{URL: "https://h/p?b=2"})
	got := samples(t, c)
	absent(t, got, up)
	want(t, got, `descry_checks_not_run_total{reason="EMFILE"}`, 3)

	// A never-completed URL under a label nobody completed creates nothing.
	_, _ = chk.Run(ctx, check.Target{URL: "https://other/"})
	if n := len(perTargetOf(samples(t, c), "https://other/")); n != 0 {
		t.Errorf("a not-run on an unseen label created %d per-target series; want 0", n)
	}
}

// TestCheck_CancelledAfterSeries: a cancelled run leaves an existing target's
// series exactly as they were, whether descry_check_up is present or already
// removed by not-runs, and a target's first run that errors creates up at 0.
//
// Value: protects=a shutdown-cancelled run never moves (or recreates)
// descry_check_up once a target has series, and a first-run Run error still
// exports up=0; fails_when=the cancel test moves after record, or series()
// stops creating up through upState.set; why_new=TestCheck_CancelledRecordsNothing
// uses a fresh collector with no series; seam=none; no_claim=none beyond protects
func TestCheck_CancelledAfterSeries(t *testing.T) {
	tgt := check.Target{URL: "https://a.example/"}
	const up = `descry_check_up{url="https://a.example/"}`
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	down := check.Observation{Status: check.StatusDown, ErrorClass: check.ErrUnknown}

	t.Run("present", func(t *testing.T) {
		c := newTestCollector(t, Options{}, fakeNow)
		f := &fakeCheck{obs: check.Observation{Status: check.StatusUp, StatusCode: 200, ObservedAt: t0}}
		chk := c.Check(f)
		if _, err := chk.Run(context.Background(), tgt); err != nil {
			t.Fatal(err)
		}
		before := samples(t, c)
		f.obs = down
		_, _ = chk.Run(cancelled, tgt)
		got := samples(t, c)
		want(t, got, up, 1)
		for k, v := range before {
			want(t, got, k, v)
		}
	})
	t.Run("removed", func(t *testing.T) {
		c := newTestCollector(t, Options{}, fakeNow)
		f := &fakeCheck{obs: check.Observation{Status: check.StatusUp, StatusCode: 200, ObservedAt: t0}}
		chk := c.Check(f)
		if _, err := chk.Run(context.Background(), tgt); err != nil {
			t.Fatal(err)
		}
		f.obs, f.err = check.Observation{}, notRunErr(syscall.EMFILE)
		for range 3 {
			_, _ = chk.Run(context.Background(), tgt)
		}
		absent(t, samples(t, c), up)
		f.obs, f.err = down, nil
		_, _ = chk.Run(cancelled, tgt)
		absent(t, samples(t, c), up)
	})
	t.Run("first run error", func(t *testing.T) {
		c := newTestCollector(t, Options{}, fakeNow)
		_, _ = c.Check(&fakeCheck{err: errors.New("boom")}).Run(context.Background(), tgt)
		want(t, samples(t, c), up, 0)
	})
}

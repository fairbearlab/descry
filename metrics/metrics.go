package metrics

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/fairbearlab/descry/check"
)

// RunnerStats is the subset of *runner.Runner the collector reads. It matches
// runner.Runner's Skipped/Dropped methods so a *runner.Runner satisfies it.
type RunnerStats interface {
	Skipped() int64
	Dropped() int64
}

// SkipKind is the kind label of descry_runner_skips_observed_total.
type SkipKind string

// The skip kinds ObserveSkip accepts; any other value is ignored.
const (
	SkipInFlight SkipKind = "in_flight" // runner.ErrSkipped: the target's previous run was still in flight
	SkipQueued   SkipKind = "queued"    // runner.ErrSkippedQueued: the worker queue was full
)

// Options configures a Collector. The zero value is valid.
type Options struct {
	Version string // descry_build_info{version}
	Commit  string // descry_build_info{commit}

	// DisablePerTarget drops the per-target family. Each target costs about
	// seven series (six per-target names; runs_total has two status values),
	// roughly 70,000 series at 10,000 targets, so large fleets may want only
	// the fleet series, which stay registered either way.
	DisablePerTarget bool

	// ResultErrors returns the running count of non-skip Result errors from
	// the caller's single Results() drain; it is read at scrape time. Like
	// skips_observed_total it is best-effort: Results the runner drops under
	// saturation are never seen (dropped_results_total bounds the gap). nil
	// exports 0, so a constant 0 can also mean it was never wired.
	ResultErrors func() int64
}

// durationBuckets cover HTTP probes from a few milliseconds up to past the
// bundled HTTP check's 10s default timeout. prometheus.DefBuckets stops at
// 10s, which would put every timed-out probe in +Inf.
var durationBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}

// errorClasses is the closed label set of descry_check_errors_total. Every
// class is pre-initialised at 0; any other ErrorClass maps to unknown.
var errorClasses = []check.ErrorClass{
	check.ErrTimeout,
	check.ErrConnectionRefused,
	check.ErrDNSFailure,
	check.ErrTLSError,
	check.ErrSSRFBlocked,
	check.ErrHTTPError,
	check.ErrUnknown,
}

// The reason label values of descry_checks_not_run_total. NotRunReason maps
// an error to one of them.
const (
	NotRunEMFILE        = "EMFILE"        // the process is out of file descriptors
	NotRunENFILE        = "ENFILE"        // the system is out of file descriptors
	NotRunEADDRNOTAVAIL = "EADDRNOTAVAIL" // no local address or ephemeral port is free
	NotRunENOBUFS       = "ENOBUFS"       // the kernel is out of socket buffer space
	NotRunENOMEM        = "ENOMEM"        // the kernel is out of memory
	NotRunOther         = "other"         // any other reason, or none given
)

// notRunReasons is the closed reason label set, in the order NotRunReason
// tests them; every value is pre-initialised at 0.
var notRunReasons = []struct {
	reason string
	errno  syscall.Errno
}{
	{NotRunEMFILE, syscall.EMFILE},
	{NotRunENFILE, syscall.ENFILE},
	{NotRunEADDRNOTAVAIL, syscall.EADDRNOTAVAIL},
	{NotRunENOBUFS, syscall.ENOBUFS},
	{NotRunENOMEM, syscall.ENOMEM},
}

// NotRunReason returns the reason label descry_checks_not_run_total uses for
// err: the name of the first of EMFILE, ENFILE, EADDRNOTAVAIL, ENOBUFS and
// ENOMEM that errors.Is finds in err's chain, else NotRunOther. It does not
// test for check.ErrNotRun itself; callers do that first.
func NotRunReason(err error) string {
	for _, r := range notRunReasons {
		if errors.Is(err, r.errno) {
			return r.reason
		}
	}
	return NotRunOther
}

// notRunsBeforeRemoval is how many consecutive not-run results remove a
// target's descry_check_up. More than one, so intermittent local pressure
// does not keep resetting DescryTargetDown's `for` window.
const notRunsBeforeRemoval = 3

// Collector records descry metrics on its own registry. Its methods and the
// checks it decorates are safe for concurrent use.
type Collector struct {
	reg   *prometheus.Registry
	stats atomic.Pointer[runnerRef] // set by SetRunner; the runner CounterFuncs read 0 until then
	now   func() time.Time          // clock for last_run on a Run error or an Observation without ObservedAt; time.Now outside tests

	// Per-target vecs; all nil when Options.DisablePerTarget is set.
	up, statusCode, latency, lastRun, tlsExpiry *prometheus.GaugeVec
	runs                                        *prometheus.CounterVec

	perTargetActive bool

	mu    sync.RWMutex
	byURL map[string]*targetSeries // keyed by the raw target URL; values are resolved once
	ups   map[string]*upState      // keyed by the url label, shared by every raw URL with that label

	// Fleet series. errsBy is built in New and read-only afterwards.
	duration      prometheus.Histogram
	errsBy        map[check.ErrorClass]prometheus.Counter
	errUnknown    prometheus.Counter
	skipsInFlight prometheus.Counter
	skipsQueued   prometheus.Counter
	targetCount   prometheus.Gauge
	notRunBy      map[string]prometheus.Counter // built in New and read-only afterwards
	notRunLast    prometheus.Gauge
}

// targetSeries holds one target's pre-curried children, so a steady-state Run
// neither renders the label nor looks the children up.
type targetSeries struct {
	label                     string
	up                        *upState // removable; see upState
	statusCode, latency, last prometheus.Gauge
	runsUp, runsDown          prometheus.Counter
	// tls is resolved on the first Observation with a TLSExpiry, so targets
	// that never report one (plain HTTP) export no tls series at all.
	tls atomic.Pointer[tlsGauge]
}

type tlsGauge struct{ g prometheus.Gauge }

// upState is one url label's descry_check_up child, which consecutive
// not-run results remove and the next completed run recreates. Deleting a
// child from the vec orphans every cached handle to it (a Set on the old
// handle never reaches a scrape again), so the handle is re-resolved with
// WithLabelValues after a delete, and it lives here, once per label, so that
// raw URLs sharing a label all see the same handle. mu serialises a run's Set
// with a concurrent delete, so a completed run never writes to an orphan.
type upState struct {
	mu      sync.Mutex
	g       prometheus.Gauge // nil while the series is removed
	notRuns int              // consecutive not-run results, capped at notRunsBeforeRemoval
}

// set records a completed run: it resets the not-run streak and sets the
// gauge, recreating the series if not-runs removed it.
func (u *upState) set(vec *prometheus.GaugeVec, label string, v float64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.notRuns = 0
	if u.g == nil {
		u.g = vec.WithLabelValues(label)
	}
	u.g.Set(v)
}

// notRun records a not-run result: the series is removed on the
// notRunsBeforeRemoval-th in a row and stays removed until set.
func (u *upState) notRun(vec *prometheus.GaugeVec, label string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.notRuns < notRunsBeforeRemoval {
		u.notRuns++
	}
	if u.notRuns >= notRunsBeforeRemoval && u.g != nil {
		vec.DeleteLabelValues(label)
		u.g = nil
	}
}

// runnerRef boxes a RunnerStats so it can sit in an atomic.Pointer.
type runnerRef struct{ RunnerStats }

// New builds a Collector and registers every series on a fresh registry. It
// returns an error only when registration fails.
//
// The runner is attached afterwards with SetRunner, because runner.New takes
// the check that Collector.Check decorates: build the Collector, decorate the
// check, build the runner, then call SetRunner and SetTargetCount.
func New(opts Options) (*Collector, error) {
	c := &Collector{
		reg:             prometheus.NewRegistry(),
		now:             time.Now,
		byURL:           make(map[string]*targetSeries),
		ups:             make(map[string]*upState),
		notRunBy:        make(map[string]prometheus.Counter, len(notRunReasons)+1),
		errsBy:          make(map[check.ErrorClass]prometheus.Counter, len(errorClasses)),
		perTargetActive: !opts.DisablePerTarget,
	}
	var cs []prometheus.Collector

	if c.perTargetActive {
		gauge := func(name, help string) *prometheus.GaugeVec {
			v := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "descry", Subsystem: "check", Name: name, Help: help}, []string{"url"})
			cs = append(cs, v)
			return v
		}
		c.up = gauge("up", "1 when the target's last completed run was up, else 0; removed after 3 consecutive runs that could not run.")
		c.statusCode = gauge("status_code", "HTTP status code of the target's last run; 0 with no response.")
		c.latency = gauge("latency_seconds", "Latency of the target's last completed run.")
		c.lastRun = gauge("last_run_timestamp_seconds", "Unix time of the target's last run.")
		c.tlsExpiry = gauge("tls_expiry_timestamp_seconds", "Unix time the target's TLS certificate expires.")
		c.runs = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "descry", Subsystem: "check", Name: "runs_total",
			Help: "Runs per target by status (up or down; a Run error counts as down).",
		}, []string{"url", "status"})
		cs = append(cs, c.runs)
	}

	c.duration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "descry", Subsystem: "check", Name: "duration_seconds",
		Help:    "Latency of every completed check run.",
		Buckets: durationBuckets,
	})
	errs := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "descry", Subsystem: "check", Name: "errors_total",
		Help: "Failed check runs by error class.",
	}, []string{"class"})
	for _, ec := range errorClasses {
		c.errsBy[ec] = errs.WithLabelValues(string(ec))
	}
	c.errUnknown = c.errsBy[check.ErrUnknown]

	resultErrors := opts.ResultErrors
	if resultErrors == nil {
		resultErrors = func() int64 { return 0 }
	}
	skips := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "descry", Subsystem: "runner", Name: "skips_observed_total",
		Help: "Skipped slots seen on the results channel by kind; best-effort triage, lags skipped_slots_total by buffered and dropped results.",
	}, []string{"kind"})
	c.skipsInFlight = skips.WithLabelValues(string(SkipInFlight))
	c.skipsQueued = skips.WithLabelValues(string(SkipQueued))
	c.targetCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "descry", Subsystem: "runner", Name: "targets",
		Help: "Number of targets the runner schedules.",
	})
	buildInfo := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "descry", Name: "build_info",
		Help:        "Build metadata; constant 1.",
		ConstLabels: prometheus.Labels{"version": opts.Version, "commit": opts.Commit},
	})
	buildInfo.Set(1)

	notRun := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "descry", Subsystem: "checks", Name: "not_run_total",
		Help: "Runs that could not run (the check returned check.ErrNotRun), by reason; no verdict was recorded for them.",
	}, []string{"reason"})
	for _, r := range notRunReasons {
		c.notRunBy[r.reason] = notRun.WithLabelValues(r.reason)
	}
	c.notRunBy[NotRunOther] = notRun.WithLabelValues(NotRunOther)
	c.notRunLast = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "descry", Subsystem: "checks", Name: "not_run_last_timestamp_seconds",
		Help: "Unix time of the last run that could not run; 0 until one happens.",
	})

	cs = append(cs, c.duration, errs, skips, c.targetCount, buildInfo, notRun, c.notRunLast,
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "descry", Subsystem: "runner", Name: "skipped_slots_total",
			Help: "Slots skipped because the target's prior run was still running or still queued (both skip kinds); exact.",
		}, func() float64 {
			if r := c.stats.Load(); r != nil {
				return float64(r.Skipped())
			}
			return 0
		}),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "descry", Subsystem: "runner", Name: "dropped_results_total",
			Help: "Results discarded because the results channel was full.",
		}, func() float64 {
			if r := c.stats.Load(); r != nil {
				return float64(r.Dropped())
			}
			return 0
		}),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "descry", Subsystem: "runner", Name: "result_errors_total",
			Help: "Non-skip errors seen on the results channel; best-effort, misses results counted in dropped_results_total.",
		}, func() float64 { return float64(resultErrors()) }),
	)
	for _, col := range cs {
		if err := c.reg.Register(col); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// SetRunner attaches the runner whose Skipped and Dropped counters back
// descry_runner_skipped_slots_total and descry_runner_dropped_results_total;
// both read 0 until it is called. Call it once per Collector: a later call
// switches the source, and the counters would jump. A nil stats is ignored,
// but a typed nil (a nil *runner.Runner in the interface) is not detected: it
// is stored, and every Gather then panics in the caller's goroutine. Served
// through net/http the panic is recovered per request and the scrape fails
// with a dropped connection; a direct Gather call panics the caller. Pass the
// value runner.New returned.
func (c *Collector) SetRunner(stats RunnerStats) {
	if stats != nil {
		c.stats.Store(&runnerRef{stats})
	}
}

// Registry returns the Collector's own registry, for promhttp.HandlerFor or
// for adding further collectors.
func (c *Collector) Registry() *prometheus.Registry { return c.reg }

// ObserveSkip counts one skipped slot seen on the runner's results channel.
// It is best-effort by construction (see the package doc's Skips section);
// unknown kinds are ignored. runner.ErrSkippedQueued wraps runner.ErrSkipped,
// so a drain must test errors.Is(err, runner.ErrSkippedQueued) first, or every
// queued skip is counted as SkipInFlight.
func (c *Collector) ObserveSkip(kind SkipKind) {
	switch kind {
	case SkipInFlight:
		c.skipsInFlight.Inc()
	case SkipQueued:
		c.skipsQueued.Inc()
	}
}

// SetTargetCount sets descry_runner_targets.
func (c *Collector) SetTargetCount(n int) { c.targetCount.Set(float64(n)) }

// Check returns inner decorated to record every run. The decorator's Name
// delegates to inner, and Run returns inner's Observation and error
// unchanged. inner must be non-nil.
//
// A run whose ctx was cancelled by the time inner returns (the runner
// cancels in-flight runs at shutdown) records nothing at all, whatever inner
// returned: the process is exiting, and the run is no evidence about the
// target. A per-call deadline is not a cancellation, so a run that hit one is
// recorded as usual.
//
// A Run error that wraps check.ErrNotRun is not a verdict either: the check
// could not run. descry_checks_not_run_total{reason} counts one (the reason
// is NotRunReason(err)) and descry_checks_not_run_last_timestamp_seconds
// takes the collector's clock. Nothing else changes: status_code, latency,
// last_run, runs_total, errors_total, the histogram and the tls gauge keep
// their values, so a target that cannot be checked goes stale. Its
// descry_check_up keeps its value for the first two not-runs in a row and is
// removed on the third, so a verdict alert stops firing on evidence that is
// no longer being refreshed; the next completed run recreates it. A not-run
// on a target with no per-target series yet creates none.
//
// On any other Run error, up and status_code go to 0, last_run takes the
// collector's clock, runs_total{status="down"} and errors_total{class="unknown"}
// count one, and latency, the histogram and the tls gauge are left alone. A
// later Observation without a TLSExpiry also leaves the tls gauge at its last
// value. An Observation that never made a request (an SSRF-blocked target,
// say) records its 0 latency.
//
// errors_total counts Observations with an ErrorClass other than ErrNone,
// while runs_total{status="down"} counts those with a Status other than
// StatusUp. The bundled HTTP check keeps the two in step, but a custom check
// may report StatusDown with ErrNone or an ErrorClass with StatusUp, so for
// custom checks the two series can disagree.
//
// The per-target series are cached by the raw target URL and never evicted.
// A static target set (as the CLI's is) keeps the cache bounded; a caller
// that runs ever-new URLs through the decorator grows it without bound.
func (c *Collector) Check(inner check.Check) check.Check {
	return &instrumented{inner: inner, c: c}
}

type instrumented struct {
	inner check.Check
	c     *Collector
}

func (i *instrumented) Name() string { return i.inner.Name() }

func (i *instrumented) Run(ctx context.Context, t check.Target) (check.Observation, error) {
	obs, err := i.inner.Run(ctx, t)
	switch {
	case errors.Is(ctx.Err(), context.Canceled):
		// Abandoned at shutdown (or by the caller): no evidence either way.
	case errors.Is(err, check.ErrNotRun):
		i.c.recordNotRun(t.URL, err)
	default:
		i.c.record(t.URL, &obs, err)
	}
	return obs, err
}

// recordNotRun updates the not-run series for one run that could not run.
func (c *Collector) recordNotRun(rawURL string, err error) {
	c.notRunBy[NotRunReason(err)].Inc()
	c.notRunLast.Set(unixSeconds(c.now()))
	if !c.perTargetActive {
		return
	}
	c.mu.RLock()
	ts := c.byURL[rawURL]
	var up *upState
	label := ""
	if ts != nil {
		up, label = ts.up, ts.label
	} else {
		// This raw URL has no completed run, but another one with the same
		// label may have: its up series is this target's too, so the streak
		// must count here as well, or a stale verdict would outlive the
		// not-runs. Rendering the label allocates, but only on this path.
		label = TargetLabel(rawURL)
		up = c.ups[label]
	}
	c.mu.RUnlock()
	if up == nil {
		return // no completed run under this label: create nothing, which would export up=0
	}
	up.notRun(c.up, label)
}

// record updates every series for one run.
func (c *Collector) record(rawURL string, obs *check.Observation, err error) {
	var ts *targetSeries
	if c.perTargetActive {
		ts = c.series(rawURL)
	}
	if err != nil {
		c.errUnknown.Inc()
		if ts != nil {
			ts.up.set(c.up, ts.label, 0)
			ts.statusCode.Set(0)
			ts.last.Set(unixSeconds(c.now()))
			ts.runsDown.Inc()
		}
		return
	}

	latency := float64(obs.LatencyMs) / 1000
	c.duration.Observe(latency)
	if obs.ErrorClass != check.ErrNone {
		ec, ok := c.errsBy[obs.ErrorClass]
		if !ok {
			ec = c.errUnknown
		}
		ec.Inc()
	}
	if ts == nil {
		return
	}
	if obs.Status == check.StatusUp {
		ts.up.set(c.up, ts.label, 1)
		ts.runsUp.Inc()
	} else {
		ts.up.set(c.up, ts.label, 0)
		ts.runsDown.Inc()
	}
	ts.statusCode.Set(float64(obs.StatusCode))
	ts.latency.Set(latency)
	at := obs.ObservedAt
	if at.IsZero() {
		at = c.now()
	}
	ts.last.Set(unixSeconds(at))
	if obs.TLSExpiry != nil {
		ts.tlsGauge(c.tlsExpiry).Set(unixSeconds(*obs.TLSExpiry))
	}
}

// series returns the cached children for rawURL, resolving them on first use.
func (c *Collector) series(rawURL string) *targetSeries {
	c.mu.RLock()
	ts := c.byURL[rawURL]
	c.mu.RUnlock()
	if ts != nil {
		return ts
	}
	// Resolve the children before taking the write lock: WithLabelValues can
	// wait on the vec's lock for a whole scrape, and holding c.mu meanwhile
	// would stall every cached lookup too. Racing resolvers get the same
	// children from the vecs, so the loser's copy is simply dropped.
	label := TargetLabel(rawURL)
	ts = &targetSeries{
		label:      label,
		statusCode: c.statusCode.WithLabelValues(label),
		latency:    c.latency.WithLabelValues(label),
		last:       c.lastRun.WithLabelValues(label),
		// Both statuses exist from the first run, so a target that starts up
		// shows its first later failure as a 0 to 1 step. A failure on the
		// very first run still creates the down series at 1, which
		// increase() does not count.
		runsUp:   c.runs.WithLabelValues(label, string(check.StatusUp)),
		runsDown: c.runs.WithLabelValues(label, string(check.StatusDown)),
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if prior := c.byURL[rawURL]; prior != nil {
		return prior
	}
	// The up child is resolved lazily by the first upState.set, which every
	// caller of series makes, so it is only ever created with a value.
	up := c.ups[label]
	if up == nil {
		up = &upState{}
		c.ups[label] = up
	}
	ts.up = up
	c.byURL[rawURL] = ts
	return ts
}

// tlsGauge returns the target's tls child, resolving it on first use. Racing
// resolvers get the same child from the vec, so losing the swap is harmless.
func (ts *targetSeries) tlsGauge(vec *prometheus.GaugeVec) prometheus.Gauge {
	if g := ts.tls.Load(); g != nil {
		return g.g
	}
	g := &tlsGauge{g: vec.WithLabelValues(ts.label)}
	ts.tls.CompareAndSwap(nil, g)
	return g.g
}

// unixSeconds is t as fractional unix seconds.
func unixSeconds(t time.Time) float64 {
	return float64(t.Unix()) + float64(t.Nanosecond())/1e9
}

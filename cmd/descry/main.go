// Command descry is a lightweight uptime/observability probe. It loads a
// YAML config, runs the configured Check against each target on an interval,
// and publishes CloudEvents to stdout or an append-only file.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fairbearlab/descry/check"
	httpcheck "github.com/fairbearlab/descry/checks/http"
	"github.com/fairbearlab/descry/config"
	"github.com/fairbearlab/descry/event"
	"github.com/fairbearlab/descry/metrics"
	"github.com/fairbearlab/descry/runner"
	"github.com/fairbearlab/descry/sink"
)

// Populated by GoReleaser ldflags; fall back to "dev" / "none" / "unknown" in
// local builds.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// notifyContext is signal.NotifyContext; tests swap it for a context they
// cancel, to drive run's signal-driven shutdown without signalling the process.
// It is package state: a test that swaps it, or calls run while another test
// may have swapped it, must not use t.Parallel.
var notifyContext = signal.NotifyContext

// newFileSink is sink.NewFileSink behind an interface so a test can inject a
// sink whose Close fails and drive run's exit code without a real file. Same
// package-state caveat as notifyContext: no t.Parallel in tests that touch it.
var newFileSink = func(path string) (interface {
	sink.EventSink
	io.Closer
}, error) {
	return sink.NewFileSink(path)
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is main's testable body: it returns the process exit code instead of
// calling os.Exit, so deferred cleanup runs and tests can drive it. code is a
// named return so the deferred file-sink Close can turn a would-be 0 into 1: a
// failed Close (the final flush or the file close itself) is a sink failure a
// supervisor must be able to see.
//
// Exit codes:
//   - 0: -h, --version, a clean SIGINT/SIGTERM shutdown (with or without
//     --listen), or Run returning nil.
//   - 1: a runtime failure: --listen cannot bind (before the sink opens, so no
//     events file is created), the admin server fails (the run is cancelled
//     first), Run fails other than by cancellation, the file sink's Close
//     fails on an otherwise clean exit, or the metrics collector cannot be
//     built.
//   - 2: a usage or config error: a bad flag, no --config, a --listen that is
//     not host:port, a config that does not load, a bad sink setting or event
//     source, or a file sink that cannot be opened.
//
// With --listen, shutdown runs in this order: /readyz turns 503 and the run
// context is cancelled, Run returns, the results drain finishes (so a last
// scrape still sees every counter), the admin server shuts down (5s for
// in-flight requests, then closed, which is logged but does not change the
// exit code), and the file sink closes last.
func run(args []string, stdout, stderr io.Writer) (code int) {
	fs := flag.NewFlagSet("descry", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfgPath := fs.String("config", "", "path to YAML config")
	sinkOverride := fs.String("sink", "", `override config sink ("stdout" | "file")`)
	fileOverride := fs.String("file", "", "override config file_path (sink=file)")
	showVersion := fs.Bool("version", false, "print version and exit")
	listenAddr := fs.String("listen", "", "serve /metrics, /healthz and /readyz on this address (host:port, e.g. :9090); empty disables the server")
	perTarget := fs.Bool("metrics-per-target", true, "export per-target series labelled by the target URL (scheme, host and path); false keeps only fleet-wide series; only with --listen")
	if err := fs.Parse(args); err != nil {
		// -h/--help printed usage; exit 0 as flag.Parse's ExitOnError did.
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	if *showVersion {
		_, _ = fmt.Fprintf(stdout, "descry %s (%s) %s\n", version, commit, date)
		return 0
	}

	if *cfgPath == "" {
		_, _ = fmt.Fprintln(stderr, "error: --config is required")
		return 2
	}

	// A malformed address is a usage error, caught before anything is loaded;
	// whether it can be bound is only known later.
	listening := *listenAddr != ""
	if listening {
		if _, _, err := net.SplitHostPort(*listenAddr); err != nil {
			_, _ = fmt.Fprintf(stderr, "error: --listen %q: %v\n", *listenAddr, err)
			return 2
		}
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "error: %v\n", err)
		return 2
	}

	// CLI flags override config values; re-validate the combined result through
	// the same invariant Load uses.
	if *sinkOverride != "" {
		cfg.Sink = *sinkOverride
	}
	if *fileOverride != "" {
		cfg.FilePath = *fileOverride
	}
	if err := config.ValidateSink(cfg.Sink, cfg.FilePath); err != nil {
		_, _ = fmt.Fprintf(stderr, "error: %v\n", err)
		return 2
	}

	// Validate the event config before opening the sink: config.Load accepts
	// any non-empty source, but an unusable one would otherwise only surface
	// from Run, after the sink was created (and a file sink truncated/created).
	evCfg := event.Config{Source: cfg.Source}
	if _, err := event.NewEncoder(evCfg); err != nil {
		_, _ = fmt.Fprintf(stderr, "error: invalid event config (source %q): %v\n", check.RedactURL(cfg.Source), err)
		return 2
	}

	// Bind before the sink opens: a port already in use is the likeliest
	// runtime failure, and it should not leave a new events file behind. The
	// deferred Close is for the early returns below; after a served run the
	// server has closed the listener already.
	var ln net.Listener
	if listening {
		ln, err = bindAdmin(*listenAddr)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "error: --listen %s: %v\n", *listenAddr, err)
			return 1
		}
		defer func() { _ = ln.Close() }()
	}

	// Build the event sink.
	var s sink.EventSink = sink.NewStdoutSink(stdout)
	if cfg.Sink == "file" {
		fsink, err := newFileSink(cfg.FilePath)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "error: %v\n", err)
			return 2
		}
		defer func() {
			if err := fsink.Close(); err != nil {
				_, _ = fmt.Fprintf(stderr, "error: closing file sink: %v\n", err)
				if code == 0 {
					code = 1
				}
			}
		}()
		s = fsink
	}

	// failures is bumped by the drain as it goes, so it can be read while the
	// drain is live (the metrics collector reads it at scrape time), not only
	// after it returns.
	var failures atomic.Int64
	targets := buildTargets(cfg, slog.Default())

	sigCtx, stop := notifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// ctx is cancelled by a signal or by an admin server failure.
	ctx, cancel := context.WithCancel(sigCtx)
	defer cancel()

	// The check is wrapped so a redirect's final URL reaches the event in the
	// same clean form as the subject.
	chk := check.Check(sanitizedCheck{inner: httpcheck.New(cfg.Timeout)})
	// The collector exists only with --listen: without it nobody can scrape,
	// and the decorator's per-run bookkeeping would be waste. Order: the
	// collector decorates the check, the runner takes the decorated check,
	// then the collector is pointed at the runner.
	var col *metrics.Collector
	if listening {
		col, err = metrics.New(metrics.Options{
			Version:          version,
			Commit:           commit,
			DisablePerTarget: !*perTarget,
			ResultErrors:     failures.Load,
		})
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "error: metrics: %v\n", err)
			return 1
		}
		chk = col.Check(chk)
	}

	r := runner.New(
		chk,
		s,
		evCfg,
		targets,
		cfg.Interval,
		cfg.Concurrency,
	)
	var observeSkip func(metrics.SkipKind)
	if col != nil {
		col.SetRunner(r)
		col.SetTargetCount(len(targets))
		observeSkip = col.ObserveSkip
	}

	// Drain results to stderr + count failures. The goroutine exits when the
	// runner closes the results channel during shutdown. It is the only
	// Results() reader.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		drainResults(r.Results(), stderr, cfg.Interval, time.Now, &failures, observeSkip)
	}()

	ready := &readyState{}
	var adm *adminServer
	if listening {
		adm = serveAdmin(ln, newAdminMux(col.Registry(), ready), cancel)
	}

	// Once the first signal (or an admin server failure) has cancelled ctx,
	// report not ready and restore default signal handling, so a second
	// Ctrl-C / SIGTERM terminates the process even if shutdown is waiting on
	// an in-flight check or a sink that ignores its context.
	go func() {
		<-ctx.Done()
		ready.set(false)
		stop()
	}()

	// Run until interrupted. context.Canceled on shutdown is not fatal; any
	// other error is. Without WithShutdownGrace (never set here: this wait
	// relies on it) Run closes Results() before returning on every path; wait
	// for the drain so the last buffered diagnostics reach stderr, and the
	// last counts reach a scrape, before the process exits. The ctx re-check
	// after set(true) closes the race with the goroutine above clearing the
	// flag first.
	ready.set(true)
	if ctx.Err() != nil {
		ready.set(false)
	}
	runErr := r.Run(ctx)
	ready.set(false) // Run can also return on a non-cancel error
	<-drained
	// Shut the admin server down here, not in a defer: its error feeds the
	// exit code, and the server must stop before the file sink's deferred
	// Close.
	var serveErr error
	if adm != nil {
		serveErr = adm.shutdown(adminShutdownTimeout)
	}
	code = exitCodeFor(runErr)
	if code != 0 {
		_, _ = fmt.Fprintf(stderr, "error: %v\n", runErr)
	}
	if serveErr != nil {
		_, _ = fmt.Fprintf(stderr, "error: admin server: %v\n", serveErr)
		code = 1
	}
	return code
}

// exitCodeFor maps Run's return to the process exit code: nil or a
// cancellation (the normal signal-driven shutdown) is 0; any other error is a
// runtime failure. A grace timeout joined with ctx.Err() also maps to 0, but
// run never sets WithShutdownGrace.
func exitCodeFor(runErr error) int {
	if runErr == nil || errors.Is(runErr, context.Canceled) {
		return 0
	}
	return 1
}

// drainResults prints each Result's diagnostic to w until results is closed
// and adds one to failures for each check/mapping/publish failure seen, as it
// is seen, so a caller can read the running count while the drain is live.
// failures must be non-nil. Skipped slots are not check failures (the
// scheduler chose not to run, not a bad probe) and are printed distinctly;
// ErrSkippedQueued is tested before ErrSkipped since it wraps it. Each skip is
// passed to observeSkip (when non-nil) before the rate limit below, so a skip
// counter sees every skip, not only the printed ones. Under
// saturation every slot of every target skips, so skip lines are rate-limited
// to one per target per interval (with a suppressed count on the next line
// printed) — otherwise a slow stderr would stall this drain, fill Results(),
// and turn skips into drops. The window is the target's own effective
// interval (its override, else interval), so a fast target under a slow
// default is not silenced for several of its own slots. Failures are always
// printed.
func drainResults(results <-chan runner.Result, w io.Writer, interval time.Duration, now func() time.Time,
	failures *atomic.Int64, observeSkip func(metrics.SkipKind)) {
	// Diagnostics are best-effort: a failed stderr write is not something this
	// loop can act on, so write errors are deliberately ignored.
	type skipState struct {
		last       time.Time
		suppressed int
	}
	skips := map[string]*skipState{}
	for res := range results {
		var reason string
		var kind metrics.SkipKind
		switch {
		case errors.Is(res.Err, runner.ErrSkippedQueued):
			reason, kind = "prior run still queued (pool too small)", metrics.SkipQueued
		case errors.Is(res.Err, runner.ErrSkipped):
			reason, kind = "prior run still running (slow check)", metrics.SkipInFlight
		case res.Err != nil:
			failures.Add(1)
			_, _ = fmt.Fprintf(w, "check failed: %s: %v\n", check.RedactURL(res.Target.URL), res.Err)
			continue
		default:
			continue
		}
		if observeSkip != nil {
			observeSkip(kind)
		}
		st, ok := skips[res.Target.URL]
		if !ok {
			st = &skipState{}
			skips[res.Target.URL] = st
		}
		window := res.Target.Interval
		if window <= 0 {
			window = interval
		}
		t := now()
		if !st.last.IsZero() && t.Sub(st.last) < window && t.Sub(st.last) >= 0 {
			st.suppressed++
			continue
		}
		st.last = t
		if st.suppressed > 0 {
			_, _ = fmt.Fprintf(w, "check skipped: %s: %s (%d more skips for this target since the last line)\n",
				check.RedactURL(res.Target.URL), reason, st.suppressed)
			st.suppressed = 0
		} else {
			_, _ = fmt.Fprintf(w, "check skipped: %s: %s\n", check.RedactURL(res.Target.URL), reason)
		}
	}
}

// buildTargets maps config targets to engine targets. It warns once, at
// startup, for any target whose effective interval is not longer than the
// check timeout: that is not a Load error (a fast endpoint on a tight cadence
// is legitimate), but a response that uses its full timeout cannot finish
// before the next slot and will surface as ErrSkipped at runtime. It also
// warns once per URL that appears more than once: duplicates are independent
// targets to the runner (probed and reported separately, sharing a slot when
// their intervals match), which is rarely what a copy-pasted entry intended.
//
// A target without a "url" label gets eventURL(URL), which becomes the event
// subject; a "url" label set in the config is kept and published as-is. Since
// eventURL drops userinfo and query, different URLs can share a subject
// (https://h/p?region=a and https://h/p?region=b); that is warned once per
// subject, since consumers keyed on the subject can no longer tell them apart.
func buildTargets(cfg config.Config, log *slog.Logger) []check.Target {
	targets := make([]check.Target, len(cfg.Targets))
	seen := make(map[string]int, len(cfg.Targets))
	subjects := make(map[string]string, len(cfg.Targets)) // subject -> first raw URL with it
	warnedSubject := map[string]bool{}
	for i, t := range cfg.Targets {
		if seen[t.URL]++; seen[t.URL] == 2 {
			log.Warn("duplicate target URL; each entry is probed independently",
				"url", check.RedactURL(t.URL))
		}
		// Copy the labels: the engine target must not alias the config's map.
		lbl := make(map[string]string, len(t.Labels)+1)
		for k, v := range t.Labels {
			lbl[k] = v
		}
		// Ensure the "url" label is always present (used by the CloudEvent
		// subject). An operator-set label is published verbatim.
		if _, ok := lbl["url"]; !ok {
			lbl["url"] = eventURL(t.URL)
		}
		subject := lbl["url"]
		if first, ok := subjects[subject]; !ok {
			subjects[subject] = t.URL
		} else if first != t.URL && !warnedSubject[subject] {
			warnedSubject[subject] = true
			// RedactURL is the identity on a derived subject; it only matters
			// for an operator-set label that still carries userinfo.
			log.Warn("targets share an event subject; set a distinct url label to tell them apart",
				"subject", check.RedactURL(subject))
		}
		targets[i] = check.Target{URL: t.URL, Labels: lbl, Interval: t.Interval}

		effInterval := t.Interval
		if effInterval <= 0 {
			effInterval = cfg.Interval
		}
		if effInterval <= cfg.Timeout {
			log.Warn("check timeout exceeds interval; slow responses will surface as ErrSkipped",
				"url", check.RedactURL(t.URL), "interval", effInterval, "timeout", cfg.Timeout)
		}
	}
	return targets
}

// eventURL is the URL form published in events (subject, final_url): the
// metrics label rule, scheme://host[:port]/path with no userinfo, query or
// fragment, and "<unparseable>" when the URL has no safe rendering (see
// metrics.TargetLabel). Empty stays empty, so an Observation with no final
// URL still omits the field.
func eventURL(raw string) string {
	if raw == "" {
		return ""
	}
	return metrics.TargetLabel(raw)
}

// sanitizedCheck rewrites Observation.FinalURL with eventURL before the runner
// encodes it: after a redirect, or with none, the final URL is the request URL
// net/http followed, which still carries the target's userinfo (a relative
// redirect keeps it) and the query. Name and the error pass through; Labels
// are not touched (buildTargets already set the url label, and obs.Labels
// aliases the target's map).
type sanitizedCheck struct{ inner check.Check }

func (c sanitizedCheck) Name() string { return c.inner.Name() }

func (c sanitizedCheck) Run(ctx context.Context, t check.Target) (check.Observation, error) {
	obs, err := c.inner.Run(ctx, t)
	obs.FinalURL = eventURL(obs.FinalURL)
	return obs, err
}

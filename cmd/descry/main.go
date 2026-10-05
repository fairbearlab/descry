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
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fairbearlab/descry/check"
	httpcheck "github.com/fairbearlab/descry/checks/http"
	"github.com/fairbearlab/descry/config"
	"github.com/fairbearlab/descry/event"
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

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is main's testable body: it returns the process exit code (0 ok, 1
// runtime failure, 2 usage/config error) instead of calling os.Exit, so
// deferred cleanup runs and tests can drive it.
func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("descry", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfgPath := fs.String("config", "", "path to YAML config")
	sinkOverride := fs.String("sink", "", `override config sink ("stdout" | "file")`)
	fileOverride := fs.String("file", "", "override config file_path (sink=file)")
	showVersion := fs.Bool("version", false, "print version and exit")
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
		_, _ = fmt.Fprintf(stderr, "error: invalid event config (source %q): %v\n", cfg.Source, err)
		return 2
	}

	// Build the event sink.
	var s sink.EventSink = sink.NewStdoutSink(stdout)
	if cfg.Sink == "file" {
		fsink, err := sink.NewFileSink(cfg.FilePath)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "error: %v\n", err)
			return 2
		}
		defer func() {
			if err := fsink.Close(); err != nil {
				_, _ = fmt.Fprintf(stderr, "error: closing file sink: %v\n", err)
			}
		}()
		s = fsink
	}

	targets := buildTargets(cfg, slog.Default())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	r := runner.New(
		httpcheck.New(cfg.Timeout),
		s,
		evCfg,
		targets,
		cfg.Interval,
		cfg.Concurrency,
	)

	// Drain results to stderr + count failures. The goroutine exits when the
	// runner closes the results channel during shutdown.
	var failures atomic.Int64
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		failures.Store(drainResults(r.Results(), stderr, cfg.Interval, time.Now))
	}()

	// Once the first signal has cancelled ctx, restore default signal handling
	// so a second Ctrl-C / SIGTERM terminates the process even if shutdown is
	// waiting on an in-flight check or a sink that ignores its context.
	go func() {
		<-ctx.Done()
		stop()
	}()

	// Run until interrupted. context.Canceled on shutdown is not fatal; any
	// other error is. Without WithShutdownGrace (never set here: this wait
	// relies on it) Run closes Results() before returning on every path; wait
	// for the drain so the last buffered diagnostics reach stderr before the
	// process exits.
	runErr := r.Run(ctx)
	<-drained
	_ = failures.Load() // available for future exit-code logic
	code := exitCodeFor(runErr)
	if code != 0 {
		_, _ = fmt.Fprintf(stderr, "error: %v\n", runErr)
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
// and returns the number of check/mapping/publish failures seen. Skipped slots
// are not check failures (the scheduler chose not to run, not a bad probe) and
// are printed distinctly; ErrSkippedQueued is tested before ErrSkipped since it
// wraps it. Under saturation every slot of every target skips, so skip lines
// are rate-limited to one per target per interval (with a suppressed count on
// the next line printed) — otherwise a slow stderr would stall this drain, fill
// Results(), and turn skips into drops. The window is the target's own
// effective interval (its override, else interval), so a fast target under a
// slow default is not silenced for several of its own slots. Failures are
// always printed.
func drainResults(results <-chan runner.Result, w io.Writer, interval time.Duration, now func() time.Time) int64 {
	// Diagnostics are best-effort: a failed stderr write is not something this
	// loop can act on, so write errors are deliberately ignored.
	var failures int64
	type skipState struct {
		last       time.Time
		suppressed int
	}
	skips := map[string]*skipState{}
	for res := range results {
		var reason string
		switch {
		case errors.Is(res.Err, runner.ErrSkippedQueued):
			reason = "prior run still queued (pool too small)"
		case errors.Is(res.Err, runner.ErrSkipped):
			reason = "prior run still running (slow check)"
		case res.Err != nil:
			failures++
			_, _ = fmt.Fprintf(w, "check failed: %s: %v\n", check.RedactURL(res.Target.URL), res.Err)
			continue
		default:
			continue
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
	return failures
}

// buildTargets maps config targets to engine targets. It warns once, at
// startup, for any target whose effective interval is not longer than the
// check timeout: that is not a Load error (a fast endpoint on a tight cadence
// is legitimate), but a response that uses its full timeout cannot finish
// before the next slot and will surface as ErrSkipped at runtime. It also
// warns once per URL that appears more than once: duplicates are independent
// targets to the runner (probed and reported separately, sharing a slot when
// their intervals match), which is rarely what a copy-pasted entry intended.
func buildTargets(cfg config.Config, log *slog.Logger) []check.Target {
	targets := make([]check.Target, len(cfg.Targets))
	seen := make(map[string]int, len(cfg.Targets))
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
		// Ensure the "url" label is always present (used by the CloudEvent subject).
		if _, ok := lbl["url"]; !ok {
			lbl["url"] = t.URL
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

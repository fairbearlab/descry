package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

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
	cfgPath := flag.String("config", "", "path to YAML config")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("descry %s (%s) %s\n", version, commit, date)
		return
	}

	if *cfgPath == "" {
		fmt.Fprintln(os.Stderr, "error: --config is required")
		os.Exit(2)
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	// Build the event sink.
	var s sink.EventSink = sink.NewStdoutSink(os.Stdout)
	if cfg.Sink == "file" {
		if cfg.FilePath == "" {
			fmt.Fprintln(os.Stderr, "error: file_path is required when sink is \"file\"")
			os.Exit(2)
		}
		fs, err := sink.NewFileSink(cfg.FilePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
		defer func() {
			if err := fs.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "error: closing file sink: %v\n", err)
			}
		}()
		s = fs
	}

	// Map config targets to engine targets.
	targets := make([]check.Target, len(cfg.Targets))
	for i, t := range cfg.Targets {
		lbl := t.Labels
		if lbl == nil {
			lbl = map[string]string{}
		}
		// Ensure the "url" label is always present (used by the CloudEvent subject).
		if _, ok := lbl["url"]; !ok {
			lbl["url"] = t.URL
		}
		targets[i] = check.Target{URL: t.URL, Labels: lbl}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	r := runner.New(
		httpcheck.New(cfg.Timeout),
		s,
		event.Config{Source: cfg.Source},
		targets,
		cfg.Interval,
		cfg.Concurrency,
	)

	// Drain results to stderr + count failures. The goroutine exits when the
	// runner closes the results channel during shutdown.
	var failures atomic.Int64
	go func() {
		for res := range r.Results() {
			if res.Err != nil {
				failures.Add(1)
				fmt.Fprintf(os.Stderr, "check failed: %s: %v\n", res.Target.URL, res.Err)
			}
		}
	}()

	// Run until interrupted. ctx.Err() on shutdown is not fatal.
	_ = r.Run(ctx)
	_ = failures.Load() // available for future exit-code logic
}

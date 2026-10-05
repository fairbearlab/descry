package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fairbearlab/descry/check"
	"github.com/fairbearlab/descry/config"
	"github.com/fairbearlab/descry/runner"
)

// TestBuildTargets_WarnsWhenIntervalShorterThanTimeout: exactly one Warn per
// target whose effective interval (its own, or the runner default when zero)
// is not longer than the check timeout — equal counts, because a response
// that uses its full timeout lands exactly on the next slot; none otherwise.
// The URL in the log line is redacted, and the mapping itself carries Interval
// and the "url" label.
func TestBuildTargets_WarnsWhenIntervalShorterThanTimeout(t *testing.T) {
	pw := strings.Repeat("s3cret", 1) // built at runtime so nothing in the source is a literal credential
	cfg := config.Config{
		Interval: 30 * time.Second,
		Timeout:  10 * time.Second,
		Targets: []config.Target{
			{URL: "https://user:" + pw + "@tight.example/", Interval: 5 * time.Second}, // own interval < timeout → warn
			{URL: "https://fine.example/", Interval: 15 * time.Second},                 // own interval > timeout
			{URL: "https://default.example/"},                                          // 0 → default 30s > timeout
			{URL: "https://equal.example/", Interval: 10 * time.Second},                // equal is not "shorter"
		},
	}

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	targets := buildTargets(cfg, log)

	if len(targets) != 4 {
		t.Fatalf("targets = %d, want 4", len(targets))
	}
	if targets[0].Interval != 5*time.Second || targets[2].Interval != 0 {
		t.Errorf("Interval not passed through: %v / %v", targets[0].Interval, targets[2].Interval)
	}
	if targets[2].Labels["url"] != "https://default.example/" {
		t.Errorf("url label missing: %v", targets[2].Labels)
	}

	out := buf.String()
	if n := strings.Count(out, "check timeout exceeds interval"); n != 2 {
		t.Fatalf("want exactly 2 warnings (tight, equal), got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "tight.example") || !strings.Contains(out, "equal.example") {
		t.Errorf("warnings do not name both targets:\n%s", out)
	}
	if strings.Contains(out, pw) {
		t.Errorf("warning leaks userinfo:\n%s", out)
	}
	if strings.Contains(out, "fine.example") || strings.Contains(out, "default.example") {
		t.Errorf("warning fired for a target it should not have:\n%s", out)
	}
}

// TestBuildTargets_NoWarnWhenIntervalsAreWide: the common case is silent.
func TestBuildTargets_NoWarnWhenIntervalsAreWide(t *testing.T) {
	cfg := config.Config{
		Interval: 30 * time.Second,
		Timeout:  10 * time.Second,
		Targets:  []config.Target{{URL: "https://a.example/"}, {URL: "https://b.example/", Interval: time.Minute}},
	}
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	buildTargets(cfg, log)
	if buf.Len() != 0 {
		t.Fatalf("unexpected log output:\n%s", buf.String())
	}
}

// TestBuildTargets_PreservesExistingURLLabel: a target that already carries a
// "url" label keeps it; buildTargets only fills the label in when absent.
func TestBuildTargets_PreservesExistingURLLabel(t *testing.T) {
	cfg := config.Config{
		Interval: 30 * time.Second,
		Timeout:  10 * time.Second,
		Targets: []config.Target{
			{URL: "https://a.example/", Labels: map[string]string{"url": "custom", "env": "prod"}},
			{URL: "https://b.example/", Labels: map[string]string{"env": "prod"}},
		},
	}
	targets := buildTargets(cfg, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	if got := targets[0].Labels["url"]; got != "custom" {
		t.Errorf("existing url label overwritten: got %q, want %q", got, "custom")
	}
	if got := targets[0].Labels["env"]; got != "prod" {
		t.Errorf("other labels not preserved: %v", targets[0].Labels)
	}
	if got := targets[1].Labels["url"]; got != "https://b.example/" {
		t.Errorf("missing url label not filled: got %q", got)
	}
}

// TestBuildTargets_WarnsOnceForDuplicateURL: a URL listed twice is two
// independent targets to the runner; warn once per duplicated URL (not per
// extra copy, not for unique URLs), redacted.
func TestBuildTargets_WarnsOnceForDuplicateURL(t *testing.T) {
	dup := "https://user:" + strings.Repeat("pw", 1) + "@dup.example/" // built at runtime: no literal credential in source
	cfg := config.Config{
		Interval: 30 * time.Second,
		Timeout:  10 * time.Second,
		Targets: []config.Target{
			{URL: dup},
			{URL: "https://unique.example/"},
			{URL: dup},
			{URL: dup, Interval: time.Minute},
		},
	}
	var buf bytes.Buffer
	targets := buildTargets(cfg, slog.New(slog.NewTextHandler(&buf, nil)))
	if len(targets) != 4 {
		t.Fatalf("duplicates must not be dropped: got %d targets, want 4", len(targets))
	}
	out := buf.String()
	if n := strings.Count(out, "duplicate target URL"); n != 1 {
		t.Fatalf("want exactly 1 duplicate warning, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "dup.example") || strings.Contains(out, "unique.example") || strings.Contains(out, "pw@") {
		t.Errorf("duplicate warning names the wrong URL or leaks userinfo:\n%s", out)
	}
}

// TestDrainResults_RateLimitsSkipsPerTarget: skips print once per target per
// interval with a suppressed count on the next line; failures always print;
// nil Results print nothing; the failure count is returned.
func TestDrainResults_RateLimitsSkipsPerTarget(t *testing.T) {
	ch := make(chan runner.Result, 16)
	a := check.Target{URL: "https://a.example/"}
	b := check.Target{URL: "https://b.example/"}
	clock := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clock }

	ch <- runner.Result{Target: a, Err: runner.ErrSkipped}       // printed
	ch <- runner.Result{Target: a, Err: runner.ErrSkipped}       // suppressed (same interval)
	ch <- runner.Result{Target: a, Err: runner.ErrSkippedQueued} // suppressed
	ch <- runner.Result{Target: b, Err: runner.ErrSkippedQueued} // printed (different target)
	ch <- runner.Result{Target: a}                               // ok: silent
	ch <- runner.Result{Target: a, Err: errors.New("publish failed")}
	close(ch)

	var out bytes.Buffer
	if got := drainResults(ch, &out, 30*time.Second, now); got != 1 {
		t.Fatalf("failures = %d, want 1", got)
	}
	s := out.String()
	if n := strings.Count(s, "check skipped: https://a.example/"); n != 1 {
		t.Errorf("a.example skip lines = %d, want 1 (rate-limited):\n%s", n, s)
	}
	if !strings.Contains(s, "check skipped: https://b.example/: prior run still queued") {
		t.Errorf("b.example queued skip missing:\n%s", s)
	}
	if !strings.Contains(s, "check failed: https://a.example/: publish failed") {
		t.Errorf("failure line missing:\n%s", s)
	}

	// After the interval passes, the next skip prints again and reports the
	// suppressed count.
	ch2 := make(chan runner.Result, 4)
	ch2 <- runner.Result{Target: a, Err: runner.ErrSkipped}
	ch2 <- runner.Result{Target: a, Err: runner.ErrSkipped}
	ch2 <- runner.Result{Target: a, Err: runner.ErrSkipped}
	close(ch2)
	out.Reset()
	calls := 0
	tick := func() time.Time {
		calls++
		if calls == 3 {
			clock = clock.Add(31 * time.Second)
		}
		return clock
	}
	drainResults(ch2, &out, 30*time.Second, tick)
	s = out.String()
	if n := strings.Count(s, "check skipped:"); n != 2 {
		t.Fatalf("skip lines across an interval boundary = %d, want 2:\n%s", n, s)
	}
	if !strings.Contains(s, "(1 more skips for this target since the last line)") {
		t.Errorf("suppressed count not reported:\n%s", s)
	}
}

// TestDrainResults_SkipWindowUsesTargetInterval: the rate-limit window is the
// target's own effective interval, not the top-level default. A 5s target under
// a 30s default prints again 5s later; a target with no override still uses
// the default.
func TestDrainResults_SkipWindowUsesTargetInterval(t *testing.T) {
	fast := check.Target{URL: "https://fast.example/", Interval: 5 * time.Second}
	dflt := check.Target{URL: "https://dflt.example/"}
	clock := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	calls := 0
	now := func() time.Time {
		calls++
		if calls == 3 { // before the second round: past fast's window, inside dflt's
			clock = clock.Add(6 * time.Second)
		}
		return clock
	}

	ch := make(chan runner.Result, 8)
	ch <- runner.Result{Target: fast, Err: runner.ErrSkipped} // printed, t=0
	ch <- runner.Result{Target: dflt, Err: runner.ErrSkipped} // printed, t=0
	ch <- runner.Result{Target: fast, Err: runner.ErrSkipped} // t=6s: printed again (5s window)
	ch <- runner.Result{Target: dflt, Err: runner.ErrSkipped} // t=6s: suppressed (30s window)
	close(ch)
	var out bytes.Buffer
	drainResults(ch, &out, 30*time.Second, now)

	s := out.String()
	if n := strings.Count(s, "check skipped: https://fast.example/"); n != 2 {
		t.Errorf("fast.example skip lines = %d, want 2 (5s window):\n%s", n, s)
	}
	if n := strings.Count(s, "check skipped: https://dflt.example/"); n != 1 {
		t.Errorf("dflt.example skip lines = %d, want 1 (30s default window):\n%s", n, s)
	}
}

// TestRun_InvalidSourceFailsBeforeSink: a source config.Load accepts but the
// event encoder rejects ("%zz" is a bad percent-escape) must exit 2 with a
// clear message, before the sink is opened: nothing on stdout and no file
// created for a file sink.
func TestRun_InvalidSourceFailsBeforeSink(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "descry.yaml")
	yaml := "source: \"%zz\"\ninterval: 30s\ntimeout: 5s\ntargets:\n  - url: https://example.com/\n"
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	outFile := filepath.Join(dir, "events.jsonl")

	for name, args := range map[string][]string{
		"stdout": {"--config", cfgPath},
		"file":   {"--config", cfgPath, "--sink", "file", "--file", outFile},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(args, &stdout, &stderr); code != 2 {
				t.Fatalf("exit code = %d, want 2; stderr:\n%s", code, stderr.String())
			}
			if stdout.Len() != 0 {
				t.Errorf("sink wrote output despite invalid source: %q", stdout.String())
			}
			if !strings.Contains(stderr.String(), "invalid event config") || !strings.Contains(stderr.String(), "%zz") {
				t.Errorf("stderr lacks a clear message: %q", stderr.String())
			}
			if _, err := os.Stat(outFile); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("file sink was opened before validation (stat err = %v)", err)
			}
		})
	}
}

// TestRun_FlagParseExitCodes: -h/--help print usage and exit 0 (as the old
// flag.Parse ExitOnError path did); an unknown flag is a usage error, exit 2.
func TestRun_FlagParseExitCodes(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want int
	}{
		{[]string{"-h"}, 0},
		{[]string{"--help"}, 0},
		{[]string{"--no-such-flag"}, 2},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(tc.args, &stdout, &stderr); code != tc.want {
				t.Fatalf("exit code = %d, want %d; stderr:\n%s", code, tc.want, stderr.String())
			}
			if !strings.Contains(stderr.String(), "-config") {
				t.Errorf("usage not printed to stderr: %q", stderr.String())
			}
		})
	}
}

// Value: protects=run's exit-code contract: -version 0 and prints; missing/unreadable config, bad sink override, unopenable file sink exit 2
// Value: fails_when=a refactor returns the wrong code, drops a message, or -version stops printing the build info line
// Value: why_new=existing run tests cover only -h, unknown flag and an invalid source; these pre-run branches were untested; seam=none
func TestRun_UsageAndConfigErrors(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "descry.yaml")
	yaml := "source: https://example.com/descry\ninterval: 30s\ntimeout: 5s\ntargets:\n  - url: https://example.com/\n"
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	unopenable := filepath.Join(dir, "no-such-dir", "events.jsonl")

	for _, tc := range []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		{"version", []string{"-version"}, 0, "descry ", ""},
		{"missing config flag", nil, 2, "", "--config is required"},
		{"config file absent", []string{"--config", filepath.Join(dir, "absent.yaml")}, 2, "", "open config"},
		{"sink override invalid", []string{"--config", cfgPath, "--sink", "bogus"}, 2, "", "must be"},
		{"file override without path", []string{"--config", cfgPath, "--sink", "file"}, 2, "", "file_path is required"},
		{"file sink unopenable", []string{"--config", cfgPath, "--sink", "file", "--file", unopenable}, 2, "", "error:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(tc.args, &stdout, &stderr); code != tc.wantCode {
				t.Fatalf("exit code = %d, want %d; stderr:\n%s", code, tc.wantCode, stderr.String())
			}
			if !strings.Contains(stdout.String(), tc.wantStdout) {
				t.Errorf("stdout = %q, want substring %q", stdout.String(), tc.wantStdout)
			}
			if tc.wantStdout == "" && stdout.Len() != 0 {
				t.Errorf("unexpected stdout: %q", stdout.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Errorf("stderr = %q, want substring %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

// TestExitCodeFor: a signal-driven shutdown (ctx cancelled, alone or joined
// with ErrShutdownTimeout under WithShutdownGrace) exits 0; any other Run
// error exits 1. Guards against a refactor that drops the Canceled filter and
// turns every Ctrl-C into a failure exit.
func TestExitCodeFor(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"nil", nil, 0},
		{"canceled", context.Canceled, 0},
		{"canceled with shutdown timeout", errors.Join(context.Canceled, runner.ErrShutdownTimeout), 0},
		{"other error", errors.New("runner: event config: boom"), 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := exitCodeFor(tc.err); got != tc.want {
				t.Errorf("exitCodeFor(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}

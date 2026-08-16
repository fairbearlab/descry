package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/fairbearlab/descry/config"
)

// TestBuildTargets_WarnsWhenIntervalShorterThanTimeout: exactly one Warn per
// target whose effective interval (its own, or the runner default when zero)
// is shorter than the check timeout; none otherwise. The URL in the log line
// is redacted, and the mapping itself carries Interval and the "url" label.
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
	if n := strings.Count(out, "check timeout exceeds interval"); n != 1 {
		t.Fatalf("want exactly 1 warning, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "tight.example") {
		t.Errorf("warning does not name the target:\n%s", out)
	}
	if strings.Contains(out, pw) {
		t.Errorf("warning leaks userinfo:\n%s", out)
	}
	if strings.Contains(out, "fine.example") || strings.Contains(out, "default.example") || strings.Contains(out, "equal.example") {
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

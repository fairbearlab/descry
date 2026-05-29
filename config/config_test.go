package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoad_Defaults(t *testing.T) {
	p := writeConfig(t, "source: descry/test\ntargets:\n  - url: https://example.com\n")
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Interval != 30*time.Second {
		t.Errorf("interval = %v, want 30s", cfg.Interval)
	}
	if cfg.Timeout != 10*time.Second {
		t.Errorf("timeout = %v, want 10s", cfg.Timeout)
	}
	if cfg.Concurrency != 4 {
		t.Errorf("concurrency = %d, want 4", cfg.Concurrency)
	}
	if cfg.Sink != "stdout" {
		t.Errorf("sink = %q, want stdout", cfg.Sink)
	}
}

func TestLoad_ParsesValues(t *testing.T) {
	p := writeConfig(t, "source: s\nsink: file\nfile_path: /tmp/out.jsonl\n"+
		"interval: 5s\ntimeout: 2s\nconcurrency: 8\ntargets:\n  - url: https://example.com\n")
	cfg, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Interval != 5*time.Second || cfg.Timeout != 2*time.Second || cfg.Concurrency != 8 {
		t.Errorf("parsed durations/concurrency wrong: %+v", cfg)
	}
	if cfg.Sink != "file" || cfg.FilePath != "/tmp/out.jsonl" {
		t.Errorf("sink/file_path wrong: %+v", cfg)
	}
}

func TestLoad_Errors(t *testing.T) {
	cases := map[string]string{
		"missing source":   "targets:\n  - url: https://x.com\n",
		"bad sink":         "source: s\nsink: kafka\ntargets:\n  - url: https://x.com\n",
		"file no path":     "source: s\nsink: file\ntargets:\n  - url: https://x.com\n",
		"no targets":       "source: s\n",
		"empty target url": "source: s\ntargets:\n  - url: \"\"\n",
		"unknown field":    "source: s\nbogus: 1\ntargets:\n  - url: https://x.com\n",
		"bad interval":     "source: s\ninterval: notaduration\ntargets:\n  - url: https://x.com\n",
		"bad timeout":      "source: s\ntimeout: notaduration\ntargets:\n  - url: https://x.com\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, body)); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestLoad_OpenError(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml")); err == nil {
		t.Fatal("expected open error, got nil")
	}
}

func TestValidateSink(t *testing.T) {
	if err := ValidateSink("stdout", ""); err != nil {
		t.Errorf("stdout: %v", err)
	}
	if err := ValidateSink("file", "/tmp/x"); err != nil {
		t.Errorf("file with path: %v", err)
	}
	if err := ValidateSink("file", ""); err == nil {
		t.Error("file without path: want error")
	}
	if err := ValidateSink("kafka", ""); err == nil {
		t.Error("bad sink: want error")
	}
}

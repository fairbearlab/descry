package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Target is a single monitored URL with opaque labels.
type Target struct {
	URL    string            `yaml:"url"`
	Labels map[string]string `yaml:"labels"`
}

// rawConfig is the on-disk shape; duration fields come in as strings.
type rawConfig struct {
	Source      string            `yaml:"source"`
	Sink        string            `yaml:"sink"`      // "stdout" | "file"
	FilePath    string            `yaml:"file_path"` // required when sink == "file"
	Interval    string            `yaml:"interval"`
	Timeout     string            `yaml:"timeout"`
	Concurrency int               `yaml:"concurrency"`
	Targets     []Target          `yaml:"targets"`
	Labels      map[string]string `yaml:"labels"` // global labels (unused in v1, reserved)
}

// Config is the parsed, validated config.
type Config struct {
	Source      string
	Sink        string // "stdout" | "file"
	FilePath    string
	Interval    time.Duration
	Timeout     time.Duration
	Concurrency int
	Targets     []Target
}

// Load reads and validates the YAML config at path.
// Unknown fields are rejected (KnownFields(true)).
func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()

	var raw rawConfig
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&raw); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}

	cfg := Config{
		Source:      raw.Source,
		Sink:        raw.Sink,
		FilePath:    raw.FilePath,
		Concurrency: raw.Concurrency,
		Targets:     raw.Targets,
	}

	// Parse duration strings; fall back to defaults on empty.
	if raw.Interval != "" {
		d, err := time.ParseDuration(raw.Interval)
		if err != nil {
			return Config{}, fmt.Errorf("parse interval %q: %w", raw.Interval, err)
		}
		cfg.Interval = d
	}
	if raw.Timeout != "" {
		d, err := time.ParseDuration(raw.Timeout)
		if err != nil {
			return Config{}, fmt.Errorf("parse timeout %q: %w", raw.Timeout, err)
		}
		cfg.Timeout = d
	}

	// Apply defaults.
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.Sink == "" {
		cfg.Sink = "stdout"
	}

	return cfg, nil
}

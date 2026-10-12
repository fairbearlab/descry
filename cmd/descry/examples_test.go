package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/fairbearlab/descry/config"
	"github.com/fairbearlab/descry/event"
)

// TestShippedConfigsLoad keeps the example configs usable: a renamed or
// removed config field, or a config that run rejects after loading it, would
// otherwise surface only as a pod in a crash loop. Past config.Load it runs
// the config checks run applies before the sink opens (label collisions,
// event source), as the Deployment runs it (--listen with per-target
// series on, so shared metrics labels are refused).
func TestShippedConfigsLoad(t *testing.T) {
	raw, err := os.ReadFile("../../examples/kubernetes/configmap.yaml")
	if err != nil {
		t.Fatalf("read ConfigMap: %v", err)
	}
	var cm struct {
		Data map[string]string `yaml:"data"`
	}
	if err := yaml.Unmarshal(raw, &cm); err != nil {
		t.Fatalf("parse ConfigMap: %v", err)
	}
	embedded, ok := cm.Data["config.yaml"]
	if !ok {
		t.Fatal("ConfigMap has no data.config.yaml, which the Deployment mounts")
	}
	k8s := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(k8s, []byte(embedded), 0o600); err != nil {
		t.Fatal(err)
	}

	for name, path := range map[string]string{
		"example.yaml":                       "../../example.yaml",
		"examples/kubernetes/configmap.yaml": k8s,
	} {
		cfg, err := config.Load(path)
		if err != nil {
			t.Errorf("%s: config.Load: %v", name, err)
			continue
		}
		if len(cfg.Targets) == 0 {
			t.Errorf("%s: no targets", name)
		}
		var stderr strings.Builder
		if !checkLabelCollisions(cfg.Targets, &stderr) {
			t.Errorf("%s: %s", name, strings.TrimSpace(stderr.String()))
		}
		if _, err := event.NewEncoder(event.Config{Source: cfg.Source}); err != nil {
			t.Errorf("%s: invalid event config: %v", name, err)
		}
	}
}

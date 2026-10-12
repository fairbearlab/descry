package main

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestReleaseImageMatchesKustomization keeps the published image and the
// Kubernetes example in step. The release workflow pushes the image that
// .goreleaser.yaml's kos entry names, and examples/kubernetes pins it by
// name and newTag. goreleaser's {{ .Version }} drops the v that newTag
// carries, so an image tagged with it would leave `kubectl apply -k` pulling
// a tag that was never pushed; only a release would show it.
func TestReleaseImageMatchesKustomization(t *testing.T) {
	var gr struct {
		Kos []struct {
			Repositories []string `yaml:"repositories"`
			Tags         []string `yaml:"tags"`
		} `yaml:"kos"`
	}
	readYAML(t, "../../.goreleaser.yaml", &gr)
	var kz struct {
		Images []struct {
			Name   string `yaml:"name"`
			NewTag string `yaml:"newTag"`
		} `yaml:"images"`
	}
	readYAML(t, "../../examples/kubernetes/kustomization.yaml", &kz)

	if len(gr.Kos) != 1 {
		t.Fatalf(".goreleaser.yaml: want 1 kos entry, got %d", len(gr.Kos))
	}
	if len(kz.Images) != 1 {
		t.Fatalf("kustomization.yaml: want 1 images entry, got %d", len(kz.Images))
	}
	ko, img := gr.Kos[0], kz.Images[0]

	if !slices.Equal(ko.Repositories, []string{img.Name}) {
		t.Errorf("kos repositories %q, want exactly the kustomization image %q", ko.Repositories, img.Name)
	}
	// Compare templates with the spaces inside {{ }} removed, so "{{.Tag}}"
	// and "{{ .Tag }}" are the same tag.
	squash := strings.NewReplacer(" ", "", "\t", "")
	var tags []string
	for _, tag := range ko.Tags {
		tags = append(tags, squash.Replace(tag))
	}
	if !slices.Contains(tags, "{{.Tag}}") {
		t.Errorf("kos tags %q: want {{ .Tag }}, which keeps the v that newTag has", ko.Tags)
	}
	for i, tag := range tags {
		if strings.Contains(tag, ".Version") {
			t.Errorf("kos tag %q uses .Version, which drops the v; newTag is %q", ko.Tags[i], img.NewTag)
		}
	}
	if !regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+$`).MatchString(img.NewTag) {
		t.Errorf("kustomization newTag %q: want a release tag like v1.2.3", img.NewTag)
	}

	var dep struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []struct {
						Image string `yaml:"image"`
					} `yaml:"containers"`
				} `yaml:"spec"`
			} `yaml:"template"`
		} `yaml:"spec"`
	}
	readYAML(t, "../../examples/kubernetes/deployment.yaml", &dep)
	for _, c := range dep.Spec.Template.Spec.Containers {
		if c.Image != img.Name {
			t.Errorf("deployment image %q is not the kustomization image %q, so newTag would not apply", c.Image, img.Name)
		}
	}
}

func readYAML(t *testing.T, path string, v any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if err := yaml.Unmarshal(raw, v); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
}

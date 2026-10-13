package main

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Value: protects=published image name/tag matches the Kubernetes example; base pinned by digest; latest skipped on prereleases; both platforms
// fails_when=bare is dropped, kos tag becomes .Version, base loses its sha256 pin, any latest tag loses its .Prerelease guard, a platform is dropped, newName is set, or no deployment container decodes
// why_new=goreleaser check and the snapshot build validate syntax only; none runs the prerelease or tag-name paths
// seam=none; no_claim=does not prove the push or signatures, which run first on a tag
//
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
			Bare         bool     `yaml:"bare"`
			Tags         []string `yaml:"tags"`
			BaseImage    string   `yaml:"base_image"`
			Platforms    []string `yaml:"platforms"`
		} `yaml:"kos"`
	}
	readYAML(t, "../../.goreleaser.yaml", &gr)
	var kz struct {
		Images []struct {
			Name    string `yaml:"name"`
			NewName string `yaml:"newName"`
			NewTag  string `yaml:"newTag"`
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
	// Without bare, ko appends the import path to the repository, so the
	// pushed name would not be the one the kustomization pulls.
	if !ko.Bare {
		t.Errorf("kos bare = false: ko would push under %q plus an import-path suffix, not %q", ko.Repositories, img.Name)
	}
	if img.NewName != "" {
		t.Errorf("kustomization newName %q would pull another image than the published %q", img.NewName, img.Name)
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
	// latest must move only on final releases: its template renders empty on a
	// prerelease (ko drops empty tags), so a prerelease cannot take latest.
	// Every tag that names latest must be exactly the guarded form, so latest
	// cannot sit outside the conditional or in an else branch.
	sawLatest, guarded := false, true
	for _, tag := range tags {
		if strings.Contains(tag, "latest") {
			sawLatest = true
			guarded = guarded && tag == "{{ifnot.Prerelease}}latest{{end}}"
		}
	}
	if !sawLatest || !guarded {
		t.Errorf("kos tags %q: want latest, and only inside {{ if not .Prerelease }}", ko.Tags)
	}
	if !regexp.MustCompile(`@sha256:[0-9a-f]{64}$`).MatchString(ko.BaseImage) {
		t.Errorf("kos base_image %q: want it pinned by sha256 digest", ko.BaseImage)
	}
	for _, p := range []string{"linux/amd64", "linux/arm64"} {
		if !slices.Contains(ko.Platforms, p) {
			t.Errorf("kos platforms %q: missing %s, which the README promises", ko.Platforms, p)
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
	if len(dep.Spec.Template.Spec.Containers) == 0 {
		t.Fatal("deployment.yaml: no containers decoded, so the image check would pass without checking anything")
	}
	for _, c := range dep.Spec.Template.Spec.Containers {
		if c.Image != img.Name {
			t.Errorf("deployment image %q is not the kustomization image %q, so newTag would not apply", c.Image, img.Name)
		}
	}
}

// goreleaserRun is what the tests need from goreleaser-action args.
type goreleaserRun struct {
	sub      string   // subcommand: release, build, check
	skips    []string // from --skip=a,b and --skip a,b
	snapshot bool
	config   bool // --config or -f: a file other than .goreleaser.yaml
}

func parseGoreleaserArgs(args string) goreleaserRun {
	f := strings.Fields(args)
	var r goreleaserRun
	if len(f) > 0 {
		r.sub = f[0]
	}
	for i := 1; i < len(f); i++ {
		a := f[i]
		switch {
		case a == "--skip" && i+1 < len(f):
			i++
			r.skips = append(r.skips, strings.Split(f[i], ",")...)
		case strings.HasPrefix(a, "--skip="):
			r.skips = append(r.skips, strings.Split(strings.TrimPrefix(a, "--skip="), ",")...)
		case a == "--snapshot":
			r.snapshot = true
		case a == "--config", a == "-f", strings.HasPrefix(a, "--config="), strings.HasPrefix(a, "-f="):
			r.config = true
		}
	}
	return r
}

func containsAll(list []string, want ...string) bool {
	for _, w := range want {
		if !slices.Contains(list, w) {
			return false
		}
	}
	return true
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

// Value: protects=keyless signing can run on a tag: OIDC and packages write only on the release job, cache off, signs by digest with --bundle and --yes, no --skip or --snapshot
// fails_when=id-token or packages dropped, workflow-wide write added, cache re-enabled, the trigger is not tags v*, the run is not a plain release (skip, snapshot, other config), a cosign flag dropped, or image signed by tag
// why_new=signing and the GHCR push run only on a tag, so a missing permission shows up at release time, not in CI
// seam=none; no_claim=does not run cosign or push; a real tag is the end-to-end check
func TestReleaseSigningContract(t *testing.T) {
	var gr struct {
		Signs []struct {
			Cmd       string   `yaml:"cmd"`
			Artifacts string   `yaml:"artifacts"`
			Signature string   `yaml:"signature"`
			Args      []string `yaml:"args"`
		} `yaml:"signs"`
		DockerSigns []struct {
			Cmd       string   `yaml:"cmd"`
			Artifacts string   `yaml:"artifacts"`
			Args      []string `yaml:"args"`
		} `yaml:"docker_signs"`
	}
	readYAML(t, "../../.goreleaser.yaml", &gr)
	// --bundle=${signature} is what writes the .sigstore.json file goreleaser
	// uploads, and --yes keeps cosign from prompting on a runner with no tty.
	if len(gr.Signs) != 1 || gr.Signs[0].Cmd != "cosign" || gr.Signs[0].Artifacts != "checksum" ||
		!strings.HasSuffix(gr.Signs[0].Signature, ".sigstore.json") ||
		!containsAll(gr.Signs[0].Args, "sign-blob", "--bundle=${signature}", "${artifact}", "--yes") {
		t.Errorf("signs %+v: want one cosign sign-blob of the checksum file into a .sigstore.json bundle, as README.md verifies", gr.Signs)
	}
	if len(gr.DockerSigns) != 1 || gr.DockerSigns[0].Cmd != "cosign" || gr.DockerSigns[0].Artifacts != "manifests" ||
		!containsAll(gr.DockerSigns[0].Args, "sign", "${artifact}@${digest}", "--yes") {
		t.Errorf("docker_signs %+v: want one cosign signature of the manifest list by digest, so the tag README.md verifies resolves to it", gr.DockerSigns)
	}

	var wf struct {
		On          map[string]map[string][]string `yaml:"on"`
		Permissions map[string]string              `yaml:"permissions"`
		Jobs        struct {
			Release struct {
				Permissions map[string]string `yaml:"permissions"`
				Steps       []struct {
					Uses string         `yaml:"uses"`
					With map[string]any `yaml:"with"`
				} `yaml:"steps"`
			} `yaml:"release"`
		} `yaml:"jobs"`
	}
	readYAML(t, "../../.github/workflows/release.yml", &wf)
	// The signing identity README.md verifies is release.yml@refs/tags/<tag>;
	// any other trigger would sign under a different ref.
	if len(wf.On) != 1 || len(wf.On["push"]) != 1 || !slices.Equal(wf.On["push"]["tags"], []string{"v*"}) {
		t.Errorf("release.yml on = %v: want only push on tags v*, the refs/tags identity README.md verifies", wf.On)
	}
	if len(wf.Permissions) != 1 || wf.Permissions["contents"] != "read" {
		t.Errorf("workflow permissions %v: want only contents: read, with writes granted on the release job", wf.Permissions)
	}
	for _, p := range []string{"contents", "packages", "id-token"} {
		if wf.Jobs.Release.Permissions[p] != "write" {
			t.Errorf("release job permission %s = %q, want write (release, GHCR push, keyless OIDC)", p, wf.Jobs.Release.Permissions[p])
		}
	}
	seen := map[string]bool{}
	for _, st := range wf.Jobs.Release.Steps {
		name, _, _ := strings.Cut(st.Uses, "@")
		seen[name] = true
		switch name {
		case "actions/setup-go":
			if st.With["cache"] != false {
				t.Errorf("setup-go cache = %v, want false so a tag build restores nothing it signs", st.With["cache"])
			}
		case "goreleaser/goreleaser-action":
			// --snapshot skips publishing and signing just as --skip does, and
			// --config would run a file these tests do not read.
			args, _ := st.With["args"].(string)
			if r := parseGoreleaserArgs(args); r.sub != "release" || len(r.skips) > 0 || r.snapshot || r.config {
				t.Errorf("goreleaser args %q: want release with no --skip, --snapshot or --config; a tag must publish and sign", args)
			}
		}
	}
	for _, name := range []string{"sigstore/cosign-installer", "docker/login-action", "actions/setup-go", "goreleaser/goreleaser-action"} {
		if !seen[name] {
			t.Errorf("release job has no %s step", name)
		}
	}
}

// Value: protects=the CI snapshot still builds the image, the only build of it before a tag
// fails_when=release-check drops its snapshot step, stops using the release subcommand, points at another config, or skips ko (either --skip form)
// why_new=a skipped ko pipe keeps CI green while the image config goes unbuilt until a tag
// seam=none; no_claim=does not check the image the snapshot builds
func TestReleaseCheckBuildsImage(t *testing.T) {
	var ci struct {
		Jobs map[string]struct {
			Steps []struct {
				Uses string         `yaml:"uses"`
				With map[string]any `yaml:"with"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	readYAML(t, "../../.github/workflows/ci.yml", &ci)
	var snapshot bool
	for _, st := range ci.Jobs["release-check"].Steps {
		args, _ := st.With["args"].(string)
		r := parseGoreleaserArgs(args)
		if !r.snapshot {
			continue
		}
		snapshot = true
		// Only the release subcommand runs the ko pipe; build does not.
		if r.sub != "release" || slices.Contains(r.skips, "ko") || r.config {
			t.Errorf("release-check args %q: want release with ko not skipped and no --config, so the image is built before a tag", args)
		}
	}
	if !snapshot {
		t.Error("release-check has no goreleaser --snapshot step")
	}
}

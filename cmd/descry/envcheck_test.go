package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// Value: protects=the d.goreleaser env-check row's verdicts: PASS only on an exact pin match, WARN on a mismatch, prerelease or unreadable version, SKIP when goreleaser is not runnable, and the pin read from the goreleaser-action step only
// fails_when=a prerelease passes against a plain pin, a GitVersion line without a version falls back to another number, another step's version: is taken as the pin, a missing or broken goreleaser stops being SKIP, or the script exits non-zero
// why_new=the engine's needs gate keeps goreleaser.sh from running where goreleaser is missing and CI never runs bin/env-check, so nothing else runs its verdict logic
// seam=none; no_claim=does not run a real goreleaser or the env-check engine, only the script against a stub on a minimal PATH
//
// TestGoreleaserProbe exists because nothing else exercises
// .loops/checks/goreleaser.sh: the env-check engine skips the row on machines
// without goreleaser and CI never runs bin/env-check. Each case runs the
// script in a throwaway repo holding a fake release.yml, with a stub
// goreleaser first on a PATH that holds only links to awk, grep, head and cat, and
// checks the exit code and the verdict line.
func TestGoreleaserProbe(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the check script is POSIX sh")
	}
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("sh is not on PATH")
	}
	script, err := filepath.Abs("../../.loops/checks/goreleaser.sh")
	if err != nil {
		t.Fatal(err)
	}
	// The script's PATH holds only the stub dir and links to the tools the
	// script and the stub call, so a goreleaser installed on the host never
	// runs in place of the stub.
	toolDir := t.TempDir()
	for _, tool := range []string{"awk", "grep", "head", "cat"} {
		p, err := exec.LookPath(tool)
		if err != nil {
			t.Skipf("%s is not on PATH", tool)
		}
		if err := os.Symlink(p, filepath.Join(toolDir, tool)); err != nil {
			t.Fatal(err)
		}
	}

	workflow := func(pre, version string) string {
		return `name: release
on:
  push:
    tags: ["v*"]
jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
` + pre + `      - uses: goreleaser/goreleaser-action@v7
        with:
` + version + `          args: release --clean
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
`
	}
	plain := workflow("", "          version: v2.18.3\n")
	koFirst := workflow("      - uses: ko-build/setup-ko@v0.9\n        with:\n          version: v0.17.1\n",
		"          version: v2.18.3 # pinned\n")
	noAction := `name: release
jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: ko-build/setup-ko@v0.9
        with:
          version: v0.17.1
`

	tests := []struct {
		name    string
		yml     string
		stub    bool
		out     string
		rc      int
		verdict string
		substr  string
	}{
		{"match", plain, true, "GitVersion:    2.18.3\nGoVersion:     go1.26.3\n", 0, "PASS", "matches CI's pin"},
		{"mismatch", plain, true, "GitVersion:    2.17.0\n", 0, "WARN", "differs from CI's pin 2.18.3"},
		{"prerelease", plain, true, "GitVersion:    2.18.3-rc1\n", 0, "WARN", "differs from CI's pin 2.18.3"},
		{"nightly", plain, true, "GitVersion:    nightly\nGoVersion:     go1.26.3\n", 0, "WARN", "version is unreadable"},
		{"no GitVersion line", plain, true, "goreleaser version 2.18.3\n", 0, "PASS", "matches CI's pin"},
		{"not runnable", plain, true, "", 126, "SKIP", "not runnable here"},
		{"absent", plain, false, "", 0, "SKIP", "goreleaser is not runnable here"},
		{"pin scoping", koFirst, true, "GitVersion:    2.18.3\n", 0, "PASS", "matches CI's pin"},
		{"quoted uses", strings.Replace(plain, "goreleaser/goreleaser-action@v7", `"goreleaser/goreleaser-action@v7"`, 1), true, "GitVersion:    2.18.3\n", 0, "PASS", "matches CI's pin"},
		{"pin unparseable", noAction, true, "GitVersion:    2.18.3\n", 0, "WARN", "cannot parse the goreleaser pin"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := t.TempDir()
			wf := filepath.Join(repo, ".github", "workflows")
			if err := os.MkdirAll(wf, 0o750); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(wf, "release.yml"), []byte(tc.yml), 0o600); err != nil {
				t.Fatal(err)
			}
			stubDir := t.TempDir()
			if tc.stub {
				body := "#!/bin/sh\ncat <<'EOT'\n" + tc.out + "EOT\nexit " + strconv.Itoa(tc.rc) + "\n"
				// #nosec G306 -- the stub must be executable; it lives in t.TempDir.
				if err := os.WriteFile(filepath.Join(stubDir, "goreleaser"), []byte(body), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			var stdout, stderr bytes.Buffer
			cmd := exec.Command(sh, script) // #nosec G204 -- sh and a fixed repo script path, not user input.
			cmd.Dir = repo
			cmd.Env = []string{"PATH=" + stubDir + ":" + toolDir}
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Run(); err != nil {
				t.Fatalf("script failed: %v\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
			}
			line := strings.TrimSpace(stdout.String())
			if first, _, _ := strings.Cut(line, " "); first != tc.verdict {
				t.Errorf("verdict = %q, want %q (output %q)", first, tc.verdict, line)
			}
			if !strings.Contains(line, tc.substr) {
				t.Errorf("output %q does not contain %q", line, tc.substr)
			}
		})
	}
}

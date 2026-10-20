package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// The tests in this file drive bin/env-check, the pre-flight script that
// compares local tool pins with CI, checks the test toolchain and refuses a
// shared checkout. Every behavioral case runs against a small synthetic
// repository, so real-repo drift (a dependabot bump of go.mod, say) never
// turns CI red here. bin/env-check is not run as a gate against the real
// repository; TestEnvCheckRealRepoParses only proves its parsers still read
// the real workflow formats.

// envCheckInterp is one bash to run the script under. The script is invoked
// as `<bash> <path>`, never through its exec bit, which a module zip or a
// non-git copy of the tree does not carry.
type envCheckInterp struct{ name, path string }

// envCheckInterps lists the bashes to cover: the one on PATH, plus macOS's
// /bin/bash (3.2) when that is a different file, since the script must stay
// bash 3.2 compatible.
func envCheckInterps(t *testing.T) []envCheckInterp {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not on PATH")
	}
	out := []envCheckInterp{{"bash", bash}}
	if runtime.GOOS == "darwin" && bash != "/bin/bash" {
		if _, err := os.Stat("/bin/bash"); err == nil {
			out = append(out, envCheckInterp{"binbash", "/bin/bash"})
		}
	}
	return out
}

// envCheckRoot returns the repository root when this package sits inside
// descry's own git work tree, and skips otherwise: a module zip, a vendored
// copy or `go test all` from a dependent module has no .git and no exec bit to check.
func envCheckRoot(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("bin/env-check is a bash script")
	}
	for _, tool := range []string{"bash", "git"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not on PATH", tool)
		}
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("evalsymlinks: %v", err)
	}
	out, err := exec.Command("git", "-C", resolved, "rev-parse", "--show-toplevel").Output() // #nosec G204 -- test-owned git, run on the module root
	if err != nil {
		t.Skip("not inside a git work tree")
	}
	top, err := filepath.EvalSymlinks(strings.TrimSpace(string(out)))
	if err != nil || top != resolved {
		t.Skip("module root is not a git work tree root")
	}
	if _, err := os.Stat(filepath.Join(resolved, "bin", "env-check")); err != nil {
		t.Fatalf("bin/env-check: %v", err)
	}
	return resolved
}

// envCheckSysPath is the PATH the script's children get besides the stubs:
// the directories of the real git and bash, then the system directories.
func envCheckSysPath(t *testing.T) string {
	t.Helper()
	var dirs []string
	for _, tool := range []string{"git", "bash"} {
		p, err := exec.LookPath(tool)
		if err != nil {
			t.Skipf("%s not on PATH", tool)
		}
		dirs = append(dirs, filepath.Dir(p))
	}
	dirs = append(dirs, "/usr/bin", "/bin")
	return strings.Join(dirs, string(os.PathListSeparator))
}

// envCheckEnv builds the child environment from scratch, not from the
// test's: a git hook exports GIT_DIR and GIT_INDEX_FILE, which would point git
// at another repo; BASH_ENV runs code in every bash; CI and GITHUB_ACTIONS
// would change behavior; and a git config or global ignore under the user's
// HOME would leak in. HOME is an empty directory, GIT_CEILING_DIRECTORIES
// stops repository discovery above the fixture, and GOTOOLCHAIN=local keeps
// any real go from downloading a toolchain.
func envCheckEnv(t *testing.T, ceiling, path string, extra ...string) []string {
	t.Helper()
	env := []string{
		"PATH=" + path,
		"HOME=" + t.TempDir(),
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CEILING_DIRECTORIES=" + ceiling,
		"GOENV=off", "GOWORK=off", "GOTOOLCHAIN=local",
	}
	if tmp := os.Getenv("TMPDIR"); tmp != "" {
		env = append(env, "TMPDIR="+tmp)
	}
	return append(env, extra...)
}

// envCheckResult is one parsed run of the script.
type envCheckResult struct {
	code    int
	out     string
	lines   []envCheckLine
	summary string
}

type envCheckLine struct{ status, id, reason string }

var envCheckLineRE = regexp.MustCompile(`^(PASS|FAIL|WARN|SKIP) ([a-z][a-z-]*): (.*)$`)

func runEnvCheckCmd(dir string, env []string, name string, args ...string) (string, int, error) {
	cmd := exec.Command(name, args...) // #nosec G204 -- test-owned bash/git on bin/env-check in a t.TempDir repo
	cmd.Dir = dir
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code, err = ee.ExitCode(), nil
	}
	return string(out), code, err
}

func parseEnvCheck(t *testing.T, out string, code int) envCheckResult {
	t.Helper()
	r := envCheckResult{code: code, out: out}
	for _, l := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		if strings.HasPrefix(l, "env-check: ") {
			r.summary = l
			continue
		}
		m := envCheckLineRE.FindStringSubmatch(l)
		if m == nil {
			t.Errorf("unparsed output line %q\nfull output:\n%s", l, out)
			continue
		}
		r.lines = append(r.lines, envCheckLine{m[1], m[2], m[3]})
	}
	if r.summary == "" {
		t.Errorf("no summary line\nfull output:\n%s", out)
	}
	return r
}

func (r envCheckResult) count(status string) int {
	n := 0
	for _, l := range r.lines {
		if l.status == status {
			n++
		}
	}
	return n
}

// statusOf returns the status of the id's lines, or "" when there is none;
// when an id has several lines the worst one wins.
func (r envCheckResult) statusOf(id string) string {
	got := ""
	rank := map[string]int{"": 0, "PASS": 1, "SKIP": 2, "WARN": 3, "FAIL": 4}
	for _, l := range r.lines {
		if l.id == id && rank[l.status] > rank[got] {
			got = l.status
		}
	}
	return got
}

// envExp is the wanted status of one check id, and a substring that one of
// that id's lines with that status must contain.
type envExp struct{ status, sub string }

// check asserts the exit code and the wanted statuses, and that no FAIL
// line appears for an id that was not expected to FAIL. strict also demands
// that the only WARN lines are wanted ones, or the worktree WARN of a full run
// under ENV_CHECK_ALLOW_MAIN.
func (r envCheckResult) check(t *testing.T, wantCode int, want map[string]envExp, strict bool, allowWorktreeWarn bool) {
	t.Helper()
	if r.code != wantCode {
		t.Errorf("exit code = %d, want %d\n%s", r.code, wantCode, r.out)
	}
	for id, e := range want {
		if got := r.statusOf(id); got != e.status {
			t.Errorf("%s: status %q, want %q\n%s", id, got, e.status, r.out)
			continue
		}
		if e.sub == "" {
			continue
		}
		found := false
		for _, l := range r.lines {
			if l.id == id && l.status == e.status && strings.Contains(l.reason, e.sub) {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: no %s line contains %q\n%s", id, e.status, e.sub, r.out)
		}
	}
	for _, l := range r.lines {
		switch l.status {
		case "FAIL":
			if want[l.id].status != "FAIL" {
				t.Errorf("unexpected FAIL %s: %s\n%s", l.id, l.reason, r.out)
			}
		case "WARN":
			if strict && want[l.id].status != "WARN" && (!allowWorktreeWarn || l.id != "worktree") {
				t.Errorf("unexpected WARN %s: %s\n%s", l.id, l.reason, r.out)
			}
		}
	}
}

// Fixture contents. The pins are synthetic and independent of the real ones.
const (
	envFixtureGoMod        = "module example.com/envcheckfixture\n\ngo 1.26.9\n"
	envFixtureToolVersions = "golang 1.26.9\ngolangci-lint 2.12.2\n"

	envFixtureGolangciStep = `      - uses: golangci/golangci-lint-action@0000000000000000000000000000000000000001 # v9.3.0
        with:
          version: v2.12.2
`

	envFixtureCI = `name: ci
on: [push]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/setup-go@0000000000000000000000000000000000000002 # v7.0.0
        with:
          go-version-file: go.mod
          cache: true
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/setup-go@0000000000000000000000000000000000000002 # v7.0.0
        with:
          go-version-file: go.mod
          cache: true
@GOLANGCI@      - uses: goreleaser/goreleaser-action@0000000000000000000000000000000000000003 # v7.2.3
        with:
          version: v2.18.3
          args: check
  release-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/setup-go@0000000000000000000000000000000000000002 # v7.0.0
        with:
          go-version-file: go.mod
      - uses: goreleaser/goreleaser-action@0000000000000000000000000000000000000003 # v7.2.3
        with:
          version: v2.18.3
          args: release --snapshot
`

	envFixtureRelease = `name: release
on:
  push:
    tags: ["v*"]
jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/setup-go@0000000000000000000000000000000000000002 # v7.0.0
        with: { go-version-file: go.mod, cache: false }
      - uses: goreleaser/goreleaser-action@0000000000000000000000000000000000000003 # v7.2.3
        with:
          version: v2.18.3
          args: release --clean
`

	// envFixtureExtraJob opens one more ci.yml job; a case appends its steps.
	envFixtureExtraJob = `  extra:
    runs-on: ubuntu-latest
    steps:
`
)

func envFixtureFiles(t *testing.T) map[string]string {
	t.Helper()
	script, err := os.ReadFile(filepath.Join("..", "..", "bin", "env-check"))
	if err != nil {
		t.Fatalf("read bin/env-check: %v", err)
	}
	return map[string]string{
		"go.mod":                        envFixtureGoMod,
		"go.sum":                        "",
		".tool-versions":                envFixtureToolVersions,
		".golangci.yml":                 "version: \"2\"\n",
		".github/workflows/ci.yml":      strings.Replace(envFixtureCI, "@GOLANGCI@", envFixtureGolangciStep, 1),
		".github/workflows/release.yml": envFixtureRelease,
		"bin/env-check":                 string(script),
	}
}

func replaceOnce(t *testing.T, m map[string]string, file, old, repl string) {
	t.Helper()
	if strings.Count(m[file], old) < 1 {
		t.Fatalf("fixture %s has no %q", file, old)
	}
	m[file] = strings.Replace(m[file], old, repl, 1)
}

func writeEnvFixtureFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, body := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatalf("mkdir for %s: %v", name, err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
}

// envFixture is a committed synthetic repository plus stubs for go,
// golangci-lint and goreleaser, so full-mode runs never depend on the tools
// installed on the machine running the test.
type envFixture struct {
	t      *testing.T
	interp envCheckInterp
	parent string
	repo   string
	stubs  string
	path   string // PATH override set by omit; the stubs plus the system path when empty
	files  map[string]string
}

const envStubGo = `#!/bin/sh
case "$1 $2" in
  "env GOVERSION") echo "${STUB_GOVERSION:-go1.26.9}" ;;
  "list "*) exit "${STUB_GO_LIST_RC:-0}" ;;
  "tool "*) exit "${STUB_GO_TOOL_RC:-0}" ;;
  *) exit 0 ;;
esac
`

const envStubGolangci = `#!/bin/sh
case "$1 $2" in
  "version --short") echo "${STUB_GOLANGCI:-2.12.2}" ;;
  *) echo "golangci-lint has version ${STUB_GOLANGCI:-2.12.2} built with stub" ;;
esac
`

// envStubGoreleaser prints a version; STUB_GORELEASER_BROKEN mimics an
// asdf/mise shim with no version set (exit 1, nothing on stdout) and
// STUB_GORELEASER_BLANK a goreleaser that exits 0 without naming a version.
const envStubGoreleaser = `#!/bin/sh
if [ -n "${STUB_GORELEASER_BROKEN:-}" ]; then echo "No version is set for command goreleaser" >&2; exit 1; fi
if [ -n "${STUB_GORELEASER_BLANK:-}" ]; then echo "goreleaser"; exit 0; fi
echo "GitVersion:    ${STUB_GORELEASER:-2.18.3}"
`

func newEnvFixture(t *testing.T, interp envCheckInterp, mutate func(*testing.T, map[string]string)) *envFixture {
	t.Helper()
	envCheckRoot(t)
	fx := &envFixture{t: t, interp: interp, parent: t.TempDir()}
	fx.repo = filepath.Join(fx.parent, "repo")
	fx.stubs = filepath.Join(fx.parent, "stubs")
	if err := os.MkdirAll(fx.stubs, 0o750); err != nil {
		t.Fatalf("mkdir stubs: %v", err)
	}
	for name, body := range map[string]string{"go": envStubGo, "golangci-lint": envStubGolangci, "goreleaser": envStubGoreleaser} {
		if err := os.WriteFile(filepath.Join(fx.stubs, name), []byte(body), 0o700); err != nil { // #nosec G306 -- test stub on PATH must be executable
			t.Fatalf("write stub %s: %v", name, err)
		}
	}
	fx.files = envFixtureFiles(t)
	if mutate != nil {
		mutate(t, fx.files)
	}
	if err := os.MkdirAll(fx.repo, 0o750); err != nil {
		t.Fatalf("mkdir repo: %v", err)
	}
	writeEnvFixtureFiles(t, fx.repo, fx.files)
	fx.git(fx.repo, "init", "-q", "--template=") // no hooks from the system template dir
	fx.git(fx.repo, "add", "-A")
	fx.git(fx.repo, "update-index", "--chmod=+x", "bin/env-check")
	fx.git(fx.repo, "-c", "user.name=test", "-c", "user.email=test@example.com", "-c", "commit.gpgsign=false", "commit", "-q", "-m", "fixture")
	return fx
}

func (fx *envFixture) env(extra ...string) []string {
	path := fx.path
	if path == "" {
		path = fx.stubs + string(os.PathListSeparator) + envCheckSysPath(fx.t)
	}
	return envCheckEnv(fx.t, fx.parent, path, extra...)
}

// envCheckTools are the external commands bin/env-check runs besides the
// stubbed go, golangci-lint and goreleaser.
var envCheckTools = []string{"git", "awk", "sed", "grep", "head", "tr", "basename"}

// omit removes the named stubs and narrows PATH to the stubs left plus
// one-line wrappers for envCheckTools. The system path is dropped because a
// real go or golangci-lint can live in the directory of git or in /usr/bin,
// and a case about a missing tool must not depend on the machine.
func (fx *envFixture) omit(names ...string) {
	fx.t.Helper()
	for _, name := range names {
		if err := os.Remove(filepath.Join(fx.stubs, name)); err != nil {
			fx.t.Fatalf("remove stub %s: %v", name, err)
		}
	}
	wrap := filepath.Join(fx.parent, "tools")
	if err := os.MkdirAll(wrap, 0o750); err != nil {
		fx.t.Fatalf("mkdir tools: %v", err)
	}
	for _, tool := range envCheckTools {
		resolved, err := exec.LookPath(tool)
		if err != nil {
			fx.t.Skipf("%s not on PATH", tool)
		}
		body := "#!/bin/sh\nexec " + resolved + " \"$@\"\n"
		if err := os.WriteFile(filepath.Join(wrap, tool), []byte(body), 0o700); err != nil { // #nosec G306 -- test wrapper on PATH must be executable
			fx.t.Fatalf("write wrapper %s: %v", tool, err)
		}
	}
	fx.path = fx.stubs + string(os.PathListSeparator) + wrap
}

func (fx *envFixture) git(dir string, args ...string) {
	fx.t.Helper()
	out, code, err := runEnvCheckCmd(dir, fx.env(), "git", args...)
	if err != nil || code != 0 {
		fx.t.Fatalf("git %s: code %d err %v\n%s", strings.Join(args, " "), code, err, out)
	}
}

// run executes the fixture's copy of bin/env-check in dir.
func (fx *envFixture) run(dir string, args []string, extra ...string) envCheckResult {
	fx.t.Helper()
	script := filepath.Join(dir, "bin", "env-check")
	out, code, err := runEnvCheckCmd(dir, fx.env(extra...), fx.interp.path, append([]string{script}, args...)...)
	if err != nil {
		fx.t.Fatalf("run env-check: %v\n%s", err, out)
	}
	return parseEnvCheck(fx.t, out, code)
}

// forEachEnvInterp runs fn once per bash under a subtest of its own.
func forEachEnvInterp(t *testing.T, fn func(t *testing.T, interp envCheckInterp)) {
	t.Helper()
	envCheckRoot(t)
	for _, in := range envCheckInterps(t) {
		t.Run(in.name, func(t *testing.T) { fn(t, in) })
	}
}

const envAllowMain = "ENV_CHECK_ALLOW_MAIN=1"

// Value: protects=bin/env-check parses under bash 3.2 and 5 and ships with the executable bit in git
// fails_when=the script uses a syntax error or a newer-bash-only construct that fails -n, or its git mode is not 100755
// why_new=nothing else in CI parses the script or checks its mode; a missing exec bit only shows when someone runs it directly
// seam=none; no_claim=does not prove runtime behavior, which the other TestEnvCheck tests cover
//
// TestEnvCheckSyntax parses bin/env-check under each bash and checks the mode
// git records for it.
func TestEnvCheckSyntax(t *testing.T) {
	root := envCheckRoot(t)
	script := filepath.Join(root, "bin", "env-check")
	env := envCheckEnv(t, filepath.Dir(root), envCheckSysPath(t))
	for _, in := range envCheckInterps(t) {
		t.Run(in.name, func(t *testing.T) {
			if out, code, err := runEnvCheckCmd(root, env, in.path, "-n", script); err != nil || code != 0 {
				t.Fatalf("%s -n: code %d err %v\n%s", in.path, code, err, out)
			}
		})
	}
	out, code, err := runEnvCheckCmd(root, env, "git", "ls-files", "-s", "bin/env-check")
	if err != nil || code != 0 {
		t.Fatalf("git ls-files: code %d err %v\n%s", code, err, out)
	}
	if f := strings.Fields(out); len(f) == 0 || f[0] != "100755" {
		t.Errorf("git mode of bin/env-check = %q, want 100755 (git update-index --chmod=+x)", out)
	}
}

// Value: protects=the pin parsers still read the real workflow formats, including release.yml's flow-style with: { ... } and ci.yml's step layout
// fails_when=a workflow reformat or a parser change makes env-check report cannot parse, exit 2, or lose a check line on the real repository
// why_new=the synthetic fixtures in the other tests cannot notice a real workflow shape the parsers no longer read
// seam=none; no_claim=does not assert the pins agree, a mismatch is only logged
//
// TestEnvCheckRealRepoParses runs --static on the real tree.
func TestEnvCheckRealRepoParses(t *testing.T) {
	root := envCheckRoot(t)
	script := filepath.Join(root, "bin", "env-check")
	env := envCheckEnv(t, filepath.Dir(root), envCheckSysPath(t))
	for _, in := range envCheckInterps(t) {
		t.Run(in.name, func(t *testing.T) {
			out, code, err := runEnvCheckCmd(root, env, in.path, script, "--static")
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			r := parseEnvCheck(t, out, code)
			if r.code != 0 && r.code != 1 {
				t.Errorf("exit code = %d, want 0 or 1\n%s", r.code, out)
			}
			for _, id := range []string{"pins-go-workflows", "pins-tool-versions-go", "pins-golangci", "containers", "compose-project", "harness-files"} {
				if r.statusOf(id) == "" {
					t.Errorf("no line for %s\n%s", id, out)
				}
			}
			for _, l := range r.lines {
				if l.status == "FAIL" && strings.HasPrefix(l.reason, "cannot parse") {
					t.Errorf("parser regression: %s %s: %s", l.status, l.id, l.reason)
				}
				if l.status == "FAIL" || l.status == "WARN" {
					t.Logf("%s %s: %s", l.status, l.id, l.reason)
				}
			}
		})
	}
}

// Value: protects=each pin comparison in env-check fails (or warns, under --static) on drift, and fails closed when it cannot read a pin
// fails_when=a drifted .tool-versions, go.mod toolchain, workflow go-version, golangci-lint or goreleaser version still passes, a setup-go step without a version key passes, a quoted or re-spaced uses: line hides a step from the scan, a missing step or key borrows another step's version:, a missing go or golangci-lint passes, or a flow-style, commented or ./go.mod value is misread
// why_new=a silent pass here would let local and CI toolchains drift unseen; the real repo has no drift to exercise it
// seam=none; no_claim=does not prove the goreleaser pins agree across workflows, TestGoreleaserVersionPinned does
//
// TestEnvCheckPinDrift mutates the synthetic fixture one pin at a time.
func TestEnvCheckPinDrift(t *testing.T) {
	removeGolangci := func(t *testing.T, m map[string]string) {
		replaceOnce(t, m, ".github/workflows/ci.yml", envFixtureGolangciStep, "")
	}
	cases := []struct {
		name     string
		static   bool
		mutate   func(*testing.T, map[string]string)
		env      map[string]string // STUB_* overrides
		omit     []string          // stubs to leave off PATH
		wantCode int
		want     map[string]envExp
		strict   bool
	}{
		{name: "baseline static", static: true, strict: true},
		{name: "baseline full", strict: true},
		{
			name: "tool-versions golang drift static is a warning", static: true,
			mutate: func(t *testing.T, m map[string]string) {
				replaceOnce(t, m, ".tool-versions", "golang 1.26.9", "golang 1.26.8")
			},
			wantCode: 0,
			want:     map[string]envExp{"pins-tool-versions-go": {"WARN", "bump .tool-versions golang to 1.26.9"}},
			strict:   true,
		},
		{
			name: "tool-versions golang drift full fails",
			mutate: func(t *testing.T, m map[string]string) {
				replaceOnce(t, m, ".tool-versions", "golang 1.26.9", "golang 1.26.8")
			},
			wantCode: 1,
			want:     map[string]envExp{"pins-tool-versions-go": {"FAIL", "bump .tool-versions golang to 1.26.9"}},
		},
		{
			name:     "go.mod toolchain newer than the installed go",
			mutate:   func(_ *testing.T, m map[string]string) { m["go.mod"] += "\ntoolchain go1.26.10\n" },
			wantCode: 1,
			want: map[string]envExp{
				"go-installed":          {"FAIL", "go1.26.10"},
				"pins-tool-versions-go": {"FAIL", ""},
				"harness-modcache":      {"SKIP", ""},
				"harness-govulncheck":   {"SKIP", ""},
			},
		},
		{
			name:   "workflow sets go-version",
			static: true,
			mutate: func(t *testing.T, m map[string]string) {
				replaceOnce(t, m, ".github/workflows/ci.yml", "go-version-file: go.mod", "go-version: '1.25'")
			},
			wantCode: 1,
			want:     map[string]envExp{"pins-go-workflows": {"FAIL", "ci.yml:"}},
		},
		{
			name:   "workflow reads another file",
			static: true,
			mutate: func(t *testing.T, m map[string]string) {
				replaceOnce(t, m, ".github/workflows/ci.yml", "go-version-file: go.mod", "go-version-file: other/go.mod")
			},
			wantCode: 1,
			want:     map[string]envExp{"pins-go-workflows": {"FAIL", "other/go.mod"}},
		},
		{
			name:   "no workflow steps anywhere fails closed",
			static: true,
			mutate: func(_ *testing.T, m map[string]string) {
				m[".github/workflows/ci.yml"] = "name: ci\non: [push]\njobs: {}\n"
				m[".github/workflows/release.yml"] = "name: r\non: [push]\njobs: {}\n"
			},
			wantCode: 1,
			want: map[string]envExp{
				"pins-go-workflows": {"FAIL", "cannot parse"},
				"pins-golangci":     {"FAIL", "cannot parse"},
				"pins-goreleaser":   {"FAIL", "cannot parse"},
			},
		},
		{
			name:   "golangci-lint version drift",
			static: true,
			mutate: func(t *testing.T, m map[string]string) {
				replaceOnce(t, m, ".github/workflows/ci.yml", "version: v2.12.2", "version: v2.12.3")
			},
			wantCode: 1,
			want:     map[string]envExp{"pins-golangci": {"FAIL", "2.12.3"}},
		},
		{
			name:   "golangci-lint version line removed does not borrow goreleaser's",
			static: true,
			mutate: func(t *testing.T, m map[string]string) {
				replaceOnce(t, m, ".github/workflows/ci.yml", envFixtureGolangciStep, "      - uses: golangci/golangci-lint-action@0000000000000000000000000000000000000001 # v9.3.0\n")
			},
			wantCode: 1,
			want:     map[string]envExp{"pins-golangci": {"FAIL", "cannot parse"}},
		},
		{
			name:     "golangci-lint step removed",
			static:   true,
			mutate:   removeGolangci,
			wantCode: 1,
			want:     map[string]envExp{"pins-golangci": {"FAIL", "cannot parse"}},
		},
		{
			name:   "golangci-lint flow style with quotes",
			static: true,
			mutate: func(t *testing.T, m map[string]string) {
				replaceOnce(t, m, ".github/workflows/ci.yml", "        with:\n          version: v2.12.2\n", "        with: { version: 'v2.12.2' }\n")
			},
			want:   map[string]envExp{"pins-golangci": {"PASS", ""}},
			strict: true,
		},
		{
			name:   "golangci-lint trailing comment",
			static: true,
			mutate: func(t *testing.T, m map[string]string) {
				replaceOnce(t, m, ".github/workflows/ci.yml", "version: v2.12.2\n", "version: \"v2.12.2\" # pin\n")
			},
			want:   map[string]envExp{"pins-golangci": {"PASS", ""}},
			strict: true,
		},
		{
			name:   "golangci-lint flow style with trailing comment",
			static: true,
			mutate: func(t *testing.T, m map[string]string) {
				replaceOnce(t, m, ".github/workflows/ci.yml", "        with:\n          version: v2.12.2\n", "        with: { version: v2.12.2 } # pin\n")
			},
			want:   map[string]envExp{"pins-golangci": {"PASS", ""}},
			strict: true,
		},
		{
			name:     "tool-versions golang line removed",
			static:   true,
			mutate:   func(t *testing.T, m map[string]string) { replaceOnce(t, m, ".tool-versions", "golang 1.26.9\n", "") },
			wantCode: 1,
			want:     map[string]envExp{"pins-tool-versions-go": {"FAIL", "cannot parse"}},
		},
		{
			name:   "tool-versions golangci-lint line removed",
			static: true,
			mutate: func(t *testing.T, m map[string]string) {
				replaceOnce(t, m, ".tool-versions", "golangci-lint 2.12.2\n", "")
			},
			wantCode: 1,
			want:     map[string]envExp{"pins-golangci": {"FAIL", "cannot parse"}},
		},
		{
			name:     "installed go differs",
			env:      map[string]string{"STUB_GOVERSION": "go1.25.1"},
			wantCode: 1,
			want: map[string]envExp{
				"go-installed":        {"FAIL", "go1.25.1"},
				"harness-modcache":    {"SKIP", ""},
				"harness-govulncheck": {"SKIP", ""},
			},
		},
		{
			name:     "installed golangci-lint differs",
			env:      map[string]string{"STUB_GOLANGCI": "2.11.0"},
			wantCode: 1,
			want:     map[string]envExp{"golangci-installed": {"FAIL", "2.11.0"}},
		},
		{
			name:   "setup-go step with a with block but no version key fails",
			static: true,
			mutate: func(t *testing.T, m map[string]string) {
				replaceOnce(t, m, ".github/workflows/ci.yml", "        with:\n          go-version-file: go.mod\n          cache: true\n", "        with:\n          cache: true\n")
			},
			wantCode: 1,
			want:     map[string]envExp{"pins-go-workflows": {"FAIL", "neither go-version-file nor go-version"}},
		},
		{
			name:   "setup-go step with no with block fails",
			static: true,
			mutate: func(t *testing.T, m map[string]string) {
				replaceOnce(t, m, ".github/workflows/release.yml", "        with: { go-version-file: go.mod, cache: false }\n", "")
			},
			wantCode: 1,
			want:     map[string]envExp{"pins-go-workflows": {"FAIL", "release.yml:"}},
		},
		{
			name:   "double-quoted setup-go step with no version key fails",
			static: true,
			mutate: func(_ *testing.T, m map[string]string) {
				m[".github/workflows/ci.yml"] += envFixtureExtraJob + "      - uses: \"actions/setup-go@0000000000000000000000000000000000000002\"\n"
			},
			wantCode: 1,
			want:     map[string]envExp{"pins-go-workflows": {"FAIL", "neither go-version-file nor go-version"}},
		},
		{
			name:   "setup-go step with extra blanks after uses: and no version key fails",
			static: true,
			mutate: func(_ *testing.T, m map[string]string) {
				m[".github/workflows/ci.yml"] += envFixtureExtraJob + "      - uses:   actions/setup-go@0000000000000000000000000000000000000002\n"
			},
			wantCode: 1,
			want:     map[string]envExp{"pins-go-workflows": {"FAIL", "neither go-version-file nor go-version"}},
		},
		{
			name:   "single-quoted setup-go step that reads go.mod is counted",
			static: true,
			mutate: func(_ *testing.T, m map[string]string) {
				m[".github/workflows/ci.yml"] += envFixtureExtraJob + "      - name: setup\n        uses: 'actions/setup-go@0000000000000000000000000000000000000002'\n        with: { go-version-file: \"go.mod\" }\n"
			},
			want:   map[string]envExp{"pins-go-workflows": {"PASS", "5 setup-go steps"}},
			strict: true,
		},
		{
			name:   "quoted goreleaser step pinning another version fails",
			static: true,
			mutate: func(_ *testing.T, m map[string]string) {
				m[".github/workflows/release.yml"] += "      - uses: \"goreleaser/goreleaser-action@0000000000000000000000000000000000000003\"\n        with:\n          version: v2.19.0\n"
			},
			wantCode: 1,
			want:     map[string]envExp{"pins-goreleaser": {"FAIL", "different versions"}},
		},
		{
			name:   "go-version-file ./go.mod is the same file",
			static: true,
			mutate: func(t *testing.T, m map[string]string) {
				replaceOnce(t, m, ".github/workflows/ci.yml", "go-version-file: go.mod", "go-version-file: ./go.mod")
			},
			want:   map[string]envExp{"pins-go-workflows": {"PASS", "4 setup-go steps"}},
			strict: true,
		},
		{
			name:   "commented-out golangci-lint step above the real one",
			static: true,
			mutate: func(t *testing.T, m map[string]string) {
				replaceOnce(t, m, ".github/workflows/ci.yml", envFixtureGolangciStep,
					"      # - uses: golangci/golangci-lint-action@deadbeef # old pin\n      #   with:\n      #     version: v1.0.0\n"+envFixtureGolangciStep)
			},
			want:   map[string]envExp{"pins-golangci": {"PASS", ""}},
			strict: true,
		},
		{
			name:   "goreleaser versions differ between ci.yml and release.yml",
			static: true,
			mutate: func(t *testing.T, m map[string]string) {
				replaceOnce(t, m, ".github/workflows/release.yml", "version: v2.18.3", "version: v2.19.0")
			},
			wantCode: 1,
			want:     map[string]envExp{"pins-goreleaser": {"FAIL", "different versions"}},
		},
		{
			name:   "goreleaser versions differ inside ci.yml",
			static: true,
			mutate: func(t *testing.T, m map[string]string) {
				replaceOnce(t, m, ".github/workflows/ci.yml", "version: v2.18.3", "version: v2.19.0")
			},
			wantCode: 1,
			want:     map[string]envExp{"pins-goreleaser": {"FAIL", "different versions"}},
		},
		{
			name:   "goreleaser step with no version fails closed",
			static: true,
			mutate: func(t *testing.T, m map[string]string) {
				replaceOnce(t, m, ".github/workflows/release.yml", "          version: v2.18.3\n", "")
			},
			wantCode: 1,
			want:     map[string]envExp{"pins-goreleaser": {"FAIL", "cannot parse"}},
		},
		{
			name: "go.mod pins a minor only and the installed go is within it",
			mutate: func(_ *testing.T, m map[string]string) {
				m["go.mod"] = "module example.com/envcheckfixture\n\ngo 1.26\n"
			},
			wantCode: 0,
			want: map[string]envExp{
				"go-installed":          {"WARN", "minor only"},
				"pins-tool-versions-go": {"WARN", "minor only"},
			},
			strict: true,
		},
		{
			name: "go.mod pins a minor only and .tool-versions is another minor",
			mutate: func(t *testing.T, m map[string]string) {
				m["go.mod"] = "module example.com/envcheckfixture\n\ngo 1.26\n"
				replaceOnce(t, m, ".tool-versions", "golang 1.26.9", "golang 1.25.3")
			},
			wantCode: 1,
			want: map[string]envExp{
				"go-installed":          {"WARN", "minor only"},
				"pins-tool-versions-go": {"FAIL", "1.26 release"},
			},
		},
		{
			name:     "go.mod has no go line",
			mutate:   func(_ *testing.T, m map[string]string) { m["go.mod"] = "module example.com/envcheckfixture\n" },
			wantCode: 1,
			want: map[string]envExp{
				"go-installed":          {"FAIL", "cannot parse"},
				"pins-tool-versions-go": {"FAIL", "cannot parse"},
				"harness-modcache":      {"SKIP", "go.mod has no go line"},
				"harness-govulncheck":   {"SKIP", "go.mod has no go line"},
			},
		},
		{
			name:     "go missing from PATH",
			omit:     []string{"go"},
			wantCode: 1,
			want: map[string]envExp{
				"go-installed":        {"FAIL", "go not found"},
				"harness-tools":       {"FAIL", "missing from PATH: go"},
				"harness-modcache":    {"SKIP", "go not on PATH"},
				"harness-govulncheck": {"SKIP", "go not on PATH"},
			},
		},
		{
			name:     "golangci-lint missing from PATH",
			omit:     []string{"golangci-lint"},
			wantCode: 1,
			want: map[string]envExp{
				"golangci-installed": {"FAIL", "not found"},
				"harness-tools":      {"FAIL", "golangci-lint"},
			},
		},
		{
			name:     "goreleaser not installed is skipped",
			omit:     []string{"goreleaser"},
			wantCode: 0,
			want:     map[string]envExp{"goreleaser-installed": {"SKIP", "not installed"}},
			strict:   true,
		},
		{
			name:     "goreleaser shim with no runnable version is skipped",
			env:      map[string]string{"STUB_GORELEASER_BROKEN": "1"},
			wantCode: 0,
			want:     map[string]envExp{"goreleaser-installed": {"SKIP", "not runnable"}},
			strict:   true,
		},
		{
			name:     "goreleaser that names no version is a warning",
			env:      map[string]string{"STUB_GORELEASER_BLANK": "1"},
			wantCode: 0,
			want:     map[string]envExp{"goreleaser-installed": {"WARN", "unreadable"}},
			strict:   true,
		},
		{
			name:     "incomplete module cache is a warning",
			env:      map[string]string{"STUB_GO_LIST_RC": "1"},
			wantCode: 0,
			want:     map[string]envExp{"harness-modcache": {"WARN", "module cache incomplete"}},
			strict:   true,
		},
		{
			name:     "govulncheck unavailable offline is a warning",
			env:      map[string]string{"STUB_GO_TOOL_RC": "1"},
			wantCode: 0,
			want:     map[string]envExp{"harness-govulncheck": {"WARN", "unavailable offline"}},
			strict:   true,
		},
		{
			name:     "installed goreleaser differs is a warning",
			env:      map[string]string{"STUB_GORELEASER": "2.17.0"},
			wantCode: 0,
			want:     map[string]envExp{"goreleaser-installed": {"WARN", "2.18.3"}},
			strict:   true,
		},
	}
	forEachEnvInterp(t, func(t *testing.T, interp envCheckInterp) {
		for _, tc := range cases {
			t.Run(strings.ReplaceAll(tc.name, " ", "_"), func(t *testing.T) {
				fx := newEnvFixture(t, interp, tc.mutate)
				if len(tc.omit) > 0 {
					fx.omit(tc.omit...)
				}
				args := []string(nil)
				extra := []string{envAllowMain}
				if tc.static {
					args = []string{"--static"}
					extra = nil
				}
				for k, v := range tc.env {
					extra = append(extra, k+"="+v)
				}
				r := fx.run(fx.repo, args, extra...)
				r.check(t, tc.wantCode, tc.want, tc.strict, !tc.static)
			})
		}
	})
}

// Value: protects=a full run FAILs in a main checkout, ENV_CHECK_ALLOW_MAIN=1 is the only downgrade, CI variables do not bypass it, a linked worktree passes, and a non-git copy never passes
// fails_when=the main checkout passes without the flag, CI or GITHUB_ACTIONS skips the check, a linked worktree is called a main checkout, or a copy of the tree with no .git exits 0
// why_new=the worktree rule is the check that keeps parallel agent sessions from sharing a checkout; nothing else exercises it
// seam=none; no_claim=does not prove anything about the user's real checkout layout
//
// TestEnvCheckWorktree covers the worktree and not-a-repository paths.
func TestEnvCheckWorktree(t *testing.T) {
	forEachEnvInterp(t, func(t *testing.T, interp envCheckInterp) {
		t.Run("main checkout fails", func(t *testing.T) {
			fx := newEnvFixture(t, interp, nil)
			r := fx.run(fx.repo, nil)
			r.check(t, 1, map[string]envExp{"worktree": {"FAIL", "ENV_CHECK_ALLOW_MAIN"}}, false, false)
		})
		t.Run("allow main downgrades to a warning", func(t *testing.T) {
			fx := newEnvFixture(t, interp, nil)
			r := fx.run(fx.repo, nil, envAllowMain)
			r.check(t, 0, map[string]envExp{"worktree": {"WARN", "ENV_CHECK_ALLOW_MAIN=1"}}, true, false)
		})
		t.Run("CI variables do not bypass", func(t *testing.T) {
			fx := newEnvFixture(t, interp, nil)
			r := fx.run(fx.repo, nil, "CI=true", "GITHUB_ACTIONS=true")
			r.check(t, 1, map[string]envExp{"worktree": {"FAIL", ""}}, false, false)
		})
		t.Run("only the value 1 downgrades", func(t *testing.T) {
			fx := newEnvFixture(t, interp, nil)
			r := fx.run(fx.repo, nil, "ENV_CHECK_ALLOW_MAIN=yes")
			r.check(t, 1, map[string]envExp{"worktree": {"FAIL", ""}}, false, false)
		})
		t.Run("static skips the worktree check", func(t *testing.T) {
			fx := newEnvFixture(t, interp, nil)
			r := fx.run(fx.repo, []string{"--static"})
			r.check(t, 0, map[string]envExp{"worktree": {"SKIP", ""}}, true, false)
		})
		t.Run("linked worktree passes", func(t *testing.T) {
			fx := newEnvFixture(t, interp, nil)
			wt := filepath.Join(fx.parent, "wt")
			fx.git(fx.repo, "worktree", "add", "-q", "--detach", wt)
			r := fx.run(wt, nil)
			r.check(t, 0, map[string]envExp{"worktree": {"PASS", "wt"}}, true, false)
		})
		t.Run("not a git work tree", func(t *testing.T) {
			fx := newEnvFixture(t, interp, nil)
			plain := filepath.Join(fx.parent, "plain")
			writeEnvFixtureFiles(t, plain, fx.files)
			r := fx.run(plain, nil, envAllowMain)
			r.check(t, 1, map[string]envExp{"git": {"FAIL", "not inside a git work tree"}}, false, false)
			if n := r.count("PASS"); n != 0 {
				t.Errorf("a non-git copy produced %d PASS lines\n%s", n, r.out)
			}
		})
	})
}

// Value: protects=tracking a Compose file fails env-check until it checks containers and derives a per-worktree project name; look-alike names are not matched
// fails_when=a tracked compose.yaml, dev.compose.yaml or docker-compose.override.yml still passes or SKIPs, or docs/composer.yml is taken for a Compose file
// why_new=descry has no Compose files today, so only a fixture exercises the guard that stops two worktrees sharing one project name
// seam=none; no_claim=does not prove any container check, there is none to run
//
// TestEnvCheckComposeGuard commits Compose-named files into the fixture.
func TestEnvCheckComposeGuard(t *testing.T) {
	cases := []struct {
		name string
		path string
		fail bool
	}{
		{"none", "", false},
		{"compose.yaml", "compose.yaml", true},
		{"dev.compose.yaml", "dev.compose.yaml", true},
		{"docker-compose.override.yml", "deploy/docker-compose.override.yml", true},
		{"compose.prod.yml", "compose.prod.yml", true},
		{"composer.yml is not compose", "docs/composer.yml", false},
	}
	forEachEnvInterp(t, func(t *testing.T, interp envCheckInterp) {
		for _, tc := range cases {
			for _, static := range []bool{true, false} {
				mode := "full"
				if static {
					mode = "static"
				}
				t.Run(strings.ReplaceAll(tc.name, " ", "_")+"/"+mode, func(t *testing.T) {
					fx := newEnvFixture(t, interp, func(_ *testing.T, m map[string]string) {
						if tc.path != "" {
							m[tc.path] = "services: {}\n"
						}
					})
					args, extra := []string(nil), []string{envAllowMain}
					if static {
						args, extra = []string{"--static"}, nil
					}
					r := fx.run(fx.repo, args, extra...)
					if tc.fail {
						r.check(t, 1, map[string]envExp{
							"containers":      {"FAIL", filepath.Base(tc.path)},
							"compose-project": {"FAIL", filepath.Base(tc.path)},
						}, false, false)
						return
					}
					r.check(t, 0, map[string]envExp{
						"containers":      {"SKIP", ""},
						"compose-project": {"SKIP", ""},
					}, true, !static)
				})
			}
		}
	})
}

// Value: protects=a copy of the tree that lost a harness file, deleted tracked files, or dropped the executable bit never reports ready
// fails_when=a tracked file removed from the working tree, an untracked go.sum, or a bin/env-check indexed as 100644 still exits 0
// why_new=an agent starting from a partial copy would otherwise find out at the first go test
// seam=none; no_claim=does not prove the module cache or the Go version, the stubs cover those
//
// TestEnvCheckPartialCopy removes pieces of the committed fixture.
func TestEnvCheckPartialCopy(t *testing.T) {
	forEachEnvInterp(t, func(t *testing.T, interp envCheckInterp) {
		t.Run("tracked file deleted from the working tree", func(t *testing.T) {
			fx := newEnvFixture(t, interp, nil)
			if err := os.Remove(filepath.Join(fx.repo, ".golangci.yml")); err != nil {
				t.Fatal(err)
			}
			r := fx.run(fx.repo, []string{"--static"})
			r.check(t, 1, map[string]envExp{"harness-files": {"FAIL", ".golangci.yml"}}, false, false)
		})
		t.Run("deleted files are counted and capped at five names", func(t *testing.T) {
			fx := newEnvFixture(t, interp, func(_ *testing.T, m map[string]string) {
				for i := range 8 {
					m[fmt.Sprintf("extra/f%d.txt", i)] = "x\n"
				}
			})
			if err := os.RemoveAll(filepath.Join(fx.repo, "extra")); err != nil {
				t.Fatal(err)
			}
			r := fx.run(fx.repo, []string{"--static"})
			r.check(t, 1, map[string]envExp{"harness-files": {"FAIL", "8 tracked files deleted"}}, false, false)
			for _, l := range r.lines {
				if l.id == "harness-files" && strings.Contains(l.reason, "deleted") {
					if n := strings.Count(l.reason, "extra/f"); n != 5 {
						t.Errorf("harness-files names %d paths, want 5: %s", n, l.reason)
					}
				}
			}
		})
		t.Run("go.sum removed from the index", func(t *testing.T) {
			fx := newEnvFixture(t, interp, nil)
			fx.git(fx.repo, "rm", "-q", "--cached", "go.sum")
			r := fx.run(fx.repo, []string{"--static"})
			r.check(t, 1, map[string]envExp{"harness-files": {"FAIL", "go.sum"}}, false, false)
		})
		t.Run("exec bit dropped from the index", func(t *testing.T) {
			fx := newEnvFixture(t, interp, nil)
			fx.git(fx.repo, "update-index", "--chmod=-x", "bin/env-check")
			r := fx.run(fx.repo, []string{"--static"})
			r.check(t, 1, map[string]envExp{"harness-exec-bit": {"FAIL", "100644"}}, false, false)
		})
		t.Run("sparse checkout warns", func(t *testing.T) {
			fx := newEnvFixture(t, interp, nil)
			fx.git(fx.repo, "config", "core.sparseCheckout", "true")
			r := fx.run(fx.repo, []string{"--static"})
			r.check(t, 0, map[string]envExp{"harness-sparse": {"WARN", ""}}, true, false)
		})
	})
}

// Value: protects=the command line contract: -h exits 0, an unknown flag exits 2 without running any check
// fails_when=an unknown flag is ignored or exits 1, or --help runs the checks
// why_new=callers and docs rely on the exit codes 0, 1 and 2 meaning ready, not ready and misuse
// seam=none; no_claim=does not cover the flags' effect on checks, the other tests do
//
// TestEnvCheckUsage covers --help and an unknown flag.
func TestEnvCheckUsage(t *testing.T) {
	forEachEnvInterp(t, func(t *testing.T, interp envCheckInterp) {
		fx := newEnvFixture(t, interp, nil)
		script := filepath.Join(fx.repo, "bin", "env-check")
		out, code, err := runEnvCheckCmd(fx.repo, fx.env(), interp.path, script, "--bogus")
		if err != nil || code != 2 || strings.Contains(out, "PASS ") || !strings.Contains(out, "unknown option") {
			t.Errorf("--bogus: code %d err %v, want 2 with a usage message and no checks\n%s", code, err, out)
		}
		out, code, err = runEnvCheckCmd(fx.repo, fx.env(), interp.path, script, "--help")
		if err != nil || code != 0 || strings.Contains(out, "PASS ") || !strings.Contains(out, "usage:") {
			t.Errorf("--help: code %d err %v, want 0 with usage and no checks\n%s", code, err, out)
		}
		if !slices.Contains(strings.Fields(out), "--static") {
			t.Errorf("--help does not mention --static\n%s", out)
		}
	})
}

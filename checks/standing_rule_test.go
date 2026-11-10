// Package checks_test enforces the standing rule for check implementations:
// every check package under checks/ carries a fault table, TestVerdictMatrix,
// in its verdict_matrix_test.go. CONTRIBUTING.md#transport-and-check-changes
// states the rule; this test enforces the file's presence, and adding the
// fault case for a change is a review duty.
package checks_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const (
	matrixFile   = "verdict_matrix_test.go"
	ruleAnchor   = "CONTRIBUTING.md#transport-and-check-changes"
	matrixMarker = "func TestVerdictMatrix("
)

var matrixFunc = regexp.MustCompile(`(?m)^func TestVerdictMatrix\(`)

// TestEveryCheckHasVerdictMatrix applies the rule to every directory under
// checks/ that holds non-test Go files.
func TestEveryCheckHasVerdictMatrix(t *testing.T) {
	problems, packages, err := missingMatrices(".")
	if err != nil {
		t.Fatal(err)
	}
	if packages == 0 {
		t.Fatal("found no check packages under checks/; the directory walk is broken")
	}
	for _, p := range problems {
		t.Error(p)
	}
}

// TestMissingMatrices checks the walker itself on a synthetic tree.
func TestMissingMatrices(t *testing.T) {
	root := t.TempDir()
	write := func(rel, content string) {
		t.Helper()
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("good/good.go", "package good\n")
	write("good/"+matrixFile, "package good\n\nfunc TestVerdictMatrix(t *testing.T) {}\n")
	write("missing/missing.go", "package missing\n")
	write("empty/empty.go", "package empty\n")
	write("empty/"+matrixFile, "package empty\n\n// TestVerdictMatrix is planned.\n")
	write("testsonly/x_test.go", "package testsonly\n")        // no non-test Go files: exempt
	write("missing/testdata/fixture.go", "package fixture\n")  // testdata is not a package
	write("nested/deeper/deeper.go", "package deeper\n")       // nested packages are checked too
	write("docsonly/README.md", "not a package\n")             // no Go files: exempt
	write("good/sub/"+matrixFile, "func TestVerdictMatrix(\n") // matrix without code: exempt, not an error

	problems, packages, err := missingMatrices(root)
	if err != nil {
		t.Fatal(err)
	}
	if packages != 4 {
		t.Errorf("found %d check packages, want 4 (good, missing, empty, nested/deeper)", packages)
	}
	want := map[string]bool{"missing": false, "empty": false, filepath.Join("nested", "deeper"): false}
	for _, p := range problems {
		dir := strings.SplitN(strings.TrimPrefix(p, "checks/"), ":", 2)[0]
		if _, ok := want[filepath.FromSlash(dir)]; !ok {
			t.Errorf("unexpected problem: %s", p)
			continue
		}
		want[filepath.FromSlash(dir)] = true
		if !strings.Contains(p, matrixFile) || !strings.Contains(p, ruleAnchor) {
			t.Errorf("problem %q does not name the file to create and %s", p, ruleAnchor)
		}
	}
	for dir, seen := range want {
		if !seen {
			t.Errorf("no problem reported for %s", dir)
		}
	}
}

// missingMatrices walks root and returns one message per check package that
// lacks a verdict_matrix_test.go holding func TestVerdictMatrix, and the number
// of check packages found. A check package is a directory with at least one
// non-test .go file; testdata and directories starting with "." or "_" are
// skipped, as the go tool skips them.
func missingMatrices(root string) (problems []string, packages int, err error) {
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		name := d.Name()
		if path != root && (name == "testdata" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
			return filepath.SkipDir
		}
		entries, err := os.ReadDir(path)
		if err != nil {
			return err
		}
		hasCode := false
		for _, e := range entries {
			n := e.Name()
			if !e.IsDir() && strings.HasSuffix(n, ".go") && !strings.HasSuffix(n, "_test.go") {
				hasCode = true
				break
			}
		}
		if !hasCode {
			return nil
		}
		packages++
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		dir := "checks"
		if rel != "." {
			dir += "/" + filepath.ToSlash(rel)
		}
		src, readErr := os.ReadFile(filepath.Join(path, matrixFile)) // #nosec G304 -- path comes from walking the repo's own checks/ tree
		switch {
		case os.IsNotExist(readErr):
			problems = append(problems, dir+": no "+matrixFile+"; create "+dir+"/"+matrixFile+
				" with "+matrixMarker+"t *testing.T) covering this check's fault cases; see the standing rule in "+ruleAnchor)
		case readErr != nil:
			return readErr
		case !matrixFunc.Match(src):
			problems = append(problems, dir+": "+matrixFile+" has no "+matrixMarker+"t *testing.T); add it to "+dir+"/"+matrixFile+
				" with this check's fault cases; see the standing rule in "+ruleAnchor)
		}
		return nil
	})
	return problems, packages, err
}

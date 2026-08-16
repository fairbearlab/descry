package sink

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// TestFileSink_NoTornLines writes N events concurrently and verifies that every
// line in the output file is valid JSON (no torn/interleaved writes).
func TestFileSink_NoTornLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	fs, err := NewFileSink(path)
	if err != nil {
		t.Fatal(err)
	}

	const n = 200
	var wg sync.WaitGroup
	var pubErrs atomic.Int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := cloudevents.NewEvent()
			e.SetID("01ARZ3NDEKTSV4RRFFQ69G5FAV") // fixed ULID-shaped id
			e.SetSource("test")
			e.SetType("dev.descry.observation.v1")
			if err := fs.Publish(context.Background(), e); err != nil {
				pubErrs.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := pubErrs.Load(); got > 0 {
		t.Fatalf("%d of %d Publish calls failed", got, n)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Errorf("close output: %v", err)
		}
	}()

	lines := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		// Each line must be valid JSON.
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("torn/invalid line %d: %v\nline: %s", lines+1, err, sc.Text())
		}
		lines++
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan error: %v", err)
	}
	if lines != n {
		t.Fatalf("lines = %d, want %d", lines, n)
	}
}

// TestFileSink_AppendMode verifies that opening an existing file appends
// rather than truncating.
func TestFileSink_AppendMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")

	// First sink: write one event.
	fs1, err := NewFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	e := cloudevents.NewEvent()
	e.SetID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	e.SetSource("test")
	e.SetType("dev.descry.observation.v1")
	if err := fs1.Publish(context.Background(), e); err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	if err := fs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Second sink: write another event to the same file.
	fs2, err := NewFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := fs2.Publish(context.Background(), e); err != nil {
		t.Fatalf("publish 2: %v", err)
	}
	if err := fs2.Close(); err != nil {
		t.Fatalf("close 2: %v", err)
	}

	// Expect 2 lines total.
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	sc := bufio.NewScanner(f)
	lines := 0
	for sc.Scan() {
		lines++
	}
	if lines != 2 {
		t.Fatalf("lines = %d, want 2 (append mode broken)", lines)
	}
}

// TestFileSink_Permissions pins Decision 8A: NewFileSink always leaves the
// underlying file at mode 0o600, both for a file it creates and for a
// pre-existing file it merely reopens. The chmod-on-every-open behavior is
// deterministic on purpose — O_CREATE's mode argument only applies to newly
// created files, so without an explicit chmod a pre-existing file with looser
// permissions (e.g. 0o644) would keep them forever.
func TestFileSink_Permissions(t *testing.T) {
	t.Run("fresh file is created at 0600", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "events.jsonl")

		fs, err := NewFileSink(path)
		if err != nil {
			t.Fatalf("NewFileSink: %v", err)
		}
		if err := fs.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		fi, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if got := fi.Mode().Perm(); got != 0o600 {
			t.Fatalf("mode = %o, want 0600", got)
		}
	})

	t.Run("pre-existing 0644 file is tightened to 0600", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "events.jsonl")

		// WriteFile's mode is masked by the process umask, so it alone can't
		// guarantee a 0644 file under a stricter umask (e.g. 077). Chmod
		// explicitly afterward to pin the precondition regardless of umask.
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatalf("seed file: %v", err)
		}
		// #nosec G302 -- deliberately loosening to 0644 to exercise the tightening behavior (Decision 8A)
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatalf("seed chmod: %v", err)
		}
		if fi, err := os.Stat(path); err != nil {
			t.Fatalf("stat before: %v", err)
		} else if got := fi.Mode().Perm(); got != 0o644 {
			t.Fatalf("precondition failed: seeded mode = %o, want 0644", got)
		}

		fs, err := NewFileSink(path)
		if err != nil {
			t.Fatalf("NewFileSink: %v", err)
		}
		if err := fs.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		fi, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat after: %v", err)
		}
		if got := fi.Mode().Perm(); got != 0o600 {
			t.Fatalf("mode = %o, want 0600 (pre-existing file not tightened)", got)
		}
	})
}

// TestNewFileSink_OpenError verifies that an unopenable path surfaces a
// wrapped, user-facing error rather than a nil sink.
func TestNewFileSink_OpenError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist", "events.jsonl")
	fs, err := NewFileSink(path)
	if err == nil {
		_ = fs.Close()
		t.Fatal("expected error for path in nonexistent directory, got nil")
	}
	if fs != nil {
		t.Errorf("expected nil sink on error, got %v", fs)
	}
	if !strings.Contains(err.Error(), "open sink file:") {
		t.Errorf("error = %q, want it wrapped with %q", err.Error(), "open sink file:")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("error = %v, want errors.Is(err, os.ErrNotExist)", err)
	}
}

// TestFileSink_UseAfterClose pins that a closed sink surfaces errors instead
// of silently dropping events: Publish must fail on the closed fd, and a
// second Close must report the underlying close error rather than swallow it.
func TestFileSink_UseAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	fs, err := NewFileSink(path)
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	e := cloudevents.NewEvent()
	e.SetID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	e.SetSource("test")
	e.SetType("dev.descry.observation.v1")
	if err := fs.Publish(context.Background(), e); err == nil {
		t.Error("Publish after Close: expected error, got nil")
	}

	if err := fs.Close(); !errors.Is(err, os.ErrClosed) {
		t.Errorf("second Close: err = %v, want errors.Is(err, os.ErrClosed)", err)
	}

	// Nothing should have reached the file.
	b, err := os.ReadFile(path) // #nosec G304 -- test-owned temp path
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if len(b) != 0 {
		t.Errorf("file should be empty after use-after-close, got %d bytes", len(b))
	}
}

// TestFileSink_PublishWritesGoldenLine pins the exact output bytes across the
// writeLine refactor: marshaled JSON followed by a single '\n', nothing
// more (the prior `append(b, '\n')` could reallocate but wrote the same
// bytes; this pins that the byte content is unchanged).
func TestFileSink_PublishWritesGoldenLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	fs, err := NewFileSink(path)
	if err != nil {
		t.Fatal(err)
	}

	e := cloudevents.NewEvent()
	e.SetID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	e.SetSource("test")
	e.SetType("dev.descry.observation.v1")
	want, err := e.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	want = append(want, '\n')

	if err := fs.Publish(context.Background(), e); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	got, err := os.ReadFile(path) // #nosec G304 -- test-owned temp path
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("output = %q, want %q", got, want)
	}
}

// TestFileSink_PublishAllocs guards the writeLine write path:
// Publish must not allocate beyond what marshaling the event itself costs.
// Bound measured 2026-08-16 on go1.26.6 darwin/arm64; re-measure and update
// if it moves (bounds are "<=" the measured value).
func TestFileSink_PublishAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are unreliable under -race")
	}
	path := filepath.Join(t.TempDir(), "events.jsonl")
	fs, err := NewFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fs.Close() }()

	e := cloudevents.NewEvent()
	e.SetID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	e.SetSource("test")
	e.SetType("dev.descry.observation.v1")

	got := testing.AllocsPerRun(200, func() {
		if err := fs.Publish(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	})
	if got > 3 {
		t.Errorf("allocs/op = %v, want <= 3", got)
	}
}

// BenchmarkFileSink_Publish is the before/after evidence for the writeLine
// refactor: run with -benchmem to see allocs/op.
func BenchmarkFileSink_Publish(b *testing.B) {
	path := filepath.Join(b.TempDir(), "events.jsonl")
	fs, err := NewFileSink(path)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = fs.Close() }()

	e := cloudevents.NewEvent()
	e.SetID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	e.SetSource("test")
	e.SetType("dev.descry.observation.v1")

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := fs.Publish(context.Background(), e); err != nil {
			b.Fatal(err)
		}
	}
}

// TestFileSink_ReopenTerminatesTornTail: a file whose last line has no newline
// (a previous process died mid-write) must not have the next record appended
// onto the fragment. The reopened sink starts torn and its first publish emits
// the separator first, so every line after the fragment parses.
func TestFileSink_ReopenTerminatesTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	if err := os.WriteFile(path, []byte(`{"specversion":"1.0","id":"01ARZ3ND`), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := NewFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	if !s.torn {
		t.Fatal("reopened sink on a file without a trailing newline should start torn")
	}
	if err := s.Publish(context.Background(), newTestEvent()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimRight(data, "\n"), []byte("\n"))
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2 (fragment + record):\n%s", len(lines), data)
	}
	if json.Valid(lines[0]) || !json.Valid(lines[1]) {
		t.Fatalf("expected [fragment, record], got %q / %q", lines[0], lines[1])
	}

	// A clean file (trailing newline) reopens untorn.
	s2, err := NewFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	if s2.torn {
		t.Fatal("file ending in newline should reopen untorn")
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestNewFileSink_WriteOnlyExistingFile: an existing sink file that grants
// write but not read permission (mode 0200) must still open — the sink only
// needs to write, and NewFileSink chmods to 0600 before it inspects the tail.
// Opening O_RDWR would have failed here before the chmod could run.
func TestNewFileSink_WriteOnlyExistingFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file permission bits")
	}
	path := filepath.Join(t.TempDir(), "events.jsonl")
	if err := os.WriteFile(path, []byte(`{"torn":`), 0o200); err != nil {
		t.Fatal(err)
	}
	if _, err := os.ReadFile(path); err == nil {
		t.Skip("filesystem does not enforce a write-only mode; nothing to test")
	}
	s, err := NewFileSink(path)
	if err != nil {
		t.Fatalf("NewFileSink on a write-only existing file: %v", err)
	}
	if !s.torn {
		t.Fatal("write-only file with an unterminated tail should start torn")
	}
	if err := s.Publish(context.Background(), newTestEvent()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o, want 0600 after tightening", st.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimRight(data, "\n"), []byte("\n"))
	if len(lines) != 2 || !json.Valid(lines[1]) {
		t.Fatalf("want [fragment, record], got %d lines:\n%s", len(lines), data)
	}
}

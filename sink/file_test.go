package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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
	defer f.Close()

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
	defer f.Close()
	sc := bufio.NewScanner(f)
	lines := 0
	for sc.Scan() {
		lines++
	}
	if lines != 2 {
		t.Fatalf("lines = %d, want 2 (append mode broken)", lines)
	}
}

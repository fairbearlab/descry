package sink

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

func newTestEvent() cloudevents.Event {
	e := cloudevents.NewEvent()
	e.SetID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	e.SetSource("test")
	e.SetType("dev.descry.observation.v1")
	return e
}

// TestStdoutSink_PublishConcurrent verifies the EventSink concurrency-safety
// contract: N concurrent Publish calls produce N intact JSON lines (no torn
// or interleaved writes).
func TestStdoutSink_PublishConcurrent(t *testing.T) {
	var buf bytes.Buffer
	s := NewStdoutSink(&buf)

	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Publish(context.Background(), newTestEvent())
		}()
	}
	wg.Wait()

	lines := 0
	sc := bufio.NewScanner(&buf)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("torn/invalid line %d: %v", lines+1, err)
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

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, errors.New("boom") }

// TestStdoutSink_WriteError verifies the write-error path surfaces the error.
func TestStdoutSink_WriteError(t *testing.T) {
	s := NewStdoutSink(errWriter{})
	if err := s.Publish(context.Background(), newTestEvent()); err == nil {
		t.Fatal("expected write error, got nil")
	}
}

// TestStdoutSink_PublishWritesGoldenLine pins the exact output bytes across
// the writeLine refactor: marshaled JSON followed by a single '\n',
// nothing more.
func TestStdoutSink_PublishWritesGoldenLine(t *testing.T) {
	e := newTestEvent()
	want, err := e.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	want = append(want, '\n')

	var buf bytes.Buffer
	s := NewStdoutSink(&buf)
	if err := s.Publish(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	if got := buf.Bytes(); !bytes.Equal(got, want) {
		t.Errorf("output = %q, want %q", got, want)
	}
}

// TestStdoutSink_PublishAllocs guards the writeLine write path:
// Publish must not allocate beyond what marshaling the event itself costs.
// Bound measured 2026-08-16 on go1.26.6 darwin/arm64; re-measure and update
// if it moves (bounds are "<=" the measured value).
func TestStdoutSink_PublishAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are unreliable under -race")
	}
	e := newTestEvent()
	s := NewStdoutSink(io.Discard)
	got := testing.AllocsPerRun(200, func() {
		if err := s.Publish(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	})
	if got > 3 {
		t.Errorf("allocs/op = %v, want <= 3", got)
	}
}

// BenchmarkStdoutSink_Publish is the before/after evidence for the writeLine
// refactor: run with -benchmem to see allocs/op.
func BenchmarkStdoutSink_Publish(b *testing.B) {
	e := newTestEvent()
	s := NewStdoutSink(io.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := s.Publish(context.Background(), e); err != nil {
			b.Fatal(err)
		}
	}
}

// flakyWriter fails its first n writes and then behaves like a bytes.Buffer.
type flakyWriter struct {
	failFirst int
	calls     int
	buf       bytes.Buffer
}

func (w *flakyWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls <= w.failFirst {
		return 0, errors.New("transient")
	}
	return w.buf.Write(p)
}

// TestStdoutSink_RecoversAfterTransientWriteError: bufio.Writer latches its
// first error, so without a reset one failed write of the caller's writer
// would make every later Publish fail too. The sink must report the failed
// Publish and then succeed on the next one, writing exactly that next line.
func TestStdoutSink_RecoversAfterTransientWriteError(t *testing.T) {
	w := &flakyWriter{failFirst: 1}
	s := NewStdoutSink(w)
	e := newTestEvent()

	if err := s.Publish(context.Background(), e); err == nil {
		t.Fatal("first Publish: expected transient error, got nil")
	}
	if err := s.Publish(context.Background(), e); err != nil {
		t.Fatalf("second Publish: expected recovery, got %v", err)
	}
	if err := s.Publish(context.Background(), e); err != nil {
		t.Fatalf("third Publish: %v", err)
	}
	line, err := e.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte{}, line...), '\n')
	want = append(want, want...)
	if got := w.buf.Bytes(); !bytes.Equal(got, want) {
		t.Errorf("after recovery output = %q, want exactly two lines %q", got, want)
	}
}

// TestWriteLine_DirectWriteError exercises writeLine's own error return (a
// Write that fails before Flush), not just the flush-time path: with a
// zero-size buffer bufio writes straight through, so the failing writer is
// hit inside writeLine.
func TestWriteLine_DirectWriteError(t *testing.T) {
	bw := bufio.NewWriterSize(errWriter{}, 16)
	if err := writeLine(bw, bytes.Repeat([]byte("x"), 64)); err == nil {
		t.Fatal("expected write error from writeLine, got nil")
	}
}

// partialWriter accepts the first n bytes of one Write and then fails, the way
// a regular file behaves on ENOSPC; every later Write succeeds.
type partialWriter struct {
	failOnce int // bytes accepted before the one-time failure
	failed   bool
	buf      bytes.Buffer
}

func (w *partialWriter) Write(p []byte) (int, error) {
	if !w.failed {
		w.failed = true
		n := min(w.failOnce, len(p))
		w.buf.Write(p[:n])
		return n, errors.New("no space left")
	}
	return w.buf.Write(p)
}

// TestStdoutSink_PartialWriteDoesNotMergeRecords: a partial write leaves a
// torn fragment in the output. The next successful Publish must terminate the
// fragment with its own newline so the following record is still a parseable
// line on its own — every non-empty line after the failure must be valid JSON.
func TestStdoutSink_PartialWriteDoesNotMergeRecords(t *testing.T) {
	w := &partialWriter{failOnce: 7}
	s := NewStdoutSink(w)
	e := newTestEvent()

	if err := s.Publish(context.Background(), e); err == nil {
		t.Fatal("first Publish: expected partial-write error, got nil")
	}
	for i := 0; i < 2; i++ {
		if err := s.Publish(context.Background(), e); err != nil {
			t.Fatalf("Publish after failure: %v", err)
		}
	}
	lines := bytes.Split(bytes.TrimRight(w.buf.Bytes(), "\n"), []byte("\n"))
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3 (fragment + 2 records):\n%s", len(lines), w.buf.String())
	}
	if !bytes.HasPrefix(lines[0], []byte(`{"specv`)) || json.Valid(lines[0]) {
		t.Errorf("line 0 should be the torn 7-byte fragment, got %q", lines[0])
	}
	for _, l := range lines[1:] {
		if !json.Valid(l) {
			t.Errorf("record merged with fragment, not valid JSON: %q", l)
		}
	}
}

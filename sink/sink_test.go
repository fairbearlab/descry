package sink

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

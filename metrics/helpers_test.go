package metrics

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fairbearlab/descry/check"
)

// This file holds the test doubles and the gather helpers shared by the
// package's tests.

// fakeCheck returns obs and err on every Run. Tests that change them between
// runs do so from a single goroutine.
type fakeCheck struct {
	obs check.Observation
	err error
}

func (f *fakeCheck) Name() string { return "fake" }

func (f *fakeCheck) Run(context.Context, check.Target) (check.Observation, error) {
	return f.obs, f.err
}

// fakeStats is a RunnerStats with settable counters.
type fakeStats struct{ skipped, dropped atomic.Int64 }

func (s *fakeStats) Skipped() int64 { return s.skipped.Load() }
func (s *fakeStats) Dropped() int64 { return s.dropped.Load() }

// newTestCollector builds a Collector over fakeStats with a fixed clock.
func newTestCollector(t *testing.T, opts Options, now time.Time) *Collector {
	t.Helper()
	c, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.SetRunner(&fakeStats{})
	c.now = func() time.Time { return now }
	return c
}

// samples gathers c's registry into a map keyed by series, e.g.
// `descry_check_up{url="https://h/"}`. Labels keep Gather's (sorted) order. A
// histogram contributes name_count and name_sum.
func samples(t *testing.T, c *Collector) map[string]float64 {
	t.Helper()
	mfs, err := c.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := make(map[string]float64)
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			var pairs []string
			for _, l := range m.GetLabel() {
				pairs = append(pairs, l.GetName()+"="+strconv.Quote(l.GetValue()))
			}
			labels := ""
			if len(pairs) > 0 {
				labels = "{" + strings.Join(pairs, ",") + "}"
			}
			name := mf.GetName()
			switch {
			case m.GetGauge() != nil:
				out[name+labels] = m.GetGauge().GetValue()
			case m.GetCounter() != nil:
				out[name+labels] = m.GetCounter().GetValue()
			case m.GetHistogram() != nil:
				out[name+"_count"+labels] = float64(m.GetHistogram().GetSampleCount())
				out[name+"_sum"+labels] = m.GetHistogram().GetSampleSum()
			default:
				t.Fatalf("unexpected metric type for %s", name)
			}
		}
	}
	return out
}

// familyNames returns the sorted metric family names in c's registry.
func familyNames(t *testing.T, c *Collector) []string {
	t.Helper()
	mfs, err := c.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	names := make([]string, 0, len(mfs))
	for _, mf := range mfs {
		names = append(names, mf.GetName())
	}
	sort.Strings(names)
	return names
}

// labelText returns every label name and value in c's registry.
func labelText(t *testing.T, c *Collector) []string {
	t.Helper()
	mfs, err := c.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var out []string
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				out = append(out, l.GetName(), l.GetValue())
			}
		}
	}
	return out
}

// want asserts the series key has value v.
func want(t *testing.T, got map[string]float64, key string, v float64) {
	t.Helper()
	g, ok := got[key]
	if !ok {
		t.Errorf("series %s missing", key)
		return
	}
	if g != v {
		t.Errorf("%s = %v, want %v", key, g, v)
	}
}

// absent asserts the series key is not exported.
func absent(t *testing.T, got map[string]float64, key string) {
	t.Helper()
	if v, ok := got[key]; ok {
		t.Errorf("series %s = %v, want absent", key, v)
	}
}

// waitFor polls cond until it holds or a generous deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

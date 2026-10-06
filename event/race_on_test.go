//go:build race

package event

// raceEnabled reports whether the race detector is instrumenting this test
// binary. AllocsPerRun guards skip under -race: the detector's own shadow
// tracking adds allocations unrelated to the code under test.
const raceEnabled = true

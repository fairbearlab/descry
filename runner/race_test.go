//go:build race

package runner

// raceEnabled reports that the race detector is on. The scale harness in
// scale_test.go skips under it: its numbers are meaningless with the detector's
// overhead and its 10k-target regime would dominate the -race job's wall time.
const raceEnabled = true

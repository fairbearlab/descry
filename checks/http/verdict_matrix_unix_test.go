//go:build unix

package http

import (
	"os"
	"syscall"
)

const rlimitSupported = true

// lowerNoFileLimit sets the soft RLIMIT_NOFILE to the lowest free descriptor
// number, so the next descriptor this process opens fails with EMFILE. The
// returned func puts the old limit back: the test binary still opens files
// after the test, such as the coverage data it writes at exit under -cover.
func lowerNoFileLimit() (restore func() error, err error) {
	f, err := os.Open(os.DevNull)
	if err != nil {
		return nil, err
	}
	next := f.Fd()
	if err := f.Close(); err != nil {
		return nil, err
	}
	var old syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &old); err != nil {
		return nil, err
	}
	lowered := old
	lowered.Cur = uint64(next)
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &lowered); err != nil {
		return nil, err
	}
	return func() error { return syscall.Setrlimit(syscall.RLIMIT_NOFILE, &old) }, nil
}

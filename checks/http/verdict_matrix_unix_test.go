//go:build unix

package http

import (
	"os"
	"syscall"
)

const rlimitSupported = true

// lowerNoFileLimit sets the soft RLIMIT_NOFILE to the lowest free descriptor
// number, so the next descriptor this process opens fails with EMFILE.
func lowerNoFileLimit() error {
	f, err := os.Open(os.DevNull)
	if err != nil {
		return err
	}
	next := f.Fd()
	if err := f.Close(); err != nil {
		return err
	}
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err != nil {
		return err
	}
	rl.Cur = uint64(next)
	return syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rl)
}

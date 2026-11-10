//go:build !unix

package http

import "errors"

const rlimitSupported = false

func lowerNoFileLimit() error { return errors.New("RLIMIT_NOFILE needs unix") }

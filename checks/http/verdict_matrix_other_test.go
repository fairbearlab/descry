//go:build !unix

package http

import "errors"

const rlimitSupported = false

func lowerNoFileLimit() (func() error, error) { return nil, errors.New("RLIMIT_NOFILE needs unix") }

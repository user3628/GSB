//go:build windows

package force

import "os"

// Terminate uses the conventional forced-exit status on Windows. No deferred
// cleanup or graceful shutdown handler is run.
func Terminate() { os.Exit(137) }

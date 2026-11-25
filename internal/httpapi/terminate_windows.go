//go:build windows

package httpapi

import "os"

func terminateProcess() {
	os.Exit(137)
}

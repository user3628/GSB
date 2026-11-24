//go:build windows

package verify

import "os/exec"

func killedBySigkill(err *exec.ExitError) bool {
	return err != nil && err.ExitCode() != 0
}

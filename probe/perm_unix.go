//go:build !windows

package probe

import "os"

type denyClass int

const (
	denyRead denyClass = iota
	denyWrite
	denyExec
)

func applyDeny(path string, class denyClass) (func() error, error) {
	mode := os.FileMode(0o600)
	switch class {
	case denyRead:
		mode = 0o000
	case denyWrite:
		mode = 0o444
	case denyExec:
		mode = 0o644
	}
	if err := os.Chmod(path, mode); err != nil {
		return nil, err
	}
	restoreMode := os.FileMode(0o600)
	if class == denyExec {
		restoreMode = 0o755
	}
	return func() error { return os.Chmod(path, restoreMode) }, nil
}

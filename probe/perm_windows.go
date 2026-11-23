//go:build windows

package probe

import (
	"fmt"
	"os/exec"
	"os/user"
	"strings"
)

type denyClass int

const (
	denyRead denyClass = iota
	denyWrite
	denyExec
)

// applyDeny adds an explicit deny ACE for the current user using the built-in
// Windows icacls utility. The ACE is removed before the temporary workspace is
// deleted. No path outside the probe workspace is passed to icacls.
func applyDeny(path string, class denyClass) (func() error, error) {
	account, err := user.Current()
	if err != nil || strings.TrimSpace(account.Username) == "" {
		return nil, fmt.Errorf("resolve current Windows account")
	}
	rights := "R"
	switch class {
	case denyWrite:
		rights = "W"
	case denyExec:
		rights = "RX"
	}
	output, err := exec.Command("icacls", path, "/deny", account.Username+":("+rights+")").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("icacls deny %s: %w (%s)", rights, err, strings.TrimSpace(string(output)))
	}
	restore := func() error {
		output, err := exec.Command("icacls", path, "/remove:d", account.Username).CombinedOutput()
		if err != nil {
			return fmt.Errorf("icacls remove deny: %w (%s)", err, strings.TrimSpace(string(output)))
		}
		return nil
	}
	return restore, nil
}

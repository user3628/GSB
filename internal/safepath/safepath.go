// Package safepath confines every filesystem operation to a caller-supplied
// workspace root. Relative paths use forward slashes inside reports.
package safepath

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var ErrEscape = errors.New("path escapes workspace")

func Root(root string) (string, error) {
	if strings.TrimSpace(root) == "" {
		return "", errors.New("workspace root is required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return "", err
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", err
	}
	return real, nil
}

func CleanRel(rel string) (string, error) {
	if strings.ContainsRune(rel, 0) {
		return "", errors.New("path contains NUL")
	}
	rel = strings.ReplaceAll(strings.TrimSpace(rel), "\\", "/")
	if rel == "" || rel == "." {
		return ".", nil
	}
	if strings.HasPrefix(rel, "/") {
		return "", ErrEscape
	}
	clean := filepath.ToSlash(filepath.Clean(rel))
	if clean == ".." || strings.HasPrefix(clean, "../") || strings.Contains(clean, "/../") {
		return "", ErrEscape
	}
	if filepath.IsAbs(clean) || filepath.VolumeName(clean) != "" {
		return "", ErrEscape
	}
	return clean, nil
}

func Join(root, rel string) (string, error) {
	r, err := CleanRel(rel)
	if err != nil {
		return "", err
	}
	base, err := Root(root)
	if err != nil {
		return "", err
	}
	if r == "." {
		return base, nil
	}
	target := filepath.Join(base, filepath.FromSlash(r))
	inside, err := filepath.Rel(base, target)
	if err != nil || inside == ".." || strings.HasPrefix(inside, ".."+string(filepath.Separator)) {
		return "", ErrEscape
	}
	return target, nil
}

func EnsureNoSymlink(root, target string) error {
	base, err := Root(root)
	if err != nil {
		return err
	}
	abs, err := filepath.Abs(target)
	if err != nil {
		return err
	}
	inside, err := filepath.Rel(base, abs)
	if err != nil || inside == ".." || strings.HasPrefix(inside, ".."+string(filepath.Separator)) {
		return ErrEscape
	}
	// Walk from base to target and reject any symlink component.
	cur := base
	for _, part := range strings.Split(filepath.ToSlash(inside), "/") {
		if part == "" || part == "." {
			continue
		}
		cur = filepath.Join(cur, part)
		info, err := os.Lstat(cur)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink component rejected: %s", cur)
		}
	}
	return nil
}

func MustRel(base, target string) (string, error) {
	rel, err := filepath.Rel(base, target)
	if err != nil {
		return "", err
	}
	rel = filepath.ToSlash(rel)
	if rel == ".." || strings.HasPrefix(rel, "../") {
		return "", ErrEscape
	}
	return rel, nil
}

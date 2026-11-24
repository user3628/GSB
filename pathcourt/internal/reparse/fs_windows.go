//go:build windows

package reparse

// FSPath returns the path spelling used for file-system access. On Windows
// the normalized back-slash spelling is already native.
func FSPath(p string) string { return p }

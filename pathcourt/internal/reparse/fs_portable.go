//go:build !windows

package reparse

import "strings"

// FSPath converts a Windows-normalized path into the host native spelling
// used for file-system access on portable (non-Windows) hosts.
//
// Paths produced inside the generated workspace use forward slashes already
// on these hosts (the generator calls filepath.Join), so only Windows-style
// absolute paths (drive rooted or UNC) need an explicit translation.
func FSPath(p string) string {
	if p == "" {
		return p
	}
	// Drive rooted: "C:\foo" -> "/foo".
	if len(p) >= 3 && p[1] == ':' && (p[2] == '\\' || p[2] == '/') {
		return "/" + strings.ReplaceAll(p[3:], `\`, "/")
	}
	// UNC: "\\server\share\foo" -> "/server/share/foo".
	if strings.HasPrefix(p, `\\`) {
		return "/" + strings.ReplaceAll(strings.TrimPrefix(p, `\\`), `\`, "/")
	}
	// Host native absolute or relative path: normalize separators only.
	return strings.ReplaceAll(p, `\`, "/")
}

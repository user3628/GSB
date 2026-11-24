// Package pathnorm implements deterministic Windows-style path
// normalization used for containment checks and de-duplication while the
// original spelling is retained in reports.
//
// The rules intentionally mirror Windows path semantics:
//   - forward and back slashes are equivalent and collapse to a back slash;
//   - "." components are dropped and ".." components pop the previous
//     component (a leading ".." on an absolute path is clamped at the root);
//   - drive letters and UNC server/share prefixes are preserved;
//   - comparison keys are lower cased because the Windows file system is
//     case-insensitive.
package pathnorm

import "strings"

const sep = `\`

// Split returns the non-empty path components of p after converting all
// separators. Drive letters and UNC prefixes are returned as ordinary
// components (e.g. "C:" and the leading double slash is consumed).
func Split(p string) []string {
	p = strings.ReplaceAll(p, "/", `\`)
	// A UNC path begins with exactly two separators; three or more collapse
	// to a single rooted path because the leading components are empty.
	unc := strings.HasPrefix(p, `\\`) && !strings.HasPrefix(p, `\\\`)
	raw := strings.Split(p, `\`)
	out := make([]string, 0, len(raw))
	for i, c := range raw {
		if c == "" {
			if i == 0 && unc {
				out = append(out, `\\`)
			}
			continue
		}
		out = append(out, c)
	}
	return out
}

// Normalize returns the canonical absolute spelling of p. Relative input is
// resolved against base (already absolute). The returned path keeps the
// original drive/UNC casing but normalizes separators and dot components.
func Normalize(base, p string) string {
	p = strings.ReplaceAll(p, "/", `\`)
	if base != "" && !IsAbsolute(p) {
		p = Join(base, p)
	}
	comps := Split(p)
	if len(comps) == 0 {
		return sep
	}

	// Root anchor: drive component ("C:"), UNC marker ("\\" + share) or a
	// single leading separator.
	var root string
	var rest []string
	switch {
	case comps[0] == `\\`:
		// UNC: \\ server share dir...
		root = `\\`
		if len(comps) > 1 {
			root += comps[1]
		}
		if len(comps) > 2 {
			root += `\` + comps[2]
		}
		root += `\`
		rest = comps[3:]
	case isDrive(comps[0]):
		root = comps[0] + `\`
		rest = comps[1:]
	default:
		root = sep
		rest = comps
	}

	var stack []string
	for _, c := range rest {
		switch c {
		case ".":
			continue
		case "..":
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
			continue
		default:
			stack = append(stack, c)
		}
	}
	if len(stack) == 0 {
		return strings.TrimRight(root, `\`)
	}
	return root + strings.Join(stack, sep)
}

// Key returns the case-insensitive comparison key for a normalized path.
func Key(normalized string) string {
	return strings.ToLower(normalized)
}

// IsAbsolute reports whether p is absolute on Windows (drive rooted, UNC or
// a separator rooted path).
func IsAbsolute(p string) bool {
	if p == "" {
		return false
	}
	if hasSingleLeadingSep(p) {
		return true
	}
	if LeadingUNC(p) {
		return true
	}
	comps := Split(p)
	return len(comps) > 0 && isDrive(comps[0])
}

// LeadingUNC reports whether p starts with an exact double-separator UNC
// prefix (rather than three or more collapsed separators).
func LeadingUNC(p string) bool {
	bs := strings.HasPrefix(p, `\\`) && !strings.HasPrefix(p, `\\\`)
	fs := strings.HasPrefix(p, "//") && !strings.HasPrefix(p, "///")
	return bs || fs
}

// hasSingleLeadingSep reports a separator-rooted (but not UNC or collapsed)
// path such as "\windows" or "/windows".
func hasSingleLeadingSep(p string) bool {
	if p == "" {
		return false
	}
	first := p[0]
	if first != '/' && first != '\\' {
		return false
	}
	// Relative traversal such as "..\\foo" or "../foo" is not rooted.
	if len(p) >= 2 && p[0] == '.' && p[1] == '.' {
		return false
	}
	if len(p) > 1 && (p[1] == '/' || p[1] == '\\') {
		return false
	}
	return true
}

// Join joins elements with a single back slash without normalizing dots.
func Join(parts ...string) string {
	cleaned := make([]string, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			continue
		}
		cleaned = append(cleaned, strings.TrimRight(strings.ReplaceAll(p, "/", `\`), `\`))
	}
	return strings.Join(cleaned, sep)
}

// Within reports whether normalized target is the same as, or nested below,
// the normalized root using case-insensitive Windows semantics.
func Within(root, target string) bool {
	r := Key(strings.TrimRight(root, `\`))
	t := Key(strings.TrimRight(target, `\`))
	if r == t {
		return true
	}
	return strings.HasPrefix(t, r+sep)
}

// Rel returns target relative to root using back slashes, or "" when target
// is not within root.
func Rel(root, target string) string {
	r := strings.TrimRight(Normalize("", root), `\`)
	t := Normalize("", target)
	if !Within(r, t) {
		return ""
	}
	rel := strings.TrimPrefix(t, r)
	return strings.TrimLeft(rel, `\`)
}

func isDrive(c string) bool {
	return len(c) == 2 && c[1] == ':' &&
		((c[0] >= 'a' && c[0] <= 'z') || (c[0] >= 'A' && c[0] <= 'Z'))
}

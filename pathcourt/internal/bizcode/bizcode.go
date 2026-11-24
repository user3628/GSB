// Package bizcode defines the stable business verdict codes emitted by the
// path resolution workbench. Codes are part of the persisted and exported
// contract and must never be renumbered or reused.
package bizcode

// Code is a stable business verdict code. The zero value OK means the path
// resolved fully and remained inside the restricted root.
type Code string

const (
	// OK: fully resolved and contained by the restricted root.
	OK Code = "OK"

	// E_PATH_ESCAPE: the resolved target exists but lies outside the
	// restricted root. FirstEscape records the first escaping component.
	E_PATH_ESCAPE Code = "E_PATH_ESCAPE"

	// E_LINK_CYCLE: the same reparse target repeats while resolving links.
	E_LINK_CYCLE Code = "E_LINK_CYCLE"

	// E_LINK_DEPTH: the number of reparse jumps exceeds the configured cap.
	E_LINK_DEPTH Code = "E_LINK_DEPTH"

	// E_TARGET_MISSING: a component or a link target does not exist while the
	// referenced location is still inside the restricted root.
	E_TARGET_MISSING Code = "E_TARGET_MISSING"

	// E_ESCAPE_DANGLING: a link points outside the restricted root and that
	// outside target has not been created (dangling escape).
	E_ESCAPE_DANGLING Code = "E_ESCAPE_DANGLING"

	// E_NOT_DIRECTORY: a non-final path component resolves to a regular file,
	// so the remainder of the path cannot be traversed.
	E_NOT_DIRECTORY Code = "E_NOT_DIRECTORY"

	// E_SNAPSHOT_VERSION: a snapshot version chain has a gap; the store rolls
	// back to the most recent contiguous version.
	E_SNAPSHOT_VERSION Code = "E_SNAPSHOT_VERSION"

	// E_PATH_BUDGET: a manifest exceeded the per-session path count budget;
	// already completed paths are still committed.
	E_PATH_BUDGET Code = "E_PATH_BUDGET"
)

// Terminal reports whether the code aborts the current path resolution.
func (c Code) Terminal() bool { return c != OK }

// String implements fmt.Stringer.
func (c Code) String() string { return string(c) }

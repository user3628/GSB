// Package reparse abstracts the Windows reparse point primitives the
// workbench relies on: symbolic links, directory junctions (mount points)
// and relative-target links. A portable fallback implementation is provided
// for non-Windows development and CI hosts so the whole resolution pipeline
// stays executable and testable; on Windows the native reparse buffer is
// inspected so junctions and symlinks are distinguished precisely.
package reparse

import "os"

// Kind classifies a reparse point.
type Kind string

const (
	// KindNone: not a reparse point (plain directory or file).
	KindNone Kind = "none"
	// KindSymlink: IO_REPARSE_TAG_SYMLINK (file or directory symlink).
	KindSymlink Kind = "symlink"
	// KindJunction: IO_REPARSE_TAG_MOUNT_POINT directory junction.
	KindJunction Kind = "junction"
)

// Info describes a single file system entry from the resolver's point of
// view. Target holds the raw link target spelling (relative or absolute)
// for reparse points and is empty otherwise.
type Info struct {
	Kind       Kind
	IsDir      bool
	Exists     bool
	Target     string
	Emulated   bool // true when the host cannot create the native artefact
	LinkPath   string
}

// Inspector returns reparse metadata for path.
type Inspector interface {
	Inspect(path string) (Info, error)
}

// Factory abstracts reparse point creation so the deterministic generator
// can adapt to host privileges (developer mode / elevation on Windows).
type Factory interface {
	// Symlink creates a symbolic link (file or directory target).
	Symlink(link, target string) error
	// Junction creates a directory junction / mount point.
	Junction(link, target string) error
	// SupportsSymlinks reports whether native symlink creation works.
	SupportsSymlinks() bool
	// SupportsJunctions reports whether native junction creation works.
	SupportsJunctions() bool
	// Capabilities returns a human-readable capability summary.
	Capabilities() Capabilities
}

// Capabilities summarizes what the host allows the generator to create.
type Capabilities struct {
	Symlinks       bool `json:"symlinks"`
	Junctions      bool `json:"junctions"`
	RelativeLinks  bool `json:"relative_links"`
	SymlinkProbe   string `json:"symlink_probe"`
	JunctionProbe  string `json:"junction_probe"`
	EmulatedLinks  bool   `json:"emulated_links"`
}

// Lstat mirrors os.Lstat but goes through the OS-specific reparse inspector.
func Lstat(path string) (os.FileInfo, error) { return os.Lstat(path) }

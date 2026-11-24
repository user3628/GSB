//go:build !windows

// Portable fallback used on development and CI hosts that are not Windows.
// Symbolic links are real OS symlinks; directory junctions are emulated with
// directory symlinks tracked in a process-local registry so the resolver can
// still classify them as "junction" and the full business pipeline, snapshot
// store and report writer remain executable and testable. The Windows build
// uses native reparse buffers instead.
package reparse

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	junctionMu sync.Mutex
	junctions  = map[string]junctionRecord{}
)

type junctionRecord struct {
	// target is the logical target spelling used by the resolver
	// (Windows-normalized), while physical is the on-disk symlink target.
	target   string
	physical string
	emulated bool
}

// ResetJunctionRegistry clears emulated junction state. Test-only helper.
func ResetJunctionRegistry() {
	junctionMu.Lock()
	defer junctionMu.Unlock()
	junctions = map[string]junctionRecord{}
}

func junctionKey(link string) string {
	abs, err := filepath.Abs(link)
	if err == nil {
		link = abs
	}
	return strings.ReplaceAll(link, "/", `\`)
}

func registerJunction(link, logicalTarget, physicalTarget string, emulated bool) {
	key := junctionKey(link)
	junctionMu.Lock()
	junctions[key] = junctionRecord{
		target:   strings.ReplaceAll(logicalTarget, "/", `\`),
		physical: physicalTarget,
		emulated: emulated,
	}
	junctionMu.Unlock()
}

func lookupJunction(link string) (junctionRecord, bool) {
	key := junctionKey(link)
	junctionMu.Lock()
	defer junctionMu.Unlock()
	r, ok := junctions[key]
	return r, ok
}

type portableInspector struct{}

// NewInspector returns the portable inspector.
func NewInspector() Inspector { return portableInspector{} }

func (portableInspector) Inspect(path string) (Info, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return Info{}, err
	}
	info := Info{LinkPath: abs}
	resolved := caseFoldPath(path)
	fi, err := os.Lstat(resolved)
	if err != nil {
		if os.IsNotExist(err) {
			info.Exists = false
			return info, nil
		}
		return Info{}, err
	}
	info.Exists = true
	info.IsDir = fi.IsDir()
	if fi.Mode()&os.ModeSymlink == 0 {
		info.Kind = KindNone
		return info, nil
	}
	target, err := os.Readlink(resolved)
	if err != nil {
		return Info{}, err
	}
	info.Target = target
	if rec, ok := lookupJunction(path); ok {
		info.Kind = KindJunction
		info.Emulated = rec.emulated
		info.Target = rec.target
		return info, nil
	}
	info.Kind = KindSymlink
	return info, nil
}

type portableFactory struct {
	caps Capabilities
}

// NewFactory probes symlink creation inside probeDir and returns a portable
// factory. Junctions are always emulated on non-Windows hosts.
func NewFactory(probeDir string) (Factory, error) {
	f := &portableFactory{}
	if err := os.MkdirAll(probeDir, 0o755); err != nil {
		return nil, err
	}
	target := filepath.Join(probeDir, "symlink-target")
	link := filepath.Join(probeDir, "symlink-probe")
	if err := os.Mkdir(target, 0o755); err != nil {
		return nil, err
	}
	if err := os.Symlink(target, link); err != nil {
		f.caps.Symlinks = false
		f.caps.SymlinkProbe = err.Error()
	} else {
		f.caps.Symlinks = true
		f.caps.SymlinkProbe = "ok"
		_ = os.Remove(link)
	}
	_ = os.Remove(target)
	// Emulated junctions ride on symlink support; recorded as junctions.
	f.caps.Junctions = f.caps.Symlinks
	f.caps.JunctionProbe = "emulated:directory-symlink"
	f.caps.RelativeLinks = f.caps.Symlinks
	f.caps.EmulatedLinks = true
	return f, nil
}

func (f *portableFactory) SupportsSymlinks() bool  { return f.caps.Symlinks }
func (f *portableFactory) SupportsJunctions() bool { return f.caps.Junctions }
func (f *portableFactory) Capabilities() Capabilities { return f.caps }

func (f *portableFactory) Symlink(link, target string) error {
	// The host kernel only understands native separators inside link
	// targets; Windows-style back slashes are translated for portable hosts.
	native := strings.ReplaceAll(target, `\`, "/")
	return os.Symlink(native, link)
}

// Junction emulates a mount point with a directory symlink and records the
// classification in the process-local registry. The physical symlink uses a
// native host path while the logical target keeps Windows normalization.
func (f *portableFactory) Junction(link, target string) error {
	physical := strings.ReplaceAll(target, `\`, "/")
	if err := os.Symlink(physical, link); err != nil {
		return err
	}
	registerJunction(link, target, physical, true)
	return nil
}

// caseFoldPath emulates the Windows case-insensitive file system on portable
// hosts: if path does not exist but a sibling matches component names case
// insensitively, the existing path is returned. It never creates entries.
func caseFoldPath(path string) string {
	if _, err := os.Lstat(path); err == nil {
		return path
	}
	clean := filepath.Clean(path)
	vol := filepath.VolumeName(clean)
	rest := strings.TrimPrefix(clean, vol)
	parts := strings.Split(strings.Trim(filepath.ToSlash(rest), "/"), "/")
	current := vol
	if current == "" {
		current = string(filepath.Separator)
	}
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		if part == ".." {
			current = filepath.Dir(current)
			continue
		}
		candidate := filepath.Join(current, part)
		if _, err := os.Lstat(candidate); err == nil {
			current = candidate
			continue
		}
		entries, err := os.ReadDir(current)
		if err != nil {
			return path
		}
		matched := false
		for _, e := range entries {
			if strings.EqualFold(e.Name(), part) {
				current = filepath.Join(current, e.Name())
				matched = true
				break
			}
		}
		if !matched {
			return path
		}
	}
	return current
}

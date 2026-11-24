//go:build windows

package reparse

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

const (
	ioReparseTagMountPoint = 0xA0000003
	ioReparseTagSymlink    = 0xA000000C
	symlinkFlagRelative    = 0x00000001

	fsctlGetReparsePoint = 0x000900A8
	fsctlSetReparsePoint = 0x000900A4

	flagOpenReparsePoint = 0x00200000
	flagBackupSemantics  = 0x02000000
	maxReparseBuffer     = 16 * 1024
)

// windowsInspector reads the raw reparse buffer via DeviceIoControl so that
// mount points (junctions) and symbolic links are classified by tag.
type windowsInspector struct{}

// NewInspector returns the native Windows inspector.
func NewInspector() Inspector { return windowsInspector{} }

func openReparse(path string) (windows.Handle, error) {
	ptr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	h, err := windows.CreateFile(
		ptr,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		flagOpenReparsePoint|flagBackupSemantics,
		0,
	)
	if err != nil {
		return 0, err
	}
	return h, nil
}

func (windowsInspector) Inspect(path string) (Info, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return Info{}, err
	}
	info := Info{LinkPath: abs}

	fi, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
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

	h, err := openReparse(path)
	if err != nil {
		return Info{}, &os.PathError{Op: "reparse", Path: path, Err: err}
	}
	defer windows.CloseHandle(h)

	buf := make([]byte, maxReparseBuffer)
	var bytesReturned uint32
	err = windows.DeviceIoControl(h, fsctlGetReparsePoint, nil, 0, &buf[0], uint32(len(buf)), &bytesReturned, nil)
	if err != nil {
		return Info{}, &os.PathError{Op: "fsctl", Path: path, Err: err}
	}
	return parseReparse(abs, buf[:bytesReturned], info), nil
}

// parseReparse decodes REPARSE_DATA_BUFFER.
//
//	0  Tag            uint32
//	4  DataLength     uint16
//	6  Reserved       uint16
//	8  SubstOffset    uint16   \
//	10 SubstLength    uint16    | mount point header ends here (16)
//	12 PrintOffset    uint16    | symlink adds Flags uint32 (header 20)
//	14 PrintLength    uint16   /
//	16 Flags          uint32 (symlink only)
//	.. UTF-16 name buffer
func parseReparse(abs string, b []byte, info Info) Info {
	if len(b) < 8 {
		info.Kind = KindSymlink
		return info
	}
	tag := binary.LittleEndian.Uint32(b[0:4])
	headerLen := 16
	switch tag {
	case ioReparseTagMountPoint:
		info.Kind = KindJunction
	case ioReparseTagSymlink:
		info.Kind = KindSymlink
		headerLen = 20
	default:
		info.Kind = KindSymlink
	}
	if len(b) < headerLen {
		return info
	}
	substOff := binary.LittleEndian.Uint16(b[8:10])
	substLen := binary.LittleEndian.Uint16(b[10:12])
	printOff := binary.LittleEndian.Uint16(b[12:14])
	printLen := binary.LittleEndian.Uint16(b[14:16])

	readName := func(off, length uint16) string {
		start := headerLen + int(off)
		end := start + int(length)
		if end > len(b) || start >= end {
			return ""
		}
		u16 := make([]uint16, length/2)
		for i := range u16 {
			u16[i] = binary.LittleEndian.Uint16(b[start+2*i:])
		}
		return windows.UTF16ToString(u16)
	}

	target := readName(printOff, printLen)
	if target == "" {
		target = strings.TrimPrefix(readName(substOff, substLen), `\??\`)
	}
	info.Target = target
	info.LinkPath = abs
	return info
}

type windowsFactory struct {
	caps Capabilities
}

// NewFactory probes symlink and junction creation inside probeDir and
// returns a native Windows factory.
func NewFactory(probeDir string) (Factory, error) {
	f := &windowsFactory{}
	if err := os.MkdirAll(probeDir, 0o755); err != nil {
		return nil, err
	}

	// Symlink probe: creating a directory symlink needs developer mode or
	// elevation on Windows.
	link := filepath.Join(probeDir, "symlink-probe")
	target := filepath.Join(probeDir, "symlink-target")
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

	// Junction probe: mount points do not require elevation.
	jlink := filepath.Join(probeDir, "junction-probe")
	jtarget := filepath.Join(probeDir, "junction-target")
	if err := os.Mkdir(jtarget, 0o755); err != nil {
		return nil, err
	}
	if err := f.Junction(jlink, jtarget); err != nil {
		f.caps.Junctions = false
		f.caps.JunctionProbe = err.Error()
	} else {
		f.caps.Junctions = true
		f.caps.JunctionProbe = "ok"
		_ = os.Remove(jlink)
	}
	_ = os.Remove(jtarget)
	f.caps.RelativeLinks = f.caps.Symlinks || f.caps.Junctions
	return f, nil
}

func (f *windowsFactory) SupportsSymlinks() bool { return f.caps.Symlinks }
func (f *windowsFactory) SupportsJunctions() bool { return f.caps.Junctions }
func (f *windowsFactory) Capabilities() Capabilities { return f.caps }

func (f *windowsFactory) Symlink(link, target string) error {
	return os.Symlink(target, link)
}

// Junction creates an IO_REPARSE_TAG_MOUNT_POINT reparse point. link must
// not exist; target must be an existing absolute directory path.
func (f *windowsFactory) Junction(link, target string) error {
	absTarget, err := filepath.Abs(target)
	if err != nil {
		return err
	}
	if err := os.Mkdir(link, 0o755); err != nil {
		return err
	}
	subName := `\??\` + absTarget
	printName := absTarget
	return setMountPoint(link, subName, printName)
}

func setMountPoint(link, subName, printName string) error {
	subUTF16 := windows.StringToUTF16(subName)
	printUTF16 := windows.StringToUTF16(printName)
	// StringToUTF16 includes a terminating NUL; the reparse buffer stores it.
	subBytes := len(subUTF16) * 2
	printBytes := len(printUTF16) * 2

	buf := make([]byte, 8+8+subBytes+printBytes)
	binary.LittleEndian.PutUint32(buf[0:4], ioReparseTagMountPoint)
	binary.LittleEndian.PutUint16(buf[4:6], uint16(8+subBytes+printBytes))
	binary.LittleEndian.PutUint16(buf[6:8], 0)
	binary.LittleEndian.PutUint16(buf[8:10], 0)
	binary.LittleEndian.PutUint16(buf[10:12], uint16(subBytes))
	binary.LittleEndian.PutUint16(buf[12:14], uint16(subBytes))
	binary.LittleEndian.PutUint16(buf[14:16], uint16(printBytes))
	offset := 16
	for _, u := range subUTF16 {
		binary.LittleEndian.PutUint16(buf[offset:], u)
		offset += 2
	}
	for _, u := range printUTF16 {
		binary.LittleEndian.PutUint16(buf[offset:], u)
		offset += 2
	}

	ptr, err := windows.UTF16PtrFromString(link)
	if err != nil {
		return err
	}
	h, err := windows.CreateFile(ptr, windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, flagOpenReparsePoint|flagBackupSemantics, 0)
	if err != nil {
		return &os.PathError{Op: "junction", Path: link, Err: err}
	}
	defer windows.CloseHandle(h)
	var returned uint32
	if err := windows.DeviceIoControl(h, fsctlSetReparsePoint, &buf[0], uint32(len(buf)), nil, 0, &returned, nil); err != nil {
		return &os.PathError{Op: "fsctl-set", Path: link, Err: err}
	}
	return nil
}


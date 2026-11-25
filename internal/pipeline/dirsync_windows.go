//go:build windows

package pipeline

// Windows has no directory fsync equivalent needed for rename durability here;
// MoveFileEx is atomic on NTFS. This is an intentional no-op.
func syncDir(dir string) {}

package pipeline

import (
	"os"
	"path/filepath"
)

// AtomicWriteFile writes data via a temp file in the same directory followed by
// an atomic rename. A crash at any point therefore leaves either the previous
// complete file or the new complete file, never a half-written record
// (requirement 5). The temp file is fsynced and the directory is fsynced where
// supported so the rename is durable.
func AtomicWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return bizError(CodeInternal, "mkdir: %v", err)
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return bizError(CodeInternal, "create temp: %v", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		cleanup()
		return bizError(CodeInternal, "write temp: %v", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		cleanup()
		return bizError(CodeInternal, "fsync temp: %v", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return bizError(CodeInternal, "close temp: %v", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return bizError(CodeInternal, "rename: %v", err)
	}
	syncDir(dir)
	return nil
}

// WriteArtifact serializes any artifact canonically and atomically, returning
// its content hash for the checkpoint record.
func WriteArtifact(path string, v any) (string, error) {
	data, err := CanonicalEncode(v)
	if err != nil {
		return "", err
	}
	if err := AtomicWriteFile(path, data); err != nil {
		return "", err
	}
	sum := sha256Bytes(data)
	return sum, nil
}

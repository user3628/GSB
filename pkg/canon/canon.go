// Package canon provides deterministic JSON encoding and hashing helpers.
package canon

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// JSON encodes v without HTML escaping. encoding/json sorts map keys, which
// makes this representation stable for checkpoint and result digests.
func JSON(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func HashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func HashString(s string) string { return HashBytes([]byte(s)) }

func Hash(v any) (string, error) {
	b, err := JSON(v)
	if err != nil {
		return "", err
	}
	return HashBytes(b), nil
}

// WriteFileAtomic writes data through a sibling temporary file and rename. A
// killed process can leave a temporary file behind, but never a truncated
// final result.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	ok := false
	defer func() {
		_ = f.Close()
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if err := f.Chmod(perm); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename %s: %w", path, err)
	}
	ok = true
	return nil
}

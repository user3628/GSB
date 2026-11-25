package atomicfs

import (
	"os"
	"path/filepath"
)

// WriteFile replaces path atomically with data. The caller never observes a
// partially written destination.
func WriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-"+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

func WriteJSON(path string, v any, pretty bool) error {
	var (
		data []byte
		err  error
	)
	if pretty {
		data, err = marshalPretty(v)
	} else {
		data, err = marshalCompact(v)
	}
	if err != nil {
		return err
	}
	return WriteFile(path, data, 0o644)
}

func marshalCompact(v any) ([]byte, error) {
	return canonical(v, false)
}

func marshalPretty(v any) ([]byte, error) {
	return canonical(v, true)
}

func canonical(v any, pretty bool) ([]byte, error) {
	// Kept local to avoid importing domain into low-level file utilities.
	return jsonMarshal(v, pretty)
}

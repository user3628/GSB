package main

import (
	"io"
	"os"
	"path/filepath"
	"sort"

	"ticketpipeline/internal/determ"
)

// digestTree fingerprints every regular file under dir in sorted path order.
// It is used to prove generated inputs are byte-stable across runs.
func digestTree(dir string) (string, error) {
	paths := []string{}
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(paths)
	type entry struct {
		Path string `json:"path"`
		Hash string `json:"hash"`
	}
	entries := make([]entry, 0, len(paths))
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			return "", err
		}
		rel, _ := filepath.Rel(dir, p)
		entries = append(entries, entry{Path: rel, Hash: determ.FingerprintBytes(raw)})
	}
	return determ.Fingerprint(entries)
}

func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	})
}

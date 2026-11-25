package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadResolvesDatabaseRelativeToConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "router.json")
	raw := `{"listen":"127.0.0.1:19999","database":"./data/router.db","seed":42}`
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != "127.0.0.1:19999" || cfg.Seed != 42 {
		t.Fatalf("unexpected config: %#v", cfg)
	}
	want := filepath.Join(dir, "data", "router.db")
	if cfg.Database != want {
		t.Fatalf("expected database %s, got %s", want, cfg.Database)
	}
}

func TestLoadRejectsUnknownAndEmptyFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "router.json")
	if err := os.WriteFile(path, []byte(`{"listen":"127.0.0.1:1","database":"x","unknown":true}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected unknown-field error, got %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"listen":"","database":"x"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected empty listen error")
	}
}

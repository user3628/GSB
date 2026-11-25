package generator_test

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"dialogalign/internal/domain"
	"dialogalign/internal/generator"
)

func TestGenerateIsDeterministicAndCoversDemoCases(t *testing.T) {
	rootA, rootB := t.TempDir(), t.TempDir()
	if err := generator.Generate(rootA); err != nil {
		t.Fatal(err)
	}
	if err := generator.Generate(rootB); err != nil {
		t.Fatal(err)
	}
	files := []string{"sessions.jsonl", "roles.json", "config.json", "active-map.json", "role_map_v1.json", "role_map_v2.json", "manifest.json"}
	for _, name := range files {
		a, err := os.ReadFile(filepath.Join(rootA, "data", name))
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(rootB, "data", name))
		if err != nil {
			t.Fatal(err)
		}
		if string(a) != string(b) {
			t.Fatalf("%s is not deterministic", name)
		}
	}
	turns, err := generator.LoadTurns(filepath.Join(rootA, "data", "sessions.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	sessions := map[string]struct{}{}
	languages := map[string]struct{}{}
	oldSlots, newSlots := 0, 0
	for _, turn := range turns {
		sessions[turn.SessionID] = struct{}{}
		languages[turn.Language] = struct{}{}
		oldSlots += len(turn.OldSlots)
		newSlots += len(turn.NewSlots)
	}
	if len(sessions) < 4 {
		t.Fatalf("expected at least four sessions, got %d", len(sessions))
	}
	if len(languages) < 3 {
		t.Fatalf("expected mixed languages, got %v", languages)
	}
	if oldSlots == 0 || newSlots == 0 {
		t.Fatalf("expected both generation annotations, old=%d new=%d", oldSlots, newSlots)
	}
	var manifest map[string]string
	blob, err := os.ReadFile(filepath.Join(rootA, "data", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := jsonUnmarshal(blob, &manifest); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(manifest))
	for key := range manifest {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) != 6 {
		t.Fatalf("manifest entries=%d", len(keys))
	}
	for _, key := range keys {
		data, err := os.ReadFile(filepath.Join(rootA, "data", key))
		if err != nil {
			t.Fatal(err)
		}
		if manifest[key] != domain.DigestBytes(data) {
			t.Fatalf("manifest digest mismatch for %s", key)
		}
	}
}

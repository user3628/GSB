package generator

import (
	"os"
	"testing"

	"ocrlab/internal/canonical"
	"ocrlab/internal/workspace"
)

func TestGeneratedCorpusIsDeterministic(t *testing.T) {
	leftPaths, err := workspace.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rightPaths, err := workspace.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	left, err := Generate(leftPaths)
	if err != nil {
		t.Fatal(err)
	}
	right, err := Generate(rightPaths)
	if err != nil {
		t.Fatal(err)
	}
	if left.ManifestSHA != right.ManifestSHA {
		t.Fatalf("manifest digest changed: %s vs %s", left.ManifestSHA, right.ManifestSHA)
	}
	leftManifest, err := os.ReadFile(left.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	rightManifest, err := os.ReadFile(right.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(leftManifest) != string(rightManifest) {
		t.Fatal("manifest bytes are not deterministic")
	}
	if len(left.Manifest.Images) != 60 {
		t.Fatalf("expected 60 images, got %d", len(left.Manifest.Images))
	}
	expected := map[string]int{"small": 4, "medium": 16, "large": 40}
	for _, group := range left.Manifest.Groups {
		if group.Observed.ShardCount != expected[group.Name] {
			t.Fatalf("unexpected shard count for %s: %+v", group.Name, group)
		}
		if group.Observed.TotalCharacters <= 0 {
			t.Fatalf("group %s has no explicit character observation", group.Name)
		}
	}
	for _, imageSpec := range left.Manifest.Images {
		leftPath, _ := leftPaths.ImagePath(imageSpec.File)
		rightPath, _ := rightPaths.ImagePath(imageSpec.File)
		leftBytes, err := os.ReadFile(leftPath)
		if err != nil {
			t.Fatal(err)
		}
		rightBytes, err := os.ReadFile(rightPath)
		if err != nil {
			t.Fatal(err)
		}
		if canonical.DigestBytes(leftBytes) != canonical.DigestBytes(rightBytes) {
			t.Fatalf("PNG bytes differ for %s", imageSpec.ImageID)
		}
	}
}

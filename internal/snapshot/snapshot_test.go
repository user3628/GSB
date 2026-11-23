package snapshot_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"depverify/internal/model"
	"depverify/internal/snapshot"
)

func sampleResult(marker string) model.ClosureResult {
	return model.ClosureResult{
		Roots:        []string{"acme/app"},
		Closure:      []string{"acme/app", "acme/core"},
		CodeSequence: []string{},
		Path:         model.PathPrecise,
		Threshold:    32,
		ClosureSize:  2,
		SampleMarker: marker,
	}
}

func TestSaveAndLoadContinuous(t *testing.T) {
	store, err := snapshot.Open(filepath.Join(t.TempDir(), "snap.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	r1, err := store.Save(ctx, sampleResult("a"), []string{"acme/app"})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := store.Save(ctx, sampleResult("b"), []string{"acme/app"})
	if err != nil {
		t.Fatal(err)
	}
	if r1.Version != 1 || r2.Version != 2 {
		t.Fatalf("versions want 1,2 got %d,%d", r1.Version, r2.Version)
	}
	loaded, report, err := store.Load(ctx, 2)
	if err != nil {
		t.Fatalf("continuous chain must load: %v", err)
	}
	if !report.Continuous || loaded.Version != 2 {
		t.Fatalf("load report wrong: %+v", report)
	}
	// 内容不同 -> 哈希不同，识别整体替换而非只看版本号。
	if snapshot.CompareContent(r1, r2) {
		t.Fatal("different content must produce different hashes")
	}
}

func TestGapFallback(t *testing.T) {
	store, err := snapshot.Open(filepath.Join(t.TempDir(), "snap.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		if _, err := store.Save(ctx, sampleResult(string(rune('a'+i))), []string{"acme/app"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.InjectGap(ctx, 2); err != nil {
		t.Fatal(err)
	}
	versions, _ := store.AllVersions(ctx)
	if len(versions) != 3 || versions[1] != 3 {
		t.Fatalf("gap injection wrong: %v", versions)
	}
	rec, report, err := store.Load(ctx, 4)
	var ce *snapshot.ChainError
	if !errors.As(err, &ce) {
		t.Fatalf("want ChainError, got %v", err)
	}
	if report.Code != model.ESnapshotVersion {
		t.Fatalf("want E_SNAPSHOT_VERSION, got %s", report.Code)
	}
	if report.FallbackVersion != 1 || report.LoadedVersion != 1 || rec.Version != 1 {
		t.Fatalf("fallback target wrong: %+v", report)
	}
}

func TestLatestAndEmpty(t *testing.T) {
	store, err := snapshot.Open(filepath.Join(t.TempDir(), "snap.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Latest(context.Background()); err == nil {
		t.Fatal("empty store must return error")
	}
	ctx := context.Background()
	rec, _ := store.Save(ctx, sampleResult("x"), nil)
	latest, err := store.Latest(ctx)
	if err != nil || latest.Version != rec.Version {
		t.Fatalf("latest wrong: %v %v", latest, err)
	}
}

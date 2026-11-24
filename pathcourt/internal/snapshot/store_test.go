package snapshot_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"pathcourt/internal/bizcode"
	"pathcourt/internal/resolver"
	"pathcourt/internal/snapshot"
)

func testRows(manifest string, codes ...bizcode.Code) []snapshot.Row {
	rows := make([]snapshot.Row, 0, len(codes))
	for i, code := range codes {
		rows = append(rows, snapshot.Row{
			ManifestID: manifest,
			PathIndex:  i,
			Input:      manifest + "-input",
			Result: &resolver.Result{
				ManifestID: manifest,
				PathIndex:  i,
				Input:      manifest + "-input",
				Code:       code,
				Steps:      []resolver.Step{},
				Hops:       []resolver.Hop{},
			},
		})
	}
	return rows
}

func openStore(t *testing.T) *snapshot.Store {
	t.Helper()
	db := filepath.Join(t.TempDir(), "snap.db")
	store, err := snapshot.Open(context.Background(), db)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestCommitContiguousVersions(t *testing.T) {
	store := openStore(t)
	ctx := context.Background()
	root := `C:\work\restricted-root`
	for v := int64(1); v <= 3; v++ {
		ver, err := store.Commit(ctx, root, "m", testRows("m", bizcode.OK))
		if err != nil {
			t.Fatalf("commit %d: %v", v, err)
		}
		if ver.Version != v {
			t.Fatalf("version = %d, want %d", ver.Version, v)
		}
	}
	versions, err := store.LoadChain(ctx)
	if err != nil {
		t.Fatalf("load chain: %v", err)
	}
	if len(versions) != 3 {
		t.Fatalf("loaded %d versions, want 3", len(versions))
	}
}

func TestVersionGapRollback(t *testing.T) {
	store := openStore(t)
	ctx := context.Background()
	root := `C:\work\restricted-root`
	if _, err := store.Commit(ctx, root, "m1", testRows("m1", bizcode.OK)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, root, "m2", testRows("m2", bizcode.E_PATH_ESCAPE)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitAtVersion(ctx, 7, root, "gap", testRows("gap", bizcode.OK)); err != nil {
		t.Fatal(err)
	}
	versions, err := store.LoadChain(ctx)
	if !errors.Is(err, snapshot.ErrVersionGap) {
		t.Fatalf("err = %v, want ErrVersionGap", err)
	}
	if len(versions) != 2 {
		t.Fatalf("contiguous prefix = %d, want 2 (rollback)", len(versions))
	}
	if versions[1].ManifestID != "m2" {
		t.Fatalf("last contiguous version = %s, want m2", versions[1].ManifestID)
	}
}

func TestCompareWholeReplacement(t *testing.T) {
	v1 := snapshot.Version{Version: 1, Rows: testRows("manifest-a", bizcode.OK), ContentHash: "a"}
	v2 := snapshot.Version{Version: 2, Rows: testRows("manifest-b", bizcode.OK), ContentHash: "b"}
	diff := snapshot.Compare(v1, v2)
	if diff.Kind != snapshot.ChangeWholeReplacement {
		t.Fatalf("kind = %s, want whole-replacement", diff.Kind)
	}

	inc := snapshot.Version{Version: 3, Rows: append(testRows("manifest-a", bizcode.E_LINK_CYCLE)), ContentHash: "c"}
	diff2 := snapshot.Compare(v1, inc)
	if diff2.Kind != snapshot.ChangeIncremental || diff2.Changed != 1 {
		t.Fatalf("incremental diff wrong: %+v", diff2)
	}

	identical := snapshot.Version{Version: 4, Rows: v1.Rows, ContentHash: "a"}
	if snapshot.Compare(v1, identical).Kind != snapshot.ChangeIdentical {
		t.Fatal("identical content across versions not detected")
	}
}

func TestRootIDStableAcrossLocations(t *testing.T) {
	a := snapshot.RootID(`/tmp/tmp.AAA/work/restricted-root`)
	b := snapshot.RootID(`/tmp/tmp.BBB/work/restricted-root`)
	if a != b {
		t.Fatalf("root id depends on location: %s vs %s", a, b)
	}
}

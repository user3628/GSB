package snapshot

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"svcworkbench/internal/domain"
)

func TestCommitAndLoadVersionChain(t *testing.T) {
	store := openTestStore(t)
	config := testConfig("v2")
	meta1, state1, err := store.Commit(context.Background(), config, []byte(`{"version":1}`), []Progress{{
		SourceID: "binary", Fingerprint: "fp1", Kind: "binary_policy", FormatVersion: "policy-v2", Accepted: 7,
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if meta1.Version != 1 || state1.ActiveVersion != 1 || len(state1.Versions) != 1 {
		t.Fatalf("unexpected first snapshot: %+v %+v", meta1, state1)
	}
	meta2, _, err := store.Commit(context.Background(), config, []byte(`{"version":2}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if meta2.Version != 2 || meta2.ParentHash != meta1.ContentHash {
		t.Fatalf("unexpected second snapshot: %+v", meta2)
	}
	loaded, chain, err := store.Load(context.Background(), 2, config)
	if err != nil {
		t.Fatal(err)
	}
	if string(loaded.Payload) != `{"version":2}` || chain.ActiveVersion != 2 || chain.HeadVersion != 2 {
		t.Fatalf("unexpected loaded state: %+v %+v", loaded, chain)
	}
	progress, err := store.Progress(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(progress) != 1 || progress[0].SnapshotVersion != 1 {
		t.Fatalf("unexpected progress: %+v", progress)
	}
}

func TestIncompatibleConfigRejectsWholeSnapshot(t *testing.T) {
	store := openTestStore(t)
	config := testConfig("v2")
	if _, _, err := store.Commit(context.Background(), config, []byte(`{"ok":true}`), nil, nil); err != nil {
		t.Fatal(err)
	}
	result, chain, err := store.Load(context.Background(), 0, testConfig("v1"))
	if err == nil || result != nil {
		t.Fatalf("expected incompatibility rejection")
	}
	var business *SnapshotError
	if !errors.As(err, &business) || business.Code != domain.CodeSnapshotVersion {
		t.Fatalf("error = %v", err)
	}
	if chain.ConfigCompatible || chain.ActiveVersion != 0 {
		t.Fatalf("unexpected chain state: %+v", chain)
	}
}

func TestLegacySchemaIsRejectedWithBusinessCode(t *testing.T) {
	store := openTestStore(t)
	config := testConfig("v2")
	if _, _, err := store.Commit(context.Background(), config, []byte(`{"ok":true}`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE snapshot_meta SET schema_version='service-adjudication-snapshot-v0' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	result, chain, err := store.Load(context.Background(), 0, config)
	if result != nil {
		t.Fatalf("legacy snapshot should be rejected")
	}
	var business *SnapshotError
	if !errors.As(err, &business) || business.Code != domain.CodeSnapshotVersion {
		t.Fatalf("error = %v", err)
	}
	if chain.ConfigCompatible {
		t.Fatalf("legacy schema should be incompatible: %+v", chain)
	}
}

func TestLegacySnapshotTableSchemaDoesNotBreakOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE snapshot_meta (id INTEGER PRIMARY KEY, legacy_version INTEGER NOT NULL); INSERT INTO snapshot_meta (id, legacy_version) VALUES (1, 0)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := Open(path)
	if err != nil {
		t.Fatalf("open should tolerate legacy schema for a business rejection: %v", err)
	}
	defer store.Close()
	result, chain, err := store.Load(context.Background(), 0, testConfig("v2"))
	if result != nil {
		t.Fatalf("legacy schema should not load")
	}
	var business *SnapshotError
	if !errors.As(err, &business) || business.Code != domain.CodeSnapshotVersion {
		t.Fatalf("error = %v", err)
	}
	if chain.ConfigCompatible {
		t.Fatalf("legacy schema chain should be incompatible: %+v", chain)
	}
}

func TestSnapshotGapFallsBackToContiguousVersion(t *testing.T) {
	store := openTestStore(t)
	config := testConfig("v2")
	if _, _, err := store.Commit(context.Background(), config, []byte(`{"version":1}`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit(context.Background(), config, []byte(`{"version":2}`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.InjectSnapshotGap(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, chain, err := store.Load(context.Background(), 0, config)
	var business *SnapshotError
	if !errors.As(err, &business) || business.Code != domain.CodeSnapshotVersion {
		t.Fatalf("error = %v", err)
	}
	if chain.FallbackVersion != 2 || chain.ActiveVersion != 2 {
		t.Fatalf("unexpected fallback: %+v", chain)
	}
	if len(chain.Versions) != 3 || chain.Versions[0] != 1 || chain.Versions[1] != 2 || chain.Versions[2] != 4 {
		t.Fatalf("unexpected chain versions: %#v", chain.Versions)
	}
}

func TestPayloadReplacementIsDetected(t *testing.T) {
	store := openTestStore(t)
	config := testConfig("v2")
	if _, _, err := store.Commit(context.Background(), config, []byte(`{"version":1}`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit(context.Background(), config, []byte(`{"version":2}`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE snapshots SET payload=? WHERE version=2`, []byte(`{"version":2,"replaced":true}`)); err != nil {
		t.Fatal(err)
	}
	_, chain, err := store.Load(context.Background(), 0, config)
	var business *SnapshotError
	if !errors.As(err, &business) || business.Code != domain.CodeSnapshotVersion {
		t.Fatalf("error = %v", err)
	}
	if chain.FallbackVersion != 1 || chain.ActiveVersion != 1 {
		t.Fatalf("unexpected fallback: %+v", chain)
	}
}

func TestWholeSnapshotReplacementIsDetectedByChainTip(t *testing.T) {
	store := openTestStore(t)
	config := testConfig("v2")
	if _, _, err := store.Commit(context.Background(), config, []byte(`{"version":1}`), nil, nil); err != nil {
		t.Fatal(err)
	}
	second, _, err := store.Commit(context.Background(), config, []byte(`{"version":2}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	replacement := []byte(`{"version":2,"replacement":true}`)
	replacementHash := computeContentHash(second.Version, second.ParentHash, config.SchemaVersion, second.Marker, second.TargetFormat, second.ConfigHash, replacement)
	if _, err := store.db.Exec(`UPDATE snapshots SET payload=?, content_hash=? WHERE version=2`, replacement, replacementHash); err != nil {
		t.Fatal(err)
	}
	_, chain, err := store.Load(context.Background(), 0, config)
	var business *SnapshotError
	if !errors.As(err, &business) || business.Code != domain.CodeSnapshotVersion {
		t.Fatalf("error = %v", err)
	}
	if chain.FallbackVersion != 1 || chain.ActiveVersion != 1 {
		t.Fatalf("unexpected fallback: %+v", chain)
	}
	if chain.Detail != "snapshot table tip does not match committed chain metadata" {
		t.Fatalf("unexpected detail: %s", chain.Detail)
	}
}

func TestCommitRollbackDropsSnapshotAndProgress(t *testing.T) {
	store := openTestStore(t)
	config := testConfig("v2")
	if _, _, err := store.Commit(context.Background(), config, []byte(`{"version":1}`), []Progress{{
		SourceID: "binary", Fingerprint: "fp1", Kind: "binary_policy", FormatVersion: "policy-v2", Accepted: 7,
	}}, nil); err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatalf("expected simulated crash panic")
			}
		}()
		_, _, _ = store.Commit(context.Background(), config, []byte(`{"version":2}`), []Progress{{
			SourceID: "json", Fingerprint: "fp2", Kind: "json_manifest", FormatVersion: "jsonl-v1", Accepted: 7,
		}}, func(point string) {
			panic("simulated crash at " + point)
		})
	}()
	loaded, chain, err := store.Load(context.Background(), 0, config)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Meta.Version != 1 || chain.HeadVersion != 1 {
		t.Fatalf("uncommitted snapshot survived: %+v %+v", loaded.Meta, chain)
	}
	progress, err := store.Progress(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(progress) != 1 || progress[0].SourceID != "binary" || progress[0].SnapshotVersion != 1 {
		t.Fatalf("uncommitted progress survived: %+v", progress)
	}
	meta, _, err := store.Commit(context.Background(), config, []byte(`{"version":2}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Version != 2 {
		t.Fatalf("version after recovery = %d", meta.Version)
	}
}

func TestCompareDetectsWholeReplacement(t *testing.T) {
	left := SnapshotMeta{Version: 2, ChainID: "chain-a", ContentHash: "hash-a", ConfigHash: "config"}
	replaced := SnapshotMeta{Version: 2, ChainID: "chain-a", ContentHash: "hash-b", ConfigHash: "config"}
	comparison := Compare(left, replaced)
	if !comparison.SameVersion || !comparison.SameChain || !comparison.WholeReplacement {
		t.Fatalf("comparison = %+v", comparison)
	}
	next := SnapshotMeta{Version: 3, ChainID: "chain-a", ContentHash: "hash-c", ConfigHash: "config"}
	if Compare(left, next).WholeReplacement {
		t.Fatalf("normal append was treated as replacement")
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "workbench.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func testConfig(target string) Config {
	return Config{
		SchemaVersion: domain.SnapshotSchemaVersion,
		Marker:        domain.ReportMarker,
		TargetFormat:  target,
		SourceVersions: map[string]string{
			"binary_policy": "policy-" + target,
			"json_manifest": "jsonl-v1",
			"legacy_text":   "legacy-text-v1",
		},
		Priorities: []string{"binary_policy", "json_manifest", "legacy_text"},
	}
}

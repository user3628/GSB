package snapshot

import (
	"context"
	"path/filepath"
	"testing"

	"baselineworkbench/internal/codes"
	"baselineworkbench/internal/engine"
	"baselineworkbench/internal/ledger"
	"baselineworkbench/internal/policy"
)

func testItem(id, value string) policy.Item {
	return policy.Item{
		ID:               id,
		Unit:             "second",
		RawUnit:          "second",
		RawValue:         value,
		WeightRaw:        "1.0",
		WeightMicros:     1_000_000,
		Family:           "duration",
		Normalized:       100,
		NormalizedScaled: 100_000_000,
		Source:           "baseline",
	}
}

func TestSnapshotVersionsAndWholeReplacement(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "snapshots.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	payload1, _ := EncodeContent("packages/p", "SAMPLE", []policy.Item{testItem("a", "1")})
	payload2, _ := EncodeContent("packages/p", "SAMPLE", []policy.Item{testItem("a", "2"), testItem("b", "1")})
	payload3, _ := EncodeContent("packages/p", "SAMPLE", []policy.Item{testItem("c", "1"), testItem("d", "1")})
	v1, business := store.Append(ctx, "packages/p", payload1, "SAMPLE", AppendOptions{})
	if business != nil || v1.Version != 1 {
		t.Fatalf("append v1: %+v %v", v1, business)
	}
	v2, business := store.Append(ctx, "packages/p", payload2, "SAMPLE", AppendOptions{})
	if business != nil || v2.Version != 2 || !v2.ContentChanged || v2.WholeReplacement {
		t.Fatalf("append v2: %+v %v", v2, business)
	}
	v3, business := store.Append(ctx, "packages/p", payload3, "SAMPLE", AppendOptions{})
	if business != nil || v3.Version != 3 || !v3.ContentChanged || !v3.WholeReplacement {
		t.Fatalf("append v3: %+v %v", v3, business)
	}
	chain, err := store.LoadChain(ctx, "packages/p")
	if err != nil {
		t.Fatal(err)
	}
	if chain.ErrorCode != codes.None || len(chain.Snapshots) != 3 || chain.Active == nil || chain.Active.Version != 3 {
		t.Fatalf("unexpected chain: %+v", chain)
	}
	content, err := DecodeContent(chain.Snapshots[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(content.Items) != 1 || content.Items[0].ID != "a" || content.Items[0].RawValue != "1" {
		t.Fatalf("version 1 payload was not retained: %+v", content)
	}
}

func TestForcedVersionGapDoesNotAdvance(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "snapshots.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	payload, _ := EncodeContent("packages/p", "SAMPLE", []policy.Item{testItem("a", "1")})
	if _, business := store.Append(ctx, "packages/p", payload, "SAMPLE", AppendOptions{}); business != nil {
		t.Fatal(business)
	}
	if _, business := store.Append(ctx, "packages/p", payload, "SAMPLE", AppendOptions{ForcedVersion: 3}); business == nil || business.Code != codes.SnapshotVersion {
		t.Fatalf("expected E_SNAPSHOT_VERSION, got %v", business)
	}
	version, err := store.Version(ctx, "packages/p")
	if err != nil || version != 1 {
		t.Fatalf("version advanced to %d: %v", version, err)
	}
	chain, err := store.LoadChain(ctx, "packages/p")
	if err != nil {
		t.Fatal(err)
	}
	if chain.ErrorCode != codes.None || chain.Active == nil || chain.Active.Version != 1 {
		t.Fatalf("chain changed after rejected append: %+v", chain)
	}
}

func TestLoadDetectsGapAndRecoversDiagnostic(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "snapshots.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	result := engine.Result{
		PackagePath:      "packages/p",
		Complete:         true,
		Items:            []policy.Item{testItem("a", "1")},
		ScoreMicros:      42,
		ErrorUpperMicros: 7,
		ScoreInterval:    engine.Interval{Lower: 35, Upper: 49, Unit: "score_1e-6"},
		Comparisons:      []engine.Comparison{},
		Codes:            []codes.Code{},
		Ledger:           ledger.Ledger{Entries: []ledger.Entry{}, Terminal: ledger.TerminalCommitted},
	}
	payload1, _ := EncodeResult("packages/p", "SAMPLE", result)
	v1, business := store.Append(ctx, "packages/p", payload1, "SAMPLE", AppendOptions{})
	if business != nil {
		t.Fatal(business)
	}
	payloadGap, _ := EncodeContent("packages/p", "SAMPLE", []policy.Item{testItem("z", "9")})
	_, err = store.db.ExecContext(ctx, `
INSERT INTO snapshots (package_path, version, parent_version, content_digest, parent_digest,
                       content_changed, whole_replacement, sample_marker, payload)
VALUES (?, 3, 2, ?, ?, 1, 0, ?, ?)`,
		"packages/p", digestBytes(payloadGap), "missing-parent", "SAMPLE", payloadGap)
	if err != nil {
		t.Fatal(err)
	}
	chain, err := store.LoadChain(ctx, "packages/p")
	if err != nil {
		t.Fatal(err)
	}
	if chain.ErrorCode != codes.SnapshotVersion || chain.GapAt != 3 {
		t.Fatalf("expected version gap, got %+v", chain)
	}
	if chain.Active == nil || chain.Active.Version != v1.Version {
		t.Fatalf("did not recover latest contiguous snapshot: %+v", chain)
	}
	content, err := DecodeContent(chain.Active.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if content.Diagnostic == nil || content.Diagnostic.ScoreMicros != 42 || content.Diagnostic.ErrorUpperMicros != 7 {
		t.Fatalf("recovered diagnostic mismatch: %+v", content.Diagnostic)
	}
}

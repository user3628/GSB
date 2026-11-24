package snapshot_test

import (
	"context"
	"path/filepath"
	"testing"

	"supplyaudit/internal/model"
	"supplyaudit/internal/snapshot"
)

func TestSnapshotChainCompatibilityAndReplacement(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "audit.db")
	store, err := snapshot.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	priority := []string{"binary", "jsonl", "textblock"}
	accepted := []string{"binary/v1", "binary/v2", "json/v1", "textblock/v1"}
	first, err := store.Append(ctx, []byte("payload-one\n"), []string{"binary/v1", "json/v1"}, priority, accepted)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Append(ctx, []byte("payload-two\n"), []string{"binary/v2"}, priority, accepted)
	if err != nil {
		t.Fatal(err)
	}
	if first.Version != 1 || second.Version != 2 || second.ParentVersion != 1 {
		t.Fatalf("unexpected chain versions: %#v %#v", first, second)
	}
	compat := snapshot.Compatibility{SchemaVersion: snapshot.SchemaVersion, AcceptedFormats: accepted, Priority: priority}
	loaded, err := store.Load(ctx, 1, compat)
	if err != nil {
		t.Fatal(err)
	}
	if string(loaded.Payload) != "payload-one\n" {
		t.Fatalf("unexpected payload %q", loaded.Payload)
	}

	_, err = store.Load(ctx, 2, snapshot.Compatibility{SchemaVersion: snapshot.SchemaVersion, AcceptedFormats: []string{"binary/v1", "json/v1", "textblock/v1"}, Priority: priority})
	if model.CodeOf(err) != model.CodeSnapshotVersion {
		t.Fatalf("required format incompatibility should be E_SNAPSHOT_VERSION, got %v", err)
	}
	_, err = store.Load(ctx, 1, snapshot.Compatibility{SchemaVersion: snapshot.SchemaVersion, AcceptedFormats: accepted, Priority: []string{"jsonl", "binary", "textblock"}})
	if model.CodeOf(err) != model.CodeSnapshotVersion {
		t.Fatalf("priority incompatibility should be E_SNAPSHOT_VERSION, got %v", err)
	}

	if err := store.ReplacePayloadFixture(ctx, 1, []byte("tampered\n")); err != nil {
		t.Fatal(err)
	}
	_, err = store.Load(ctx, 1, compat)
	if model.CodeOf(err) != model.CodeSnapshotVersion {
		t.Fatalf("whole payload replacement should be E_SNAPSHOT_VERSION, got %v", err)
	}
}

func TestSnapshotGapRejected(t *testing.T) {
	ctx := context.Background()
	store, err := snapshot.Open(filepath.Join(t.TempDir(), "gap.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	priority := []string{"binary", "jsonl", "textblock"}
	accepted := []string{"binary/v1", "binary/v2", "json/v1", "textblock/v1"}
	if _, err := store.AppendGapFixture(ctx, []byte("gap\n"), []string{"json/v1"}, priority, accepted); err != nil {
		t.Fatal(err)
	}
	_, err = store.Load(ctx, 3, snapshot.Compatibility{SchemaVersion: snapshot.SchemaVersion, AcceptedFormats: accepted, Priority: priority})
	if model.CodeOf(err) != model.CodeSnapshotVersion {
		t.Fatalf("gap should be E_SNAPSHOT_VERSION, got %v", err)
	}
	if _, err := store.Append(ctx, []byte("next\n"), []string{"json/v1"}, priority, accepted); model.CodeOf(err) != model.CodeSnapshotVersion {
		t.Fatalf("append to corrupt chain should be rejected, got %v", err)
	}
}

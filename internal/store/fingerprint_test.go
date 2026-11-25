package store

import (
	"context"
	"path/filepath"
	"testing"

	"flashattention/internal/errs"
	"flashattention/internal/versioning"
)

func TestFingerprintChangeRequiresRegisteredInvalidation(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "fp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cp := checkpoint("fp", 1, 4)
	if _, err := st.Publish(ctx, cp); err != nil {
		t.Fatal(err)
	}

	next := checkpoint("fp", 2, 8)
	next.VersionFP = "fp-v2"
	_, err = st.Publish(ctx, next)
	if errs.Code(err) != errs.EStaleCheckpoint {
		t.Fatalf("code=%s err=%v", errs.Code(err), err)
	}
	if errs.As(err).Fields["kind"] != "unregistered_fingerprint_change" {
		t.Fatalf("unexpected kind: %+v", errs.As(err).Fields)
	}

	err = st.ApplyInvalidation(ctx, versioning.Range{
		ShardID: "fp", Trigger: versioning.TriggerStrategy,
		OldFingerprint: "fp-v1", NewFingerprint: "fp-v2",
		FirstRowBlock: 0, LastRowBlock: 0, ColStart: 0, ColEnd: 8,
		AllRowBlocks: true, AllColBlocks: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Publish(ctx, next); err != nil {
		t.Fatalf("registered invalidation should allow advance: %v", err)
	}
}

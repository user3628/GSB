package store

import (
	"path/filepath"
	"testing"

	"ticketpipeline/internal/model"
)

func openStore(t *testing.T) *Store {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "out")
	st, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func cp(seq int64, stage, ticket, fp string) Checkpoint {
	return Checkpoint{
		StageSeq: seq, Stage: stage, TicketID: ticket, PolicyVersion: 1,
		InputFP: "in", ConfigFP: "cfg", ArtifactFP: fp, Summary: "s",
		Status: StatusOK,
	}
}

func TestPublishContiguousAndDuplicate(t *testing.T) {
	st := openStore(t)
	if o := st.Publish(cp(1, model.StageNormalize, "T1", "a")); o.Error != nil || o.Kind != "accepted" {
		t.Fatalf("first publish: %+v", o)
	}
	// Identical republish is an idempotent duplicate, not an error.
	if o := st.Publish(cp(1, model.StageNormalize, "T1", "a")); o.Kind != "duplicate" {
		t.Fatalf("want duplicate, got %+v", o)
	}
	if o := st.Publish(cp(2, model.StageNormalize, "T2", "b")); o.Error != nil {
		t.Fatalf("second publish: %v", o.Error)
	}
	// Out-of-order sequence must be rejected.
	if o := st.Publish(cp(9, model.StageNormalize, "T3", "c")); o.Error == nil || o.Error.Code != model.CodeStaleCheckpoint {
		t.Fatalf("want E_STALE_CHECKPOINT, got %+v", o)
	}
	if err := st.AssertContiguous(model.StageNormalize); err != nil {
		t.Fatalf("contiguity: %v", err)
	}
}

func TestReplacementAndRollbackDetection(t *testing.T) {
	st := openStore(t)
	st.Publish(cp(1, model.StageIntent, "T1", "a"))
	st.Publish(cp(2, model.StageIntent, "T2", "b"))
	// Same sequence but changed fingerprint => wholesale replacement.
	rep := cp(2, model.StageIntent, "T2", "changed")
	if o := st.Publish(rep); o.Error == nil || o.Error.Code != model.CodeStaleCheckpoint {
		t.Fatalf("replacement must be rejected: %+v", o)
	}
	// Lower sequence => rollback.
	back := cp(1, model.StageIntent, "T2", "b")
	if o := st.Publish(back); o.Error == nil || o.Error.Code != model.CodeStaleCheckpoint {
		t.Fatalf("rollback must be rejected: %+v", o)
	}
}

func TestDeleteStageResetsSequence(t *testing.T) {
	st := openStore(t)
	st.Publish(cp(1, model.StageNormalize, "T1", "a"))
	st.Publish(cp(2, model.StageNormalize, "T2", "b"))
	st.Publish(cp(1, model.StageIntent, "T1", "c"))
	if err := st.DeleteStage(model.StageIntent); err != nil {
		t.Fatal(err)
	}
	if o := st.Publish(cp(1, model.StageIntent, "T1", "c2")); o.Error != nil {
		t.Fatalf("rebuilt intent must restart at seq 1: %v", o.Error)
	}
	// Normalize is untouched and remains contiguous.
	if err := st.AssertContiguous(model.StageNormalize); err != nil {
		t.Fatal(err)
	}
}

func TestOneEffectiveRecordPerTicket(t *testing.T) {
	st := openStore(t)
	st.Publish(cp(1, model.StagePriority, "T1", "a"))
	st.Publish(cp(2, model.StagePriority, "T2", "b"))
	rows, err := st.ListCheckpoints(model.StagePriority)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, r := range rows {
		if seen[r.TicketID] {
			t.Fatal("duplicate effective record")
		}
		seen[r.TicketID] = true
	}
}

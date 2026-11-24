package reconcile_test

import (
	"testing"

	"maskrevision/internal/model"
	"maskrevision/internal/reconcile"
)

func TestClassifyAppendReplacementRollbackForkAndInputChange(t *testing.T) {
	old := testLog(
		model.Revision{Revision: 1, ParentRevision: 0, Action: model.ActionRepaint, Region: model.Region{X: 0, Y: 0, W: 2, H: 2}, CategoryID: 2},
		model.Revision{Revision: 2, ParentRevision: 1, Action: model.ActionIgnore, Region: model.Region{X: 2, Y: 0, W: 2, H: 2}},
	)
	appendLog := testLog(append(append([]model.Revision(nil), old.Revisions...), model.Revision{
		Revision: 3, ParentRevision: 2, Action: model.ActionReassign, Region: model.Region{X: 0, Y: 0, W: 2, H: 1}, TargetSegmentID: 2,
	})...)
	if got := reconcile.Classify(old, appendLog, "in", "in"); got.Kind != reconcile.KindAppend || got.EarliestAffectedRevision != 3 {
		t.Fatalf("append: %+v", got)
	}
	edited := testLog(append([]model.Revision(nil), old.Revisions...)...)
	edited.Revisions[1].Region = model.Region{X: 2, Y: 2, W: 2, H: 2}
	if got := reconcile.Classify(old, edited, "in", "in"); got.Kind != reconcile.KindReplacement || got.EarliestAffectedRevision != 2 {
		t.Fatalf("replacement: %+v", got)
	}
	rollback := testLog(old.Revisions[:1]...)
	if got := reconcile.Classify(old, rollback, "in", "in"); got.Kind != reconcile.KindRollback || got.EarliestAffectedRevision != 2 {
		t.Fatalf("rollback: %+v", got)
	}
	fork := testLog(append(append([]model.Revision(nil), old.Revisions...), model.Revision{
		Revision: 3, ParentRevision: 1, Action: model.ActionRepaint, Region: model.Region{X: 1, Y: 1, W: 2, H: 2}, CategoryID: 3,
	})...)
	fork.ForkOf = old.LogID
	if got := reconcile.Classify(old, fork, "in", "in"); got.Kind != reconcile.KindFork || got.EarliestAffectedRevision != 3 {
		t.Fatalf("fork: %+v", got)
	}
	if got := reconcile.Classify(old, old, "old-input", "new-input"); got.Kind != reconcile.KindReplacement || got.EarliestAffectedRevision != 1 {
		t.Fatalf("input change: %+v", got)
	}
	if got := reconcile.Classify(old, old, "in", "in"); got.Kind != reconcile.KindUnchanged {
		t.Fatalf("unchanged: %+v", got)
	}
}

func TestClassifyFingerprintsFromCheckpoints(t *testing.T) {
	log := testLog(
		model.Revision{Revision: 1, ParentRevision: 0, Action: model.ActionRepaint, Region: model.Region{X: 0, Y: 0, W: 2, H: 2}, CategoryID: 2},
		model.Revision{Revision: 2, ParentRevision: 1, Action: model.ActionIgnore, Region: model.Region{X: 2, Y: 0, W: 2, H: 2}},
		model.Revision{Revision: 3, ParentRevision: 2, Action: model.ActionUndo},
	)
	history := []reconcile.RevisionHistoryEntry{
		{Revision: 1, ParentRevision: 0, RevisionFP: model.RevisionDigest(log.Revisions[0])},
		{Revision: 2, ParentRevision: 1, RevisionFP: model.RevisionDigest(log.Revisions[1])},
	}
	if got := reconcile.ClassifyFingerprints(history, "small", "in", log, "in"); got.Kind != reconcile.KindAppend || got.ReusableRevision != 2 {
		t.Fatalf("checkpoint append: %+v", got)
	}
}

func testLog(revisions ...model.Revision) model.Log {
	return model.Log{SchemaVersion: 2, LogID: "log", ShardID: "small", Width: 4, Height: 4, Revisions: revisions}
}

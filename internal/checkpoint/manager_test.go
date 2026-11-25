package checkpoint

import (
	"context"
	"testing"

	"regworkbench/internal/workerr"
)

func TestSequenceStageAndFoldProtection(t *testing.T) {
	ctx := context.Background()
	m, err := OpenManager(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	publish := func(seq int64, stage string, fold int) error {
		_, err := m.Publish(ctx, Record{Seq: seq, Stage: stage, Fold: fold, InputFingerprint: "in", ConfigVersion: "cfg"})
		return err
	}
	if err := publish(1, "final", -1); err == nil {
		t.Fatal("expected invalid stage jump")
	} else if we, ok := workerr.As(err); !ok || we.Code != workerr.EInvalidStage {
		t.Fatalf("err=%v want E_INVALID_STAGE", err)
	}
	if err := publish(1, "preprocess", -1); err != nil {
		t.Fatal(err)
	}
	if err := publish(2, "split", -1); err != nil {
		t.Fatal(err)
	}
	if err := publish(3, "fold", 1); err == nil {
		t.Fatal("expected cross-fold stale error")
	} else if we, ok := workerr.As(err); !ok || we.Code != workerr.EStaleCheckpoint {
		t.Fatalf("err=%v want E_STALE_CHECKPOINT", err)
	}
	if err := publish(3, "fold", 0); err != nil {
		t.Fatal(err)
	}
	if err := publish(3, "fold", 0); err == nil {
		t.Fatal("duplicate sequence must be stale")
	} else if we, ok := workerr.As(err); !ok || we.Code != workerr.EStaleCheckpoint || we.ExpectedSeq != 4 {
		t.Fatalf("duplicate err=%v", err)
	}
	if err := publish(4, "fold", 1); err != nil {
		t.Fatal(err)
	}
	if err := publish(5, "fold", 2); err != nil {
		t.Fatal(err)
	}
	if err := publish(6, "selection", -1); err != nil {
		t.Fatal(err)
	}
	if err := publish(7, "final", -1); err != nil {
		t.Fatal(err)
	}
	if err := publish(7, "final", -1); err == nil {
		t.Fatal("late publish must be stale")
	}
}

func TestMissingPredecessorForSelection(t *testing.T) {
	ctx := context.Background()
	m, err := OpenManager(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for _, r := range []Record{
		{Seq: 1, Stage: "preprocess", Fold: -1},
		{Seq: 2, Stage: "split", Fold: -1},
		{Seq: 3, Stage: "fold", Fold: 0},
	} {
		if _, err := m.Publish(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	_, err = m.Publish(ctx, Record{Seq: 4, Stage: "selection", Fold: -1})
	if we, ok := workerr.As(err); !ok || we.Code != workerr.EInvalidStage {
		t.Fatalf("err=%v want E_INVALID_STAGE for missing folds", err)
	}
}

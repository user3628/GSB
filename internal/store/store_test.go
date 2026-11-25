package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"layoutread/internal/model"
)

type testArtifact struct {
	Value string `json:"value"`
}

func checkpoint(docID, stage string, seq int64, value string) Checkpoint {
	data, _ := json.Marshal(testArtifact{Value: value})
	fingerprint := model.DigestBytes(data)
	return Checkpoint{
		DocID:               docID,
		Stage:               stage,
		Seq:                 seq,
		InputFingerprint:    model.DigestBytes([]byte(docID + stage + value)),
		StrategyVersion:     3,
		ArtifactSummary:     fingerprint,
		ArtifactFingerprint: fingerprint,
		ArtifactJSON:        string(data),
		ContentFingerprint:  "content-" + docID,
	}
}

func openStore(t *testing.T, path string) *Store {
	t.Helper()
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func errorCode(t *testing.T, err error) string {
	t.Helper()
	var modelErr *model.Error
	if !errors.As(err, &modelErr) {
		t.Fatalf("not a structured model error: %v", err)
	}
	return modelErr.Code
}

func TestTransactionalPublishAndStaleSequence(t *testing.T) {
	st := openStore(t, filepath.Join(t.TempDir(), "checkpoints.sqlite"))
	ctx := context.Background()

	if _, err := st.Publish(ctx, checkpoint("doc-a", model.StageNormalize, 2, "gap")); err == nil || errorCode(t, err) != model.CodeStaleCheckpoint {
		t.Fatalf("expected sequence gap rejection, got %v", err)
	}
	if _, err := st.Publish(ctx, checkpoint("doc-b", model.StageNormalize, 2, "cross-doc")); err == nil || errorCode(t, err) != model.CodeStaleCheckpoint {
		t.Fatalf("expected cross-document sequence rejection, got %v", err)
	}
	if _, err := st.Publish(ctx, checkpoint("doc-a", model.StageNormalize, 1, "first")); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if _, err := st.Publish(ctx, checkpoint("doc-a", model.StageNormalize, 1, "first")); err == nil || errorCode(t, err) != model.CodeStaleCheckpoint {
		t.Fatalf("expected duplicate rejection, got %v", err)
	}
	if _, err := st.Publish(ctx, checkpoint("doc-a", model.StageColumns, 2, "second")); err != nil {
		t.Fatalf("second publish: %v", err)
	}

	st.failpoint = func(stage string) error {
		if stage == model.StageOrder {
			return errors.New("injected before commit")
		}
		return nil
	}
	if _, err := st.Publish(ctx, checkpoint("doc-a", model.StageOrder, 3, "third")); err == nil {
		t.Fatal("expected injected transaction failure")
	}
	if _, found, err := st.GetCheckpoint(ctx, "doc-a", model.StageOrder); err != nil || found {
		t.Fatalf("failed transaction left a half-written checkpoint: found=%v err=%v", found, err)
	}
	documents, err := st.ListDocuments(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(documents) != 1 || documents[0].Seq != 2 {
		t.Fatalf("document head advanced despite rollback: %+v", documents)
	}

	st.failpoint = nil
	if _, err := st.Publish(ctx, checkpoint("doc-a", model.StageOrder, 3, "third")); err != nil {
		t.Fatalf("publish after rollback: %v", err)
	}
	checkpoints, err := st.ListCheckpoints(ctx, "doc-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoints) != 3 {
		t.Fatalf("expected one effective record per stage, got %d", len(checkpoints))
	}
}

func TestArtifactSummaryMismatchRejected(t *testing.T) {
	st := openStore(t, filepath.Join(t.TempDir(), "artifact.sqlite"))
	ctx := context.Background()
	badFingerprint := checkpoint("doc-a", model.StageNormalize, 1, "one")
	badFingerprint.ArtifactFingerprint = "not-the-digest"
	if _, err := st.Publish(ctx, badFingerprint); err == nil || errorCode(t, err) != model.CodeCheckpointCorrupt {
		t.Fatalf("expected artifact fingerprint rejection, got %v", err)
	}
	badSummary := checkpoint("doc-a", model.StageNormalize, 1, "one")
	badSummary.ArtifactSummary = "wrong-summary"
	if _, err := st.Publish(ctx, badSummary); err == nil || errorCode(t, err) != model.CodeCheckpointCorrupt {
		t.Fatalf("expected artifact summary rejection, got %v", err)
	}
}

func TestConcurrentPublishersAllowOnlyOneContiguousAdvance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.sqlite")
	st1 := openStore(t, path)
	st2, err := Open(path)
	if err != nil {
		t.Fatalf("open second store: %v", err)
	}
	defer st2.Close()
	ctx := context.Background()
	seq1, err := st1.NextSequence(ctx, "doc-a")
	if err != nil {
		t.Fatal(err)
	}
	seq2, err := st2.NextSequence(ctx, "doc-a")
	if err != nil {
		t.Fatal(err)
	}
	if seq1 != 1 || seq2 != 1 {
		t.Fatalf("unexpected next sequences: %d %d", seq1, seq2)
	}
	results := make(chan error, 2)
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		_, err := st1.Publish(ctx, checkpoint("doc-a", model.StageNormalize, seq1, "publisher-one"))
		results <- err
	}()
	go func() {
		defer wait.Done()
		_, err := st2.Publish(ctx, checkpoint("doc-a", model.StageNormalize, seq2, "publisher-two"))
		results <- err
	}()
	wait.Wait()
	close(results)
	successes := 0
	stale := 0
	for err := range results {
		if err == nil {
			successes++
			continue
		}
		if errorCode(t, err) == model.CodeStaleCheckpoint {
			stale++
		}
	}
	if successes != 1 || stale != 1 {
		t.Fatalf("expected one success and one stale rejection, got success=%d stale=%d", successes, stale)
	}
}

func TestSnapshotRollbackDetectedOnOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rollback.sqlite")
	ctx := context.Background()
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Publish(ctx, checkpoint("doc-a", model.StageNormalize, 1, "first")); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	backup, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	st, err = Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := st.Publish(ctx, checkpoint("doc-a", model.StageColumns, 2, "second")); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(path, backup, 0o600); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")
	_, err = Open(path)
	if err == nil {
		t.Fatal("expected rollback snapshot rejection")
	}
	if code := errorCode(t, err); code != model.CodeCheckpointRollback {
		t.Fatalf("expected E_CHECKPOINT_ROLLBACK, got %s (%v)", code, err)
	}
}

func TestSnapshotReplacementDetectedOnOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replacement.sqlite")
	ctx := context.Background()
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Publish(ctx, checkpoint("doc-a", model.StageNormalize, 1, "first")); err != nil {
		t.Fatal(err)
	}
	_, err = st.db.ExecContext(ctx, `UPDATE stage_checkpoints SET artifact_summary='tampered', artifact_fingerprint='tampered' WHERE doc_id='doc-a'`)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = Open(path)
	if err == nil {
		t.Fatal("expected replacement snapshot rejection")
	}
	if code := errorCode(t, err); code != model.CodeSnapshotReplaced {
		t.Fatalf("expected E_SNAPSHOT_REPLACED, got %s (%v)", code, err)
	}
}

func TestStrategyVersionMustAdvanceForChanges(t *testing.T) {
	st := openStore(t, filepath.Join(t.TempDir(), "strategy.sqlite"))
	ctx := context.Background()
	strategy := model.DefaultStrategy()
	strategy.Version = 2
	if err := st.SaveStrategy(ctx, strategy); err != nil {
		t.Fatal(err)
	}
	older := strategy
	older.Version = 1
	if err := st.SaveStrategy(ctx, older); err == nil || errorCode(t, err) != model.CodeStrategyInvalid {
		t.Fatalf("expected older strategy rejection, got %v", err)
	}
	changedSameVersion := strategy
	changedSameVersion.ConfidenceColumnWeight += 0.1
	if err := st.SaveStrategy(ctx, changedSameVersion); err == nil || errorCode(t, err) != model.CodeStrategyInvalid {
		t.Fatalf("expected same-version mutation rejection, got %v", err)
	}
	next := changedSameVersion
	next.Version = 3
	if err := st.SaveStrategy(ctx, next); err != nil {
		t.Fatalf("monotonic strategy update failed: %v", err)
	}
}

package store

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"localsearch/multirecall/internal/model"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "checkpoints.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func payload(value any) (string, []byte) {
	data, _ := json.Marshal(value)
	return model.SHA256(data), data
}

func publishNotice(t *testing.T, st *Store, queryID, stage string, revision int, variant string) (string, []byte) {
	t.Helper()
	digest, data := payload(map[string]string{"query_id": queryID, "stage": stage, "variant": variant})
	request := PublishRequest{
		RunID: "run", QueryID: queryID, Stage: stage, Sequence: model.StageSequence(stage),
		Revision: revision, InputFingerprint: model.HashParts("input", queryID, stage, variant),
		ConfigVersion: "v1", ArtifactDigest: digest, Payload: data,
	}
	if stage != model.StageRecall {
		upstream, ok, err := st.ValidCheckpoint(context.Background(), queryID, upstreamStage(stage))
		if err != nil || !ok {
			t.Fatalf("load upstream for %s: ok=%v err=%v", queryID, ok, err)
		}
		request.PrerequisiteDigest = upstream.ArtifactDigest
	}
	if _, err := st.Publish(context.Background(), request); err != nil {
		t.Fatalf("publish %s %s revision %d: %v", queryID, stage, revision, err)
	}
	return digest, data
}

func TestPublishLifecycleAndStaleClassification(t *testing.T) {
	st := newStore(t)
	digest, data := publishNotice(t, st, "q1", model.StageRecall, 1, "a")
	request := PublishRequest{
		RunID: "run", QueryID: "q1", Stage: model.StageRecall, Sequence: 1, Revision: 1,
		InputFingerprint: model.HashParts("input", "q1", model.StageRecall, "a"),
		ConfigVersion:    "v1", ArtifactDigest: digest, Payload: data,
	}
	if _, err := st.Publish(context.Background(), request); staleClassification(err) != "duplicate" {
		t.Fatalf("expected duplicate stale checkpoint, got %v", err)
	}
	request.InputFingerprint = "different"
	if _, err := st.Publish(context.Background(), request); staleClassification(err) != "tampered" {
		t.Fatalf("expected same-revision replacement to be tampered, got %v", err)
	}
	publishNotice(t, st, "q1", model.StageFusion, 1, "f1")
	publishNotice(t, st, "q1", model.StageEvaluation, 1, "e1")
	publishNotice(t, st, "q1", model.StageFusion, 2, "f2")
	var valid int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM checkpoints WHERE query_id='q1' AND stage='fusion' AND valid=1`).Scan(&valid); err != nil {
		t.Fatal(err)
	}
	if valid != 1 {
		t.Fatalf("expected exactly one valid fusion checkpoint, got %d", valid)
	}
	var validEval int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM checkpoints WHERE query_id='q1' AND stage='evaluation' AND valid=1`).Scan(&validEval); err != nil {
		t.Fatal(err)
	}
	if validEval != 0 {
		t.Fatalf("new fusion must invalidate evaluation, got %d valid rows", validEval)
	}
}

func TestPublishStageErrors(t *testing.T) {
	st := newStore(t)
	digest, data := payload(map[string]string{"x": "y"})
	_, err := st.Publish(context.Background(), PublishRequest{
		RunID: "run", QueryID: "q", Stage: model.StageFusion, Sequence: 1, Revision: 1,
		InputFingerprint: "fp", ConfigVersion: "v1", ArtifactDigest: digest, Payload: data,
	})
	if model.AsAppError(err).Code != model.CodeIllegalStageTransition {
		t.Fatalf("expected illegal stage transition, got %v", err)
	}
	publishNotice(t, st, "q2", model.StageRecall, 1, "r")
	if _, err := st.Publish(context.Background(), PublishRequest{
		RunID: "run", QueryID: "q2", Stage: model.StageEvaluation, Sequence: 3, Revision: 1,
		InputFingerprint: "fp", ConfigVersion: "v1", ArtifactDigest: digest, Payload: data,
	}); model.AsAppError(err).Code != model.CodeMissingPrerequisite {
		t.Fatalf("expected missing prerequisite, got %v", err)
	}
	publishNotice(t, st, "q3", model.StageRecall, 1, "r3")
	publishNotice(t, st, "q4", model.StageRecall, 1, "r4")
	recall4, _, err := st.ValidCheckpoint(context.Background(), "q4", model.StageRecall)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Publish(context.Background(), PublishRequest{
		RunID: "run", QueryID: "q3", Stage: model.StageFusion, Sequence: 2, Revision: 1,
		InputFingerprint: "fp", ConfigVersion: "v1", ArtifactDigest: digest,
		PrerequisiteDigest: recall4.ArtifactDigest, Payload: data,
	}); staleClassification(err) != "cross_query_misalignment" {
		t.Fatalf("expected cross-query misalignment, got %v", err)
	}
}

func TestTamperAndRevisionRollbackDetection(t *testing.T) {
	st := newStore(t)
	_, data := publishNotice(t, st, "q1", model.StageRecall, 1, "one")
	publishNotice(t, st, "q1", model.StageRecall, 2, "two")
	request := PublishRequest{
		RunID: "run", QueryID: "q1", Stage: model.StageRecall, Sequence: 1, Revision: 1,
		InputFingerprint: model.HashParts("input", "q1", model.StageRecall, "one"),
		ConfigVersion:    "v1", ArtifactDigest: model.SHA256(data), Payload: data,
	}
	if _, err := st.Publish(context.Background(), request); staleClassification(err) != "rollback" {
		t.Fatalf("expected revision rollback, got %v", err)
	}

	_, _ = publishNotice(t, st, "q2", model.StageRecall, 1, "x")
	if _, err := st.db.Exec(`UPDATE checkpoints SET input_fingerprint='replaced' WHERE query_id='q2' AND stage='recall' AND valid=1`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.ValidCheckpoint(context.Background(), "q2", model.StageRecall); staleClassification(err) != "tampered" {
		t.Fatalf("expected head/row replacement to be tampered, got %v", err)
	}

	digest, original := publishDigest(t, st, "q3")
	if _, err := st.db.Exec(`UPDATE checkpoints SET payload=? WHERE query_id='q3' AND stage='recall' AND valid=1`, []byte("corrupt")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.LoadReusable(context.Background(), "q3", model.StageRecall, model.HashParts("input", "q3", model.StageRecall, "v")); staleClassification(err) != "tampered" {
		t.Fatalf("expected payload digest mismatch to be tampered, got %v", err)
	}
	if model.SHA256(original) != digest {
		t.Fatal("test digest helper mismatch")
	}
}

func publishDigest(t *testing.T, st *Store, queryID string) (string, []byte) {
	t.Helper()
	return publishNotice(t, st, queryID, model.StageRecall, 1, "v")
}

func TestConcurrentPublishUsesConditionalRevision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.db")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	publishNotice(t, first, "q", model.StageRecall, 1, "base")
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for index, st := range []*Store{first, second} {
		wg.Add(1)
		go func(index int, st *Store) {
			defer wg.Done()
			<-start
			digest, data := payload(map[string]int{"writer": index})
			_, err := st.Publish(context.Background(), PublishRequest{
				RunID: "run", QueryID: "q", Stage: model.StageRecall, Sequence: 1, Revision: 2,
				InputFingerprint: model.HashParts("input", "concurrent", string(rune('a'+index))),
				ConfigVersion:    "v1", ArtifactDigest: digest, Payload: data,
			})
			errs <- err
		}(index, st)
	}
	close(start)
	wg.Wait()
	close(errs)
	successes := 0
	stale := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		if model.AsAppError(err).Code == model.CodeStaleCheckpoint {
			stale++
		}
	}
	if successes != 1 || stale != 1 {
		t.Fatalf("expected one winner and one stale publisher, got success=%d stale=%d", successes, stale)
	}
}

func staleClassification(err error) string {
	if err == nil {
		return ""
	}
	appErr := model.AsAppError(err)
	if appErr.Code != model.CodeStaleCheckpoint || appErr.Details == nil {
		return ""
	}
	value, _ := appErr.Details["classification"].(string)
	return value
}

func TestStaleClassificationIsStable(t *testing.T) {
	var appErr *model.AppError
	err := model.NewErrorDetails(model.CodeStaleCheckpoint, "x", "q", map[string]any{"classification": "rollback"})
	if !errors.As(err, &appErr) || !strings.Contains(appErr.Error(), "E_STALE_CHECKPOINT") {
		t.Fatal("stable stale error code is not preserved")
	}
}

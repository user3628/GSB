package engine

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"hostacl-workbench/internal/apperr"
	"hostacl-workbench/internal/model"
	"hostacl-workbench/internal/store"
)

func testEngine(t *testing.T) *Engine {
	t.Helper()
	root := filepath.Join(t.TempDir(), "root")
	db := filepath.Join(t.TempDir(), "workbench.db")
	eng, err := New(Config{Root: root, DBPath: db, MaxConcurrency: 2, QueueSize: 4, VerifyBudget: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

func TestStaleReadAndStaleBase(t *testing.T) {
	ctx := context.Background()
	eng := testEngine(t)
	sample := model.DeterministicSample()
	if _, err := eng.SubmitSnapshot(ctx, *sample.SnapshotV1); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Read(ctx, "u-api", "data", 99); errorCode(err) != string(apperr.StaleRead) {
		t.Fatalf("expected E_STALE_READ, got %v", err)
	}
	result, err := eng.Read(ctx, "u-api", "data", 1)
	if err != nil || !result.Safe || result.UsedVersion != 1 {
		t.Fatalf("version-bound read failed: %+v %v", result, err)
	}
	if _, err := eng.SubmitIncrement(ctx, *sample.IncrementV2); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.SubmitIncrement(ctx, *sample.IncrementV2); errorCode(err) != string(apperr.StaleBase) {
		t.Fatalf("expected E_STALE_BASE, got %v", err)
	}
}

func TestConcurrentIncrementsFromSameBase(t *testing.T) {
	ctx := context.Background()
	eng := testEngine(t)
	sample := model.DeterministicSample()
	if _, err := eng.SubmitSnapshot(ctx, *sample.SnapshotV1); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.SubmitIncrement(ctx, *sample.IncrementV2); err != nil {
		t.Fatal(err)
	}
	inc := *sample.IncrementV3
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			local := inc
			_, errs[index] = eng.SubmitIncrement(ctx, local)
		}(i)
	}
	wg.Wait()
	success, stale := 0, 0
	for _, err := range errs {
		switch errorCode(err) {
		case "":
			success++
		case string(apperr.StaleBase):
			stale++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if success != 1 || stale != 1 {
		t.Fatalf("success=%d stale=%d, want 1/1", success, stale)
	}
}

func TestRecoveryAbortsPendingIntent(t *testing.T) {
	ctx := context.Background()
	eng := testEngine(t)
	sample := model.DeterministicSample()
	if _, err := eng.SubmitSnapshot(ctx, *sample.SnapshotV1); err != nil {
		t.Fatal(err)
	}
	intent := store.Intent{
		ID: "pending-test", Kind: "increment", BaseVersion: 1, TargetVersion: 2,
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), PayloadJSON: "{}",
	}
	if err := eng.Store().StartIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	report, err := eng.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "complete" || report.LastCompleteVersion != 1 || len(report.PendingIntents) != 1 || report.PendingIntents[0].Action != "abort_pending_intent" {
		t.Fatalf("unexpected recovery report: %+v", report)
	}
	head, err := eng.Head(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if head.Version != 1 {
		t.Fatalf("head moved to %d after recovery", head.Version)
	}
}

func TestQueueFullBeforeKernelVerification(t *testing.T) {
	ctx := context.Background()
	eng := testEngine(t)
	sample := model.DeterministicSample()
	if _, err := eng.SubmitSnapshot(ctx, *sample.SnapshotV1); err != nil {
		t.Fatal(err)
	}
	_, err := eng.Verify(ctx, model.VerificationOptions{
		MaxConcurrency: 1, QueueSize: 1, Budget: time.Second,
		SubjectIDs: []string{"u-api", "svc-indexer"}, Paths: []string{"data", "data/private.json"},
	})
	if errorCode(err) != string(apperr.VerifyQueueFull) {
		t.Fatalf("expected E_VERIFY_QUEUE_FULL, got %v", err)
	}
}

func TestPolicyCommitSupersedesPriorVerification(t *testing.T) {
	ctx := context.Background()
	eng := testEngine(t)
	sample := model.DeterministicSample()
	if _, err := eng.SubmitSnapshot(ctx, *sample.SnapshotV1); err != nil {
		t.Fatal(err)
	}
	_, err := eng.Store().DB().ExecContext(ctx, `INSERT INTO verification_runs(id,intent_id,version,status,budget_ms,max_concurrency,queue_size,report_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)`,
		"verify-prior", "intent-prior", 1, "complete", 1000, 1, 1, "{}", time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.SubmitIncrement(ctx, *sample.IncrementV2); err != nil {
		t.Fatal(err)
	}
	status, err := eng.Store().VerificationRunStatus(ctx, "verify-prior")
	if err != nil {
		t.Fatal(err)
	}
	if status == nil || status.Status != "superseded" {
		t.Fatalf("prior verification was not superseded: %+v", status)
	}
}

func errorCode(err error) string {
	if err == nil {
		return ""
	}
	var appErr *apperr.Error
	if errors.As(err, &appErr) {
		return string(appErr.Code)
	}
	return "other"
}

package service_test

import (
	"context"
	"testing"

	"netpolcompiler/internal/model"
	"netpolcompiler/internal/sample"
	"netpolcompiler/internal/service"
	"netpolcompiler/internal/store"
	"sync"
)

func newTestService(t *testing.T, hooks service.Hooks) *service.Service {
	t.Helper()
	svc, _, err := service.New(context.Background(), service.Config{DataDir: t.TempDir(), Hooks: hooks})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

func seedSample(t *testing.T, svc *service.Service) model.Snapshot {
	t.Helper()
	snapshot := sample.GenerateSnapshot(sample.DefaultSeed)
	result := svc.SubmitSnapshot(context.Background(), snapshot)
	if result.Status != 200 {
		t.Fatalf("seed snapshot status=%d body=%s", result.Status, string(result.Body))
	}
	return snapshot
}

func TestConcurrentIncrementsSameBaseOnlyOneSucceeds(t *testing.T) {
	svc := newTestService(t, service.Hooks{})
	seedSample(t, svc)

	first := sample.GenerateIncrement(1, "increment-a")
	second := sample.GenerateIncrement(1, "increment-b")
	results := make(chan service.RawResult, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); results <- svc.SubmitIncrement(context.Background(), first) }()
	go func() { defer wg.Done(); results <- svc.SubmitIncrement(context.Background(), second) }()
	wg.Wait()
	close(results)

	successes, staleBases := 0, 0
	for result := range results {
		switch {
		case result.Status == 200:
			successes++
		case result.Biz != nil && result.Biz.Code == "E_STALE_BASE":
			staleBases++
		default:
			t.Fatalf("unexpected result status=%d biz=%+v body=%s", result.Status, result.Biz, string(result.Body))
		}
	}
	if successes != 1 || staleBases != 1 {
		t.Fatalf("successes=%d staleBases=%d, want 1/1", successes, staleBases)
	}
	current, err := svc.Store().CurrentVersion(context.Background())
	if err != nil || current != 2 {
		t.Fatalf("current version=%d err=%v, want 2", current, err)
	}
}

func TestSameRequestIDConcurrentExecutesOnce(t *testing.T) {
	svc := newTestService(t, service.Hooks{})
	seedSample(t, svc)
	increment := sample.GenerateIncrement(1, "same-request")

	const callers = 8
	var wg sync.WaitGroup
	results := make(chan service.RawResult, callers)
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			results <- svc.SubmitIncrement(context.Background(), increment)
		}()
	}
	wg.Wait()
	close(results)

	for result := range results {
		if result.Status != 200 {
			t.Fatalf("duplicate caller got status=%d body=%s", result.Status, string(result.Body))
		}
	}
	var snapshotRows int
	if err := svc.Store().DB.QueryRow(`SELECT COUNT(*) FROM snapshots`).Scan(&snapshotRows); err != nil {
		t.Fatal(err)
	}
	if snapshotRows != 2 {
		t.Fatalf("expected exactly snapshot v1 and one increment v2, got %d rows", snapshotRows)
	}
	record, err := svc.Store().GetRequest(context.Background(), "same-request")
	if err != nil || record == nil || record.Status != "succeeded" {
		t.Fatalf("request record=%+v err=%v", record, err)
	}
}

func TestConflictingRequestID(t *testing.T) {
	svc := newTestService(t, service.Hooks{})
	first := sample.GenerateSnapshot(sample.DefaultSeed)
	first.RequestID = "conflict-id"
	if result := svc.SubmitSnapshot(context.Background(), first); result.Status != 200 {
		t.Fatalf("first request failed: %s", string(result.Body))
	}
	second := sample.GenerateSnapshot(sample.DefaultSeed)
	second.RequestID = "conflict-id"
	second.Version = 2
	second.Digest = model.DigestSnapshot(second)
	second.Rules[0].Priority = second.Rules[0].Priority + 1
	result := svc.SubmitSnapshot(context.Background(), second)
	if result.Biz == nil || result.Biz.Code != "E_CONFLICTING_REQUEST" {
		t.Fatalf("expected E_CONFLICTING_REQUEST, got status=%d body=%s", result.Status, string(result.Body))
	}
}

func TestStaleReadWhileCascadePending(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	hooks := service.Hooks{AfterIntent: func(targetVersion int64, namespaces []string) {
		if targetVersion == 2 {
			once.Do(func() { close(entered) })
			<-release
		}
	}}
	svc := newTestService(t, hooks)
	seedSample(t, svc)

	snapshot := sample.GenerateSnapshot(sample.DefaultSeed)
	snapshot.Version = 2
	snapshot.Digest = model.DigestSnapshot(snapshot)
	resultCh := make(chan service.RawResult, 1)
	go func() { resultCh <- svc.SubmitSnapshot(context.Background(), snapshot) }()

	<-entered
	_, staleError := svc.GetView("demo", 2)
	if staleError == nil || staleError.Code != "E_STALE_READ" {
		t.Fatalf("expected E_STALE_READ, got %+v", staleError)
	}
	if staleError.Context["viewVersion"] != int64(1) || staleError.Context["observedVersion"] != int64(2) {
		t.Fatalf("unexpected stale context: %+v", staleError.Context)
	}
	ready, bizError := svc.GetView("demo", 1)
	if bizError != nil || ready.UsedVersion != 1 {
		t.Fatalf("old complete version should remain readable: %+v %+v", ready, bizError)
	}

	close(release)
	result := <-resultCh
	if result.Status != 200 {
		t.Fatalf("blocked snapshot failed: %s", string(result.Body))
	}
	after, bizError := svc.GetView("demo", 2)
	if bizError != nil || after.UsedVersion != 2 {
		t.Fatalf("expected version 2 after atomic rebuild, view=%+v err=%+v", after, bizError)
	}
}

func TestRecoveryAbortsPreparedIntent(t *testing.T) {
	svc := newTestService(t, service.Hooks{})
	seedSample(t, svc)
	intent := store.Intent{
		IntentID: "intent-crash-test", Kind: "snapshot_commit_and_compile", TargetVersion: 99,
		Namespaces: `["demo"]`, Status: "prepared", CreatedAt: store.NowRFC3339(), UpdatedAt: store.NowRFC3339(),
	}
	if err := svc.Store().CreateIntent(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	report, err := svc.Recover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "recovered_unfinished_intents" || len(report.AbortedIntentIDs) != 1 {
		t.Fatalf("unexpected recovery report: %+v", report)
	}
	view, bizError := svc.GetView("demo", 1)
	if bizError != nil || view.UsedVersion != 1 {
		t.Fatalf("last complete view must survive recovery: %+v %+v", view, bizError)
	}
}

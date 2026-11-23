package session_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"depverify/internal/model"
	"depverify/internal/samples"
	"depverify/internal/session"
	"depverify/internal/snapshot"
)

func TestConcurrentSessionsIsolationAndQueue(t *testing.T) {
	store, err := snapshot.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	mgr := session.NewManager(store, session.Config{MaxConcurrent: 2, ItemBudget: 0, Threshold: 32})
	bundles := samples.All()
	results := make([]session.RunResult, len(bundles))
	var wg sync.WaitGroup
	for i, b := range bundles {
		wg.Add(1)
		go func(i int, b samples.Bundle) {
			defer wg.Done()
			results[i] = mgr.SubmitAndWait(ctx, session.RunRequest{
				TaskID: "t-" + string(b.Kind), ManifestBuf: b.Manifest,
				EdgesBuf: []byte(b.Edges), Roots: b.Roots,
				PersistSnapshot: b.Kind == samples.KindValid,
			})
		}(i, b)
	}
	wg.Wait()

	codeSeen := map[string]bool{}
	validCount := 0
	for _, r := range results {
		if r.Result != nil {
			for _, c := range r.Result.CodeSequence {
				codeSeen[c] = true
			}
			if r.TaskID == "t-valid" {
				validCount++
			}
		}
		if r.ParseError != nil {
			codeSeen[r.ParseError.Code] = true
		}
	}
	for _, want := range []string{
		model.ETruncated, model.EBadMagic, model.ELengthOverflow,
		model.EEdgeDangling, model.EVersionDowngrade, model.ECycle, model.EConstraintConflict,
	} {
		if !codeSeen[want] {
			t.Fatalf("expected code %s not observed: %v", want, codeSeen)
		}
	}
	if validCount != 1 {
		t.Fatalf("valid session missing")
	}
	// 只有健康会话推进快照，注入会话不推进。
	versions, err := store.AllVersions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 1 || versions[0] != 1 {
		t.Fatalf("snapshot must not advance on injected failures: %v", versions)
	}
}

func TestItemBudgetTerminatesSingleSession(t *testing.T) {
	store, _ := snapshot.Open(filepath.Join(t.TempDir(), "s.db"))
	defer store.Close()
	mgr := session.NewManager(store, session.Config{MaxConcurrent: 2, ItemBudget: 2, Threshold: 32})
	big := samples.Large(8)
	res := mgr.SubmitAndWait(context.Background(), session.RunRequest{
		TaskID: "budget", ManifestBuf: big.Manifest,
		EdgesBuf: big.Edges, Roots: big.Roots, PersistSnapshot: false,
	})
	found := false
	for _, c := range res.Result.CodeSequence {
		if c == model.EItemBudget {
			found = true
		}
	}
	if !found {
		t.Fatalf("want E_ITEM_BUDGET, got %v", res.Result.CodeSequence)
	}

	// 另一个不受预算影响的会话（独立管理器，模拟独立资源配额）仍可完成。
	healthy := samples.All()[0]
	otherMgr := session.NewManager(store, session.Config{MaxConcurrent: 2, ItemBudget: 0, Threshold: 32})
	other := otherMgr.SubmitAndWait(context.Background(), session.RunRequest{
		TaskID: "healthy", ManifestBuf: healthy.Manifest,
		EdgesBuf: []byte(healthy.Edges), Roots: healthy.Roots, PersistSnapshot: false,
	})
	if other.State != session.StateCompleted || len(other.Result.Issues) != 0 {
		t.Fatalf("other session must stay healthy: %+v", other)
	}
}

func TestRecoveryAfterRestart(t *testing.T) {
	db := filepath.Join(t.TempDir(), "restart.db")
	store, err := snapshot.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	healthy := samples.All()[0]
	mgr := session.NewManager(store, session.Config{MaxConcurrent: 1, Threshold: 32})
	res := mgr.SubmitAndWait(ctx, session.RunRequest{
		TaskID: "persisted", ManifestBuf: healthy.Manifest,
		EdgesBuf: []byte(healthy.Edges), Roots: healthy.Roots, PersistSnapshot: true,
	})
	if res.Snapshot == nil || res.Snapshot.Version != 1 {
		t.Fatalf("seed snapshot failed: %+v", res)
	}
	// 模拟进程被强制终止：直接写入 RUNNING 进度后丢弃内存态。
	if _, err := store.DB().ExecContext(ctx,
		`INSERT INTO task_runs(task_id,state,updated_marker,detail) VALUES (?,?,?,?)`,
		"zombie", session.StateRunning, model.FixedSampleMarker, `{"task_id":"zombie"}`); err != nil {
		t.Fatal(err)
	}
	store.Close()

	// 重启：已提交快照仍在，RUNNING 任务被恢复为 QUEUED。
	reopened, err := snapshot.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	versions, _ := reopened.AllVersions(ctx)
	if len(versions) != 1 || versions[0] != 1 {
		t.Fatalf("committed snapshot must survive restart: %v", versions)
	}
	mgr2 := session.NewManager(reopened, session.Config{MaxConcurrent: 1, Threshold: 32})
	recovered, err := mgr2.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 || recovered[0].TaskID != "zombie" || recovered[0].State != session.StateQueued {
		t.Fatalf("recovery wrong: %+v", recovered)
	}
}

func TestDeterministicRepeatedReport(t *testing.T) {
	store, _ := snapshot.Open(filepath.Join(t.TempDir(), "rep.db"))
	defer store.Close()
	mgr := session.NewManager(store, session.Config{MaxConcurrent: 4, Threshold: 32})
	run := func() []byte {
		var wg sync.WaitGroup
		results := make([]session.RunResult, 0)
		var mu sync.Mutex
		for _, b := range samples.All() {
			wg.Add(1)
			go func(b samples.Bundle) {
				defer wg.Done()
				r := mgr.SubmitAndWait(context.Background(), session.RunRequest{
					TaskID: fmt.Sprintf("t-%s", b.Kind), ManifestBuf: b.Manifest,
					EdgesBuf: []byte(b.Edges), Roots: b.Roots, PersistSnapshot: false,
				})
				mu.Lock()
				results = append(results, r)
				mu.Unlock()
			}(b)
		}
		wg.Wait()
		raw, err := reportBytes(results)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	first := run()
	second := run()
	if string(first) != string(second) {
		t.Fatalf("report bytes differ across runs")
	}
}

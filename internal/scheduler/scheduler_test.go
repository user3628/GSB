package scheduler

import (
	"path/filepath"
	"testing"

	"injectctl/internal/model"
	"injectctl/internal/store"
)

func openStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "sched.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func mkSamples(prefix string, n int) []model.Sample {
	out := make([]model.Sample, n)
	for i := range out {
		out[i] = model.Sample{
			ID: prefix + "-" + string(rune('a'+i)), Seq: i + 1, Surface: "path",
			Landing:   model.LandingPathSegment,
			Encodings: []string{model.EncodingPathNormalize},
			Raw:       "seg",
		}
	}
	return out
}

func TestSampleBudgetIsolation(t *testing.T) {
	st := openStore(t)
	sched := New(Config{MaxConcurrent: 1, MaxQueueDepth: 8, MaxSamplesPerBatch: 3}, st)
	sched.Start()
	if err := sched.Submit(Job{BatchID: "over", Samples: mkSamples("o", 6)}); err != nil {
		t.Fatal(err)
	}
	if err := sched.Submit(Job{BatchID: "fine", Samples: mkSamples("f", 2)}); err != nil {
		t.Fatal(err)
	}
	sched.WaitIdle()
	sched.Shutdown()

	over, _ := sched.Result("over")
	fine, _ := sched.Result("fine")
	if over.Code != model.CodeSampleBudget {
		t.Fatalf("over-budget batch code = %s", over.Code)
	}
	if fine.Code != model.CodeOK || len(fine.Verdicts) != 2 {
		t.Fatalf("clean batch polluted: %+v", fine)
	}
	exists, status, _ := st.BatchExists("over")
	if !exists || status != store.StatusBudget {
		t.Fatalf("budget terminal state not persisted: %s", status)
	}
}

func TestQueueAndInflightObservable(t *testing.T) {
	st := openStore(t)
	sched := New(Config{MaxConcurrent: 1, MaxQueueDepth: 16}, st)
	sched.Start()
	for b := 0; b < 5; b++ {
		if err := sched.Submit(Job{BatchID: "b" + string(rune('a'+b)), Samples: mkSamples("x", 1)}); err != nil {
			t.Fatal(err)
		}
	}
	if sched.Queued() < 0 {
		t.Fatal("queue not observable")
	}
	sched.WaitIdle()
	if sched.Inflight() != 0 || sched.Queued() != 0 {
		t.Fatalf("drain failed: inflight=%d queued=%d", sched.Inflight(), sched.Queued())
	}
	sched.Shutdown()
	if len(sched.AllResults()) != 5 {
		t.Fatalf("all batches must complete: %d", len(sched.AllResults()))
	}
}

func TestConcurrentBatchesIsolatedVersions(t *testing.T) {
	st := openStore(t)
	sched := New(Config{MaxConcurrent: 4, MaxQueueDepth: 16}, st)
	sched.Start()
	for b := 0; b < 8; b++ {
		if err := sched.Submit(Job{
			BatchID: "batch-" + string(rune('a'+b)),
			Samples: mkSamples("y", 3),
		}); err != nil {
			t.Fatal(err)
		}
	}
	sched.WaitIdle()
	sched.Shutdown()

	chain, err := st.LoadChain()
	if err != nil {
		t.Fatalf("snapshot chain broken under concurrency: %v", err)
	}
	if len(chain.Snapshots) != 8 {
		t.Fatalf("expected 8 continuous snapshots, got %d", len(chain.Snapshots))
	}
	for i, snap := range chain.Snapshots {
		if snap.Version != i+1 {
			t.Fatalf("version discontinuity at %d: %d", i, snap.Version)
		}
	}
}

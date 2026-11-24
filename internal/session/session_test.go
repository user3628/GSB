package session_test

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"privescworkbench/internal/domain"
	"privescworkbench/internal/sample"
	"privescworkbench/internal/session"
	"privescworkbench/internal/snapshot"
)

func mustBundle(t *testing.T, seed int64) *domain.Bundle {
	t.Helper()
	g := sample.Generate(seed, sample.ScenarioBaseline)
	b, err := sample.ParseBundle(g.Bundle.Version, g.ManifestBytes, g.StreamBytes)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestQueueWhenConcurrencyExceeded(t *testing.T) {
	mgr := session.NewManager(session.Limits{MaxConcurrent: 1, MaxProcesses: 100, MaxEvents: 100})
	observed := make(chan string, 4)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			b := mustBundle(t, int64(i+1))
			_, st, err := mgr.Submit(id(i), b, nil, 0)
			if err != nil {
				t.Error(err)
				return
			}
			observed <- st.ID
		}(i)
	}
	close(start)
	wg.Wait()
	close(observed)
	got := map[string]bool{}
	for id := range observed {
		got[id] = true
	}
	if len(got) != 3 {
		t.Fatalf("queued sessions must all complete, got %v", got)
	}
}

func TestBudgetAbortKeepsCommittedSnapshots(t *testing.T) {
	dir := t.TempDir()
	store, err := snapshot.Open(filepath.Join(dir, "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// generous manager lets the healthy batch commit first
	healthy := session.NewManager(session.Limits{MaxConcurrent: 2, MaxProcesses: 1000, MaxEvents: 1000})
	good := mustBundle(t, 1)
	if _, _, err := healthy.Submit("good", good, store, 0); err != nil {
		t.Fatal(err)
	}
	versionAfterGood, err := store.LatestVersion()
	if err != nil {
		t.Fatal(err)
	}

	strict := session.NewManager(session.Limits{MaxConcurrent: 2, MaxProcesses: 2, MaxEvents: 100})
	big := mustBundle(t, 2) // baseline has 4 processes > limit 2
	_, st, err := strict.Submit("big", big, store, 0)
	if err == nil {
		t.Fatal("expected E_SESSION_BUDGET")
	}
	ce, ok := err.(*domain.CodeError)
	if !ok || ce.Code != domain.CodeSessionBudget {
		t.Fatalf("wrong code: %v", err)
	}
	if st.Code != domain.CodeSessionBudget || st.Status != "aborted" {
		t.Fatalf("session state=%+v", st)
	}
	versionNow, err := store.LatestVersion()
	if err != nil {
		t.Fatal(err)
	}
	if versionNow != versionAfterGood {
		t.Fatalf("budget breach must not advance snapshot: %d -> %d",
			versionAfterGood, versionNow)
	}
}

func TestSessionStateIsolation(t *testing.T) {
	mgr := session.NewManager(session.DefaultLimits())
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b := mustBundle(t, int64(i+1))
			_, st, err := mgr.Submit(id(i), b, nil, 0)
			if err != nil {
				t.Error(err)
				return
			}
			if st.Processes != len(b.Processes) {
				t.Errorf("isolated state mismatch")
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < 4; i++ {
		st, ok := mgr.State(id(i))
		if !ok || st.Status != "done" {
			t.Fatalf("session %s state=%+v ok=%v", id(i), st, ok)
		}
	}
}

func TestMonotonicElapsedSampling(t *testing.T) {
	mgr := session.NewManager(session.DefaultLimits())
	before := time.Now()
	res, _, err := mgr.Submit("timed", mustBundle(t, 1), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Round.ElapsedMicros < 0 || time.Since(before).Microseconds() < res.Round.ElapsedMicros {
		t.Fatal("monotonic elapsed sampling inconsistent")
	}
	if res.Round.SampledAt != domain.FixedSampledAt {
		t.Fatal("only fixed sample marks allowed")
	}
}

func id(i int) string {
	return "sess-" + string(rune('a'+i))
}

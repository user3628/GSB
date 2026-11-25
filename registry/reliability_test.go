package registry_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	. "registrylab/registry"
	"registrylab/sample"
	"registrylab/testfixture"
)

func slowSnapshot(baseURL string, version int64) SnapshotRequest {
	var instances []Instance
	for _, service := range []string{"alpha", "beta"} {
		count := 4
		if service == "beta" {
			count = 2
		}
		for i := 0; i < count; i++ {
			instances = append(instances, Instance{
				ServiceName: service, InstanceID: fmt.Sprintf("%s-%02d", service, i), Endpoint: baseURL,
				Weight: 100 + i, LeaseTTLMS: 3000, Priority: 10 + i, SuccessThreshold: 1, FailureThreshold: 1,
				Probe: ProbeSpec{Kind: ProbeHTTP, Path: "/probe/slow", ExpectedStatus: 200,
					ExpectedMarker: testfixture.BodyOK, TimeoutMS: 180},
			})
		}
	}
	return SnapshotRequest{Version: version, GeneratedAt: fmt.Sprintf("2026-11-26T00:00:%02dZ", version),
		Instances: instances, Digest: ComputeDigest(version, fmt.Sprintf("2026-11-26T00:00:%02dZ", version), instances)}
}

func TestProbeTimeoutReleasesRealConnections(t *testing.T) {
	fixture, err := testfixture.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Close()
	e, err := Open(filepath.Join(t.TempDir(), "timeout.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	if _, opErr := e.SubmitSnapshot(ctx, slowSnapshot(fixture.BaseURL, 1)); opErr != nil {
		t.Fatal(opErr)
	}
	fixture.ResetSlowStats()
	round, opErr := e.RunProbeRound(ctx, RoundOptions{GlobalConcurrency: 2, PerServiceConcurrency: 1,
		BudgetMS: 1200, WaitQueueCapacity: 20})
	if opErr != nil {
		t.Fatal(opErr)
	}
	if round.Started != 6 || len(round.Deferred) != 0 {
		t.Fatalf("unexpected scheduling: started=%d deferred=%d", round.Started, len(round.Deferred))
	}
	for _, outcome := range round.Outcomes {
		if outcome.Code != ErrProbeTimeout {
			t.Fatalf("probe did not time out: %+v", outcome)
		}
		if outcome.LatencyMS > 600 {
			t.Fatalf("probe did not release near its deadline: %+v", outcome)
		}
	}
	if fixture.SlowMax() > 2 {
		t.Fatalf("global concurrency exceeded: max=%d", fixture.SlowMax())
	}
	deadline := time.Now().Add(time.Second)
	for fixture.SlowActive() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if active := fixture.SlowActive(); active != 0 {
		t.Fatalf("real HTTP connections remained active after timeout: %d", active)
	}
}

func TestConcurrentReadersSeeCompleteRevisions(t *testing.T) {
	e, err := Open(filepath.Join(t.TempDir(), "readers.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	snap := fixtureSample(t, 1)
	if _, opErr := e.SubmitSnapshot(ctx, snap); opErr != nil {
		t.Fatal(opErr)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan string, 64)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				view, opErr := e.ReadComplete(ctx)
				if opErr != nil {
					errCh <- opErr.Error()
					return
				}
				if view.Current.DerivedRevision != view.Derived.Revision {
					errCh <- fmt.Sprintf("pointer mismatch current=%d derived=%d", view.Current.DerivedRevision, view.Derived.Revision)
					return
				}
				if view.Derived.Status != "COMPLETE" {
					errCh <- "incomplete derived view visible"
					return
				}
				if len(view.Derived.Weights) != len(view.Current.Instances) {
					errCh <- fmt.Sprintf("weight row count %d != instance count %d", len(view.Derived.Weights), len(view.Current.Instances))
					return
				}
			}
		}()
	}
	for i := 0; i < 18; i++ {
		cur, opErr := e.Current(ctx)
		if opErr != nil {
			t.Fatal(opErr)
		}
		in := snap.Instances[(i+1)%len(snap.Instances)]
		in.Weight = 100 + (i*37)%901
		req := DeltaRequest{BaseVersion: cur.Version, Version: cur.Version + 1,
			Upserts: []Instance{in}, RequestID: fmt.Sprintf("reader-writer-%d", i)}
		if _, opErr := e.SubmitDelta(ctx, req); opErr != nil {
			t.Fatalf("delta %d: %v", i, opErr)
		}
	}
	close(stop)
	wg.Wait()
	close(errCh)
	for readerErr := range errCh {
		t.Error(readerErr)
	}
}

func TestConflictingRequestID(t *testing.T) {
	e := openTestEngine(t)
	ctx := context.Background()
	snap := fixtureSample(t, 1)
	snap.RequestID = "idem-snapshot"
	first, opErr := e.SubmitSnapshot(ctx, snap)
	if opErr != nil {
		t.Fatal(opErr)
	}
	replay, opErr := e.SubmitSnapshot(ctx, snap)
	if opErr != nil || replay.Version != first.Version || replay.Digest != first.Digest {
		t.Fatalf("same request did not replay: %+v %v", replay, opErr)
	}
	changed := snap
	changed.Instances = append([]Instance(nil), snap.Instances...)
	changed.Instances[0].Weight = 999
	changed.Digest = ComputeDigest(changed.Version, changed.GeneratedAt, changed.Instances)
	if _, opErr := e.SubmitSnapshot(ctx, changed); opErr == nil || opErr.Code != ErrConflictingRequest {
		t.Fatalf("want conflicting request, got %v", opErr)
	}
}

func TestCrashRecoveryProcess(t *testing.T) {
	if os.Getenv("REGISTRY_CRASH_HELPER") == "1" {
		runCrashHelper(t)
		return
	}
	fixture, err := testfixture.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Close()
	dbPath := filepath.Join(t.TempDir(), "crash.db")
	seed, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	snap := sample.Generate(sample.Options{Seed: 9, Version: 1, BaseURL: fixture.BaseURL,
		TCPAddr: fixture.TCPAddr, RefusedAddr: fixture.RefusedAddr})
	if _, opErr := seed.SubmitSnapshot(context.Background(), snap); opErr != nil {
		t.Fatal(opErr)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestCrashRecoveryProcess")
	cmd.Env = append(os.Environ(),
		"REGISTRY_CRASH_HELPER=1",
		"REGISTRY_CRASH_DB="+dbPath,
		"REGISTRY_CRASH_BASE="+fixture.BaseURL,
		"REGISTRY_CRASH_TCP="+fixture.TCPAddr,
		"REGISTRY_CRASH_REFUSED="+fixture.RefusedAddr,
	)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("helper did not die from SIGKILL: %s", output)
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("helper error: %v output=%s", err, output)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("helper was not SIGKILLed: %v output=%s", err, output)
	}

	reopened, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	cur, opErr := reopened.Current(context.Background())
	if opErr != nil {
		t.Fatal(opErr)
	}
	report, opErr := reopened.LastRecoveryReport(context.Background())
	if opErr != nil {
		t.Fatal(opErr)
	}
	if cur.Version != 1 || cur.DerivedRevision != 1 {
		t.Fatalf("recovery exposed partial state: version=%d revision=%d", cur.Version, cur.DerivedRevision)
	}
	if report.AbortedIntents < 1 || report.DiscardedDerivedRevisions < 1 || report.DiscardedStagedSnapshots < 1 {
		t.Fatalf("incomplete recovery report: %+v", report)
	}
}

func runCrashHelper(t *testing.T) {
	dbPath := os.Getenv("REGISTRY_CRASH_DB")
	fixture := sample.Options{Seed: 9, Version: 2, GeneratedAt: "2026-11-26T00:00:01Z",
		BaseURL: os.Getenv("REGISTRY_CRASH_BASE"), TCPAddr: os.Getenv("REGISTRY_CRASH_TCP"),
		RefusedAddr: os.Getenv("REGISTRY_CRASH_REFUSED")}
	engine, err := Open(dbPath, WithCrashHook(func(phase string) {
		if phase == "after_stage" {
			_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
			select {}
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	next := sample.Generate(fixture)
	next.RequestID = "crash-helper-snapshot"
	if _, opErr := engine.SubmitSnapshot(context.Background(), next); opErr != nil {
		t.Fatalf("submit: %v", opErr)
	}
	t.Fatal("crash hook was not reached")
}

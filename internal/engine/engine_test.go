package engine_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"secverify/internal/config"
	"secverify/internal/engine"
	"secverify/internal/model"
	"secverify/internal/sample"
)

type testEnv struct {
	engine   *engine.Engine
	items    []model.Item
	opts     engine.Options
	baseline string
}

func newTestEngine(t *testing.T, maxTx, maxProbes int, wait time.Duration) testEnv {
	t.Helper()
	root := t.TempDir()
	opts := engine.Options{
		DBPath:          filepath.Join(root, "engine.db"),
		WorkDir:         filepath.Join(root, "work"),
		MaxTransactions: maxTx,
		MaxProbes:       maxProbes,
		WaitTimeout:     wait,
		ProbeTimeout:    5 * time.Second,
	}
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	baseline, bizErr := config.ParseBaseline(sample.BaselineJSON())
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	return testEnv{engine: eng, items: baseline.Items, opts: opts, baseline: sample.Namespace}
}

func (env testEnv) initialize(t *testing.T, namespace string) {
	t.Helper()
	if _, bizErr := env.engine.Initialize(context.Background(), namespace, env.items); bizErr != nil {
		t.Fatal(bizErr)
	}
}

func TestApplyRoundsProbesClosuresAndFailureIsolation(t *testing.T) {
	env := newTestEngine(t, 4, 2, 500*time.Millisecond)
	env.initialize(t, sample.Namespace)
	rounds := sample.ValidRounds()
	results := make([]engine.RoundResult, 0, len(rounds))
	for index, round := range rounds {
		result, err := env.engine.Apply(context.Background(), engine.ApplyRequest{Namespace: sample.Namespace, RoundName: round.Name, Changeset: round.Changeset})
		if err != nil {
			t.Fatalf("round %d: %v", index+1, err)
		}
		if result.BusinessCode != model.CodeOK || !result.SnapshotAdvanced {
			t.Fatalf("round %d did not commit: %+v", index+1, result)
		}
		if result.VersionAfter != result.VersionBefore+1 {
			t.Fatalf("round %d version did not advance monotonically", index+1)
		}
		for _, probeResult := range result.Probes {
			if !probeResult.Match || probeResult.Code != model.CodeOK {
				t.Fatalf("round %d probe mismatch: %+v", index+1, probeResult)
			}
		}
		results = append(results, result)
	}
	if len(results[0].Invalidation.Items) != 8 {
		t.Fatalf("round 1 closure = %v", results[0].Invalidation.Items)
	}
	if len(results[1].Invalidation.Items) != 6 {
		t.Fatalf("round 2 closure = %v", results[1].Invalidation.Items)
	}
	if got := join(results[3].Invalidation.Items); got != "reg.hklm.defender,svc.defender" {
		t.Fatalf("delete closure = %s", got)
	}
	history, bizErr := env.engine.History(context.Background(), sample.Namespace)
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	if len(history) != 5 || history[4].Version != 4 {
		t.Fatalf("history = %+v", history)
	}

	// A failed dependency validation must not advance the committed snapshot.
	before := history[4]
	result, err := env.engine.Apply(context.Background(), engine.ApplyRequest{
		Namespace: sample.Namespace, RoundName: "missing", Changeset: sample.MissingDependencyChange(),
	})
	if err == nil || result.BusinessCode != model.CodeDepMissing {
		t.Fatalf("expected E_DEP_MISSING, result=%+v err=%v", result, err)
	}
	if result.SnapshotAdvanced || result.VersionAfter != before.Version {
		t.Fatalf("failed apply advanced version: %+v", result)
	}
	if result.Issue == nil || result.Issue.ItemID == "" || result.Issue.SourceOffset < 0 {
		t.Fatalf("failure lacks item/offset evidence: %+v", result.Issue)
	}
	history, bizErr = env.engine.History(context.Background(), sample.Namespace)
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	if history[4].ContentHash != before.ContentHash || len(history) != 5 {
		t.Fatal("failed apply polluted committed snapshots")
	}
}

func TestSameNamespaceChangeIsSerializedWithContentionTimeout(t *testing.T) {
	env := newTestEngine(t, 4, 2, 300*time.Millisecond)
	env.initialize(t, sample.Namespace)
	changeset := sample.RoundOne().Changeset
	gate := make(chan struct{})
	ready := make(chan struct{})
	type outcome struct {
		result engine.RoundResult
		err    error
	}
	winnerCh := make(chan outcome, 1)
	go func() {
		result, err := env.engine.Apply(context.Background(), engine.ApplyRequest{
			Namespace: sample.Namespace, RoundName: "winner", Changeset: changeset,
			ProbeGate: gate, ProbeReady: func() { close(ready) },
		})
		winnerCh <- outcome{result: result, err: err}
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		close(gate)
		t.Fatal("winner did not reach probe gate")
	}
	loser, err := env.engine.Apply(context.Background(), engine.ApplyRequest{
		Namespace: sample.Namespace, RoundName: "loser", Changeset: changeset, WaitTimeout: 30 * time.Millisecond,
	})
	if err == nil || loser.BusinessCode != model.CodeChangeContention {
		t.Fatalf("expected contention, result=%+v err=%v", loser, err)
	}
	if loser.SnapshotAdvanced {
		t.Fatal("contending request advanced a snapshot")
	}
	close(gate)
	winner := <-winnerCh
	if winner.err != nil || winner.result.BusinessCode != model.CodeOK {
		t.Fatalf("winner failed: result=%+v err=%v", winner.result, winner.err)
	}
	history, bizErr := env.engine.History(context.Background(), sample.Namespace)
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	if len(history) != 2 || history[1].Version != 1 {
		t.Fatalf("unexpected history after contention: %+v", history)
	}
}

func TestDifferentNamespacesCanProbeInParallel(t *testing.T) {
	env := newTestEngine(t, 2, 2, time.Second)
	env.initialize(t, "ns-a")
	env.initialize(t, "ns-b")
	changeset := sample.RoundOne().Changeset
	gateA := make(chan struct{})
	gateB := make(chan struct{})
	readyA := make(chan struct{})
	readyB := make(chan struct{})
	type outcome struct {
		result engine.RoundResult
		err    error
	}
	resultA := make(chan outcome, 1)
	resultB := make(chan outcome, 1)
	go func() {
		result, err := env.engine.Apply(context.Background(), engine.ApplyRequest{Namespace: "ns-a", RoundName: "a", Changeset: changeset, ProbeGate: gateA, ProbeReady: func() { close(readyA) }})
		resultA <- outcome{result, err}
	}()
	go func() {
		result, err := env.engine.Apply(context.Background(), engine.ApplyRequest{Namespace: "ns-b", RoundName: "b", Changeset: changeset, ProbeGate: gateB, ProbeReady: func() { close(readyB) }})
		resultB <- outcome{result, err}
	}()
	for index, ready := range []<-chan struct{}{readyA, readyB} {
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			close(gateA)
			close(gateB)
			t.Fatalf("namespace %d did not run concurrently", index)
		}
	}
	close(gateA)
	close(gateB)
	a := <-resultA
	b := <-resultB
	if a.err != nil || b.err != nil {
		t.Fatalf("parallel applies failed: a=%v b=%v", a.err, b.err)
	}
	if a.result.Concurrency.PeakTransactions < 2 || a.result.Concurrency.PeakProbes < 2 {
		t.Fatalf("parallel execution was not observed: %+v", a.result.Concurrency)
	}
}

func TestProbeLimitCausesContention(t *testing.T) {
	env := newTestEngine(t, 2, 1, 200*time.Millisecond)
	env.initialize(t, "ns-a")
	env.initialize(t, "ns-b")
	changeset := sample.RoundOne().Changeset
	gate := make(chan struct{})
	ready := make(chan struct{})
	type outcome struct {
		result engine.RoundResult
		err    error
	}
	first := make(chan outcome, 1)
	go func() {
		result, err := env.engine.Apply(context.Background(), engine.ApplyRequest{Namespace: "ns-a", RoundName: "first", Changeset: changeset, ProbeGate: gate, ProbeReady: func() { close(ready) }})
		first <- outcome{result, err}
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		close(gate)
		t.Fatal("first probe did not start")
	}
	second, err := env.engine.Apply(context.Background(), engine.ApplyRequest{Namespace: "ns-b", RoundName: "second", Changeset: changeset, WaitTimeout: 30 * time.Millisecond})
	if err == nil || second.BusinessCode != model.CodeChangeContention {
		t.Fatalf("expected probe contention, result=%+v err=%v", second, err)
	}
	close(gate)
	firstResult := <-first
	if firstResult.err != nil {
		t.Fatal(firstResult.err)
	}
}

func TestTransactionLimitCausesContention(t *testing.T) {
	env := newTestEngine(t, 1, 2, 200*time.Millisecond)
	env.initialize(t, "ns-a")
	env.initialize(t, "ns-b")
	changeset := sample.RoundOne().Changeset
	gate := make(chan struct{})
	ready := make(chan struct{})
	type outcome struct {
		result engine.RoundResult
		err    error
	}
	first := make(chan outcome, 1)
	go func() {
		result, err := env.engine.Apply(context.Background(), engine.ApplyRequest{Namespace: "ns-a", RoundName: "first", Changeset: changeset, ProbeGate: gate, ProbeReady: func() { close(ready) }})
		first <- outcome{result, err}
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		close(gate)
		t.Fatal("first transaction did not start")
	}
	second, err := env.engine.Apply(context.Background(), engine.ApplyRequest{Namespace: "ns-b", RoundName: "second", Changeset: changeset, WaitTimeout: 30 * time.Millisecond})
	if err == nil || second.BusinessCode != model.CodeChangeContention {
		t.Fatalf("expected transaction contention, result=%+v err=%v", second, err)
	}
	close(gate)
	firstResult := <-first
	if firstResult.err != nil {
		t.Fatal(firstResult.err)
	}
}

func join(values []string) string {
	result := ""
	for i, value := range values {
		if i > 0 {
			result += ","
		}
		result += value
	}
	return result
}

package workspace_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"maskrevision/internal/generator"
	"maskrevision/internal/model"
	"maskrevision/internal/revlog"
	"maskrevision/internal/store"
	"maskrevision/internal/workspace"
)

func TestWorkspaceReopenQueriesAndRecovery(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "main")
	if _, err := workspace.Prepare(root, true); err != nil {
		t.Fatal(err)
	}
	if _, err := generator.Generate(filepath.Join(root, "data"), generator.DefaultSeed); err != nil {
		t.Fatal(err)
	}
	ws, err := workspace.Open(root, workspace.Options{})
	if err != nil {
		t.Fatal(err)
	}
	digest := ws.Digest()
	summary, err := ws.ShardsSummary()
	if err != nil {
		t.Fatal(err)
	}
	if len(summary) != 4 {
		t.Fatalf("expected 4 shards, got %d", len(summary))
	}
	if summary[0].Checkpoints == 0 || summary[0].HeadRevision == 0 {
		t.Fatalf("missing checkpoints: %+v", summary[0])
	}
	if _, be := ws.Snapshot("shard-00", 2); be == nil || be.Code != "E_REVISION_UNDONE" {
		t.Fatalf("expected undone snapshot error, got %v", be)
	}
	if _, be := ws.Snapshot("shard-00", 99); be == nil || be.Code != "E_REVISION_OUT_OF_RANGE" {
		t.Fatalf("expected out-of-range error, got %v", be)
	}
	if _, be := ws.Snapshot("shard-00", 4); be != nil {
		t.Fatalf("snapshot 4 should be accessible: %v", be)
	}
	if _, be := ws.Diff("shard-00", 1, 4); be != nil {
		t.Fatalf("diff should work: %v", be)
	}
	if _, be := ws.Diff("shard-00", 1, 2); be == nil || be.Code != "E_REVISION_UNDONE" {
		t.Fatalf("diff to undone revision should fail, got %v", be)
	}
	if err := ws.Close(); err != nil {
		t.Fatal(err)
	}
	ws2, err := workspace.Open(root, workspace.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if got := ws2.Digest(); got != digest {
		t.Fatalf("reopen digest changed: %s != %s", got, digest)
	}
	for _, s := range mustSummary(t, ws2) {
		if s.SyncKind != "unchanged" || len(s.SyncReplayed) != 0 {
			t.Fatalf("expected checkpoint reuse for %s: %+v", s.ShardID, s)
		}
	}
	_ = ws2.Close()

	recoveryRoot := filepath.Join(base, "recovery")
	if _, err := workspace.Prepare(recoveryRoot, true); err != nil {
		t.Fatal(err)
	}
	if _, err := generator.Generate(filepath.Join(recoveryRoot, "data"), generator.DefaultSeed); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("simulated abrupt stop after committed checkpoint")
	if _, err := workspace.Open(recoveryRoot, workspace.Options{CrashAfter: 3, CrashFunc: func() error { return sentinel }}); !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel stop, got %v", err)
	}
	ws3, err := workspace.Open(recoveryRoot, workspace.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if got := ws3.Digest(); got != digest {
		t.Fatalf("recovered digest changed: %s != %s", got, digest)
	}
	if err := ws3.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUpgradeAndFixtureClassification(t *testing.T) {
	root := filepath.Join(t.TempDir(), "ws")
	if _, err := workspace.Prepare(root, true); err != nil {
		t.Fatal(err)
	}
	if _, err := generator.Generate(filepath.Join(root, "data"), generator.DefaultSeed); err != nil {
		t.Fatal(err)
	}
	ws, err := workspace.Open(root, workspace.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	up, be := ws.ExportAndVerifyV1("shard-01")
	if be != nil {
		t.Fatal(be)
	}
	if !up.TraceEqual || up.UnrecoverableCount != 0 {
		t.Fatalf("upgrade verification failed: %+v", up)
	}
	unrec, be := ws.UnrecoverableFixture()
	if be != nil {
		t.Fatal(be)
	}
	if unrec.UnrecoverableCount != 1 {
		t.Fatalf("expected one unrecoverable fixture action: %+v", unrec)
	}
	fork, be := ws.ClassifyFixture("shard-00", "fixtures/shard-00.fork.json")
	if be != nil {
		t.Fatal(be)
	}
	if fork.Result.Kind != "fork" || fork.ReplayErrorCode != "E_FORK_CONFLICT" || len(fork.ForkDecisions) == 0 {
		t.Fatalf("fork fixture classification failed: %+v", fork)
	}
	edited, be := ws.ClassifyFixture("shard-00", "fixtures/shard-00.edited.json")
	if be != nil {
		t.Fatal(be)
	}
	if edited.Result.Kind != "replacement" || edited.Result.EarliestAffectedRevision != 5 {
		t.Fatalf("edited fixture classification failed: %+v", edited.Result)
	}
	rollback, be := ws.ClassifyFixture("shard-00", "fixtures/shard-00.rollback.json")
	if be != nil {
		t.Fatal(be)
	}
	if rollback.Result.Kind != "rollback" {
		t.Fatalf("rollback fixture classification failed: %+v", rollback.Result)
	}
}

func TestEffectiveV1LogSurvivesCustomInference(t *testing.T) {
	root := filepath.Join(t.TempDir(), "ws")
	if _, err := workspace.Prepare(root, true); err != nil {
		t.Fatal(err)
	}
	if _, err := generator.Generate(filepath.Join(root, "data"), generator.DefaultSeed); err != nil {
		t.Fatal(err)
	}
	ws, err := workspace.Open(root, workspace.Options{Upgrade: revlog.UpgradeOptions{NeighborRadius: 2}})
	if err != nil {
		t.Fatal(err)
	}
	digest := ws.Digest()
	rt, be := ws.Shard("shard-01")
	if be != nil {
		t.Fatal(be)
	}
	if rt.Upgrade == nil || !strings.Contains(rt.Upgrade.NeighborRule, "radius=2") {
		t.Fatalf("custom inference was not applied: %+v", rt.Upgrade)
	}
	if err := ws.Close(); err != nil {
		t.Fatal(err)
	}
	ws2, err := workspace.Open(root, workspace.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer ws2.Close()
	if got := ws2.Digest(); got != digest {
		t.Fatalf("effective v1 log was not persisted: %s != %s", got, digest)
	}
	rt2, _ := ws2.Shard("shard-01")
	if rt2.Upgrade == nil || !strings.Contains(rt2.Upgrade.NeighborRule, "radius=2") {
		t.Fatalf("stored upgrade report was not reused: %+v", rt2.Upgrade)
	}
	if rt2.Sync.Kind != "unchanged" {
		t.Fatalf("reopen should reuse checkpoints, got sync kind %s", rt2.Sync.Kind)
	}
}

func TestPublishRaceUsesLockAndCAS(t *testing.T) {
	root := filepath.Join(t.TempDir(), "ws")
	if _, err := workspace.Prepare(root, true); err != nil {
		t.Fatal(err)
	}
	if _, err := generator.Generate(filepath.Join(root, "data"), generator.DefaultSeed); err != nil {
		t.Fatal(err)
	}
	ws, err := workspace.Open(root, workspace.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	if err := ws.Store.SeedPublication("shard-00", 1); err != nil {
		t.Fatal(err)
	}
	const workers = 4
	results := make(chan store.PublishResult, workers)
	errorsCh := make(chan error, workers)
	lockPath := store.PublishLockPath(root, "shard-00")
	for i := 0; i < workers; i++ {
		go func(i int) {
			res, be := ws.Store.Publish(lockPath, store.PublishRequest{
				ShardID: "shard-00", Revision: 2, ExpectedRevision: 1, Publisher: "test-worker",
			})
			if be != nil {
				errorsCh <- be
				return
			}
			results <- res
		}(i)
	}
	success, stale := 0, 0
	for i := 0; i < workers; i++ {
		select {
		case err := <-errorsCh:
			be := err.(interface{ Error() string })
			if be.Error() == "" {
				t.Fatal("empty error")
			}
			stale++
		case res := <-results:
			if res.Accepted {
				success++
			}
		}
	}
	if success != 1 || stale != workers-1 {
		t.Fatalf("expected one winner and %d stale results, got success=%d stale=%d", workers-1, success, stale)
	}
	state, ok, err := ws.Store.PublicationState("shard-00")
	if err != nil || !ok || state.Revision != 2 {
		t.Fatalf("publication state not advanced: %+v ok=%v err=%v", state, ok, err)
	}
}

func TestIncrementalInvalidationKeepsEarlierCheckpoints(t *testing.T) {
	root := filepath.Join(t.TempDir(), "ws")
	if _, err := workspace.Prepare(root, true); err != nil {
		t.Fatal(err)
	}
	if _, err := generator.Generate(filepath.Join(root, "data"), generator.DefaultSeed); err != nil {
		t.Fatal(err)
	}
	ws, err := workspace.Open(root, workspace.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	rt, be := ws.Shard("shard-00")
	if be != nil {
		t.Fatal(be)
	}
	baseSeq := checkpointSeq(t, ws, "shard-00", 4)
	log := rt.Log
	log.Revisions = append([]model.Revision(nil), log.Revisions...)
	log.Revisions = append(log.Revisions, model.Revision{
		Revision: 9, ParentRevision: 8, Action: model.ActionRepaint,
		Region: model.Region{X: 33, Y: 20, W: 5, H: 6}, CategoryID: 6, Confidence: 0.88, Actor: "append",
	})
	encoded, err := revlog.EncodeV2(log)
	if err != nil {
		t.Fatal(err)
	}
	report, be := ws.ApplyIncomingLog("shard-00", encoded)
	if be != nil {
		t.Fatal(be)
	}
	if report.Kind != "append" || report.HeadRevision != 9 || len(report.ReplayedRevisions) != 1 || report.ReplayedRevisions[0] != 9 {
		t.Fatalf("unexpected append report: %+v", report)
	}
	if got := checkpointSeq(t, ws, "shard-00", 4); got != baseSeq {
		t.Fatalf("earlier checkpoint was rebuilt: seq %d != %d", got, baseSeq)
	}

	log.Revisions[4].Region = model.Region{X: 33, Y: 12, W: 5, H: 5}
	encoded, err = revlog.EncodeV2(log)
	if err != nil {
		t.Fatal(err)
	}
	report, be = ws.ApplyIncomingLog("shard-00", encoded)
	if be != nil {
		t.Fatal(be)
	}
	if report.Kind != "replacement" || report.EarliestAffected != 5 || report.RemovedCheckpoints != 5 {
		t.Fatalf("unexpected replacement report: %+v", report)
	}
	if got := checkpointSeq(t, ws, "shard-00", 4); got != baseSeq {
		t.Fatalf("checkpoint 4 should be reused, seq %d != %d", got, baseSeq)
	}
	if _, be := ws.Snapshot("shard-00", 7); be != nil {
		t.Fatalf("replacement snapshot missing: %v", be)
	}

	log.Revisions = log.Revisions[:6]
	encoded, err = revlog.EncodeV2(log)
	if err != nil {
		t.Fatal(err)
	}
	report, be = ws.ApplyIncomingLog("shard-00", encoded)
	if be != nil {
		t.Fatal(be)
	}
	if report.Kind != "rollback" || report.HeadRevision != 6 || report.RemovedCheckpoints != 3 || len(report.ReplayedRevisions) != 0 {
		t.Fatalf("unexpected rollback report: %+v", report)
	}
}

func TestForcedKillRecovery(t *testing.T) {
	if os.Getenv("MASKREV_CRASH_CHILD") == "1" {
		root := os.Getenv("MASKREV_CRASH_WORK")
		_, err := workspace.Open(root, workspace.Options{CrashAfter: 3, CrashFunc: func() error {
			process, err := os.FindProcess(os.Getpid())
			if err != nil {
				return err
			}
			_ = process.Kill()
			time.Sleep(5 * time.Second)
			return fmt.Errorf("forced process kill did not terminate")
		}})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(3)
		}
		os.Exit(4)
	}
	base := t.TempDir()
	baselineRoot := filepath.Join(base, "baseline")
	if _, err := workspace.Prepare(baselineRoot, true); err != nil {
		t.Fatal(err)
	}
	if _, err := generator.Generate(filepath.Join(baselineRoot, "data"), generator.DefaultSeed); err != nil {
		t.Fatal(err)
	}
	baseline, err := workspace.Open(baselineRoot, workspace.Options{})
	if err != nil {
		t.Fatal(err)
	}
	wantDigest := baseline.Digest()
	_ = baseline.Close()

	recoveryRoot := filepath.Join(base, "killed")
	if _, err := workspace.Prepare(recoveryRoot, true); err != nil {
		t.Fatal(err)
	}
	if _, err := generator.Generate(filepath.Join(recoveryRoot, "data"), generator.DefaultSeed); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestForcedKillRecovery$", "-test.count=1")
	cmd.Env = append(os.Environ(), "MASKREV_CRASH_CHILD=1", "MASKREV_CRASH_WORK="+recoveryRoot)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("crash helper exited successfully; output=%s", out)
	}
	recovered, err := workspace.Open(recoveryRoot, workspace.Options{})
	if err != nil {
		t.Fatalf("open after force kill: %v (child output=%s)", err, out)
	}
	defer recovered.Close()
	if got := recovered.Digest(); got != wantDigest {
		t.Fatalf("recovered digest %s != baseline %s", got, wantDigest)
	}
	baselineAgain, err := workspace.Open(baselineRoot, workspace.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer baselineAgain.Close()
	assertSnapshotsByteEqual(t, baselineAgain, recovered)
}

func assertSnapshotsByteEqual(t *testing.T, a, b *workspace.Workspace) {
	t.Helper()
	for _, shardID := range []string{"shard-00", "shard-01", "shard-02", "shard-03"} {
		aMetas, err := a.Store.List(shardID)
		if err != nil {
			t.Fatal(err)
		}
		bMetas, err := b.Store.List(shardID)
		if err != nil {
			t.Fatal(err)
		}
		if len(aMetas) != len(bMetas) {
			t.Fatalf("%s checkpoint count differs: %d != %d", shardID, len(aMetas), len(bMetas))
		}
		for i := range aMetas {
			aCP, _, err := a.Store.At(shardID, aMetas[i].Revision)
			if err != nil {
				t.Fatal(err)
			}
			bCP, _, err := b.Store.At(shardID, bMetas[i].Revision)
			if err != nil {
				t.Fatal(err)
			}
			aJSON, _ := json.Marshal(aCP.Snapshot)
			bJSON, _ := json.Marshal(bCP.Snapshot)
			if string(aJSON) != string(bJSON) {
				t.Fatalf("%s revision %d snapshot bytes differ", shardID, aMetas[i].Revision)
			}
		}
	}
}

func checkpointSeq(t *testing.T, ws *workspace.Workspace, shardID string, revision int) int64 {
	t.Helper()
	cp, ok, err := ws.Store.At(shardID, revision)
	if err != nil || !ok {
		t.Fatalf("checkpoint %s@%d: ok=%v err=%v", shardID, revision, ok, err)
	}
	return cp.Seq
}

func mustSummary(t *testing.T, ws *workspace.Workspace) []workspace.ShardSummary {
	t.Helper()
	s, err := ws.ShardsSummary()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"motionlab/internal/engine"
	"motionlab/internal/model"
)

func canonicalBytes(t *testing.T, report *model.RunReport) []byte {
	t.Helper()
	b, err := json.Marshal(model.CanonicalReport(*report))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDeterministicReplayAndPruneBudgetEquivalence(t *testing.T) {
	cat, _, eng := newHarness(t)
	ctx := context.Background()
	run, err := eng.EnsureRun(ctx, "harbor_slide", cat.Policy, model.RunConfig{SequenceID: "harbor_slide"}, true)
	if err != nil {
		t.Fatal(err)
	}
	first, err := eng.Run(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	run, err = eng.EnsureRun(ctx, "harbor_slide", cat.Policy, model.RunConfig{SequenceID: "harbor_slide"}, true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := eng.Run(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if first.DeterministicSHA != second.DeterministicSHA || !bytes.Equal(canonicalBytes(t, first), canonicalBytes(t, second)) {
		t.Fatalf("deterministic replay mismatch: %s != %s", first.DeterministicSHA, second.DeterministicSHA)
	}
	var survivors []int
	for _, b := range second.Branches {
		if b.Status != model.BranchPruned {
			survivors = append(survivors, b.BranchID)
		}
	}
	if len(survivors) == len(second.Branches) {
		t.Fatal("test requires at least one pruned branch")
	}
	baselineRun, err := eng.EnsureRun(ctx, "harbor_slide", cat.Policy, model.RunConfig{SequenceID: "harbor_slide", BranchFilter: survivors}, true)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := eng.Run(ctx, baselineRun.RunID)
	if err != nil {
		t.Fatal(err)
	}
	fullByID := map[int]model.BranchReport{}
	baseByID := map[int]model.BranchReport{}
	for _, b := range second.Branches {
		fullByID[b.BranchID] = b
	}
	for _, b := range baseline.Branches {
		baseByID[b.BranchID] = b
	}
	for _, id := range survivors {
		a, b := fullByID[id], baseByID[id]
		if a.Status != b.Status || a.QuotaUsed != b.QuotaUsed || a.ReclaimedQuota != b.ReclaimedQuota ||
			model.Digest(a.Steps) != model.Digest(b.Steps) || model.Digest(a.ErrorCurve) != model.Digest(b.ErrorCurve) {
			t.Fatalf("pruned branch dispatch changed survivor %d", id)
		}
	}
}

func TestGracefulStopResumeEqualsContinuous(t *testing.T) {
	cat, st, _ := newHarness(t)
	var cancel context.CancelFunc
	var commits atomic.Int64
	eng := engine.New(cat, st, engine.Options{AfterCommit: func(runID string, seq int64) {
		if commits.Add(1) == 7 && cancel != nil {
			cancel()
		}
	}})
	ctx := context.Background()
	run, err := eng.EnsureRun(ctx, "harbor_slide", cat.Policy, model.RunConfig{SequenceID: "harbor_slide"}, true)
	if err != nil {
		t.Fatal(err)
	}
	continuous, err := eng.Run(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	commits.Store(0)
	run, err = eng.EnsureRun(ctx, "harbor_slide", cat.Policy, model.RunConfig{SequenceID: "harbor_slide"}, true)
	if err != nil {
		t.Fatal(err)
	}
	stopCtx, stop := context.WithCancel(context.Background())
	cancel = stop
	paused, err := eng.Run(stopCtx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if paused.Status != "paused" {
		t.Fatalf("expected paused run, got %s", paused.Status)
	}
	cancel = nil
	recovered, err := eng.Run(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if continuous.DeterministicSHA != recovered.DeterministicSHA {
		t.Fatalf("resume mismatch: %s != %s", continuous.DeterministicSHA, recovered.DeterministicSHA)
	}
	if !bytes.Equal(canonicalBytes(t, continuous), canonicalBytes(t, recovered)) {
		t.Fatal("canonical recovered report bytes differ from continuous execution")
	}
}

func TestOverflowSharedGuardAndPolicyClassification(t *testing.T) {
	cat, _, eng := newHarness(t)
	ctx := context.Background()
	seq := cat.Sequences["orchid_drift"]
	_, err := eng.EnsureRun(ctx, "orchid_drift", cat.Policy, model.RunConfig{SequenceID: "orchid_drift", RequestedSteps: seq.Spec.TotalFrames}, true)
	if err == nil || model.AsAppError(err).Code != model.CodeTimelineOverflow {
		t.Fatalf("expected timeline overflow, got %v", err)
	}
	if details := model.AsAppError(err).Details; details["overflow_steps"] != 1 {
		t.Fatalf("wrong overflow details: %v", details)
	}
	if appErr := seq.Shared.Write("orchid_drift", 2, 0, []byte{9}); appErr == nil || appErr.SequenceID != "orchid_drift" || appErr.BranchID == nil || *appErr.BranchID != 2 {
		t.Fatalf("shared write error missing structured context: %#v", appErr)
	}

	run, err := eng.EnsureRun(ctx, "signal_turn", cat.Policy, model.RunConfig{SequenceID: "signal_turn", RequestedSteps: 4, BranchFilter: []int{0}, DisablePrune: true}, true)
	if err != nil {
		t.Fatal(err)
	}
	v2 := cat.Policy
	v2.Version = 2
	v2.ParentVersion = 1
	v2.ParentHash = cat.Policy.Hash()
	v2.PolicyID = "metadata-v2"
	d, err := eng.ApplyPolicy(ctx, run.RunID, v2)
	if err != nil || d.Classification != "advance" {
		t.Fatalf("advance classification failed: %#v %v", d, err)
	}
	_, err = eng.ApplyPolicy(ctx, run.RunID, cat.Policy)
	if err == nil || model.AsAppError(err).Code != model.CodePolicyRollback {
		t.Fatalf("expected rollback rejection, got %v", err)
	}
	v3 := v2
	v3.Version = 3
	v3.ParentVersion = 2
	v3.ParentHash = v2.Hash()
	v3.PolicyID = "replacement-v3"
	v3.PruneThreshold = 1.1
	d, err = eng.ApplyPolicy(ctx, run.RunID, v3)
	if err != nil || d.Classification != "replacement" || !d.Invalidated {
		t.Fatalf("replacement classification failed: %#v %v", d, err)
	}
	reloaded, err := eng.Store().GetRun(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.PolicyEpoch != 2 || reloaded.Status != "pending" {
		t.Fatalf("replacement did not invalidate run: epoch=%d status=%s", reloaded.PolicyEpoch, reloaded.Status)
	}
}

func TestStageModePausesAtWaveBoundaryAndResumes(t *testing.T) {
	cat, _, eng := newHarness(t)
	ctx := context.Background()
	continuousRun, err := eng.EnsureRun(ctx, "harbor_slide", cat.Policy, model.RunConfig{SequenceID: "harbor_slide"}, true)
	if err != nil {
		t.Fatal(err)
	}
	continuous, err := eng.Run(ctx, continuousRun.RunID)
	if err != nil {
		t.Fatal(err)
	}
	stagedRun, err := eng.EnsureRun(ctx, "harbor_slide", cat.Policy, model.RunConfig{SequenceID: "harbor_slide", StageMode: true}, true)
	if err != nil {
		t.Fatal(err)
	}
	stageOne, err := eng.Run(ctx, stagedRun.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if stageOne.Status != "paused" {
		t.Fatalf("stage one should pause at wave boundary, got %s", stageOne.Status)
	}
	if stageOne.Summary.CompletedSteps >= continuous.Summary.CompletedSteps {
		t.Fatalf("stage one should not execute all waves: staged=%d continuous=%d", stageOne.Summary.CompletedSteps, continuous.Summary.CompletedSteps)
	}
	stageTwo, err := eng.Run(ctx, stagedRun.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if stageTwo.Status != "completed" {
		t.Fatalf("resumed stage should complete, got %s", stageTwo.Status)
	}
	fullByID := map[int]model.BranchReport{}
	for _, b := range continuous.Branches {
		fullByID[b.BranchID] = b
	}
	for _, b := range stageTwo.Branches {
		a := fullByID[b.BranchID]
		if a.Status != b.Status || model.Digest(a.Steps) != model.Digest(b.Steps) || model.Digest(a.ErrorCurve) != model.Digest(b.ErrorCurve) {
			t.Fatalf("staged branch %d differs from continuous execution", b.BranchID)
		}
	}
}

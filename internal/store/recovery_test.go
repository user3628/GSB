package store

import (
	"context"
	"path/filepath"
	"testing"

	"h2diag/internal/engine"
	"h2diag/internal/model"
)

func TestRecoverAbortsIntentWithoutCommitAtLastBoundary(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "recovery.sqlite")
	first, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	plan := recoveryPlan(t, "recovery-abort-v1")
	if err := first.CreateSample(ctx, plan); err != nil {
		t.Fatal(err)
	}
	first.SetAfterIntentHook(func(commit model.PlanCommit) {
		if commit.Round != nil {
			panic("simulated forced termination")
		}
	})
	lastApplied := int64(0)
	crashedVersion := int64(0)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected simulated crash panic")
			}
		}()
		for _, commit := range plan.Commits {
			if commit.Round != nil {
				crashedVersion = commit.Version
				_ = first.ApplyCommit(ctx, plan.Sample.SampleID, commit)
				return
			}
			if err := first.ApplyCommit(ctx, plan.Sample.SampleID, commit); err != nil {
				t.Fatal(err)
			}
			lastApplied = commit.Version
		}
	}()
	if crashedVersion == 0 {
		t.Fatal("test sample did not reach a scheduling round")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen and recover: %v", err)
	}
	defer reopened.Close()
	meta, exists, err := reopened.GetSampleMeta(ctx, plan.Sample.SampleID)
	if err != nil || !exists {
		t.Fatalf("meta exists=%t err=%v", exists, err)
	}
	if meta.Status != "ABORTED" || meta.Version != lastApplied {
		t.Fatalf("meta=%#v, want ABORTED version %d", meta, lastApplied)
	}
	snapshot, _, err := reopened.LoadSnapshot(ctx, plan.Sample.SampleID, true)
	if err != nil {
		t.Fatalf("load recovered snapshot: %v", err)
	}
	if snapshot.Version != lastApplied || snapshot.Streams[0].TotalSent != 0 {
		t.Fatalf("recovered snapshot is not the last complete boundary: %#v", snapshot)
	}
	reports, err := reopened.ListRecoveryReports(ctx, plan.Sample.SampleID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 {
		t.Fatalf("reports=%d, want 1", len(reports))
	}
	if reports[0].Outcome != "ABORTED_AT_LAST_COMPLETE_BOUNDARY" || reports[0].Version != lastApplied {
		t.Fatalf("unexpected recovery report: %#v", reports[0])
	}
	var intentStatus string
	if err := reopened.DB().QueryRowContext(ctx, `
SELECT status FROM intents WHERE sample_id=? AND version=? ORDER BY intent_id DESC LIMIT 1`,
		plan.Sample.SampleID, crashedVersion).Scan(&intentStatus); err != nil {
		t.Fatal(err)
	}
	if intentStatus != "ABORTED" {
		t.Fatalf("crashed intent status=%s, want ABORTED", intentStatus)
	}
}

func TestRecoverRepairsCommittedIntent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "recovery-committed.sqlite")
	first, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	plan := recoveryPlan(t, "recovery-committed-v1")
	if err := first.CreateSample(ctx, plan); err != nil {
		t.Fatal(err)
	}
	commit := plan.Commits[0]
	if err := first.ApplyCommit(ctx, plan.Sample.SampleID, commit); err != nil {
		t.Fatal(err)
	}
	if _, err := first.DB().ExecContext(ctx, `
UPDATE intents SET status='RUNNING' WHERE sample_id=? AND version=?`, plan.Sample.SampleID, commit.Version); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen and recover: %v", err)
	}
	defer reopened.Close()
	reports, err := reopened.ListRecoveryReports(ctx, plan.Sample.SampleID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].Outcome != "COMMITTED_INTENT_REPAIRED" {
		t.Fatalf("unexpected recovery reports: %#v", reports)
	}
	var intentStatus string
	if err := reopened.DB().QueryRowContext(ctx, `
SELECT status FROM intents WHERE sample_id=? AND version=? AND status='COMMITTED'`,
		plan.Sample.SampleID, commit.Version).Scan(&intentStatus); err != nil {
		t.Fatal(err)
	}
	if intentStatus != "COMMITTED" {
		t.Fatalf("intent status=%s, want COMMITTED", intentStatus)
	}
}

func TestFinalizeIntentIsNotCommittedUntilSampleIsDone(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "recovery-finalize.sqlite")
	first, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	plan := recoveryPlan(t, "recovery-finalize-v1")
	if err := first.CreateSample(ctx, plan); err != nil {
		t.Fatal(err)
	}
	for _, commit := range plan.Commits {
		if err := first.ApplyCommit(ctx, plan.Sample.SampleID, commit); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := first.beginIntent(ctx, plan.Sample.SampleID, plan.Final.Version, "FINALIZE", plan.Digest); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen and recover: %v", err)
	}
	defer reopened.Close()
	meta, exists, err := reopened.GetSampleMeta(ctx, plan.Sample.SampleID)
	if err != nil || !exists {
		t.Fatalf("meta exists=%t err=%v", exists, err)
	}
	if meta.Status != "ABORTED" || meta.Version != plan.Final.Version {
		t.Fatalf("meta=%#v, want ABORTED at final committed version", meta)
	}
	reports, err := reopened.ListRecoveryReports(ctx, plan.Sample.SampleID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].IntentStatus != "ABORTED" ||
		reports[0].Outcome != "ABORTED_AT_LAST_COMPLETE_BOUNDARY" {
		t.Fatalf("unexpected finalize recovery report: %#v", reports)
	}
}

func TestRecoverWithoutIntentReportsCorruptCheckpoint(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "recovery-running-corrupt.sqlite")
	first, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	plan := recoveryPlan(t, "recovery-running-corrupt-v1")
	if err := first.CreateSample(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if err := first.ApplyCommit(ctx, plan.Sample.SampleID, plan.Commits[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := first.DB().ExecContext(ctx, `
UPDATE intents SET status='COMMITTED' WHERE sample_id=?`, plan.Sample.SampleID); err != nil {
		t.Fatal(err)
	}
	if _, err := first.DB().ExecContext(ctx, `
UPDATE streams SET window_remaining=window_remaining+1 WHERE sample_id=?`, plan.Sample.SampleID); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen and recover: %v", err)
	}
	defer reopened.Close()
	reports, err := reopened.ListRecoveryReports(ctx, plan.Sample.SampleID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].Outcome != model.CodeCheckpointCorrupt {
		t.Fatalf("unexpected recovery reports: %#v", reports)
	}
	if reports[0].Detail["checkpoint_digest_valid"] != false ||
		reports[0].Detail["error_code"] != model.CodeCheckpointCorrupt {
		t.Fatalf("recovery report did not identify corruption: %#v", reports[0].Detail)
	}
}

func recoveryPlan(t *testing.T, sampleID string) *model.Plan {
	t.Helper()
	config := model.DefaultSampleConfig()
	config.RoundBytes = 8
	sample := &model.Sample{
		SampleID: sampleID,
		Config:   config,
		Connections: []model.ConnectionSpec{{
			ID:                      "loopback",
			InitialConnectionWindow: 1000,
			MaxConnectionWindow:     model.MaxHTTP2StreamID,
			InitialStreamWindow:     1000,
			MaxStreamWindow:         model.MaxHTTP2StreamID,
			PriorityLevels:          2,
		}},
		Streams: []model.StreamSpec{{ConnectionID: "loopback", StreamID: 1, Weight: 1, Priority: 0}},
		Frames: []model.Frame{
			{Sequence: 1, Timestamp: 0, ConnectionID: "loopback", StreamID: 0, Type: model.FrameSettings, Flags: []string{}},
			{Sequence: 2, Timestamp: 1, ConnectionID: "loopback", StreamID: 1, Type: model.FrameHeaders, Flags: []string{"END_HEADERS"}},
			{Sequence: 3, Timestamp: 2, ConnectionID: "loopback", StreamID: 1, Type: model.FrameData, Flags: []string{}, PayloadBytes: 32},
		},
	}
	plan, biz := engine.Compile(sample)
	if biz != nil {
		t.Fatal(biz)
	}
	return plan
}

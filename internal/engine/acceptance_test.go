package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"featureplatform/internal/configx"
	"featureplatform/internal/engine"
	"featureplatform/internal/model"
	"featureplatform/internal/samplegen"
	"featureplatform/internal/store"
)

func mustOpen(t *testing.T) *engine.Engine {
	t.Helper()
	db := filepath.Join(t.TempDir(), "migration.db")
	eng, err := engine.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

func seed(t *testing.T, eng *engine.Engine) configx.Config {
	t.Helper()
	ctx := context.Background()
	shards := samplegen.GenerateShards()
	if _, err := eng.Store.ImportShards(ctx, shards, "seed"); err != nil {
		t.Fatal(err)
	}
	cfg := samplegen.DefaultConfig()
	if _, err := eng.CommitInitialConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func runAll(t *testing.T, eng *engine.Engine) {
	t.Helper()
	report, err := eng.RunMigration(context.Background(), engine.RunOptions{Budget: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if report.HasMore {
		t.Fatalf("migration still has deferred units: %+v", report)
	}
	if report.TotalSamples != 43 {
		t.Fatalf("total samples = %d, want 43", report.TotalSamples)
	}
	summary, err := eng.Summary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !summary.ConservationOK || summary.TotalSamples != 43 {
		t.Fatalf("conservation failed: %+v", summary)
	}
	if summary.TotalRejectedRecords == 0 {
		t.Fatal("expected deterministic rejected samples")
	}
}

func TestThreeTaskMappingStaleReadAndIncrementalInvalidation(t *testing.T) {
	eng := mustOpen(t)
	ctx := context.Background()
	seed(t, eng)
	runAll(t, eng)

	_, digest1, n, err := eng.ExportCanonical(ctx)
	if err != nil || n != 43 {
		t.Fatalf("export n=%d err=%v", n, err)
	}

	rec, coded := eng.Store.ReadAdjudicated(ctx, "TC-0001", 0)
	if coded == nil || coded.Code != model.CodeStaleRead || coded.CurrentVersion != 1 {
		t.Fatalf("expected E_STALE_READ v1, got rec=%v err=%v", rec, coded)
	}
	if coded.MinimumAcceptableVersion != 1 {
		t.Fatalf("min acceptable = %d, want 1", coded.MinimumAcceptableVersion)
	}

	rec, coded = eng.Store.ReadAdjudicated(ctx, "TC-0001", 1)
	if coded != nil || rec == nil {
		t.Fatalf("current read failed: %+v", coded)
	}
	if rec.RecordDigest == "" || len(rec.Features) == 0 {
		t.Fatalf("feature record is empty: %+v", rec)
	}

	rawVersion := json.RawMessage(`2`)
	fragment := configx.Fragment{
		FragmentID:  "inc-sentiment-v2",
		BaseVersion: 1,
		NewVersion:  2,
		Reason:      "advance sentiment task mapping",
		Changes: []configx.Change{{
			Op: "replace", Path: "/task_versions/short_text_sentiment", Value: rawVersion,
		}},
	}
	_, plan, err := eng.SubmitFragment(ctx, fragment)
	if err != nil {
		t.Fatal(err)
	}
	if plan.FullRebuild || len(plan.AffectedTasks) != 1 || plan.AffectedTasks[0] != samplegen.TaskSentiment {
		t.Fatalf("expected only sentiment task, got %+v", plan)
	}

	mplan, err := eng.PlanMigration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, unit := range mplan.Units {
		t.Logf("affected sample=%s task=%s stage=%s reason=%s", unit.Stored.Sample.SampleID, unit.Stored.Sample.Task, unit.EarliestStage, unit.Reason)
	}
	if len(mplan.Units) != 15 || mplan.ReusedCount != 28 {
		t.Fatalf("affected=%d reused=%d, want 15/28", len(mplan.Units), mplan.ReusedCount)
	}

	_, coded = eng.Store.ReadAdjudicated(ctx, "SA-0001", 1)
	if coded == nil || coded.Code != model.CodeStaleRead {
		t.Fatalf("expected old observed version stale read, got %+v", coded)
	}
	_, coded = eng.Store.ReadAdjudicated(ctx, "SA-0001", 2)
	if coded == nil || coded.Code != model.CodeStaleCheckpoint || coded.MissingStage == "" {
		t.Fatalf("expected E_STALE_CHECKPOINT with stage, got %+v", coded)
	}
	rec, coded = eng.Store.ReadAdjudicated(ctx, "TC-0001", 2)
	if coded != nil || rec == nil {
		t.Fatalf("unaffected classification should be directly reusable: %+v", coded)
	}

	report, err := eng.RunMigration(ctx, engine.RunOptions{Budget: 100})
	if err != nil {
		t.Fatal(err)
	}
	if report.RebuiltCount != 15 || report.ReusedCount != 28 {
		t.Fatalf("rebuilt=%d reused=%d want 15/28", report.RebuiltCount, report.ReusedCount)
	}
	rec, coded = eng.Store.ReadAdjudicated(ctx, "SA-0001", 2)
	if coded != nil || rec == nil {
		t.Fatalf("rebuilt sentiment read failed: %+v", coded)
	}
	_, digest2, _, err := eng.ExportCanonical(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if digest1 == digest2 {
		t.Fatal("export digest should change after sentiment version invalidation")
	}
}

func TestCrashRecoveryExportMatchesContinuousRun(t *testing.T) {
	ctx := context.Background()

	continuousPath := filepath.Join(t.TempDir(), "continuous.db")
	continuous, err := engine.Open(continuousPath)
	if err != nil {
		t.Fatal(err)
	}
	shards := samplegen.GenerateShards()
	if _, err := continuous.Store.ImportShards(ctx, shards, "seed"); err != nil {
		t.Fatal(err)
	}
	if _, err := continuous.CommitInitialConfig(ctx, samplegen.DefaultConfig()); err != nil {
		t.Fatal(err)
	}
	if _, err := continuous.RunMigration(ctx, engine.RunOptions{Budget: 1000}); err != nil {
		t.Fatal(err)
	}
	_, continuousDigest, _, err := continuous.ExportCanonical(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = continuous.Close()

	recoveryPath := filepath.Join(t.TempDir(), "recovery.db")
	recovery, err := engine.Open(recoveryPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recovery.Store.ImportShards(ctx, shards, "seed"); err != nil {
		t.Fatal(err)
	}
	if _, err := recovery.CommitInitialConfig(ctx, samplegen.DefaultConfig()); err != nil {
		t.Fatal(err)
	}

	ready := filepath.Join(t.TempDir(), "ready")
	hangCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := recovery.RunMigration(hangCtx, engine.RunOptions{
			Budget: 1000, HangAfterCheckpointWrites: 2, ReadyFile: ready,
		})
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(ready); err != nil {
		t.Fatalf("hang readiness file was not created: %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("hung run returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("hung migration did not stop after cancel")
	}

	if _, err := recovery.RunMigration(ctx, engine.RunOptions{Budget: 1000}); err != nil {
		t.Fatal(err)
	}
	_, recoveredDigest, n, err := recovery.ExportCanonical(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 43 || recoveredDigest != continuousDigest {
		t.Fatalf("recovery digest=%s n=%d continuous=%s", recoveredDigest, n, continuousDigest)
	}
}

func TestConcurrentConfigCommitsUseConditionalVersion(t *testing.T) {
	eng := mustOpen(t)
	ctx := context.Background()
	if _, err := eng.Store.ImportShards(ctx, samplegen.GenerateShards(), "seed"); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.CommitInitialConfig(ctx, samplegen.DefaultConfig()); err != nil {
		t.Fatal(err)
	}
	dbPath := eng.Store.DBPath()
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	const workers = 8
	stores := make([]*store.Store, workers)
	for i := 0; i < workers; i++ {
		st, err := store.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		stores[i] = st
		t.Cleanup(func() { _ = st.Close() })
	}
	results := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func(st *store.Store) {
			cfg := samplegen.DefaultConfig()
			cfg.ConfigVersion = 2
			_, err := st.CommitConfig(ctx, store.ConfigCommit{Config: cfg})
			results <- err
		}(stores[i])
	}
	var success, stale int
	for i := 0; i < workers; i++ {
		err := <-results
		if err == nil {
			success++
			continue
		}
		var coded *model.CodedError
		if errors.As(err, &coded) && coded.Code == model.CodeStaleVersion {
			stale++
		} else {
			t.Fatalf("unexpected concurrent commit error: %T %v", err, err)
		}
	}
	if success != 1 || stale != workers-1 {
		t.Fatalf("success=%d stale=%d, want 1/%d", success, stale, workers-1)
	}
}

func TestRollbackAndWholeMappingReplacementForceFullPlan(t *testing.T) {
	old := samplegen.DefaultConfig()
	next := old
	next.ConfigVersion = 2
	next.TaskVersions = map[string]int{}
	for k, v := range old.TaskVersions {
		next.TaskVersions[k] = v
	}
	next.TaskVersions[samplegen.TaskClassification] = 1
	fragment := configx.Fragment{Changes: []configx.Change{{
		Op: "replace", Path: "/task_versions/text_classification", Value: json.RawMessage(`1`),
	}}}
	plan := configx.PlanFragment(old, next, fragment)
	if !plan.FullRebuild {
		t.Fatalf("task version rollback must force full rebuild: %+v", plan)
	}

	replace := configx.Fragment{Changes: []configx.Change{{Op: "replace", Path: "/task_mappings", Value: json.RawMessage(`{}`)}}}
	plan = configx.PlanFragment(old, configx.Config{}, replace)
	if !plan.FullRebuild {
		t.Fatalf("whole mapping replacement must force full rebuild: %+v", plan)
	}
}

func TestCrossTaskMisalignedAndStaleRecordCommitRejected(t *testing.T) {
	eng := mustOpen(t)
	ctx := context.Background()
	seed(t, eng)
	runAll(t, eng)

	row, err := eng.Store.GetValidRecord(ctx, "TC-0001")
	if err != nil || row == nil {
		t.Fatalf("get record row=%v err=%v", row, err)
	}
	var taskSeq int64
	for _, stored := range mustListSamples(t, eng) {
		if stored.Sample.SampleID == "TC-0001" {
			taskSeq = stored.TaskSequence
		}
	}

	_, err = eng.Store.CommitRecord(ctx, store.RecordCommit{
		Record: row.Record, Task: samplegen.TaskSentiment, TaskSequence: taskSeq,
		ExpectedValidID: row.ValidID,
	})
	var coded *model.CodedError
	if !errors.As(err, &coded) || coded.Code != model.CodeStaleVersion {
		t.Fatalf("cross-task commit err=%v, want E_STALE_VERSION", err)
	}

	_, err = eng.Store.CommitRecord(ctx, store.RecordCommit{
		Record: row.Record, Task: samplegen.TaskClassification, TaskSequence: taskSeq,
		ExpectedValidID: row.ValidID + 1000,
	})
	if !errors.As(err, &coded) || coded.Code != model.CodeStaleVersion {
		t.Fatalf("stale valid id err=%v, want E_STALE_VERSION", err)
	}
}

func mustListSamples(t *testing.T, eng *engine.Engine) []store.StoredSample {
	t.Helper()
	samples, err := eng.Store.ListSamples(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return samples
}

package app_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"preprocesspipeline/internal/app"
	"preprocesspipeline/internal/domain"
	"preprocesspipeline/internal/store"
	"preprocesspipeline/internal/verify"
)

func newTestService(t *testing.T) (*app.Service, *store.Store, context.Context) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := app.NewService(st)
	if _, err := svc.EnsureDefaultPolicy(ctx); err != nil {
		t.Fatalf("default policy: %v", err)
	}
	return svc, st, ctx
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var berr *domain.BusinessError
	if !errors.As(err, &berr) {
		t.Fatalf("expected business error %s, got %v", code, err)
	}
	if berr.Code != code {
		t.Fatalf("expected code %s, got %s (%v)", code, berr.Code, err)
	}
}

func TestHappyPathVerifiesAllStages(t *testing.T) {
	svc, st, ctx := newTestService(t)
	resp, err := svc.SubmitBatch(ctx, app.BatchRequest{BatchID: "happy"})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if resp.Status != "committed" || len(resp.Stages) != 5 || len(resp.Shards) != 4 {
		t.Fatalf("unexpected response: status=%s stages=%d shards=%d", resp.Status, len(resp.Stages), len(resp.Shards))
	}
	report, err := verify.VerifyBatch(ctx, st, "happy", 1)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !report.OK || report.CheckedStages != 5 || report.CheckedShards != 4 {
		t.Fatalf("verification failed: %+v", report)
	}
}

func TestInjectionBusinessCodes(t *testing.T) {
	svc, _, ctx := newTestService(t)
	tests := []struct {
		name string
		req  app.BatchRequest
		code string
	}{
		{"malformed", app.BatchRequest{BatchID: "malformed", Inject: "record_invalid"}, domain.CodeRecordInvalid},
		{"label", app.BatchRequest{BatchID: "label", Inject: "invalid_label"}, domain.CodeLabelInvalid},
		{"shard-key", app.BatchRequest{BatchID: "shard-key", Inject: "missing_shard_key"}, domain.CodeShardKeyMissing},
		{"ratio", app.BatchRequest{BatchID: "ratio", Inject: "sample_ratio_invalid"}, domain.CodeSampleRatioInvalid},
		{"id-conflict", app.BatchRequest{BatchID: "id-conflict", Inject: "id_conflict"}, domain.CodeIDConflict},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.SubmitBatch(ctx, tc.req)
			requireCode(t, err, tc.code)
		})
	}
}

func TestInterruptRecoveryFacts(t *testing.T) {
	svc, st, ctx := newTestService(t)
	_, err := svc.SubmitBatch(ctx, app.BatchRequest{
		BatchID: "interrupted", Inject: "stage_interrupted", InterruptStage: domain.StageTensorize,
	})
	requireCode(t, err, domain.CodeStageInterrupted)

	stages, err := st.ListStageSummaries(ctx, "interrupted", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(stages) != 4 {
		t.Fatalf("expected four complete stage summaries, got %d", len(stages))
	}
	shards, err := st.ListShards(ctx, "interrupted", 1, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != 0 {
		t.Fatalf("expected no visible shards, got %d", len(shards))
	}

	report, err := svc.Recover(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	if report.UnfinishedCount != 1 {
		t.Fatalf("expected one unfinished intent, got %d", report.UnfinishedCount)
	}
	recovered := report.RecoveredIntents[0]
	if recovered.BatchID != "interrupted" || recovered.LastCompleteStage != domain.StageSample {
		t.Fatalf("unexpected recovery fact: %+v", recovered)
	}
	if len(recovered.ShardsToRebuild) != 4 || !recovered.TempDir.Removed {
		t.Fatalf("expected four rebuild shards and temp removal: %+v", recovered)
	}
	batch, err := st.GetBatch(ctx, "interrupted")
	if err != nil {
		t.Fatal(err)
	}
	if batch.Status != "aborted" || batch.ErrorCode != domain.CodeStageInterrupted {
		t.Fatalf("batch not aborted correctly: %+v", batch)
	}
}

func TestConcurrentSameBatchExecutesOnce(t *testing.T) {
	svc, _, ctx := newTestService(t)
	const workers = 24
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	intents := make(map[string]struct{})
	errs := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			resp, err := svc.SubmitBatch(ctx, app.BatchRequest{BatchID: "concurrent"})
			if err != nil {
				errs <- err
				return
			}
			mu.Lock()
			intents[resp.IntentID] = struct{}{}
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent submit: %v", err)
	}
	if len(intents) != 1 {
		t.Fatalf("expected one intent, got %d: %v", len(intents), intents)
	}
}

func TestPolicyPatchCascadeAndDigestFault(t *testing.T) {
	svc, st, ctx := newTestService(t)
	if _, err := svc.SubmitBatch(ctx, app.BatchRequest{BatchID: "cascade"}); err != nil {
		t.Fatal(err)
	}
	base := int64(1)
	ratio := 0.5
	policy2, err := svc.SubmitPolicyPatch(ctx, domain.PolicyPatch{BaseVersion: &base, SampleRatio: &ratio})
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	if policy2.Version != 2 {
		t.Fatalf("expected v2, got %d", policy2.Version)
	}
	report, err := verify.VerifyBatch(ctx, st, "cascade", 2)
	if err != nil || !report.OK {
		t.Fatalf("v2 verify failed err=%v report=%+v", err, report)
	}

	oldShards, err := st.ListShards(ctx, "cascade", 2, false)
	if err != nil || len(oldShards) != 4 {
		t.Fatalf("list v2 shards: %v len=%d", err, len(oldShards))
	}
	oldDigest := oldShards[0].ArtifactDigest
	base2 := int64(2)
	badRatio := 0.6
	_, err = svc.SubmitPolicyPatch(ctx, domain.PolicyPatch{BaseVersion: &base2, SampleRatio: &badRatio}, "digest_mismatch")
	requireCode(t, err, domain.CodeShardDigestMismatch)
	current, err := st.CurrentPolicyVersion(ctx)
	if err != nil || current != 2 {
		t.Fatalf("policy advanced after digest fault: current=%d err=%v", current, err)
	}
	afterShards, err := st.ListShards(ctx, "cascade", 2, false)
	if err != nil || afterShards[0].ArtifactDigest != oldDigest {
		t.Fatalf("old shard digest not preserved: err=%v", err)
	}
}

func TestStalePolicyVersions(t *testing.T) {
	svc, _, ctx := newTestService(t)
	policy := domain.DefaultPolicy()
	_, err := svc.SubmitFullPolicy(ctx, policy, "")
	requireCode(t, err, domain.CodeStaleSnapshot)

	wrongBase := int64(99)
	_, err = svc.SubmitPolicyPatch(ctx, domain.PolicyPatch{BaseVersion: &wrongBase})
	requireCode(t, err, domain.CodeStaleBase)
}

func TestRecoveryReportIsQueryableWhenEmpty(t *testing.T) {
	svc, _, ctx := newTestService(t)
	report, err := svc.Recover(ctx, "test-empty")
	if err != nil {
		t.Fatal(err)
	}
	if report.UnfinishedCount != 0 {
		t.Fatal(fmt.Sprintf("expected no unfinished intents, got %d", report.UnfinishedCount))
	}
	latest, err := svc.LatestRecoveryReport(ctx)
	if err != nil || latest.Trigger != "test-empty" {
		t.Fatalf("latest report mismatch: %+v err=%v", latest, err)
	}
}

package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"local/rollout-guard/internal/domain"
	"local/rollout-guard/internal/engine"
	"local/rollout-guard/internal/risk"
	"local/rollout-guard/internal/store"
	"local/rollout-guard/samples"
)

type harness struct {
	store   *store.Store
	engine  *engine.Engine
	webhook *httptest.Server
	hits    *atomic.Int64
}

func newHarness(t *testing.T, options engine.Options) *harness {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "workbench.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	var hits atomic.Int64
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("webhook method = %s", r.Method)
		}
		hits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(webhook.Close)
	options.RollbackURL = webhook.URL
	if options.Now == nil {
		options.Now = func() int64 { return 1_800_000_000_000 }
	}
	eng := engine.New(st, options)
	if _, err := eng.RecoverOnStartup(context.Background()); err != nil {
		t.Fatalf("startup recovery: %v", err)
	}
	return &harness{store: st, engine: eng, webhook: webhook, hits: &hits}
}

func (h *harness) loadSamples(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	policy, err := samples.Policy()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.engine.SubmitPolicy(ctx, policy); err != nil {
		t.Fatalf("submit policy: %v", err)
	}
	batches, err := samples.Batches()
	if err != nil {
		t.Fatal(err)
	}
	for _, batch := range batches {
		if _, err := h.engine.SubmitBatch(ctx, batch); err != nil {
			t.Fatalf("submit batch %s: %v", batch.ID, err)
		}
	}
	observations, err := samples.Observations()
	if err != nil {
		t.Fatal(err)
	}
	for batchID, reports := range observations {
		if _, err := h.engine.IngestObservations(ctx, batchID, reports); err != nil {
			t.Fatalf("ingest %s: %v", batchID, err)
		}
	}
}

func (h *harness) ingest(t *testing.T, batchID string) {
	t.Helper()
	observations, err := samples.Observations()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.engine.IngestObservations(context.Background(), batchID, observations[batchID]); err != nil {
		t.Fatalf("ingest %s: %v", batchID, err)
	}
}

func requireCode(t *testing.T, err error, code string) *domain.BizError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", code)
	}
	biz, ok := domain.AsBizError(err)
	if !ok {
		t.Fatalf("expected business error %s, got %T %v", code, err, err)
	}
	if biz.Code != code {
		t.Fatalf("error code = %s, want %s (%v)", biz.Code, code, err)
	}
	return biz
}

func TestPassFastGuardAutoRollbackAndCooldown(t *testing.T) {
	h := newHarness(t, engine.Options{})
	h.loadSamples(t)
	ctx := context.Background()

	preview, err := h.engine.Preview(ctx, "chg-pass-001", risk.ModeCompare)
	if err != nil {
		t.Fatal(err)
	}
	if preview.Comparison.Full.ScoreBP != 389 {
		t.Fatalf("pass score = %d, want 389", preview.Comparison.Full.ScoreBP)
	}
	if preview.Comparison.Full.Decision.Action != risk.ActionPass {
		t.Fatalf("full action = %s, want PASS", preview.Comparison.Full.Decision.Action)
	}
	if preview.Comparison.Fast.Decision.Action == risk.ActionPass {
		t.Fatal("fast view must not pass while unsampled observations exist")
	}
	if !preview.Comparison.Fast.Decision.SafetyGuardApplied {
		t.Fatal("fast safety guard not applied")
	}
	if preview.Comparison.Fast.AsOfCommitSeq != preview.Comparison.Full.AsOfCommitSeq {
		t.Fatal("fast/full used different commit snapshots")
	}
	if preview.Comparison.Fast.Sampling.SampleRatePPM != 666667 {
		t.Fatalf("fast sample rate = %d", preview.Comparison.Fast.Sampling.SampleRatePPM)
	}
	if preview.Comparison.Full.Metrics.P90NearestRank != 14 || preview.Comparison.Full.Metrics.LatencySamples != 15 {
		t.Fatalf("unexpected nearest-rank metadata: rank=%d n=%d", preview.Comparison.Full.Metrics.P90NearestRank, preview.Comparison.Full.Metrics.LatencySamples)
	}

	passResult, err := h.engine.Evaluate(ctx, "chg-pass-001", risk.ModeFull, true)
	if err != nil {
		t.Fatal(err)
	}
	if passResult.Batch.Status != domain.StatusPassed {
		t.Fatalf("pass status = %s", passResult.Batch.Status)
	}

	h.ingest(t, "chg-risk-002")
	riskResult, err := h.engine.Evaluate(ctx, "chg-risk-002", risk.ModeFull, true)
	if err != nil {
		t.Fatal(err)
	}
	if riskResult.Evaluation.ScoreBP != 10000 || riskResult.Evaluation.Level != risk.LevelExtreme {
		t.Fatalf("risk result = %d/%s", riskResult.Evaluation.ScoreBP, riskResult.Evaluation.Level)
	}
	if riskResult.Batch.Status != domain.StatusRolledBack {
		t.Fatalf("risk status = %s", riskResult.Batch.Status)
	}
	if riskResult.Rollback == nil || !riskResult.Rollback.Success || h.hits.Load() != 1 {
		t.Fatalf("rollback webhook not successful: %+v hits=%d", riskResult.Rollback, h.hits.Load())
	}

	h.ingest(t, "chg-cooldown-003")
	_, err = h.engine.Evaluate(ctx, "chg-cooldown-003", risk.ModeFull, true)
	biz := requireCode(t, err, "E_ROLLBACK_COOLDOWN")
	if remaining, ok := biz.Context["remaining_ms"].(int64); !ok || remaining != 120000 {
		t.Fatalf("cooldown remaining = %#v", biz.Context["remaining_ms"])
	}
	if h.hits.Load() != 1 {
		t.Fatalf("cooldown should not dispatch webhook, hits=%d", h.hits.Load())
	}
	batch, err := store.GetBatch(ctx, h.store.DB(), "chg-cooldown-003")
	if err != nil {
		t.Fatal(err)
	}
	if batch.Status != domain.StatusObserving {
		t.Fatalf("cooldown batch status = %s, want OBSERVING", batch.Status)
	}

	_, err = h.engine.Transition(ctx, "chg-pass-001", domain.StatusRollingBack, "illegal test")
	requireCode(t, err, "E_INVALID_TRANSITION")

	cooldown, err := h.engine.Cooldown(ctx, "chg-cooldown-003")
	if err != nil {
		t.Fatal(err)
	}
	if !cooldown.Active || cooldown.RemainingMS != 120000 {
		t.Fatalf("cooldown status = %+v", cooldown)
	}
}

func TestPolicyStaleAndInvalidKeepOldSnapshot(t *testing.T) {
	h := newHarness(t, engine.Options{})
	h.loadSamples(t)
	ctx := context.Background()

	policy, err := samples.Policy()
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.engine.SubmitPolicy(ctx, policy)
	requireCode(t, err, "E_STALE_SNAPSHOT")
	current, commit, err := store.CurrentPolicy(ctx, h.store.DB())
	if err != nil {
		t.Fatal(err)
	}
	if current.Version != 1 || commit != 1 {
		t.Fatalf("old policy unavailable after stale submit: version=%d commit=%d", current.Version, commit)
	}

	invalid := policy
	invalid.Version = 2
	invalid.Checksum = "0000000000000000000000000000000000000000000000000000000000000000"
	_, err = h.engine.SubmitPolicy(ctx, invalid)
	requireCode(t, err, "E_POLICY_INVALID")
	current, _, err = store.CurrentPolicy(ctx, h.store.DB())
	if err != nil || current.Version != 1 {
		t.Fatalf("old policy not available after validation failure: %+v %v", current, err)
	}
}

func TestObservationsAreAtomicIdempotentAndConflicting(t *testing.T) {
	h := newHarness(t, engine.Options{})
	h.loadSamples(t)
	ctx := context.Background()
	all, err := samples.Observations()
	if err != nil {
		t.Fatal(err)
	}
	reports := all["chg-pass-001"]

	before, err := store.GetBatch(ctx, h.store.DB(), "chg-pass-001")
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := h.engine.IngestObservations(ctx, "chg-pass-001", reports)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Accepted != 0 || duplicate.Idempotent != len(reports) || duplicate.ObservationRevision != before.Revision {
		t.Fatalf("idempotent result = %+v", duplicate)
	}

	conflict := reports[0]
	conflict.SuccessCount = 4
	_, err = h.engine.IngestObservations(ctx, "chg-pass-001", []domain.Observation{conflict})
	requireCode(t, err, "E_CONFLICTING_REPORT")
	afterConflict, err := store.GetBatch(ctx, h.store.DB(), "chg-pass-001")
	if err != nil {
		t.Fatal(err)
	}
	if afterConflict.Revision != before.Revision {
		t.Fatalf("conflicting report changed revision: %d -> %d", before.Revision, afterConflict.Revision)
	}

	regression := domain.Observation{Seq: 100, TimestampMS: reports[len(reports)-1].TimestampMS - 1, MetricType: domain.MetricErrorRate, SampleCount: 5, SuccessCount: 5}
	_, err = h.engine.IngestObservations(ctx, "chg-pass-001", []domain.Observation{regression})
	requireCode(t, err, "E_OBSERVATION_INVALID")

	invalid := domain.Observation{Seq: 102, TimestampMS: reports[len(reports)-1].TimestampMS + 3000, MetricType: "bad_metric", SampleCount: 5}
	validLooking := domain.Observation{Seq: 101, TimestampMS: reports[len(reports)-1].TimestampMS + 3000, MetricType: domain.MetricErrorRate, SampleCount: 5, SuccessCount: 5}
	_, err = h.engine.IngestObservations(ctx, "chg-pass-001", []domain.Observation{validLooking, invalid})
	requireCode(t, err, "E_OBSERVATION_INVALID")
	var count int
	if err := h.store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM observation_buckets WHERE batch_id=? AND seq=101`, "chg-pass-001").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("invalid request partially persisted a valid-looking observation")
	}
}

func TestAutoRollbackDisabledEscalates(t *testing.T) {
	h := newHarness(t, engine.Options{})
	ctx := context.Background()
	policy, err := samples.Policy()
	if err != nil {
		t.Fatal(err)
	}
	policy.AutoRollback = false
	policy.Checksum = ""
	sum, err := policy.CanonicalChecksum()
	if err != nil {
		t.Fatal(err)
	}
	policy.Checksum = sum
	if _, err := h.engine.SubmitPolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	batches, _ := samples.Batches()
	for _, batch := range batches {
		if _, err := h.engine.SubmitBatch(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	h.ingest(t, "chg-risk-002")
	result, err := h.engine.Evaluate(ctx, "chg-risk-002", risk.ModeFull, true)
	if err != nil {
		t.Fatal(err)
	}
	if result.Evaluation.Decision.Action != risk.ActionEscalate || result.Batch.Status != domain.StatusEscalated {
		t.Fatalf("auto rollback off result = %s/%s", result.Evaluation.Decision.Action, result.Batch.Status)
	}
	if h.hits.Load() != 0 {
		t.Fatalf("webhook called with auto rollback disabled: %d", h.hits.Load())
	}
}

func TestCrashBeforeCommitRecoversToLastCommittedVersion(t *testing.T) {
	h := newHarness(t, engine.Options{})
	h.loadSamples(t)
	ctx := context.Background()
	beforeCommit, err := store.CurrentCommit(ctx, h.store.DB())
	if err != nil {
		t.Fatal(err)
	}

	crashing := engine.New(h.store, engine.Options{
		RollbackURL: h.webhook.URL,
		CrashPoint:  "risk_eval_before_commit",
		CrashBatch:  "chg-recover-004",
		CrashHook: func(point string, intentID int64, batchID string) {
			panic("injected crash " + point)
		},
		Now: func() int64 { return 1_800_000_000_000 },
	})
	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("expected injected crash")
			}
		}()
		_, _ = crashing.Evaluate(ctx, "chg-recover-004", risk.ModeFull, true)
	}()

	afterCrash, err := store.CurrentCommit(ctx, h.store.DB())
	if err != nil {
		t.Fatal(err)
	}
	if afterCrash != beforeCommit {
		t.Fatalf("uncommitted risk decision leaked a commit: %d -> %d", beforeCommit, afterCrash)
	}
	batch, err := store.GetBatch(ctx, h.store.DB(), "chg-recover-004")
	if err != nil {
		t.Fatal(err)
	}
	if batch.Status != domain.StatusObserving {
		t.Fatalf("crash changed batch status to %s", batch.Status)
	}

	recoveryEngine := engine.New(h.store, engine.Options{
		RollbackURL: h.webhook.URL,
		Now:         func() int64 { return 1_800_000_001_000 },
	})
	report, err := recoveryEngine.RecoverOnStartup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.PendingFound != 1 || len(report.AbortedIntents) != 1 || report.Clean {
		t.Fatalf("recovery report = %+v", report)
	}
	if report.AbortedIntents[0].BatchID != "chg-recover-004" || report.AbortedIntents[0].Status != "ABORTED" {
		t.Fatalf("aborted intent = %+v", report.AbortedIntents[0])
	}
	if !report.FactsOnly || report.LatestCommitSeq != beforeCommit {
		t.Fatalf("recovery did not report facts only: %+v", report)
	}
	stored, err := recoveryEngine.Recovery(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Report["pending_found"].(float64) != 1 {
		t.Fatalf("persisted recovery report = %+v", stored.Report)
	}
}

func TestInsufficientSamplesDoesNotReturnZeroScore(t *testing.T) {
	h := newHarness(t, engine.Options{})
	ctx := context.Background()
	policy, _ := samples.Policy()
	if _, err := h.engine.SubmitPolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	batch := domain.Batch{
		ID: "chg-empty-999", Service: "empty-api", ChangeType: domain.ChangeConfig, BatchSize: 2,
		Baseline: domain.Baseline{TimestampMS: 1700000000000, ErrorRatePPM: 1000, P90LatencyMS: 100, SuccessRatePPM: 999000},
	}
	if _, err := h.engine.SubmitBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	_, err := h.engine.Evaluate(ctx, batch.ID, risk.ModeFull, true)
	requireCode(t, err, "E_INSUFFICIENT_SAMPLES")
}

func TestCrashDuringObservationIngestLeavesNoPartialWindow(t *testing.T) {
	h := newHarness(t, engine.Options{})
	ctx := context.Background()
	policy, _ := samples.Policy()
	if _, err := h.engine.SubmitPolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	batches, _ := samples.Batches()
	for _, batch := range batches {
		if _, err := h.engine.SubmitBatch(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	observations, _ := samples.Observations()
	beforeCommit, err := store.CurrentCommit(ctx, h.store.DB())
	if err != nil {
		t.Fatal(err)
	}
	crashing := engine.New(h.store, engine.Options{
		RollbackURL: h.webhook.URL, CrashPoint: "observation_before_commit", CrashBatch: "chg-recover-004",
		CrashHook: func(point string, intentID int64, batchID string) { panic("injected observation crash") },
		Now:       func() int64 { return 1_800_000_000_000 },
	})
	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("expected injected observation crash")
			}
		}()
		_, _ = crashing.IngestObservations(ctx, "chg-recover-004", observations["chg-recover-004"])
	}()
	afterCommit, err := store.CurrentCommit(ctx, h.store.DB())
	if err != nil {
		t.Fatal(err)
	}
	if afterCommit != beforeCommit {
		t.Fatalf("partial observation allocated commit %d -> %d", beforeCommit, afterCommit)
	}
	batch, err := store.GetBatch(ctx, h.store.DB(), "chg-recover-004")
	if err != nil {
		t.Fatal(err)
	}
	if batch.Revision != 0 || batch.LastObservationTSMS != 0 {
		t.Fatalf("partial observation changed batch boundary: %+v", batch)
	}
	var count int
	if err := h.store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM observation_buckets WHERE batch_id=?`, "chg-recover-004").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("partial observation rows persisted: %d", count)
	}
	recovery, err := engine.New(h.store, engine.Options{RollbackURL: h.webhook.URL}).RecoverOnStartup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.PendingFound != 1 || recovery.AbortedIntents[0].Kind != "observation_ingest" {
		t.Fatalf("observation recovery report = %+v", recovery)
	}
}

func TestCrashAfterRollbackCommitIsResumedOnStartup(t *testing.T) {
	h := newHarness(t, engine.Options{})
	h.loadSamples(t)
	ctx := context.Background()
	crashing := engine.New(h.store, engine.Options{
		RollbackURL: h.webhook.URL, CrashPoint: "rollback_after_commit", CrashBatch: "chg-risk-002",
		CrashHook: func(point string, intentID int64, batchID string) { panic("injected rollback crash") },
		Now:       func() int64 { return 1_800_000_000_000 },
	})
	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("expected injected rollback crash")
			}
		}()
		_, _ = crashing.Evaluate(ctx, "chg-risk-002", risk.ModeFull, true)
	}()
	batch, err := store.GetBatch(ctx, h.store.DB(), "chg-risk-002")
	if err != nil {
		t.Fatal(err)
	}
	if batch.Status != domain.StatusRollingBack {
		t.Fatalf("rollback decision not committed: %s", batch.Status)
	}
	cooldown, err := h.engine.Cooldown(ctx, "chg-risk-002")
	if err != nil {
		t.Fatal(err)
	}
	if !cooldown.Active || h.hits.Load() != 0 {
		t.Fatalf("expected committed cooldown and pending webhook, cooldown=%+v hits=%d", cooldown, h.hits.Load())
	}
	recovery, err := engine.New(h.store, engine.Options{
		RollbackURL: h.webhook.URL, Now: func() int64 { return 1_800_000_001_000 },
	}).RecoverOnStartup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovery.ResumedRollbacks) != 1 || h.hits.Load() != 1 {
		t.Fatalf("rollback resume failed: %+v hits=%d", recovery, h.hits.Load())
	}
	batch, err = store.GetBatch(ctx, h.store.DB(), "chg-risk-002")
	if err != nil {
		t.Fatal(err)
	}
	if batch.Status != domain.StatusRolledBack {
		t.Fatalf("resumed rollback status = %s", batch.Status)
	}
}

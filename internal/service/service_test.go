package service_test

import (
	"context"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"columnar-executor/internal/config"
	"columnar-executor/internal/model"
	"columnar-executor/internal/sample"
	"columnar-executor/internal/service"
	"columnar-executor/internal/snapshot"
	"columnar-executor/internal/store"
	"columnar-executor/internal/verify"
)

type testEnv struct {
	store *store.Store
	svc   *service.Service
	db    string
}

func newEnv(t *testing.T, options service.Options) *testEnv {
	t.Helper()
	db := filepath.Join(t.TempDir(), "columnar.db")
	st, err := store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, options)
	if err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return &testEnv{store: st, svc: svc, db: db}
}

func (e *testEnv) seed(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if _, err := e.svc.GenerateSnapshot(ctx, model.SnapshotGenerateRequest{
		Version: config.DefaultSnapshotVersion,
		Rows:    config.DefaultRows,
		Seed:    config.DefaultSeed,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.SubmitChain(ctx, sample.Chain("sales", 1, 1)); err != nil {
		t.Fatal(err)
	}
}

func TestDualPathAndBitmapForms(t *testing.T) {
	env := newEnv(t, service.Options{})
	env.seed(t)
	ctx := context.Background()

	small, err := env.svc.ExecuteChain(ctx, "sales", model.ExecuteRequest{
		ExecutionID: "small-all", ChainVersion: 1, BitmapForm: model.FormAll, BatchSize: 8, LargeVectorThreshold: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	large, err := env.svc.ExecuteChain(ctx, "sales", model.ExecuteRequest{
		ExecutionID: "large-all", ChainVersion: 1, BitmapForm: model.FormAll, BatchSize: 256, LargeVectorThreshold: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	if small.Path.Path != model.PathSmall || large.Path.Path != model.PathLarge {
		t.Fatalf("unexpected paths: %s %s", small.Path.Path, large.Path.Path)
	}
	if small.Path.BatchCount != 33 || large.Path.BatchCount != 1 {
		t.Fatalf("unexpected batch counts: %d %d", small.Path.BatchCount, large.Path.BatchCount)
	}
	report := verify.Compare(*small, *large, 1)
	if !report.RowsConsistent || !report.AggregatesConsistent {
		t.Fatalf("dual paths disagree: %+v", report)
	}
	if len(report.Mismatches) != 0 {
		t.Fatalf("unexpected mismatches: %+v", report.Mismatches)
	}

	formConfigs := map[string][2]model.ExecuteRequest{
		model.FormAll: {
			{ChainVersion: 1, BitmapForm: model.FormAll, BatchSize: 8, LargeVectorThreshold: 128},
			{ChainVersion: 1, BitmapForm: model.FormAll, BatchSize: 256, LargeVectorThreshold: 128},
		},
		model.FormDense: {
			{ChainVersion: 1, BitmapForm: model.FormDense, BatchSize: 8, LargeVectorThreshold: 128},
			{ChainVersion: 1, BitmapForm: model.FormDense, BatchSize: 256, LargeVectorThreshold: 128},
		},
		model.FormSparse: {
			{ChainVersion: 1, BitmapForm: model.FormSparse, BatchSize: 4, LargeVectorThreshold: 128},
			{ChainVersion: 1, BitmapForm: model.FormSparse, BatchSize: 32, LargeVectorThreshold: 16},
		},
		model.FormNone: {
			{ChainVersion: 1, BitmapForm: model.FormNone, BatchSize: 8, LargeVectorThreshold: 128},
			{ChainVersion: 1, BitmapForm: model.FormNone, BatchSize: 4, LargeVectorThreshold: 16},
		},
	}
	for form, configs := range formConfigs {
		t.Run(form, func(t *testing.T) {
			left, err := env.svc.ExecuteChain(ctx, "sales", configs[0])
			if err != nil {
				t.Fatal(err)
			}
			right, err := env.svc.ExecuteChain(ctx, "sales", configs[1])
			if err != nil {
				t.Fatal(err)
			}
			result := verify.Compare(*left, *right, 1)
			if !result.RowsConsistent || !result.AggregatesConsistent {
				t.Fatalf("form %s dual path mismatch: %+v", form, result)
			}
			if form == model.FormNone {
				if left.Path.SelectedRows != 0 || left.Path.BatchCount != 0 || len(left.Batches) != 0 {
					t.Fatalf("empty selection must produce a deterministic empty result: %+v", left.Path)
				}
				rowCount := aggregateByName(left.Aggregates, "row_count")
				if rowCount == nil || rowCount.ValueUnits != 0 {
					t.Fatalf("empty selection count = %+v", rowCount)
				}
			}
		})
	}
}

func TestSnapshotValidationKeepsOldSnapshot(t *testing.T) {
	env := newEnv(t, service.Options{})
	env.seed(t)
	ctx := context.Background()

	bad, err := snapshot.Generate(2, 257, 100)
	if err != nil {
		t.Fatal(err)
	}
	bad.Columns[4].DictIndices[0] = 999
	bad.Columns[4].Nulls[0] &= ^byte(1)
	if _, err := env.svc.LoadSnapshot(ctx, bad); err == nil {
		t.Fatal("expected invalid snapshot rejection")
	} else if biz, ok := model.AsBizError(err); !ok || biz.Code != model.CodeSnapshotInvalid {
		t.Fatalf("wrong error: %v", err)
	}
	current, err := env.svc.CurrentSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if current.Version != 1 || current.SHA256Digest == "" {
		t.Fatalf("old snapshot was not preserved: version=%d", current.Version)
	}
}

func TestStaleChainVersion(t *testing.T) {
	env := newEnv(t, service.Options{})
	env.seed(t)
	ctx := context.Background()
	if _, err := env.svc.SubmitChain(ctx, sample.Chain("sales", 2, 1)); err != nil {
		t.Fatal(err)
	}
	_, err := env.svc.SubmitChain(ctx, sample.Chain("sales", 1, 1))
	if err == nil {
		t.Fatal("expected stale version error")
	}
	biz, ok := model.AsBizError(err)
	if !ok || biz.Code != model.CodeStaleVersion {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestFaultInjectionAndNoUncommittedVisibility(t *testing.T) {
	env := newEnv(t, service.Options{})
	env.seed(t)
	ctx := context.Background()

	expected := map[string]string{
		model.FaultBitmapMismatch: model.CodeBitmapMismatch,
		model.FaultFixedOverflow:  model.CodeNumericOverflow,
		model.FaultDictIndex:      model.CodeDictIndex,
		model.FaultDivideByZero:   model.CodeDivideByZero,
		model.FaultExecAborted:    model.CodeExecAborted,
	}
	seenCodes := map[string]bool{}
	for kind, wantCode := range expected {
		executionID := "fault-" + kind
		_, err := env.svc.ExecuteChain(ctx, "sales", model.ExecuteRequest{
			ExecutionID: executionID, ChainVersion: 1, BitmapForm: model.FormAll, BatchSize: 8,
			LargeVectorThreshold: 128,
			Injections:           []model.FaultSpec{{Kind: kind, Batch: 2}},
		})
		if err == nil {
			t.Fatalf("%s injection did not fail", kind)
		}
		biz, ok := model.AsBizError(err)
		if !ok || biz.Code != wantCode {
			t.Fatalf("%s: expected %s, got %v", kind, wantCode, err)
		}
		seenCodes[biz.Code] = true
		if biz.Context["execution_id"] != executionID {
			t.Fatalf("%s missing execution id context: %+v", kind, biz.Context)
		}
		if kind == model.FaultFixedOverflow {
			if biz.Context["column"] != "price" || biz.Context["batch"] != 2 {
				t.Fatalf("overflow context = %+v", biz.Context)
			}
		}
		if kind == model.FaultDictIndex && biz.Context["column"] != "category" {
			t.Fatalf("dict context = %+v", biz.Context)
		}

		view, err := env.svc.GetExecution(ctx, executionID)
		if err != nil {
			t.Fatal(err)
		}
		if view.Published || len(view.Batches) != 0 || len(view.Aggregates) != 0 {
			t.Fatalf("%s exposed uncommitted facts: %+v", kind, view)
		}
		wantStatus := model.StatusFailed
		if kind == model.FaultExecAborted {
			wantStatus = model.StatusAborted
		}
		if view.Status != wantStatus || view.ErrorCode != wantCode {
			t.Fatalf("%s status=%s code=%s", kind, view.Status, view.ErrorCode)
		}
	}
	if len(seenCodes) != 5 {
		t.Fatalf("expected five distinct business codes, got %v", seenCodes)
	}

	probes := []string{model.FaultBitmapMismatch, model.FaultFixedOverflow, model.FaultDictIndex, model.FaultDivideByZero, model.FaultExecAborted}
	probeCodes := map[string]bool{}
	for _, kind := range probes {
		probe, err := service.ProbeFault(kind)
		if err != nil {
			t.Fatal(err)
		}
		probeCodes[probe.BusinessCode] = true
	}
	if len(probeCodes) != 5 {
		t.Fatalf("fault probe codes not distinct: %v", probeCodes)
	}

	valid, err := env.svc.ExecuteChain(ctx, "sales", model.ExecuteRequest{
		ExecutionID: "after-faults", ChainVersion: 1, BitmapForm: model.FormAll, BatchSize: 8, LargeVectorThreshold: 128,
	})
	if err != nil || !valid.Published {
		t.Fatalf("execution after faults did not succeed: %v", err)
	}
}

func TestActualArithmeticFaults(t *testing.T) {
	env := newEnv(t, service.Options{})
	env.seed(t)
	ctx := context.Background()

	divChain := sample.Chain("sales", 2, 1)
	divChain.Operators[1].Filter.Predicate = model.Predicate{Column: "price", Op: "div_gt", ValueFixed: int64Ptr(1), DivisorUnits: 0}
	if _, err := env.svc.SubmitChain(ctx, divChain); err != nil {
		t.Fatal(err)
	}
	_, err := env.svc.ExecuteChain(ctx, "sales", model.ExecuteRequest{
		ExecutionID: "actual-div-zero", ChainVersion: 2, BitmapForm: model.FormAll, BatchSize: 8, LargeVectorThreshold: 128,
	})
	assertBizCode(t, err, model.CodeDivideByZero)

	mulChain := sample.Chain("sales", 3, 1)
	mulChain.Operators[1].Filter.Predicate = model.Predicate{Column: "price", Op: "mul_gt", ValueFixed: int64Ptr(1), FactorUnits: math.MaxInt64}
	if _, err := env.svc.SubmitChain(ctx, mulChain); err != nil {
		t.Fatal(err)
	}
	_, err = env.svc.ExecuteChain(ctx, "sales", model.ExecuteRequest{
		ExecutionID: "actual-overflow", ChainVersion: 3, BitmapForm: model.FormAll, BatchSize: 8, LargeVectorThreshold: 128,
	})
	assertBizCode(t, err, model.CodeNumericOverflow)
}

func TestConcurrentReadsSeeOnlyCommittedVersion(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	env := newEnv(t, service.Options{CrashHook: func(string, int) {
		once.Do(func() { close(entered) })
		<-release
	}})
	env.seed(t)
	ctx := context.Background()

	resultCh := make(chan *model.ExecutionView, 1)
	errCh := make(chan error, 1)
	go func() {
		view, err := env.svc.ExecuteChain(ctx, "sales", model.ExecuteRequest{
			ExecutionID: "blocked", ChainVersion: 1, BitmapForm: model.FormAll, BatchSize: 8,
			LargeVectorThreshold: 128, CrashAfterBatch: 1,
		})
		resultCh <- view
		errCh <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("execution did not reach crash hook")
	}
	for i := 0; i < 8; i++ {
		view, err := env.svc.GetExecution(ctx, "blocked")
		if err != nil {
			t.Fatal(err)
		}
		if view.Status != model.StatusRunning || view.Published || len(view.Batches) != 0 || len(view.Aggregates) != 0 {
			t.Fatalf("read observed partial state: %+v", view)
		}
	}
	close(release)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	view := <-resultCh
	if view == nil || !view.Published {
		t.Fatalf("execution did not publish: %+v", view)
	}
	committed, err := env.svc.GetExecution(ctx, "blocked")
	if err != nil {
		t.Fatal(err)
	}
	if !committed.Published || len(committed.Batches) == 0 || len(committed.Aggregates) == 0 {
		t.Fatalf("committed version not readable: %+v", committed)
	}
}

func TestForcedTerminationRecovery(t *testing.T) {
	db := filepath.Join(t.TempDir(), "recovery.db")
	st, err := store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Options{CrashHook: func(string, int) {
		panic("simulated forced termination")
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := svc.GenerateSnapshot(ctx, model.SnapshotGenerateRequest{Version: 1, Rows: 257, Seed: config.DefaultSeed}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SubmitChain(ctx, sample.Chain("sales", 1, 1)); err != nil {
		t.Fatal(err)
	}

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = svc.ExecuteChain(ctx, "sales", model.ExecuteRequest{
			ExecutionID: "crashed", ChainVersion: 1, BitmapForm: model.FormAll, BatchSize: 8,
			LargeVectorThreshold: 128, CrashAfterBatch: 2,
		})
	}()
	if recovered == nil {
		t.Fatal("expected simulated termination")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted, err := service.New(reopened, service.Options{})
	if err != nil {
		t.Fatal(err)
	}
	startup := restarted.StartupRecoveryReports()
	if len(startup) != 1 {
		t.Fatalf("startup recovery reports = %d", len(startup))
	}
	report := startup[0]
	if report.ExecutionID != "crashed" || report.RecoveredStatus != model.StatusRecovered {
		t.Fatalf("unexpected recovery report: %+v", report)
	}
	if len(report.RolledBackBatches) == 0 || len(report.DiscardedAggregates) == 0 || len(report.Intents) == 0 {
		t.Fatalf("recovery report lacks persisted facts/diffs: %+v", report)
	}
	view, err := restarted.GetExecution(ctx, "crashed")
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != model.StatusRecovered || view.Published || len(view.Batches) != 0 || len(view.Aggregates) != 0 {
		t.Fatalf("recovered execution exposed uncommitted results: %+v", view)
	}
	persistedReports, err := restarted.RecoveryReports(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(persistedReports) != 1 || persistedReports[0].ExecutionID != "crashed" || persistedReports[0].ID <= 0 {
		t.Fatalf("recovery reports not persisted: %+v", persistedReports)
	}
	after, err := restarted.ExecuteChain(ctx, "sales", model.ExecuteRequest{
		ExecutionID: "after-recovery", ChainVersion: 1, BitmapForm: model.FormAll, BatchSize: 8, LargeVectorThreshold: 128,
	})
	if err != nil || !after.Published {
		t.Fatalf("execution after recovery failed: %v", err)
	}
}

func aggregateByName(values []model.AggregateValue, name string) *model.AggregateValue {
	for i := range values {
		if values[i].Name == name {
			return &values[i]
		}
	}
	return nil
}

func int64Ptr(value int64) *int64 { return &value }

func assertBizCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s", code)
	}
	biz, ok := model.AsBizError(err)
	if !ok || biz.Code != code {
		t.Fatalf("expected %s, got %v", code, err)
	}
}

package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"layoutread/internal/demo"
	"layoutread/internal/model"
	"layoutread/internal/service"
	"layoutread/internal/store"
)

func openService(t *testing.T, path string) (*store.Store, *service.Service) {
	t.Helper()
	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("store open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := service.New(st)
	if err := svc.LoadPersistedStrategy(context.Background()); err != nil {
		t.Fatalf("load strategy: %v", err)
	}
	return st, svc
}

func mustImportRun(t *testing.T, svc *service.Service, bundle model.Bundle) service.RunReport {
	t.Helper()
	if err := svc.Import(context.Background(), bundle); err != nil {
		t.Fatalf("import: %v", err)
	}
	report, err := svc.Run(context.Background(), service.RunOptions{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return report
}

func canonical(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

func TestFullRunQueryCacheAndLatency(t *testing.T) {
	_, svc := openService(t, filepath.Join(t.TempDir(), "cache.sqlite"))
	report := mustImportRun(t, svc, demo.Generate())
	if report.Processed != 4 || len(report.Documents) != 4 || report.Stopped {
		t.Fatalf("unexpected run report: %+v", report)
	}

	first, err := svc.Query(context.Background(), service.QueryRequest{
		DocID: "doc-double-en", Scope: "blocks", BudgetMS: 100, HasBudget: true, QueryID: "q-first",
	})
	if err != nil {
		t.Fatalf("first query: %v", err)
	}
	if first.CacheHit || len(first.Data) == 0 || first.QueryID != "q-first" {
		t.Fatalf("unexpected first query: %+v", first)
	}
	svc.ClearQueryCache()
	second, err := svc.Query(context.Background(), service.QueryRequest{
		DocID: "doc-double-en", Scope: "blocks", BudgetMS: 100, HasBudget: true, QueryID: "q-second",
	})
	if err != nil {
		t.Fatalf("second query: %v", err)
	}
	if second.CacheHit {
		t.Fatal("cache was not cleared")
	}
	if string(first.Data) != string(second.Data) || first.CacheKey != second.CacheKey {
		t.Fatal("recomputed query bytes differ from the first query")
	}
	third, err := svc.Query(context.Background(), service.QueryRequest{
		DocID: "doc-double-en", Scope: "blocks", BudgetMS: 100, HasBudget: true, QueryID: "q-third",
	})
	if err != nil {
		t.Fatalf("third query: %v", err)
	}
	if !third.CacheHit || string(third.Data) != string(second.Data) {
		t.Fatal("cache hit bytes differ from recomputed bytes")
	}

	_, err = svc.Query(context.Background(), service.QueryRequest{
		DocID: "doc-double-en", Scope: "blocks", BudgetMS: 0, HasBudget: true, QueryID: "q-zero",
	})
	var modelErr *model.Error
	if !errors.As(err, &modelErr) || modelErr.Code != model.CodeLatencyBudget {
		t.Fatalf("expected E_LATENCY_BUDGET, got %v", err)
	}
	if modelErr.QueryID != "q-zero" || modelErr.ElapsedMS < 0 {
		t.Fatalf("latency error lacks query identity/elapsed: %+v", modelErr)
	}

	svc.ClearQueryCache()
	start := time.Now()
	_, err = svc.Query(context.Background(), service.QueryRequest{
		DocID: "doc-double-en", Scope: "confidence", BudgetMS: 1, HasBudget: true,
		QueryID: "q-slow", SimulatedDelayMS: 30,
	})
	elapsed := time.Since(start)
	if !errors.As(err, &modelErr) || modelErr.Code != model.CodeLatencyBudget {
		t.Fatalf("expected delayed E_LATENCY_BUDGET, got %v", err)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("latency budget blocked for %s", elapsed)
	}
}

func TestIncrementalPolicyChangeEqualsFullRebuild(t *testing.T) {
	baseBundle := demo.Generate()
	incrementalPath := filepath.Join(t.TempDir(), "incremental.sqlite")
	_, incremental := openService(t, incrementalPath)
	mustImportRun(t, incremental, baseBundle)

	changed := demo.Generate()
	changed.Strategy.Version++
	changed.Strategy.ColumnGapPX += 1
	report := mustImportRun(t, incremental, changed)
	for _, document := range report.Documents {
		if !document.Changed || document.EarliestInvalidStage != model.StageColumns {
			t.Fatalf("%s should invalidate from columns: %+v", document.DocID, document)
		}
		if len(document.ReusedStages) != 1 || document.ReusedStages[0] != model.StageNormalize {
			t.Fatalf("%s should reuse only normalization: %+v", document.DocID, document.ReusedStages)
		}
	}
	incrementalExport, err := incremental.Export(context.Background())
	if err != nil {
		t.Fatalf("incremental export: %v", err)
	}

	_, full := openService(t, filepath.Join(t.TempDir(), "full.sqlite"))
	mustImportRun(t, full, changed)
	fullExport, err := full.Export(context.Background())
	if err != nil {
		t.Fatalf("full export: %v", err)
	}
	if string(canonical(t, incrementalExport)) != string(canonical(t, fullExport)) {
		t.Fatal("column-policy incremental rebuild differs from full rebuild")
	}
}

func TestContentChangeOnlyInvalidatesChangedDocument(t *testing.T) {
	baseBundle := demo.Generate()
	_, incremental := openService(t, filepath.Join(t.TempDir(), "content-incremental.sqlite"))
	mustImportRun(t, incremental, baseBundle)

	changed := demo.Generate()
	changed.Documents[0].Boxes[1].Text += " — 增量更新版本"
	report := mustImportRun(t, incremental, changed)
	for _, document := range report.Documents {
		if document.DocID == changed.Documents[0].DocID {
			if !document.Changed || document.EarliestInvalidStage != model.StageNormalize || len(document.ReusedStages) != 0 {
				t.Fatalf("changed document invalidation is incorrect: %+v", document)
			}
		} else if document.Changed || len(document.ReusedStages) != 4 {
			t.Fatalf("unaffected document did not reuse all stages: %+v", document)
		}
	}
	incrementalExport, err := incremental.Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, full := openService(t, filepath.Join(t.TempDir(), "content-full.sqlite"))
	mustImportRun(t, full, changed)
	fullExport, err := full.Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical(t, incrementalExport)) != string(canonical(t, fullExport)) {
		t.Fatal("content incremental rebuild differs from full rebuild")
	}
}

func TestGracefulStopResumeMatchesContinuousRun(t *testing.T) {
	bundle := demo.Generate()
	continuousDir := t.TempDir()
	_, continuous := openService(t, filepath.Join(continuousDir, "continuous.sqlite"))
	mustImportRun(t, continuous, bundle)
	continuousExport, err := continuous.Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	_, resumable := openService(t, filepath.Join(t.TempDir(), "resume.sqlite"))
	if err := resumable.Import(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	type runResult struct {
		report service.RunReport
		err    error
	}
	done := make(chan runResult, 1)
	go func() {
		report, err := resumable.Run(context.Background(), service.RunOptions{StageDelay: 8 * time.Millisecond})
		done <- runResult{report: report, err: err}
	}()
	time.Sleep(12 * time.Millisecond)
	resumable.RequestStop()
	result := <-done
	if result.err != nil {
		t.Fatalf("stopped run: %v", result.err)
	}
	if !result.report.Stopped || result.report.Processed != 1 {
		t.Fatalf("expected one completed document before stop, got %+v", result.report)
	}
	status, err := resumable.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	completed := 0
	for _, doc := range status.Documents {
		if len(doc.Stages) == 4 {
			completed++
		}
	}
	if completed != 1 {
		t.Fatalf("expected exactly one fully checkpointed document, got %d", completed)
	}

	resumed, err := resumable.Run(context.Background(), service.RunOptions{})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.Processed != 4 || resumed.Stopped {
		t.Fatalf("resume did not finish all documents: %+v", resumed)
	}
	resumedExport, err := resumable.Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical(t, continuousExport)) != string(canonical(t, resumedExport)) {
		t.Fatal("resumed export differs from uninterrupted run")
	}
}

func TestQueryAfterRestartUsesCommittedFacts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.sqlite")
	_, svc := openService(t, path)
	mustImportRun(t, svc, demo.Generate())
	originalExport, err := svc.Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	st2, err := store.Open(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st2.Close()
	restarted := service.New(st2)
	if err := restarted.LoadPersistedStrategy(context.Background()); err != nil {
		t.Fatal(err)
	}
	response, err := restarted.Query(context.Background(), service.QueryRequest{
		DocID: "doc-span-title-zh-en", Scope: "columns", BudgetMS: 100, HasBudget: true,
	})
	if err != nil {
		t.Fatalf("restart query: %v", err)
	}
	if len(response.Data) == 0 || response.ResultFingerprint == "" {
		t.Fatalf("empty restart query response: %+v", response)
	}
	restartedExport, err := restarted.Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical(t, originalExport)) != string(canonical(t, restartedExport)) {
		t.Fatal("export reconstructed after restart differs from original export")
	}
}

func TestImportRejectedWhileRunIsActive(t *testing.T) {
	_, svc := openService(t, filepath.Join(t.TempDir(), "import-running.sqlite"))
	bundle := demo.Generate()
	if err := svc.Import(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := svc.Run(context.Background(), service.RunOptions{StageDelay: 8 * time.Millisecond})
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for !svc.IsRunning() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !svc.IsRunning() {
		t.Fatal("run did not start")
	}
	err := svc.Import(context.Background(), bundle)
	var modelErr *model.Error
	if !errors.As(err, &modelErr) || modelErr.Code != model.CodeAlreadyRunning {
		t.Fatalf("expected E_ALREADY_RUNNING during import, got %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("run failed: %v", err)
	}
}

func TestZeroBudgetWinsBeforeMissingResult(t *testing.T) {
	_, svc := openService(t, filepath.Join(t.TempDir(), "budget-before-result.sqlite"))
	if err := svc.Import(context.Background(), demo.Generate()); err != nil {
		t.Fatal(err)
	}
	_, err := svc.Query(context.Background(), service.QueryRequest{
		DocID: "doc-double-en", Scope: "blocks", BudgetMS: 0, HasBudget: true, QueryID: "zero-before-result",
	})
	var modelErr *model.Error
	if !errors.As(err, &modelErr) || modelErr.Code != model.CodeLatencyBudget {
		t.Fatalf("expected latency budget before result lookup, got %v", err)
	}
}

func TestImportDoesNotExposeOldCheckpointResultsBeforeRun(t *testing.T) {
	_, svc := openService(t, filepath.Join(t.TempDir(), "import-before-query.sqlite"))
	if err := svc.Import(context.Background(), demo.Generate()); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Run(context.Background(), service.RunOptions{}); err != nil {
		t.Fatal(err)
	}
	changed := demo.Generate()
	changed.Strategy.Version++
	changed.Documents[0].Boxes[1].Text += " changed before rerun"
	if err := svc.Import(context.Background(), changed); err != nil {
		t.Fatal(err)
	}
	_, err := svc.Query(context.Background(), service.QueryRequest{
		DocID: changed.Documents[0].DocID, Scope: "blocks", BudgetMS: 50, HasBudget: true,
	})
	var modelErr *model.Error
	if !errors.As(err, &modelErr) || modelErr.Code != model.CodeNotFound {
		t.Fatalf("expected no result before rerun, got %v", err)
	}
	if _, err := svc.Export(context.Background()); err == nil {
		t.Fatal("export must not fall back to old checkpoints after import")
	}
	if _, err := svc.Run(context.Background(), service.RunOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Query(context.Background(), service.QueryRequest{
		DocID: changed.Documents[0].DocID, Scope: "blocks", BudgetMS: 50, HasBudget: true,
	}); err != nil {
		t.Fatalf("query after rerun failed: %v", err)
	}
}

func TestPendingRebuildSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pending-restart.sqlite")
	st, svc := openService(t, path)
	if err := svc.Import(context.Background(), demo.Generate()); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Run(context.Background(), service.RunOptions{}); err != nil {
		t.Fatal(err)
	}
	changed := demo.Generate()
	changed.Strategy.Version++
	changed.Documents[0].Boxes[1].Text += " pending restart"
	if err := svc.Import(context.Background(), changed); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := store.Open(path)
	if err != nil {
		t.Fatalf("reopen pending store: %v", err)
	}
	defer st2.Close()
	restarted := service.New(st2)
	if err := restarted.LoadPersistedStrategy(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err = restarted.Query(context.Background(), service.QueryRequest{
		DocID: changed.Documents[0].DocID, Scope: "blocks", BudgetMS: 50, HasBudget: true,
	})
	var modelErr *model.Error
	if !errors.As(err, &modelErr) || modelErr.Code != model.CodeNotFound {
		t.Fatalf("pending rebuild must reject old checkpoint query after restart, got %v", err)
	}
	if _, err := restarted.Export(context.Background()); err == nil {
		t.Fatal("pending rebuild must reject old checkpoint export after restart")
	}
	if err := restarted.Import(context.Background(), changed); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Run(context.Background(), service.RunOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Query(context.Background(), service.QueryRequest{
		DocID: changed.Documents[0].DocID, Scope: "blocks", BudgetMS: 50, HasBudget: true,
	}); err != nil {
		t.Fatalf("query after rebuild failed: %v", err)
	}
}

func TestDemonstrationFixedDigest(t *testing.T) {
	_, svc := openService(t, filepath.Join(t.TempDir(), "digest.sqlite"))
	mustImportRun(t, svc, demo.Generate())
	export, err := svc.Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	const expected = "9341a91ea9d44eefa46f6ccc4864e7f64c4a9e0bd07b0a8b215b8eda4b3279f3"
	if export.ResultFingerprint != expected {
		t.Fatalf("demo digest changed: got %s want %s", export.ResultFingerprint, expected)
	}
}

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"local/rollout-guard/internal/domain"
	"local/rollout-guard/internal/engine"
	"local/rollout-guard/internal/risk"
	"local/rollout-guard/internal/store"
	"local/rollout-guard/samples"
)

type reportCase struct {
	Name       string         `json:"name"`
	Passed     bool           `json:"passed"`
	Facts      map[string]any `json:"facts,omitempty"`
	Error      string         `json:"error,omitempty"`
	DurationMS int64          `json:"duration_ms"`
}

type demoReport struct {
	StartedAtMS    int64          `json:"started_at_ms"`
	FinishedAtMS   int64          `json:"finished_at_ms"`
	DurationMS     int64          `json:"duration_ms"`
	Passed         bool           `json:"passed"`
	PolicyVersion  int64          `json:"policy_version"`
	PolicyChecksum string         `json:"policy_checksum"`
	Cases          []reportCase   `json:"cases"`
	Recovery       map[string]any `json:"recovery,omitempty"`
	Notes          []string       `json:"notes"`
	lastMark       time.Time
}

func main() {
	reportPath := flag.String("report", "artifacts/demo-report.json", "path for the JSON verification report")
	keepDB := flag.Bool("keep-db", false, "keep the temporary SQLite database")
	flag.Parse()
	report := &demoReport{
		StartedAtMS: time.Now().UnixMilli(),
		lastMark:    time.Now(),
		Notes: []string{
			"All data is deterministic local sample data.",
			"The rollback target is an HTTP endpoint bound to a loopback ephemeral port.",
			"No Docker, external account, or public network target is used.",
		},
	}
	err := run(report, *keepDB)
	report.FinishedAtMS = time.Now().UnixMilli()
	report.DurationMS = report.FinishedAtMS - report.StartedAtMS
	report.Passed = err == nil
	if writeErr := writeReport(*reportPath, report); writeErr != nil {
		fmt.Fprintf(os.Stderr, "write report: %v\n", writeErr)
	}
	fmt.Println()
	for _, item := range report.Cases {
		mark := "PASS"
		if !item.Passed {
			mark = "FAIL"
		}
		fmt.Printf("  [%s] %-45s %6dms\n", mark, item.Name, item.DurationMS)
		if item.Error != "" {
			fmt.Printf("         %s\n", item.Error)
		}
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "\nDEMO FAILED: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("\nDEMO VERIFIED · %d checks · report=%s\n", len(report.Cases), *reportPath)
}

func run(report *demoReport, keepDB bool) error {
	root, err := findRoot()
	if err != nil {
		return err
	}
	tempDir, err := os.MkdirTemp("", "rollout-guard-demo-*")
	if err != nil {
		return err
	}
	if !keepDB {
		defer os.RemoveAll(tempDir)
	} else {
		fmt.Printf("Temporary database kept at %s\n", tempDir)
	}
	binary := filepath.Join(tempDir, "workbench")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	fmt.Println("Building local workbench…")
	if err := buildWorkbench(root, binary); err != nil {
		return err
	}

	var webhookHits atomic.Int64
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if len(body) > 0 {
			fmt.Printf("  rollback endpoint received: %s\n", strings.TrimSpace(string(body)))
		}
		webhookHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":true}`))
	}))
	defer webhook.Close()

	dbPath := filepath.Join(tempDir, "workbench.db")
	var proc *workbenchProcess
	defer func() {
		if proc != nil {
			_ = proc.stop()
		}
	}()
	step(1, "启动本地工作台与回环回滚端点")
	proc, err = startWorkbench(root, binary, dbPath, webhook.URL, nil)
	if err != nil {
		return err
	}
	client := newDemoClient(proc.baseURL)
	if err := client.waitReady(); err != nil {
		return err
	}
	if err := check(report, "HTTP 健康检查与回环绑定", true, map[string]any{"base_url": proc.baseURL, "rollback_url": webhook.URL}); err != nil {
		return err
	}
	fmt.Printf("  workbench=%s\n  rollback=%s\n", proc.baseURL, webhook.URL)

	policy, err := samples.Policy()
	if err != nil {
		return err
	}
	report.PolicyVersion = policy.Version
	report.PolicyChecksum = policy.Checksum
	batches, err := samples.Batches()
	if err != nil {
		return err
	}
	observations, err := samples.Observations()
	if err != nil {
		return err
	}

	step(2, "通过 HTTP 提交策略、批次和固定观测")
	var policyResult engine.SubmitPolicyResult
	if err := client.do(http.MethodPost, "/api/policies", policy, &policyResult); err != nil {
		return err
	}
	for _, batch := range batches {
		var result engine.SubmitBatchResult
		if err := client.do(http.MethodPost, "/api/batches", batch, &result); err != nil {
			return fmt.Errorf("submit batch %s: %w", batch.ID, err)
		}
	}
	batchIDs := make([]string, 0, len(observations))
	for batchID := range observations {
		batchIDs = append(batchIDs, batchID)
	}
	sort.Strings(batchIDs)
	ingested := 0
	for _, batchID := range batchIDs {
		var result engine.IngestResult
		if err := client.do(http.MethodPost, "/api/batches/"+batchID+"/observations",
			map[string]any{"observations": observations[batchID]}, &result); err != nil {
			return fmt.Errorf("ingest %s: %w", batchID, err)
		}
		ingested += result.Accepted
	}
	if err := check(report, "策略、批次与观测提交", ingested == 36, map[string]any{
		"policy_version": policy.Version, "policy_checksum": policy.Checksum, "batches": len(batches), "observations": ingested,
	}); err != nil {
		return err
	}
	fmt.Printf("  policy=v%d commit=%d batches=%d observations=%d\n", policy.Version, policyResult.CommitSeq, len(batches), ingested)

	step(3, "核对快速/完整同快照结论")
	var preview engine.PreviewResult
	if err := client.do(http.MethodGet, "/api/batches/chg-pass-001/risk?mode=compare", nil, &preview); err != nil {
		return err
	}
	comparison := preview.Comparison
	if comparison == nil {
		return errors.New("comparison missing")
	}
	if err := check(report, "快速视图不得越过阈值后放行", comparison.Fast.Decision.Action != risk.ActionPass && comparison.Fast.Decision.SafetyGuardApplied && comparison.Full.Decision.Action == risk.ActionPass, map[string]any{
		"fast_action": comparison.Fast.Decision.Action, "full_action": comparison.Full.Decision.Action,
		"fast_score_bp": comparison.Fast.ScoreBP, "full_score_bp": comparison.Full.ScoreBP,
		"fast_sample_rate_ppm": comparison.Fast.Sampling.SampleRatePPM,
	}); err != nil {
		return err
	}
	if err := check(report, "快速/完整共享策略与提交边界", comparison.Fast.PolicyVersion == comparison.Full.PolicyVersion && comparison.Fast.AsOfCommitSeq == comparison.Full.AsOfCommitSeq && comparison.Fast.ObservationRevision == comparison.Full.ObservationRevision, map[string]any{
		"policy_version": comparison.PolicyVersion, "as_of_commit": comparison.AsOfCommitSeq, "observation_revision": comparison.ObservationRevision,
	}); err != nil {
		return err
	}
	fmt.Printf("  fast action=%s score=%d rate=%.3f; full action=%s score=%d\n",
		comparison.Fast.Decision.Action, comparison.Fast.ScoreBP, float64(comparison.Fast.Sampling.SampleRatePPM)/1_000_000,
		comparison.Full.Decision.Action, comparison.Full.ScoreBP)

	step(4, "完整裁决放行低风险批次")
	var passResult engine.EvaluateResult
	if err := client.do(http.MethodPost, "/api/batches/chg-pass-001/evaluate?mode=full", map[string]any{}, &passResult); err != nil {
		return err
	}
	if err := check(report, "低风险批次放行", passResult.Batch.Status == domain.StatusPassed && passResult.Evaluation.ScoreBP == 389 && passResult.Evaluation.Decision.Action == risk.ActionPass, map[string]any{
		"batch_id": passResult.Batch.ID, "score_bp": passResult.Evaluation.ScoreBP,
		"level": passResult.Evaluation.Level, "status": passResult.Batch.Status,
	}); err != nil {
		return err
	}
	fmt.Printf("  chg-pass-001 score=%d level=%s status=%s\n", passResult.Evaluation.ScoreBP, passResult.Evaluation.Level, passResult.Batch.Status)

	step(5, "完整裁决触发自动回滚")
	var riskResult engine.EvaluateResult
	if err := client.do(http.MethodPost, "/api/batches/chg-risk-002/evaluate?mode=full", map[string]any{}, &riskResult); err != nil {
		return err
	}
	if err := check(report, "自动回滚触达并完成", riskResult.Batch.Status == domain.StatusRolledBack && riskResult.Evaluation.ScoreBP == 10000 && riskResult.Rollback != nil && riskResult.Rollback.Success && webhookHits.Load() == 1, map[string]any{
		"batch_id": riskResult.Batch.ID, "score_bp": riskResult.Evaluation.ScoreBP, "level": riskResult.Evaluation.Level,
		"status": riskResult.Batch.Status, "webhook_hits": webhookHits.Load(), "rollback": riskResult.Rollback,
	}); err != nil {
		return err
	}
	fmt.Printf("  chg-risk-002 score=%d status=%s webhook_hits=%d\n", riskResult.Evaluation.ScoreBP, riskResult.Batch.Status, webhookHits.Load())

	step(6, "同服务冷却期拦截")
	err = client.do(http.MethodPost, "/api/batches/chg-cooldown-003/evaluate?mode=full", map[string]any{}, nil)
	var failure *apiFailure
	if !errors.As(err, &failure) || failure.Code != "E_ROLLBACK_COOLDOWN" {
		return fmt.Errorf("expected E_ROLLBACK_COOLDOWN, got %v", err)
	}
	remaining, _ := failure.Context["remaining_ms"].(float64)
	if err := check(report, "服务冷却期阻止重复回滚", remaining > 0 && webhookHits.Load() == 1, map[string]any{
		"code": failure.Code, "remaining_ms": int64(remaining), "service": failure.Context["service"], "webhook_hits": webhookHits.Load(),
	}); err != nil {
		return err
	}
	fmt.Printf("  E_ROLLBACK_COOLDOWN remaining=%dms service=%v\n", int64(remaining), failure.Context["service"])

	step(7, "强制终止风险重算并验证启动恢复")
	var beforeSnapshot struct {
		AsOfCommitSeq int64 `json:"as_of_commit_seq"`
	}
	if err := client.do(http.MethodGet, "/api/snapshot", nil, &beforeSnapshot); err != nil {
		return err
	}
	if err := proc.stop(); err != nil {
		return err
	}
	proc = nil
	proc, err = startWorkbench(root, binary, dbPath, webhook.URL, []string{
		"-crash-point", "risk_eval_before_commit", "-crash-batch", "chg-recover-004",
	})
	if err != nil {
		return err
	}
	crashClient := newDemoClient(proc.baseURL)
	if err := crashClient.waitReady(); err != nil {
		return err
	}
	crashErr := crashClient.do(http.MethodPost, "/api/batches/chg-recover-004/evaluate?mode=full", map[string]any{}, nil)
	if crashErr == nil {
		return errors.New("expected forced termination during risk recompute")
	}
	exitErr := proc.wait(8 * time.Second)
	if exitErr == nil {
		return errors.New("workbench exited cleanly instead of being force-terminated")
	}
	proc = nil
	if err := check(report, "风险重算中途强制终止", true, map[string]any{
		"crash_point": "risk_eval_before_commit", "process_error": exitErr.Error(), "http_error": crashErr.Error(),
	}); err != nil {
		return err
	}

	proc, err = startWorkbench(root, binary, dbPath, webhook.URL, nil)
	if err != nil {
		return err
	}
	client = newDemoClient(proc.baseURL)
	if err := client.waitReady(); err != nil {
		return err
	}
	var recovery store.RecoveryRun
	if err := client.do(http.MethodGet, "/api/recovery", nil, &recovery); err != nil {
		return err
	}
	var batchView struct {
		Batch domain.Batch `json:"batch"`
	}
	if err := client.do(http.MethodGet, "/api/batches/chg-recover-004", nil, &batchView); err != nil {
		return err
	}
	var recoveryRisk engine.PreviewResult
	if err := client.do(http.MethodGet, "/api/batches/chg-recover-004/risk?mode=full", nil, &recoveryRisk); err != nil {
		return err
	}
	pendingFound, _ := recovery.Report["pending_found"].(float64)
	latestCommit, _ := recovery.Report["latest_commit_seq"].(float64)
	aborted, _ := recovery.Report["aborted_intents"].([]any)
	if err := check(report, "恢复扫描回退到最后完整提交", pendingFound == 1 && int64(latestCommit) == beforeSnapshot.AsOfCommitSeq && batchView.Batch.Status == domain.StatusObserving && recoveryRisk.Evaluation != nil && recoveryRisk.Evaluation.ScoreBP == 10000, map[string]any{
		"pending_found": int(pendingFound), "aborted_intents": len(aborted), "latest_commit_seq": int64(latestCommit),
		"batch_status": batchView.Batch.Status, "batch_revision": batchView.Batch.Revision,
		"recomputed_score_bp": valueOrNil(recoveryRisk.Evaluation),
	}); err != nil {
		return err
	}
	report.Recovery = recovery.Report
	fmt.Printf("  recovery pending=%d commit=%d batch_status=%s batch_revision=%d\n", int(pendingFound), int64(latestCommit), batchView.Batch.Status, batchView.Batch.Revision)

	var recoveredResult engine.EvaluateResult
	if err := client.do(http.MethodPost, "/api/batches/chg-recover-004/evaluate?mode=full", map[string]any{}, &recoveredResult); err != nil {
		return err
	}
	if err := check(report, "恢复后重新裁决可完成回滚", recoveredResult.Batch.Status == domain.StatusRolledBack && recoveredResult.Rollback != nil && recoveredResult.Rollback.Success && webhookHits.Load() == 2, map[string]any{
		"batch_id": recoveredResult.Batch.ID, "status": recoveredResult.Batch.Status,
		"score_bp": recoveredResult.Evaluation.ScoreBP, "webhook_hits": webhookHits.Load(),
	}); err != nil {
		return err
	}
	fmt.Printf("  chg-recover-004 recovered score=%d status=%s webhook_hits=%d\n", recoveredResult.Evaluation.ScoreBP, recoveredResult.Batch.Status, webhookHits.Load())
	return nil
}

func check(report *demoReport, name string, passed bool, facts map[string]any) error {
	now := time.Now()
	item := reportCase{Name: name, Passed: passed, Facts: facts, DurationMS: now.Sub(report.lastMark).Milliseconds()}
	report.lastMark = now
	if !passed {
		raw, _ := json.Marshal(facts)
		item.Error = "assertion failed: " + string(raw)
	}
	report.Cases = append(report.Cases, item)
	if !passed {
		return errors.New(item.Error)
	}
	return nil
}

func valueOrNil(evaluation *risk.Evaluation) any {
	if evaluation == nil {
		return nil
	}
	return evaluation.ScoreBP
}

func step(number int, title string) {
	fmt.Printf("\n[%d/7] %s\n", number, title)
}

func writeReport(path string, report *demoReport) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o644)
}

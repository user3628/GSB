package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"blocksnap/internal/metrics"
	"blocksnap/internal/sample"
	"blocksnap/internal/store"
)

const (
	volumeID   = "vol-demo"
	blockCount = int64(257)
	blockSize  = int64(4096)
	overhead   = int64(512)
)

type demo struct {
	base      string
	client    *http.Client
	serverBin string
	dbPath    string
	logPath   string
	process   *exec.Cmd
	logFile   *os.File
}

func main() {
	var (
		serverBin = flag.String("server-bin", "", "path to a built blocksnapd binary")
		workdir   = flag.String("workdir", "", "working directory; a temporary directory is used when empty")
		keep      = flag.Bool("keep", false, "keep the working directory")
	)
	flag.Parse()
	if *serverBin == "" {
		fmt.Fprintln(os.Stderr, "-server-bin is required")
		os.Exit(2)
	}
	temporaryWorkdir := *workdir == ""
	if temporaryWorkdir {
		dir, err := os.MkdirTemp("", "blocksnap-demo-")
		if err != nil {
			fatal(err)
		}
		*workdir = dir
	} else {
		if err := os.MkdirAll(*workdir, 0o755); err != nil {
			fatal(err)
		}
	}
	if !*keep && temporaryWorkdir {
		defer os.RemoveAll(*workdir)
	}
	if err := run(*serverBin, *workdir); err != nil {
		fmt.Fprintf(os.Stderr, "demo failed: %v\n", err)
		os.Exit(1)
	}
}

func run(serverBin, workdir string) error {
	addr, err := freeAddr()
	if err != nil {
		return err
	}
	d := &demo{
		base: "http://" + addr, client: &http.Client{Timeout: 20 * time.Second},
		serverBin: serverBin, dbPath: filepath.Join(workdir, "demo.db"), logPath: filepath.Join(workdir, "server.log"),
	}
	if err := d.start(); err != nil {
		return err
	}
	defer d.stop()

	ctx := context.Background()
	if err := d.post(ctx, "/v1/volumes", map[string]any{
		"request_id": "demo-create-volume", "volume_id": volumeID, "block_count": blockCount,
		"block_size": blockSize, "writeback_overhead_bytes_per_block": overhead,
	}, nil); err != nil {
		return fmt.Errorf("create volume: %w", err)
	}

	bad := sample.BlockContent("demo/v1", 200, 0, int(blockSize))
	if code, err := d.expectError(ctx, http.MethodPut, "/v1/volumes/"+volumeID+"/blocks/200", map[string]any{
		"request_id": "demo-bad-checksum", "expected_version": 0,
		"content_base64": base64.StdEncoding.EncodeToString(bad), "sha256": strings.Repeat("0", 64),
	}); err != nil || code != "E_BLOCK_CHECKSUM" {
		return fmt.Errorf("expected E_BLOCK_CHECKSUM, got code=%q err=%v", code, err)
	}
	var untouched struct {
		Block store.BlockView `json:"block"`
	}
	if err := d.get(ctx, "/v1/volumes/"+volumeID+"/blocks/200", &untouched); err != nil {
		return err
	}
	if untouched.Block.Version != 0 {
		return fmt.Errorf("checksum failure mutated block version to %d", untouched.Block.Version)
	}

	for block := int64(0); block < 48; block++ {
		if err := d.write(ctx, fmt.Sprintf("demo-initial-%d", block), block, 0, "demo/v1", block, 0); err != nil {
			return err
		}
	}
	if err := d.createSnapshot(ctx, "demo-snap-full", "snap-full", "full", "", 5); err != nil {
		return err
	}

	for block := int64(0); block < 8; block++ {
		if err := d.write(ctx, fmt.Sprintf("demo-inc1-overwrite-%d", block), block, 1, "demo/v1", block, 1); err != nil {
			return err
		}
	}
	for block := int64(48); block < 56; block++ {
		if err := d.write(ctx, fmt.Sprintf("demo-inc1-new-%d", block), block, 0, "demo/v1", block, 0); err != nil {
			return err
		}
	}
	if err := d.createSnapshot(ctx, "demo-snap-inc1", "snap-inc1", "incremental", "snap-full", 3); err != nil {
		return err
	}

	for block := int64(0); block < 4; block++ {
		if err := d.write(ctx, fmt.Sprintf("demo-inc2-overwrite-%d", block), block, 2, "demo/v1", block, 2); err != nil {
			return err
		}
	}
	for block := int64(48); block < 50; block++ {
		if err := d.write(ctx, fmt.Sprintf("demo-inc2-overwrite2-%d", block), block, 1, "demo/v1", block, 1); err != nil {
			return err
		}
	}
	for block := int64(56); block < 64; block++ {
		if err := d.write(ctx, fmt.Sprintf("demo-inc2-new-%d", block), block, 0, "demo/v1", block, 0); err != nil {
			return err
		}
	}
	firstInc2, err := d.createSnapshotRaw(ctx, "demo-snap-inc2", "snap-inc2", "incremental", "snap-inc1", 2)
	if err != nil {
		return err
	}
	secondInc2, err := d.createSnapshotRaw(ctx, "demo-snap-inc2", "snap-inc2", "incremental", "snap-inc1", 2)
	if err != nil {
		return fmt.Errorf("idempotent snapshot replay: %w", err)
	}
	if !bytes.Equal(firstInc2, secondInc2) {
		return errors.New("idempotent duplicate returned a different response")
	}

	if code, err := d.deleteExpectError(ctx, "demo-delete-referenced", "snap-full"); err != nil || code != "E_SNAPSHOT_REFERENCED" {
		return fmt.Errorf("expected E_SNAPSHOT_REFERENCED, got code=%q err=%v", code, err)
	}

	createdAnalyses := map[string]metrics.Analysis{}
	for _, id := range []string{"snap-full", "snap-inc1", "snap-inc2"} {
		a, err := d.analysis(ctx, id)
		if err != nil {
			return err
		}
		createdAnalyses[id] = a
	}

	mergeResp, err := d.postRaw(ctx, "/v1/volumes/"+volumeID+"/snapshots/merge", map[string]any{
		"request_id": "demo-merge-inc1", "parent_id": "snap-full", "child_id": "snap-inc1",
	})
	if err != nil {
		return fmt.Errorf("merge: %w", err)
	}

	decisionReq := map[string]any{
		"snapshot_ids": []string{"snap-full", "snap-inc2"},
		"margin_floor": "0.900000", "cost_budget_bytes": "1000000", "relative_tolerance": "0.000001",
	}
	evalBefore, err := d.evaluate(ctx, decisionReq)
	if err != nil {
		return fmt.Errorf("evaluate rollback: %w", err)
	}
	if evalBefore.SelectedSnapshotID != "snap-full" {
		return fmt.Errorf("stable lexical tie-break selected %q, want snap-full", evalBefore.SelectedSnapshotID)
	}

	rollbackResp, err := d.postRaw(ctx, "/v1/volumes/"+volumeID+"/rollback", map[string]any{
		"request_id": "demo-rollback", "snapshot_id": "snap-full",
		"margin_floor": "0.900000", "cost_budget_bytes": "1000000", "relative_tolerance": "0.000001",
	})
	if err != nil {
		return fmt.Errorf("rollback: %w", err)
	}

	beforeCrash := map[string]metrics.Analysis{}
	for _, id := range []string{"snap-full", "snap-inc2"} {
		a, err := d.analysis(ctx, id)
		if err != nil {
			return err
		}
		beforeCrash[id] = a
	}
	evalBeforeCrash, err := d.evaluate(ctx, decisionReq)
	if err != nil {
		return err
	}
	snapsBefore, err := d.listSnapshots(ctx)
	if err != nil {
		return err
	}
	metricsBefore := map[string][]byte{}
	for id, a := range beforeCrash {
		metricsBefore[id], _ = json.Marshal(a.Metrics)
	}
	evalJSONBefore, _ := json.Marshal(evalBeforeCrash)

	// The server calls os.Exit(86) after committing an intent and partially
	// mutating an uncommitted transaction.
	_ = d.postMayDisconnect(ctx, "/v1/debug/crash", map[string]any{
		"volume_id": volumeID, "operation": "merge", "parent_id": "snap-full", "child_id": "snap-inc2",
		"request_id": "demo-crash-merge",
	})
	exitCode, waitErr := d.waitForExit()
	if exitCode != 86 {
		return fmt.Errorf("expected forced exit code 86, got %d (wait err %v)", exitCode, waitErr)
	}
	if err := d.start(); err != nil {
		return fmt.Errorf("restart after forced termination: %w", err)
	}

	report, err := d.recoveryReport(ctx)
	if err != nil {
		return fmt.Errorf("recovery report: %w", err)
	}
	if report.Status != "complete" || report.PendingFound != 1 || report.AbortedCount != 1 {
		return fmt.Errorf("unexpected recovery report: %+v", report)
	}

	afterCrash := map[string]metrics.Analysis{}
	for _, id := range []string{"snap-full", "snap-inc2"} {
		a, err := d.analysis(ctx, id)
		if err != nil {
			return err
		}
		afterCrash[id] = a
		afterJSON, _ := json.Marshal(a.Metrics)
		if !bytes.Equal(metricsBefore[id], afterJSON) {
			return fmt.Errorf("metrics for %s changed across forced restart", id)
		}
	}
	evalAfterCrash, err := d.evaluate(ctx, decisionReq)
	if err != nil {
		return err
	}
	evalJSONAfter, _ := json.Marshal(evalAfterCrash)
	if !bytes.Equal(evalJSONBefore, evalJSONAfter) {
		return errors.New("rollback decision changed across forced restart")
	}
	snapsAfter, err := d.listSnapshots(ctx)
	if err != nil {
		return err
	}
	if snapIDs(snapsBefore) != snapIDs(snapsAfter) {
		return fmt.Errorf("snapshot chain changed across crash: before=%s after=%s", snapIDs(snapsBefore), snapIDs(snapsAfter))
	}

	summary := buildSummary(createdAnalyses, afterCrash, evalAfterCrash, evalBefore, report, rollbackResp, mergeResp)
	var arithmetic map[string]any
	if err := d.get(ctx, "/v1/diagnostics/arithmetic", &arithmetic); err != nil {
		return err
	}
	summary["arithmetic_diagnostics"] = arithmetic
	summary["verification"] = map[string]any{
		"checksum_error":                         "E_BLOCK_CHECKSUM and block version remained 0",
		"referenced_delete":                      "E_SNAPSHOT_REFERENCED",
		"idempotent_replay":                      "byte-identical snapshot response",
		"forced_exit_code":                       exitCode,
		"recovery_status":                        report.Status,
		"metrics_byte_identical_across_restart":  true,
		"decision_byte_identical_across_restart": true,
		"snapshot_chain":                         snapIDs(snapsAfter),
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(summary); err != nil {
		return err
	}
	return nil
}

func buildSummary(createdAnalyses, final map[string]metrics.Analysis, eval, evalBefore store.EvaluateRollbackResult, report store.RecoveryReport, rollbackRaw, mergeRaw []byte) map[string]any {
	order := []string{"snap-full", "snap-inc1", "snap-inc2"}
	created := make([]map[string]any, 0, len(order))
	for _, id := range order {
		if a, ok := createdAnalyses[id]; ok {
			created = append(created, metricSummary(a))
		}
	}
	finalViews := make([]map[string]any, 0, 2)
	for _, id := range []string{"snap-full", "snap-inc2"} {
		if a, ok := final[id]; ok {
			finalViews = append(finalViews, metricSummary(a))
		}
	}
	var merge store.MergeResult
	var rollback store.RollbackResult
	_ = json.Unmarshal(mergeRaw, &merge)
	_ = json.Unmarshal(rollbackRaw, &rollback)
	return map[string]any{
		"volume": map[string]any{
			"id": volumeID, "block_count": blockCount, "block_size": blockSize,
			"capacity_bytes": blockCount * blockSize, "writeback_overhead_per_block": overhead,
			"sample_seed": "demo/v1",
		},
		"created_snapshot_versions": created,
		"merge": map[string]any{
			"merged_into": merge.MergedInto, "removed_snapshot": merge.Removed,
			"parent_weight": merge.ParentWeight, "rewritten_parent_refs": merge.RewrittenRefs,
			"reparented_children": merge.ReparentedChildren,
		},
		"rollback_candidate_evaluation":        evaluationSummary(eval),
		"tie_break_evaluation_before_rollback": evaluationSummary(evalBefore),
		"rollback": map[string]any{
			"snapshot_id": rollback.SnapshotID, "accepted": rollback.Decision.Accepted,
			"written_back_blocks": rollback.WrittenBackBlocks, "written_back_bytes": rollback.WrittenBackBytes,
			"post_rollback": metricSummary(rollback.Analysis),
		},
		"recovered_snapshot_versions": finalViews,
		"recovery_report": map[string]any{
			"status": report.Status, "pending_found": report.PendingFound,
			"aborted_count": report.AbortedCount, "committed_count": report.CommittedCount,
			"details": report.Details,
		},
		"numeric_policy": map[string]any{
			"ratio_scale": 6, "rounding": "half-away-from-zero",
			"cumulative_rounding_error_units":        final["snap-full"].Metrics.Rounding.CumulativeErrorUpperUnits,
			"weighted_naive_error_bound_units":       final["snap-full"].Metrics.WeightedConsistency.NaiveErrorUpperUnits,
			"weighted_compensated_error_bound_units": final["snap-full"].Metrics.WeightedConsistency.CompensatedErrorUnits,
		},
	}
}

func evaluationSummary(eval store.EvaluateRollbackResult) map[string]any {
	rows := make([]map[string]any, 0, len(eval.Evaluations))
	for _, item := range eval.Evaluations {
		rows = append(rows, map[string]any{
			"snapshot_id":            item.SnapshotID,
			"margin_ratio":           item.Analysis.Metrics.RollbackMargin.Ratio.Decimal,
			"cost_bytes":             item.Analysis.Metrics.RollbackCost.CostBytes,
			"accepted":               item.Decision.Accepted,
			"reason":                 item.Decision.Reason,
			"margin_effective_lower": item.Decision.MarginFloorEffectiveLower,
			"cost_effective_upper":   item.Decision.CostBudgetEffectiveUpper,
		})
	}
	return map[string]any{
		"candidate_order": eval.CandidateOrder, "evaluations": rows,
		"selected_snapshot_id": eval.SelectedSnapshotID,
	}
}

func metricSummary(a metrics.Analysis) map[string]any {
	return map[string]any{
		"snapshot_id": a.Snapshot.ID, "kind": a.Snapshot.Kind, "weight": a.Snapshot.Weight,
		"chain":                            a.Chain,
		"used_capacity_ratio":              a.Metrics.CapacityUsage.Ratio.Decimal,
		"used_bytes":                       a.Metrics.CapacityUsage.UsedBytes,
		"capacity_bytes":                   a.Metrics.CapacityUsage.CapacityBytes,
		"rollback_margin_ratio":            a.Metrics.RollbackMargin.Ratio.Decimal,
		"chain_storage_bytes":              a.Metrics.RollbackMargin.ChainStorageBytes,
		"writeback_blocks":                 a.Metrics.RollbackMargin.WritebackBlocks,
		"writeback_bytes":                  a.Metrics.RollbackCost.WritebackBytes,
		"weighted_consistency":             a.Metrics.WeightedConsistency.Ratio.Decimal,
		"rollback_cost_bytes":              a.Metrics.RollbackCost.CostBytes,
		"rounding_error_bound_units":       a.Metrics.Rounding.CumulativeErrorUpperUnits,
		"naive_weighted_error_bound_units": a.Metrics.WeightedConsistency.NaiveErrorUpperUnits,
	}
}

func (d *demo) start() error {
	if d.logFile == nil {
		f, err := os.OpenFile(d.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		d.logFile = f
	}
	cmd := exec.Command(d.serverBin, "-addr", strings.TrimPrefix(d.base, "http://"), "-db", d.dbPath, "-fault-injection")
	cmd.Stdout = d.logFile
	cmd.Stderr = d.logFile
	if err := cmd.Start(); err != nil {
		return err
	}
	d.process = cmd
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := d.client.Get(d.base + "/healthz")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("server did not become ready; see %s", d.logPath)
}

func (d *demo) stop() {
	if d.process == nil || d.process.Process == nil {
		return
	}
	_ = d.process.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() { _ = d.process.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = d.process.Process.Kill()
		<-done
	}
	if d.logFile != nil {
		d.logFile.Close()
		d.logFile = nil
	}
	d.process = nil
}

func (d *demo) waitForExit() (int, error) {
	if d.process == nil {
		return -1, errors.New("no running process")
	}
	err := d.process.Wait()
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			code = exitErr.ExitCode()
		} else {
			return -1, err
		}
	}
	d.process = nil
	return code, nil
}

func (d *demo) write(ctx context.Context, reqID string, block, expected int64, seed string, genNo, generation int64) error {
	content := sample.BlockContent(seed, genNo, generation, int(blockSize))
	return d.request(ctx, http.MethodPut, fmt.Sprintf("/v1/volumes/%s/blocks/%d", volumeID, block), map[string]any{
		"request_id": reqID, "expected_version": expected,
		"content_base64": base64.StdEncoding.EncodeToString(content), "sha256": sample.DigestHex(content),
	}, nil)
}

func (d *demo) createSnapshot(ctx context.Context, reqID, snapshotID, kind, parent string, weight int64) error {
	_, err := d.createSnapshotRaw(ctx, reqID, snapshotID, kind, parent, weight)
	return err
}

func (d *demo) createSnapshotRaw(ctx context.Context, reqID, snapshotID, kind, parent string, weight int64) ([]byte, error) {
	body := map[string]any{"request_id": reqID, "snapshot_id": snapshotID, "kind": kind, "weight": weight}
	if parent != "" {
		body["parent_id"] = parent
	}
	return d.postRaw(ctx, "/v1/volumes/"+volumeID+"/snapshots", body)
}

func (d *demo) analysis(ctx context.Context, snapshotID string) (metrics.Analysis, error) {
	var env struct {
		Analysis metrics.Analysis `json:"analysis"`
	}
	err := d.get(ctx, "/v1/volumes/"+volumeID+"/snapshots/"+url.PathEscape(snapshotID)+"/analysis", &env)
	return env.Analysis, err
}

func (d *demo) evaluate(ctx context.Context, body map[string]any) (store.EvaluateRollbackResult, error) {
	var out store.EvaluateRollbackResult
	err := d.post(ctx, "/v1/volumes/"+volumeID+"/rollback/evaluate", body, &out)
	return out, err
}

func (d *demo) listSnapshots(ctx context.Context) (store.SnapshotListResult, error) {
	var out store.SnapshotListResult
	err := d.get(ctx, "/v1/volumes/"+volumeID+"/snapshots", &out)
	return out, err
}

func (d *demo) recoveryReport(ctx context.Context) (store.RecoveryReport, error) {
	var out store.RecoveryReport
	err := d.get(ctx, "/v1/recovery/reports/latest", &out)
	return out, err
}

func (d *demo) deleteExpectError(ctx context.Context, reqID, snapshotID string) (string, error) {
	body, _ := json.Marshal(map[string]string{"request_id": reqID})
	req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, d.base+"/v1/volumes/"+volumeID+"/snapshots/"+url.PathEscape(snapshotID), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 400 {
		return "", fmt.Errorf("expected error, got status %d body %s", resp.StatusCode, raw)
	}
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(raw, &envelope)
	return envelope.Error.Code, nil
}

func (d *demo) get(ctx context.Context, path string, out any) error {
	return d.request(ctx, http.MethodGet, path, nil, out)
}

func (d *demo) post(ctx context.Context, path string, body any, out any) error {
	return d.request(ctx, http.MethodPost, path, body, out)
}

func (d *demo) expectError(ctx context.Context, method, path string, body any) (string, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	req, _ := http.NewRequestWithContext(ctx, method, d.base+path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	response, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 400 {
		return "", fmt.Errorf("expected error status, got %d: %s", resp.StatusCode, response)
	}
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		return "", err
	}
	return envelope.Error.Code, nil
}

func (d *demo) postRaw(ctx context.Context, path string, body any) ([]byte, error) {
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.base+path, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	response, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, response)
	}
	return response, nil
}

func (d *demo) postMayDisconnect(ctx context.Context, path string, body any) error {
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, d.base+path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return fmt.Errorf("crash endpoint returned HTTP %d instead of terminating", resp.StatusCode)
}

func (d *demo) request(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, d.base+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, raw)
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	return nil
}

func freeAddr() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr, nil
}

func snapIDs(list store.SnapshotListResult) string {
	ids := make([]string, len(list.Snapshots))
	for i := range list.Snapshots {
		ids[i] = list.Snapshots[i].ID
	}
	return strings.Join(ids, ",")
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"elastic-scale/internal/model"
	"elastic-scale/internal/seed"
)

type demoConfig struct {
	baseURL  string
	seedPath string
}

func loadBundle(path string) (*seed.Bundle, error) {
	if path == "" {
		return seed.Generate(), nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var bundle seed.Bundle
	if err := json.Unmarshal(b, &bundle); err != nil {
		return nil, err
	}
	return &bundle, nil
}

func runDemo(args []string) error {
	cfg := demoConfig{}
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	fs.StringVar(&cfg.baseURL, "base-url", "http://127.0.0.1:18080", "platform base URL")
	fs.StringVar(&cfg.seedPath, "seed", "artifacts/seed.json", "seed bundle JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	bundle, err := loadBundle(cfg.seedPath)
	if err != nil {
		return err
	}
	if err := waitHealth(cfg.baseURL); err != nil {
		return err
	}
	report := map[string]any{"base_url": cfg.baseURL, "fixed_seed": bundle.Seed}

	catalogReq := CatalogRequestCLI{CommitID: "catalog-seed", Services: bundle.Catalog.Services}
	if _, _, _, err := doJSON(http.MethodPut, cfg.baseURL+"/v1/catalog", catalogReq, nil); err != nil {
		return fmt.Errorf("load catalog: %w", err)
	}
	report["catalog_services"] = len(bundle.Catalog.Services)
	fmt.Println("PASS catalog loaded")

	bad := bundle.Snapshot
	bad.Digest = "sha256:deliberately-invalid"
	badReq := model.SnapshotRequest{CommitID: "snapshot-invalid-digest", Snapshot: bad}
	if _, code, _, err := doJSON(http.MethodPost, cfg.baseURL+"/v1/snapshots", badReq, nil); err == nil || code != model.CodeSnapshotInvalid {
		return fmt.Errorf("expected %s for bad digest, code=%s err=%v", model.CodeSnapshotInvalid, code, err)
	}
	fmt.Println("PASS invalid digest rejected with E_SNAPSHOT_INVALID")

	validReq := model.SnapshotRequest{CommitID: "snapshot-v1", Snapshot: bundle.Snapshot}
	var loaded model.CommitResponse
	if _, _, _, err := doJSON(http.MethodPost, cfg.baseURL+"/v1/snapshots", validReq, &loaded); err != nil {
		return fmt.Errorf("load valid snapshot: %w", err)
	}
	if loaded.Version != 1 || loaded.Rebuild == nil {
		return fmt.Errorf("unexpected load response: %+v", loaded)
	}
	fmt.Printf("PASS snapshot loaded version=%d samples=%d path=%s\n", loaded.Version, len(bundle.Snapshot.Samples), loaded.Rebuild.SelectedPath)

	var replay model.CommitResponse
	if _, _, _, err := doJSON(http.MethodPost, cfg.baseURL+"/v1/snapshots", validReq, &replay); err != nil {
		return err
	}
	if !replay.IdempotentReplay || replay.Version != 1 {
		return fmt.Errorf("expected idempotent snapshot replay, got %+v", replay)
	}
	fmt.Println("PASS same commit_id replayed original result")

	conflictReq := validReq
	conflictReq.Snapshot.GeneratedAtMS++
	if _, code, _, err := doJSON(http.MethodPost, cfg.baseURL+"/v1/snapshots", conflictReq, nil); err == nil || code != model.CodeConflictRequest {
		return fmt.Errorf("expected E_CONFLICTING_REQUEST, code=%s err=%v", code, err)
	}
	fmt.Println("PASS conflicting same commit_id rejected with E_CONFLICTING_REQUEST")

	dupVersion := validReq
	dupVersion.CommitID = "snapshot-same-version-new-commit"
	if _, code, _, err := doJSON(http.MethodPost, cfg.baseURL+"/v1/snapshots", dupVersion, nil); err == nil || code != model.CodeSnapshotInvalid {
		return fmt.Errorf("expected E_SNAPSHOT_INVALID for duplicate version, code=%s err=%v", code, err)
	}
	if _, _, _, err := doJSON(http.MethodGet, cfg.baseURL+"/v1/metrics/inventory-svc?observed_version=1", nil, &model.ReadMetricsResponse{}); err != nil {
		return fmt.Errorf("old snapshot must remain usable: %w", err)
	}
	fmt.Println("PASS non-monotonic snapshot rejected and old snapshot remains readable")

	incA := bundle.SingleIncrement
	incA.CommitID = "inc-concurrent-single-a"
	incB := bundle.SingleIncrement
	incB.CommitID = "inc-concurrent-single-b"
	type concurrentResult struct {
		resp *model.CommitResponse
		code string
		err  error
	}
	var wg sync.WaitGroup
	results := make([]concurrentResult, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		var resp model.CommitResponse
		_, code, _, err := doJSON(http.MethodPost, cfg.baseURL+"/v1/increments", incA, &resp)
		results[0] = concurrentResult{resp: &resp, code: code, err: err}
	}()
	go func() {
		defer wg.Done()
		var resp model.CommitResponse
		_, code, _, err := doJSON(http.MethodPost, cfg.baseURL+"/v1/increments", incB, &resp)
		results[1] = concurrentResult{resp: &resp, code: code, err: err}
	}()
	wg.Wait()
	successes, stale := 0, 0
	var singleRun *model.RebuildRun
	for _, res := range results {
		if res.err == nil && res.resp != nil && res.resp.Version == 2 {
			successes++
			singleRun = res.resp.Rebuild
		}
		if res.code == model.CodeStaleBase {
			stale++
		}
	}
	if successes != 1 || stale != 1 {
		return fmt.Errorf("concurrent base1 increments: successes=%d stale=%d results=%+v", successes, stale, results)
	}
	if singleRun == nil || singleRun.SelectedPath != "incremental" {
		return fmt.Errorf("single-service increment should select incremental path: %+v", singleRun)
	}
	fmt.Printf("PASS concurrent same-base increments: exactly one success, one E_STALE_BASE; path=%s ratio=%d scope=%v\n",
		singleRun.SelectedPath, singleRun.TouchedRatioMicro, singleRun.RecomputeScope)
	report["single_increment_rebuild"] = singleRun

	var metrics model.ReadMetricsResponse
	if _, _, _, err := doJSON(http.MethodGet, cfg.baseURL+"/v1/metrics/inventory-svc?observed_version=2", nil, &metrics); err != nil {
		return err
	}
	var decision model.DecisionResponse
	if _, _, _, err := doJSON(http.MethodGet, cfg.baseURL+"/v1/decisions/inventory-svc?observed_version=2", nil, &decision); err != nil {
		return err
	}
	fmt.Printf("PASS decision inventory desired=%d raw=%d risk_micro=%d sample_size=%d basis_instances=%d\n",
		decision.DesiredReplicas, decision.RawReplicas, decision.RiskScoreMicro, decision.SampleSize, len(decision.Basis))

	var fullRebuild model.CommitResponse
	rebuildReq := model.RebuildRequest{CommitID: "rebuild-full-v2-compare", Mode: "full"}
	if _, _, _, err := doJSON(http.MethodPost, cfg.baseURL+"/v1/rebuilds", rebuildReq, &fullRebuild); err != nil {
		return err
	}
	if fullRebuild.Rebuild == nil || fullRebuild.Version != 2 {
		return fmt.Errorf("unexpected full rebuild %+v", fullRebuild)
	}
	if fullRebuild.Rebuild.MetricsChecksum != singleRun.MetricsChecksum || fullRebuild.Rebuild.DecisionsChecksum != singleRun.DecisionsChecksum {
		return fmt.Errorf("path mismatch:\nincremental metrics=%s decisions=%s\nfull metrics=%s decisions=%s",
			singleRun.MetricsChecksum, singleRun.DecisionsChecksum, fullRebuild.Rebuild.MetricsChecksum, fullRebuild.Rebuild.DecisionsChecksum)
	}
	fmt.Printf("PASS incremental and full paths bitwise checksums identical at v2: metrics=%s decisions=%s\n",
		singleRun.MetricsChecksum, singleRun.DecisionsChecksum)
	report["path_checksum_equal"] = true

	staleObserved := bundle.StaleReadVersion
	if _, code, ctx, err := doJSON(http.MethodGet, fmt.Sprintf("%s/v1/metrics/inventory-svc?observed_version=%d", cfg.baseURL, staleObserved), nil, nil); err == nil || code != model.CodeStaleRead {
		return fmt.Errorf("expected E_STALE_READ, code=%s err=%v", code, err)
	} else if ctx["current_materialized_version"] == nil {
		return fmt.Errorf("E_STALE_READ context missing current_materialized_version: %v", ctx)
	}
	if _, code, _, err := doJSON(http.MethodGet, fmt.Sprintf("%s/v1/decisions/inventory-svc?observed_version=%d", cfg.baseURL, staleObserved), nil, nil); err == nil || code != model.CodeStaleRead {
		return fmt.Errorf("expected decision E_STALE_READ, code=%s err=%v", code, err)
	}
	fmt.Printf("PASS observed_version=%d rejected with E_STALE_READ and current materialized version context\n", staleObserved)

	if _, code, _, err := doJSON(http.MethodGet, cfg.baseURL+"/v1/decisions/auth-svc?min_samples=999", nil, nil); err == nil || code != model.CodeInsufficientSamples {
		return fmt.Errorf("expected E_INSUFFICIENT_SAMPLES, code=%s err=%v", code, err)
	}
	fmt.Println("PASS insufficient sample size rejected with E_INSUFFICIENT_SAMPLES")

	var impact model.ImpactResponse
	if _, _, _, err := doJSON(http.MethodGet, cfg.baseURL+"/v1/impact/inventory-svc", nil, &impact); err != nil {
		return err
	}
	fmt.Printf("PASS impact trigger=inventory-svc downstream=%d upstream=%d\n", len(impact.ImpactSet), len(impact.UpstreamDependencies))

	staleReq := bundle.SingleIncrement
	staleReq.CommitID = "inc-stale-base-after-concurrent"
	staleReq.BaseVersion = 1
	if _, code, _, err := doJSON(http.MethodPost, cfg.baseURL+"/v1/increments", staleReq, nil); err == nil || code != model.CodeStaleBase {
		return fmt.Errorf("expected sequential E_STALE_BASE, code=%s err=%v", code, err)
	}
	fmt.Println("PASS late base_version=1 rejected with E_STALE_BASE")

	globalReq := bundle.GlobalIncrement
	var globalResp model.CommitResponse
	if _, _, _, err := doJSON(http.MethodPost, cfg.baseURL+"/v1/increments", globalReq, &globalResp); err != nil {
		return fmt.Errorf("global increment: %w", err)
	}
	if globalResp.Version != 3 || globalResp.Rebuild == nil || globalResp.Rebuild.SelectedPath != "full" {
		return fmt.Errorf("global increment should select full: %+v", globalResp.Rebuild)
	}
	fmt.Printf("PASS global increment version=3 path=full threshold_micro=%d actual_ratio_micro=%d touched_samples=%d/%d scope=%d\n",
		globalResp.Rebuild.ThresholdMicro, globalResp.Rebuild.TouchedRatioMicro,
		globalResp.Rebuild.TouchedSampleRecords, globalResp.Rebuild.TotalSampleRecords, len(globalResp.Rebuild.RecomputeScope))
	report["global_increment_rebuild"] = globalResp.Rebuild

	out, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(out))
	return nil
}

func runArmCrash(args []string) error {
	fs := flag.NewFlagSet("arm-crash", flag.ContinueOnError)
	baseURL := fs.String("base-url", "http://127.0.0.1:18080", "platform base URL")
	seedPath := fs.String("seed", "artifacts/seed.json", "seed bundle path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	bundle, err := loadBundle(*seedPath)
	if err != nil {
		return err
	}
	if err := waitHealth(*baseURL); err != nil {
		return err
	}
	req := bundle.CrashIncrement
	var resp model.CommitResponse
	_, code, _, err := doJSON(http.MethodPost, *baseURL+"/v1/increments", req, &resp)
	if err != nil {
		return fmt.Errorf("arm crash request failed: %w (code=%s)", err, code)
	}
	if !resp.CrashArmed {
		return fmt.Errorf("server did not arm crash intent: %+v", resp)
	}
	fmt.Printf("PASS crash intent pending commit_id=%s base_version=%d; send kill -9 now\n", resp.CommitID, req.BaseVersion)
	return nil
}

func waitHealth(baseURL string) error {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		resp, err := client.Get(baseURL + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
			last = fmt.Errorf("health status %d", resp.StatusCode)
		} else {
			last = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("service not healthy at %s: %w", baseURL, last)
}

type CatalogRequestCLI struct {
	CommitID string          `json:"commit_id"`
	Services []model.Service `json:"services"`
}

func doJSON(method, url string, body any, out any) (int, string, map[string]any, error) {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, "", nil, err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return 0, "", nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", nil, err
	}
	if resp.StatusCode >= 400 {
		var parsed struct {
			Error struct {
				Code    string         `json:"code"`
				Context map[string]any `json:"context"`
				Message string         `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(raw, &parsed)
		return resp.StatusCode, parsed.Error.Code, parsed.Error.Context, fmt.Errorf("%s %s -> %d %s: %s", method, url, resp.StatusCode, parsed.Error.Code, parsed.Error.Message)
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, "", nil, fmt.Errorf("decode response: %w; body=%s", err, string(raw))
		}
	}
	return resp.StatusCode, "", nil, nil
}

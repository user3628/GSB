package prefixcache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// DemoStep is one observable operation in the deterministic demonstration.
type DemoStep struct {
	Name             string             `json:"name"`
	Version          int64              `json:"version,omitempty"`
	Detail           string             `json:"detail,omitempty"`
	Decision         Decision           `json:"decision,omitempty"`
	Evidence         SimilarityEvidence `json:"evidence,omitempty"`
	InvalidatedStage Stage              `json:"invalidated_stage,omitempty"`
	ReusedSlotCount  int                `json:"reused_slot_count,omitempty"`
	SlotCount        int                `json:"slot_count,omitempty"`
	Error            *Error             `json:"error,omitempty"`
}

// DemoOutcomes captures the three required adjudication classes and evidence.
type DemoOutcomes struct {
	BaseDecision           Decision `json:"base_decision"`
	BaseSlotNo             int      `json:"base_slot_no"`
	NearPrefixDecision     Decision `json:"near_prefix_decision"`
	NearPrefixSimilarity   float64  `json:"near_prefix_similarity"`
	NearPrefixMargin       float64  `json:"near_prefix_threshold_margin"`
	UnrelatedDecision      Decision `json:"unrelated_decision"`
	InitialBudgetEvictions int      `json:"initial_budget_evictions"`
	InitialSlotCount       int      `json:"initial_slot_count"`
}

// DemoRecovery records the interruption and reopen consistency checks.
type DemoRecovery struct {
	InterruptedCode       string `json:"interrupted_code"`
	VersionAfterInterrupt int64  `json:"version_after_interrupt"`
	RetriedVersion        int64  `json:"retried_version"`
	ReopenVersion         int64  `json:"reopen_version"`
	FingerprintStable     bool   `json:"fingerprint_stable"`
	HTTPHealth            string `json:"http_health"`
	HTTPShutdown          string `json:"http_shutdown"`
}

// DemoReport is stable for a given seed and database lifecycle. It deliberately
// excludes wall-clock timestamps and ephemeral listener addresses.
type DemoReport struct {
	Seed             int64        `json:"seed"`
	DatasetRequests  int          `json:"dataset_requests"`
	InitialPolicy    Policy       `json:"initial_policy"`
	Outcomes         DemoOutcomes `json:"outcomes"`
	Recovery         DemoRecovery `json:"recovery"`
	Steps            []DemoStep   `json:"steps"`
	FinalVersion     int64        `json:"final_version"`
	FinalFingerprint string       `json:"final_fingerprint"`
}

// RunDemo executes the deterministic local scenario. It returns only after the
// SQLite engine has been closed and reopened once, and after the loopback HTTP
// server has completed a graceful shutdown.
func RunDemo(ctx context.Context, dbPath string, seed int64) (DemoReport, error) {
	dataset := GenerateDataset(seed)
	report := DemoReport{
		Seed:            seed,
		DatasetRequests: len(dataset.Requests),
		InitialPolicy:   dataset.Policy,
	}
	engine, err := Open(ctx, dbPath)
	if err != nil {
		return report, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = engine.Close()
		}
	}()

	initResult, err := engine.Initialize(ctx, dataset.Policy, dataset.Requests)
	if err != nil {
		return report, fmt.Errorf("demo initialize: %w", err)
	}
	report.Steps = append(report.Steps, DemoStep{
		Name:      "bootstrap",
		Version:   initResult.Version,
		Detail:    fmt.Sprintf("kind=%s slots=%d reused=%d", initResult.Kind, initResult.SlotCount, initResult.ReusedSlotCount),
		SlotCount: initResult.SlotCount,
	})
	report.Outcomes.InitialSlotCount = initResult.SlotCount
	for _, eviction := range initResult.Evictions {
		if eviction.Reason == "occupancy_budget" {
			report.Outcomes.InitialBudgetEvictions++
		}
	}

	base, err := engine.Hit(ctx, HitQuery{ObservedVersion: initResult.Version, RequestID: "req-base"})
	if err != nil {
		return report, fmt.Errorf("demo base hit: %w", err)
	}
	report.Outcomes.BaseDecision = base.Decision
	if base.SlotNo != nil {
		report.Outcomes.BaseSlotNo = *base.SlotNo
	}
	report.Steps = append(report.Steps, hitStep("hit_base", base))

	for _, requestID := range []string{"req-template-rewrite", "req-system-added", "req-system-removed", "req-append-turn", "req-similar-prefix"} {
		hit, err := engine.Hit(ctx, HitQuery{ObservedVersion: initResult.Version, RequestID: requestID})
		if err != nil {
			return report, fmt.Errorf("demo hit %s: %w", requestID, err)
		}
		report.Steps = append(report.Steps, hitStep("hit_"+requestID, hit))
	}

	near := DemoNearPrefix()
	nearHit, err := engine.Hit(ctx, HitQuery{ObservedVersion: initResult.Version, Request: &near})
	if err != nil {
		return report, fmt.Errorf("demo near hit: %w", err)
	}
	report.Outcomes.NearPrefixDecision = nearHit.Decision
	report.Outcomes.NearPrefixSimilarity = nearHit.Evidence.Similarity
	report.Outcomes.NearPrefixMargin = nearHit.Evidence.ThresholdMargin
	report.Steps = append(report.Steps, hitStep("hit_near_but_not_equal_prefix", nearHit))

	unrelated := DemoUnrelatedQuery()
	unrelatedHit, err := engine.Hit(ctx, HitQuery{ObservedVersion: initResult.Version, Request: &unrelated})
	if err != nil {
		return report, fmt.Errorf("demo unrelated hit: %w", err)
	}
	report.Outcomes.UnrelatedDecision = unrelatedHit.Decision
	report.Steps = append(report.Steps, hitStep("hit_unrelated", unrelatedHit))

	_, err = engine.Hit(ctx, HitQuery{ObservedVersion: 0, RequestID: "req-base"})
	var stale *Error
	if !errors.As(err, &stale) || stale.Code != CodeStaleVersion {
		return report, fmt.Errorf("demo stale query: expected %s, got %v", CodeStaleVersion, err)
	}
	report.Steps = append(report.Steps, DemoStep{Name: "stale_observation", Error: stale})

	threshold := 0.83
	engine.beforeCommit = func(commitInfo) error { return errors.New("simulated process termination") }
	_, err = engine.ApplyPatch(ctx, initResult.Version, PolicyPatch{MergeThreshold: &threshold})
	if !errors.As(err, &stale) || stale.Code != CodeInterrupted {
		report.Steps = append(report.Steps, DemoStep{Name: "interrupt_commit", Detail: "unexpected error shape"})
		return report, fmt.Errorf("demo interruption: expected %s, got %v", CodeInterrupted, err)
	}
	report.Recovery.InterruptedCode = stale.Code
	report.Steps = append(report.Steps, DemoStep{Name: "interrupt_commit", Error: stale})
	status, err := engine.Status(ctx)
	if err != nil {
		return report, err
	}
	report.Recovery.VersionAfterInterrupt = status.CurrentVersion
	engine.beforeCommit = nil

	retry, err := engine.ApplyPatch(ctx, initResult.Version, PolicyPatch{MergeThreshold: &threshold})
	if err != nil {
		return report, fmt.Errorf("demo retry after interruption: %w", err)
	}
	report.Recovery.RetriedVersion = retry.Version
	report.Steps = append(report.Steps, DemoStep{
		Name:             "retry_after_interruption",
		Version:          retry.Version,
		InvalidatedStage: retry.InvalidatedStage,
		ReusedSlotCount:  retry.ReusedSlotCount,
		SlotCount:        retry.SlotCount,
		Detail:           fmt.Sprintf("invalidations=%d evictions=%d", len(retry.Invalidations), len(retry.Evictions)),
	})

	minPrefix := 12
	minPatch, err := engine.ApplyPatch(ctx, retry.Version, PolicyPatch{MinPrefixLen: &minPrefix})
	if err != nil {
		return report, fmt.Errorf("demo min-prefix patch: %w", err)
	}
	report.Steps = append(report.Steps, DemoStep{
		Name:             "incremental_min_prefix",
		Version:          minPatch.Version,
		InvalidatedStage: minPatch.InvalidatedStage,
		ReusedSlotCount:  minPatch.ReusedSlotCount,
		Detail:           fmt.Sprintf("field=%s invalidations=%d", firstInvalidationField(minPatch.Invalidations), len(minPatch.Invalidations)),
	})

	maxPrefixes := 3
	maxPatch, err := engine.ApplyPatch(ctx, minPatch.Version, PolicyPatch{MaxPrefixesPerRequest: &maxPrefixes})
	if err != nil {
		return report, fmt.Errorf("demo max-prefix patch: %w", err)
	}
	report.Steps = append(report.Steps, DemoStep{
		Name:             "incremental_max_prefixes",
		Version:          maxPatch.Version,
		InvalidatedStage: maxPatch.InvalidatedStage,
		ReusedSlotCount:  maxPatch.ReusedSlotCount,
		Detail:           fmt.Sprintf("evictions=%d", len(maxPatch.Evictions)),
	})

	granularity := 6
	granularityPatch, err := engine.ApplyPatch(ctx, maxPatch.Version, PolicyPatch{Granularity: &granularity})
	if err != nil {
		return report, fmt.Errorf("demo granularity patch: %w", err)
	}
	report.Steps = append(report.Steps, DemoStep{
		Name:             "incremental_granularity",
		Version:          granularityPatch.Version,
		InvalidatedStage: granularityPatch.InvalidatedStage,
		ReusedSlotCount:  granularityPatch.ReusedSlotCount,
		Detail:           fmt.Sprintf("invalidations=%d", len(granularityPatch.Invalidations)),
	})

	status, err = engine.Status(ctx)
	if err != nil {
		return report, err
	}
	replacementPolicy := status.Policy
	replacementPolicy.Version = status.CurrentVersion + 1
	replacementPolicy.MergeThreshold = 0.78
	replacement, err := engine.SubmitPolicy(ctx, PolicySubmission{
		Policy:     replacementPolicy,
		Mode:       "auto",
		BaseDigest: "intentionally-not-the-current-digest",
	})
	if err != nil {
		return report, fmt.Errorf("demo replacement: %w", err)
	}
	report.Steps = append(report.Steps, DemoStep{
		Name:             "whole_replacement_detected",
		Version:          replacement.Version,
		InvalidatedStage: replacement.InvalidatedStage,
		ReusedSlotCount:  replacement.ReusedSlotCount,
		Detail:           fmt.Sprintf("kind=%s invalidations=%d", replacement.Kind, len(replacement.Invalidations)),
	})
	if replacement.Kind != "replacement" || replacement.ReusedSlotCount != 0 || len(replacement.Invalidations) == 0 {
		return report, fmt.Errorf("demo replacement was not classified correctly: %+v", replacement)
	}

	rollback := status.Policy
	rollback.Version = status.CurrentVersion
	_, err = engine.SubmitPolicy(ctx, PolicySubmission{Policy: rollback, Mode: "replace"})
	if !errors.As(err, &stale) || stale.Code != CodeStaleVersion {
		return report, fmt.Errorf("demo rollback: expected stale, got %v", err)
	}
	report.Steps = append(report.Steps, DemoStep{Name: "rollback_rejected", Error: stale})

	httpServer := NewHTTPServer(engine)
	listener, err := httpServer.Listen("127.0.0.1:0")
	if err != nil {
		return report, err
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- httpServer.Serve(listener) }()
	client := &http.Client{Timeout: 5 * time.Second}
	health, err := httpJSON(ctx, client, http.MethodGet, "http://"+listener.Addr().String()+"/v1/healthz", nil)
	if err != nil {
		return report, fmt.Errorf("demo HTTP health: %w", err)
	}
	report.Recovery.HTTPHealth = health
	shutdown, err := httpJSON(ctx, client, http.MethodPost, "http://"+listener.Addr().String()+"/v1/shutdown", []byte(`{}`))
	if err != nil {
		return report, fmt.Errorf("demo HTTP shutdown: %w", err)
	}
	report.Recovery.HTTPShutdown = shutdown
	select {
	case err := <-serveDone:
		if err != nil {
			return report, fmt.Errorf("demo HTTP serve: %w", err)
		}
	case <-time.After(5 * time.Second):
		return report, errors.New("demo HTTP server did not stop after shutdown request")
	}
	report.Steps = append(report.Steps, DemoStep{Name: "http_graceful_shutdown", Detail: "healthz=ok shutdown=stopping"})

	finalStatus, err := engine.Status(ctx)
	if err != nil {
		return report, err
	}
	report.FinalVersion = finalStatus.CurrentVersion
	report.FinalFingerprint = finalStatus.VersionFingerprint
	if err := engine.Close(); err != nil {
		return report, err
	}
	closed = true

	reopened, err := Open(ctx, dbPath)
	if err != nil {
		return report, fmt.Errorf("demo reopen: %w", err)
	}
	defer reopened.Close()
	reopenedStatus, err := reopened.Status(ctx)
	if err != nil {
		return report, fmt.Errorf("demo reopened status: %w", err)
	}
	report.Recovery.ReopenVersion = reopenedStatus.CurrentVersion
	report.Recovery.FingerprintStable = reopenedStatus.VersionFingerprint == report.FinalFingerprint && reopenedStatus.CurrentVersion == report.FinalVersion
	report.Steps = append(report.Steps, DemoStep{
		Name:      "reopen_after_clean_close",
		Version:   reopenedStatus.CurrentVersion,
		Detail:    fmt.Sprintf("fingerprint_stable=%t allocation_sequence=%d", report.Recovery.FingerprintStable, reopenedStatus.AllocationSequence),
		SlotCount: reopenedStatus.SlotOccupancy,
	})
	return report, nil
}

func hitStep(name string, hit HitResult) DemoStep {
	return DemoStep{
		Name:     name,
		Version:  hit.Version,
		Decision: hit.Decision,
		Evidence: hit.Evidence,
	}
}

func firstInvalidationField(invalidations []Invalidation) string {
	if len(invalidations) == 0 {
		return ""
	}
	return invalidations[0].Field
}

func httpJSON(ctx context.Context, client *http.Client, method, endpoint string, body []byte) (string, error) {
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return "", err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("HTTP %d: %s", response.StatusCode, string(payload))
	}
	var decoded struct {
		Status string `json:"status"`
		OK     bool   `json:"ok"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return "", err
	}
	if decoded.Status != "" {
		return decoded.Status, nil
	}
	if decoded.OK {
		return "ok", nil
	}
	return string(payload), nil
}

package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"sync"

	"localinfer/workbench/internal/domain"
	"localinfer/workbench/internal/scheduler"
	"localinfer/workbench/internal/store"
)

type Service struct {
	store          *store.Store
	planner        *scheduler.Planner
	modelsMu       sync.RWMutex
	catalog        *domain.ModelCatalog
	allowExitFault bool
}

func New(st *store.Store, catalog *domain.ModelCatalog, allowExitFault bool) *Service {
	return &Service{store: st, planner: scheduler.NewPlanner(), catalog: catalog, allowExitFault: allowExitFault}
}

func (s *Service) Store() *store.Store { return s.store }

func (s *Service) Catalog() *domain.ModelCatalog {
	s.modelsMu.RLock()
	defer s.modelsMu.RUnlock()
	return s.catalog
}

func (s *Service) InstallCatalog(data []byte) (*domain.ModelCatalog, bool, error) {
	catalog, err := domain.ParseModelCatalog(data)
	if err != nil {
		return nil, false, err
	}
	current := s.Catalog()
	if current != nil && current.Hash == catalog.Hash {
		return current, false, nil
	}
	if err := s.store.InstallCatalog(catalog); err != nil {
		return nil, false, err
	}
	s.modelsMu.Lock()
	s.catalog = catalog
	s.modelsMu.Unlock()
	return catalog, true, nil
}

func (s *Service) SubmitTrace(data []byte) (*domain.Trace, bool, error) {
	trace, err := domain.ParseTrace(data, s.Catalog())
	if err != nil {
		return nil, false, err
	}
	created, err := s.store.SaveTrace(trace)
	if err != nil {
		return nil, false, err
	}
	return trace, created, nil
}

func (s *Service) Organize(traceID, version string, opts domain.OrganizationOptions) (*domain.RunResult, error) {
	catalog := s.Catalog()
	trace, err := s.store.GetTrace(traceID, version)
	if err != nil {
		return nil, err
	}
	if err := trace.ValidateAgainst(catalog); err != nil {
		return nil, err
	}
	if reflect.DeepEqual(opts, domain.OrganizationOptions{}) {
		opts = domain.DefaultOrganizationOptions()
	}
	// Zeros are meaningful for tolerance fields and latency_decimals. Only
	// omitted mode/late policy are filled here; HTTP applies defaults explicitly.
	if opts.Mode == "" {
		opts.Mode = domain.ModeFull
	}
	if opts.LatePolicy == "" {
		opts.LatePolicy = domain.LatePolicyReject
	}
	normalized, err := domain.NormalizeOptions(opts)
	if err != nil {
		return nil, err
	}
	opts = normalized
	if err := s.validateInterruptFaults(opts); err != nil {
		return nil, err
	}
	optionsHash, baseRunID := runIdentity(trace, catalog, opts)
	runID, existing, err := s.selectRunAttempt(baseRunID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return s.resultOrBusinessError(existing)
	}

	scheduleOpts := opts
	execution := opts.Faults
	scheduleOpts.Faults.InterruptAfterBatches = nil
	scheduleOpts.Faults.InterruptAfterQuant = false
	plan, err := s.planner.BuildPlan(runID, trace, catalog, scheduleOpts)
	if err != nil {
		return nil, err
	}
	if execution.InterruptAfterBatches != nil && *execution.InterruptAfterBatches > len(plan.Batches) {
		return nil, domain.NewBizError(domain.CodeBadRequest, "interrupt_after_batches exceeds planned batch count", map[string]any{
			"interrupt_after_batches": *execution.InterruptAfterBatches, "planned_batches": len(plan.Batches),
		})
	}
	if err := s.store.CreateRun(runID, optionsHash, trace, catalog.Hash, plan); err != nil {
		return nil, err
	}

	for i := range plan.Batches {
		batch := &plan.Batches[i]
		if execution.InterruptAfterBatches != nil && i == *execution.InterruptAfterBatches {
			if _, err := s.store.StartPendingIntent("COMMIT_BATCH_BASE", trace.TraceID, runID, batch.BatchID, map[string]any{"interrupt": "before_batch_base", "batch_index": i}); err != nil {
				return nil, err
			}
			hardExit(87)
		}
		if err := s.store.InsertBatchBase(runID, batch); err != nil {
			return nil, err
		}
		if execution.InterruptAfterQuant && i == 0 {
			if _, err := s.store.StartPendingIntent("COMMIT_BATCH_RESULTS", trace.TraceID, runID, batch.BatchID, map[string]any{"interrupt": "after_batch_base_before_quantiles"}); err != nil {
				return nil, err
			}
			hardExit(88)
		}
		if err := s.store.InsertBatchResults(runID, batch); err != nil {
			return nil, err
		}
	}
	if execution.InterruptAfterBatches != nil && *execution.InterruptAfterBatches == len(plan.Batches) {
		if _, err := s.store.StartPendingIntent("FINALIZE_RUN", trace.TraceID, runID, "", map[string]any{"interrupt": "after_all_batches_before_finalize"}); err != nil {
			return nil, err
		}
		hardExit(89)
	}

	status := "complete"
	failureCode := ""
	if !plan.PrecisionOK {
		status = "precision_failed"
		failureCode = domain.CodePrecisionBreach
	} else if !plan.LatencyOK {
		status = "latency_failed"
		failureCode = domain.CodeLatencyLimit
	}
	if err := s.store.CompleteRun(runID, status, failureCode, plan); err != nil {
		return nil, err
	}
	result, err := s.store.GetRun(runID)
	if err != nil {
		return nil, err
	}
	return s.resultOrBusinessError(result)
}

func (s *Service) selectRunAttempt(baseRunID string) (string, *domain.RunResult, error) {
	for attempt := 1; attempt <= 1000; attempt++ {
		candidate := baseRunID
		if attempt > 1 {
			candidate = fmt.Sprintf("%s-a%d", baseRunID, attempt)
		}
		existing, err := s.store.GetRun(candidate)
		if err == nil {
			switch existing.Status {
			case "complete", "precision_failed", "latency_failed":
				return candidate, existing, nil
			case "aborted":
				// Preserve the interrupted facts and try a new deterministic
				// attempt suffix on the next loop iteration.
				continue
			default:
				return "", nil, domain.NewBizError(domain.CodeConflicting, "run is still organizing or requires recovery", map[string]any{"run_id": candidate, "status": existing.Status})
			}
		}
		biz := domain.AsBizError(err)
		if biz.Code == domain.CodeNotFound {
			return candidate, nil, nil
		}
		return "", nil, err
	}
	return "", nil, domain.NewBizError(domain.CodeConflicting, "too many aborted attempts for this deterministic run identity", map[string]any{"base_run_id": baseRunID})
}

func (s *Service) resultOrBusinessError(result *domain.RunResult) (*domain.RunResult, error) {
	switch result.Status {
	case "precision_failed":
		failed := make([]string, 0)
		for _, b := range result.Batches {
			if !b.Precision.PrecisionOK {
				failed = append(failed, b.BatchID)
			}
		}
		return nil, domain.NewBizError(domain.CodePrecisionBreach, "at least one batch exceeded the declared precision bounds; facts were persisted with validation_failed status", map[string]any{
			"run_id": result.RunID, "failed_batches": failed, "run": result,
		})
	case "latency_failed":
		failed := make([]string, 0)
		for _, b := range result.Batches {
			if !b.LatencyOK {
				failed = append(failed, b.BatchID)
			}
		}
		return nil, domain.NewBizError(domain.CodeLatencyLimit, "at least one batch exceeds its declared interaction latency limit and is not marked passed", map[string]any{
			"run_id": result.RunID, "failed_batches": failed, "run": result,
		})
	default:
		return result, nil
	}
}

func (s *Service) validateInterruptFaults(opts domain.OrganizationOptions) error {
	hasInterrupt := opts.Faults.InterruptAfterBatches != nil || opts.Faults.InterruptAfterQuant
	if !hasInterrupt {
		return nil
	}
	if !s.allowExitFault {
		return domain.NewBizError(domain.CodeBadRequest, "hard-exit fault injection is disabled on this server", map[string]any{
			"hint": "start with -allow-exit-fault",
		})
	}
	if opts.Faults.InterruptAfterBatches != nil && *opts.Faults.InterruptAfterBatches < 0 {
		return domain.NewBizError(domain.CodeBadRequest, "interrupt_after_batches must not be negative", nil)
	}
	return nil
}

type runKey struct {
	TraceContentHash        string   `json:"trace_content_hash"`
	CatalogHash             string   `json:"catalog_hash"`
	Mode                    string   `json:"mode"`
	OrganizationOrder       string   `json:"organization_order"`
	NodeVRAMBytes           int64    `json:"node_vram_bytes"`
	OrganizationBudgetMS    int64    `json:"organization_budget_ms"`
	AllowMixedModels        bool     `json:"allow_mixed_models"`
	LatencyDecimals         int      `json:"latency_decimals"`
	MaxAbsDeviationUS       int64    `json:"max_abs_deviation_us"`
	MaxRelativeDeviationPPM int64    `json:"max_relative_deviation_ppm"`
	LatePolicy              string   `json:"late_policy"`
	LateRequestIDs          []string `json:"late_request_ids,omitempty"`
	DuplicateRequestID      string   `json:"duplicate_request_id,omitempty"`
	VramShortfallBytes      int64    `json:"vram_shortfall_bytes,omitempty"`
	PrecisionOffsetUS       int64    `json:"precision_offset_us,omitempty"`
}

func runIdentity(trace *domain.Trace, catalog *domain.ModelCatalog, opts domain.OrganizationOptions) (optionsHash, runID string) {
	key := runKey{
		TraceContentHash: trace.ContentHash, CatalogHash: catalog.Hash, Mode: opts.Mode,
		OrganizationOrder: opts.OrganizationOrder,
		NodeVRAMBytes:     opts.NodeVRAMBytes, OrganizationBudgetMS: opts.OrganizationBudgetMS,
		AllowMixedModels: opts.AllowMixedModels, LatencyDecimals: opts.LatencyDecimals,
		MaxAbsDeviationUS: opts.MaxAbsDeviationUS, MaxRelativeDeviationPPM: opts.MaxRelativeDeviationPPM,
		LatePolicy: opts.LatePolicy, LateRequestIDs: append([]string(nil), opts.Faults.LateRequestIDs...),
		DuplicateRequestID: opts.Faults.DuplicateRequestID, VramShortfallBytes: opts.Faults.VramShortfallBytes,
		PrecisionOffsetUS: opts.Faults.PrecisionOffsetUS,
	}
	sort.Strings(key.LateRequestIDs)
	b, _ := json.Marshal(key)
	sum := sha256.Sum256(b)
	optionsHash = hex.EncodeToString(sum[:])
	return optionsHash, "run-" + optionsHash[:24]
}

func hardExit(code int) {
	_, _ = fmt.Fprintf(os.Stderr, "workbench: deliberate hard exit code %d after durable intent write\n", code)
	os.Exit(code)
}

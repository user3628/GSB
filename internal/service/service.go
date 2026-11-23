package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"example.com/joinshuffle/internal/engine"
	"example.com/joinshuffle/internal/model"
	"example.com/joinshuffle/internal/snapshot"
	"example.com/joinshuffle/internal/store"
	"example.com/joinshuffle/internal/sum"
)

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

type SnapshotPair struct {
	Left  *snapshot.Snapshot
	Right *snapshot.Snapshot
	Meta  model.SnapshotPairMeta
}

type SnapshotStatus struct {
	Loaded       bool                   `json:"loaded"`
	Snapshots    model.SnapshotPairMeta `json:"snapshots"`
	RestoreError string                 `json:"restore_error,omitempty"`
}

type inflight struct {
	done chan struct{}
}

type Service struct {
	store *store.Store

	loadMu     sync.Mutex
	snapMu     sync.RWMutex
	pair       *SnapshotPair
	lastMeta   *model.SnapshotPairMeta
	restoreErr string

	callsMu sync.Mutex
	calls   map[string]*inflight

	crashEnabled   bool
	crashRequestID string
	crashMarker    string
}

func New(st *store.Store) *Service {
	return &Service{
		store:          st,
		calls:          make(map[string]*inflight),
		crashEnabled:   os.Getenv("SHUFFLE_ENABLE_CRASH_INJECTION") == "1",
		crashRequestID: os.Getenv("SHUFFLE_CRASH_REQUEST_ID"),
		crashMarker:    os.Getenv("SHUFFLE_CRASH_MARKER"),
	}
}

func (s *Service) Recover(ctx context.Context) (model.RecoveryReport, error) {
	return s.store.RecoverRunning(ctx)
}

func (s *Service) RecoveryReport(ctx context.Context) (model.RecoveryReport, error) {
	return s.store.RecoveryReport(ctx)
}

func (s *Service) RestoreSnapshots(ctx context.Context) error {
	s.loadMu.Lock()
	defer s.loadMu.Unlock()
	meta, ok, err := s.store.LoadSnapshotPair(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	metaCopy := meta
	s.snapMu.Lock()
	s.lastMeta = &metaCopy
	s.snapMu.Unlock()
	left, err := snapshot.ReadFile(meta.Left.Path, meta.Left.ChecksumPath, meta.Left.SHA256)
	if err != nil {
		s.setRestoreError(err)
		return fmt.Errorf("restore left snapshot: %w", err)
	}
	right, err := snapshot.ReadFile(meta.Right.Path, meta.Right.ChecksumPath, meta.Right.SHA256)
	if err != nil {
		s.setRestoreError(err)
		return fmt.Errorf("restore right snapshot: %w", err)
	}
	if left.Meta.Side != "left" || right.Meta.Side != "right" {
		err := fmt.Errorf("stored snapshot side metadata mismatch")
		s.setRestoreError(err)
		return err
	}
	pair := &SnapshotPair{
		Left:  left,
		Right: right,
		Meta: model.SnapshotPairMeta{
			Left:  left.Meta,
			Right: right.Meta,
		},
	}
	pair.Meta.Left.LoadedAt = meta.Left.LoadedAt
	pair.Meta.Right.LoadedAt = meta.Right.LoadedAt
	s.snapMu.Lock()
	s.pair = pair
	s.lastMeta = &metaCopy
	s.restoreErr = ""
	s.snapMu.Unlock()
	return nil
}

func (s *Service) setRestoreError(err error) {
	s.snapMu.Lock()
	s.restoreErr = err.Error()
	s.snapMu.Unlock()
}

func (s *Service) SnapshotsStatus() SnapshotStatus {
	s.snapMu.RLock()
	defer s.snapMu.RUnlock()
	status := SnapshotStatus{Loaded: s.pair != nil, RestoreError: s.restoreErr}
	if s.pair != nil {
		status.Snapshots = s.pair.Meta
	}
	return status
}

func (s *Service) currentPair() *SnapshotPair {
	s.snapMu.RLock()
	defer s.snapMu.RUnlock()
	return s.pair
}

func (s *Service) LoadSnapshots(ctx context.Context, req model.LoadSnapshotsRequest) (model.SnapshotPairMeta, error) {
	s.loadMu.Lock()
	defer s.loadMu.Unlock()
	left, err := snapshot.ReadFile(req.Left.Path, req.Left.ChecksumPath, req.Left.SHA256)
	if err != nil {
		return model.SnapshotPairMeta{}, snapshotInvalid("left", req.Left.Path, err)
	}
	right, err := snapshot.ReadFile(req.Right.Path, req.Right.ChecksumPath, req.Right.SHA256)
	if err != nil {
		return model.SnapshotPairMeta{}, snapshotInvalid("right", req.Right.Path, err)
	}
	if left.Meta.Side != "left" {
		return model.SnapshotPairMeta{}, snapshotInvalid("left", req.Left.Path, fmt.Errorf("file declares side %q", left.Meta.Side))
	}
	if right.Meta.Side != "right" {
		return model.SnapshotPairMeta{}, snapshotInvalid("right", req.Right.Path, fmt.Errorf("file declares side %q", right.Meta.Side))
	}
	if left.Meta.Version == 0 || right.Meta.Version == 0 {
		return model.SnapshotPairMeta{}, snapshotInvalid("pair", "", fmt.Errorf("snapshot versions must be positive"))
	}
	if left.Meta.Version > math.MaxInt64 || right.Meta.Version > math.MaxInt64 {
		return model.SnapshotPairMeta{}, snapshotInvalid("pair", "", fmt.Errorf("snapshot version exceeds SQLite signed integer range"))
	}
	s.snapMu.RLock()
	current := s.pair
	lastMeta := s.lastMeta
	s.snapMu.RUnlock()
	floor := lastMeta
	if current != nil {
		floor = &current.Meta
	}
	if floor == nil {
		persisted, found, err := s.store.LoadSnapshotPair(ctx)
		if err != nil {
			return model.SnapshotPairMeta{}, model.NewError(model.CodeInternal, "read persisted snapshot metadata failed", "error", err.Error())
		}
		if found {
			floor = &persisted
		}
	}
	if floor != nil {
		if left.Meta.Version <= floor.Left.Version {
			return model.SnapshotPairMeta{}, snapshotInvalid("left", req.Left.Path,
				fmt.Errorf("version %d is not greater than current version %d", left.Meta.Version, floor.Left.Version))
		}
		if right.Meta.Version <= floor.Right.Version {
			return model.SnapshotPairMeta{}, snapshotInvalid("right", req.Right.Path,
				fmt.Errorf("version %d is not greater than current version %d", right.Meta.Version, floor.Right.Version))
		}
	}
	now := time.Now().UTC().UnixNano()
	left.Meta.LoadedAt = now
	right.Meta.LoadedAt = now
	pairMeta := model.SnapshotPairMeta{Left: left.Meta, Right: right.Meta}
	if err := s.store.SaveSnapshotPair(ctx, pairMeta); err != nil {
		return model.SnapshotPairMeta{}, model.NewError(model.CodeInternal, "persist snapshot metadata failed", "error", err.Error())
	}
	s.snapMu.Lock()
	s.pair = &SnapshotPair{Left: left, Right: right, Meta: pairMeta}
	s.lastMeta = &pairMeta
	s.restoreErr = ""
	s.snapMu.Unlock()
	return pairMeta, nil
}

func snapshotInvalid(side, path string, err error) *model.BusinessError {
	return model.NewError(model.CodeSnapshotInvalid,
		"snapshot validation failed; the previously loaded snapshot remains active",
		"side", side,
		"path", path,
		"cause", err.Error())
}

func (s *Service) Submit(ctx context.Context, req model.AggregateRequest) (*model.AggregateResponse, error) {
	normalized, businessErr := normalize(req)
	if businessErr != nil {
		return nil, businessErr
	}
	fingerprint, err := requestFingerprint(normalized)
	if err != nil {
		return nil, model.NewError(model.CodeInvalidRequest, "cannot fingerprint request", "cause", err.Error())
	}
	for {
		rec, err := s.store.GetRequest(ctx, normalized.RequestID)
		if err != nil {
			return nil, err
		}
		if rec != nil {
			if rec.Fingerprint != fingerprint {
				return nil, conflictError(normalized.RequestID, rec.Fingerprint, fingerprint)
			}
			switch rec.Status {
			case "completed":
				return decodeStoredResponse(rec)
			case "failed":
				return nil, decodeStoredBusinessError(rec)
			case "running":
				if err := s.waitForRunning(ctx, normalized.RequestID); err != nil {
					return nil, err
				}
				continue
			case "aborted":
				// A crash-recovered attempt may be retried with identical content.
			default:
				return nil, model.NewError(model.CodeInternal, "unknown request status", "status", rec.Status)
			}
		}
		call, owner := s.acquireCall(normalized.RequestID)
		if !owner {
			select {
			case <-call.done:
				continue
			case <-ctx.Done():
				return nil, model.NewError(model.CodeExecutionAborted, "waiting for duplicate request was cancelled", "request_id", normalized.RequestID, "cause", ctx.Err().Error())
			}
		}
		response, submitErr := s.executeLocked(ctx, normalized, fingerprint)
		s.releaseCall(normalized.RequestID, call)
		return response, submitErr
	}
}

func (s *Service) waitForRunning(ctx context.Context, requestID string) error {
	s.callsMu.Lock()
	call := s.calls[requestID]
	s.callsMu.Unlock()
	if call != nil {
		select {
		case <-call.done:
			return nil
		case <-ctx.Done():
			return model.NewError(model.CodeExecutionAborted, "waiting for duplicate request was cancelled", "request_id", requestID, "cause", ctx.Err().Error())
		}
	}
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return model.NewError(model.CodeExecutionAborted, "waiting for persisted request was cancelled", "request_id", requestID, "cause", ctx.Err().Error())
		case <-timer.C:
			return model.NewError(model.CodeExecutionInProgress, "request remains running without an in-process owner", "request_id", requestID)
		case <-ticker.C:
			rec, err := s.store.GetRequest(ctx, requestID)
			if err != nil {
				return err
			}
			if rec == nil || rec.Status != "running" {
				return nil
			}
		}
	}
}

func (s *Service) acquireCall(requestID string) (*inflight, bool) {
	s.callsMu.Lock()
	defer s.callsMu.Unlock()
	if existing := s.calls[requestID]; existing != nil {
		return existing, false
	}
	call := &inflight{done: make(chan struct{})}
	s.calls[requestID] = call
	return call, true
}

func (s *Service) releaseCall(requestID string, call *inflight) {
	s.callsMu.Lock()
	if s.calls[requestID] == call {
		delete(s.calls, requestID)
	}
	close(call.done)
	s.callsMu.Unlock()
}

func (s *Service) executeLocked(ctx context.Context, req model.AggregateRequest, fingerprint string) (*model.AggregateResponse, error) {
	rec, err := s.store.GetRequest(ctx, req.RequestID)
	if err != nil {
		return nil, err
	}
	if rec != nil {
		if rec.Fingerprint != fingerprint {
			return nil, conflictError(req.RequestID, rec.Fingerprint, fingerprint)
		}
		switch rec.Status {
		case "completed":
			return decodeStoredResponse(rec)
		case "failed":
			return nil, decodeStoredBusinessError(rec)
		case "running":
			return nil, model.NewError(model.CodeExecutionInProgress, "request is already running", "request_id", req.RequestID)
		}
	}
	pair := s.currentPair()
	if pair == nil {
		return nil, model.NewError(model.CodeNotReady, "snapshots are not loaded", "required_action", "POST /v1/snapshots/load")
	}
	dist := engine.Analyze(pair.Left, pair.Right, req.PartitionCount, req.SkewThreshold, model.JoinType(req.JoinType))
	route, fixedScaleBits := selectRoute(req, dist)
	mergeRule := req.MergeRule
	if mergeRule == "" {
		if route.Path == string(model.PathHigh) {
			mergeRule = engine.MergeMagnitudeBucket
		} else {
			mergeRule = engine.MergePartitionAscending
		}
	}
	if mergeRule != engine.MergePartitionAscending && mergeRule != engine.MergeMagnitudeBucket {
		return nil, model.NewError(model.CodeInvalidRequest, "unsupported merge_rule", "merge_rule", mergeRule)
	}
	requestJSON, _ := json.Marshal(req)
	attempt, err := s.store.BeginAttempt(ctx, req.RequestID, fingerprint, string(requestJSON))
	if err != nil {
		return nil, err
	}
	s.maybeCrashAfterIntent(req.RequestID)

	variantCounts := make([]int, 0, req.PartitionCount)
	for count := 1; count <= req.PartitionCount; count++ {
		variantCounts = append(variantCounts, count)
	}
	variants := make(map[int]*engine.Result, len(variantCounts))
	for _, count := range variantCounts {
		result, runErr := engine.Run(pair.Left, pair.Right, engine.Options{
			JoinType:       model.JoinType(req.JoinType),
			PartitionCount: count,
			SkewThreshold:  req.SkewThreshold,
			Algorithm:      model.SumAlgorithm(route.ResolvedAlgorithm),
			FixedScaleBits: fixedScaleBits,
			MergeRule:      mergeRule,
		})
		if runErr != nil {
			businessErr := asBusinessError(runErr)
			_ = s.store.FailAttempt(context.Background(), req.RequestID, businessErr)
			return nil, businessErr
		}
		variants[count] = result
	}
	mainResult := variants[req.PartitionCount]
	invariance := model.PartitionInvariance{
		Tolerance:          req.RelativeTolerance,
		Checked:            true,
		CheckedPartitions:  append([]int(nil), variantCounts...),
		ReferencePartition: 1,
		Passed:             true,
		MaxDeviationByKey:  make(map[int64]float64, len(mainResult.Groups)),
		ReferenceValues:    make(map[int64]*float64, len(mainResult.Groups)),
	}
	deviationByKey, referenceValues, precisionErr := engine.ValidatePartitionInvariance(variants, req.RelativeTolerance)
	if precisionErr != nil {
		_ = s.store.FailAttempt(context.Background(), req.RequestID, precisionErr)
		return nil, precisionErr
	}
	invariance.MaxDeviationByKey = deviationByKey
	invariance.ReferenceValues = referenceValues
	for i := range mainResult.Groups {
		mainResult.Groups[i].MaxRelativeDeviation = invariance.MaxDeviationByKey[mainResult.Groups[i].Key]
	}
	errorDeclaration := buildErrorDeclaration(route, fixedScaleBits, mainResult)
	response := &model.AggregateResponse{
		RequestID:           req.RequestID,
		Status:              "running",
		Attempt:             attempt,
		Fingerprint:         fingerprint,
		Snapshots:           pair.Meta,
		JoinType:            req.JoinType,
		PartitionCount:      req.PartitionCount,
		MergeRule:           mergeRule,
		MergeOrderBasis:     mainResult.MergeOrderBasis,
		Route:               route,
		Skew:                mainResult.Skew,
		Partitions:          mainResult.Partitions,
		Groups:              mainResult.Groups,
		ErrorDeclaration:    errorDeclaration,
		PartitionInvariance: invariance,
		ComputedAt:          time.Now().UTC().UnixNano(),
	}
	commitCtx, cancelCommit := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelCommit()
	if err := s.store.CommitAttempt(commitCtx, req.RequestID, response, mainResult.Partials, fixedScaleBits); err != nil {
		if business, ok := err.(*model.BusinessError); ok {
			return nil, business
		}
		return nil, model.NewError(model.CodeInternal, "atomic result commit failed; request remains recoverable", "request_id", req.RequestID, "cause", err.Error())
	}
	return response, nil
}

func (s *Service) GetJob(ctx context.Context, requestID string) (*model.AggregateResponse, error) {
	rec, err := s.store.GetRequest(ctx, requestID)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, model.NewError(model.CodeNotFound, "request not found", "request_id", requestID)
	}
	switch rec.Status {
	case "completed":
		return decodeStoredResponse(rec)
	case "failed", "aborted":
		return nil, decodeStoredBusinessError(rec)
	case "running":
		return nil, model.NewError(model.CodeExecutionInProgress, "request has not reached an atomic commit", "request_id", requestID)
	default:
		return nil, model.NewError(model.CodeInternal, "unknown request status", "status", rec.Status)
	}
}

func (s *Service) GetPartitions(ctx context.Context, requestID string) ([]model.PartitionStat, error) {
	if _, err := s.GetJob(ctx, requestID); err != nil {
		return nil, err
	}
	return s.store.Partitions(ctx, requestID)
}

func (s *Service) GetResults(ctx context.Context, requestID string) (map[string]any, error) {
	job, err := s.GetJob(ctx, requestID)
	if err != nil {
		return nil, err
	}
	groups, err := s.store.Groups(ctx, requestID)
	if err != nil {
		return nil, err
	}
	partials, err := s.store.PartialSums(ctx, requestID)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"request_id":             job.RequestID,
		"commit_version":         job.CommitVersion,
		"join_type":              job.JoinType,
		"partition_count":        job.PartitionCount,
		"merge_rule":             job.MergeRule,
		"merge_order_basis":      job.MergeOrderBasis,
		"groups":                 groups,
		"partial_sums":           partials,
		"partial_sums_persisted": len(partials),
	}, nil
}

func (s *Service) GetPartialSums(ctx context.Context, requestID string) ([]sum.Partial, error) {
	if _, err := s.GetJob(ctx, requestID); err != nil {
		return nil, err
	}
	return s.store.PartialSums(ctx, requestID)
}

func (s *Service) GetErrorDeclaration(ctx context.Context, requestID string) (*model.ErrorDeclaration, error) {
	job, err := s.GetJob(ctx, requestID)
	if err != nil {
		return nil, err
	}
	return &job.ErrorDeclaration, nil
}

func (s *Service) GetRoute(ctx context.Context, requestID string) (*model.RouteDecision, error) {
	job, err := s.GetJob(ctx, requestID)
	if err != nil {
		return nil, err
	}
	return &job.Route, nil
}

func normalize(req model.AggregateRequest) (model.AggregateRequest, *model.BusinessError) {
	req.RequestID = strings.TrimSpace(req.RequestID)
	if !requestIDPattern.MatchString(req.RequestID) {
		return req, model.NewError(model.CodeInvalidRequest, "request_id must match [A-Za-z0-9._:-]{1,128}", "request_id", req.RequestID)
	}
	req.JoinType = strings.ToLower(strings.TrimSpace(req.JoinType))
	if req.JoinType == "" {
		req.JoinType = string(model.JoinInner)
	}
	if req.JoinType != string(model.JoinInner) && req.JoinType != string(model.JoinLeft) {
		return req, model.NewError(model.CodeInvalidRequest, "join_type must be inner or left", "join_type", req.JoinType)
	}
	if req.PartitionCount < 1 || req.PartitionCount > 256 {
		return req, model.NewError(model.CodeInvalidRequest, "partition_count must be between 1 and 256", "partition_count", req.PartitionCount)
	}
	req.SkewThreshold = defaultFloat(req.SkewThreshold, 0.35)
	if req.SkewThreshold <= 0 || req.SkewThreshold > 1 || math.IsNaN(req.SkewThreshold) {
		return req, model.NewError(model.CodeInvalidRequest, "skew_threshold must be in (0,1]", "skew_threshold", req.SkewThreshold)
	}
	req.Algorithm = strings.ToLower(strings.TrimSpace(req.Algorithm))
	switch model.SumAlgorithm(req.Algorithm) {
	case model.SumNaive, model.SumKahan, model.SumDoubleDoubleFix, model.SumAuto:
	default:
		return req, model.NewError(model.CodeInvalidRequest, "algorithm must be naive, kahan, double_double_fixed, or auto", "algorithm", req.Algorithm)
	}
	req.RelativeTolerance = defaultFloat(req.RelativeTolerance, 1e-12)
	if req.RelativeTolerance <= 0 || math.IsNaN(req.RelativeTolerance) || math.IsInf(req.RelativeTolerance, 0) {
		return req, model.NewError(model.CodeInvalidRequest, "relative_tolerance must be finite and positive", "relative_tolerance", req.RelativeTolerance)
	}
	if req.ComputeBudget < 0 {
		return req, model.NewError(model.CodeInvalidRequest, "compute_budget must be non-negative", "compute_budget", req.ComputeBudget)
	}
	if req.FixedScaleBits != 0 && (req.FixedScaleBits < sum.MinFixedScaleBits || req.FixedScaleBits > sum.MaxFixedScaleBits) {
		return req, model.NewError(model.CodeInvalidRequest, "fixed_scale_bits is outside supported range", "fixed_scale_bits", req.FixedScaleBits, "min", sum.MinFixedScaleBits, "max", sum.MaxFixedScaleBits)
	}
	if req.MergeRule != "" && req.MergeRule != engine.MergePartitionAscending && req.MergeRule != engine.MergeMagnitudeBucket {
		return req, model.NewError(model.CodeInvalidRequest, "unsupported merge_rule", "merge_rule", req.MergeRule)
	}
	return req, nil
}

func defaultFloat(value, fallback float64) float64 {
	if value == 0 {
		return fallback
	}
	return value
}

func requestFingerprint(req model.AggregateRequest) (string, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(b)
	return hex.EncodeToString(digest[:]), nil
}

func selectRoute(req model.AggregateRequest, dist engine.Distribution) (model.RouteDecision, int) {
	resolved := model.SumAlgorithm(req.Algorithm)
	basis := make([]string, 0, 6)
	estimatedWork := dist.TotalRecords + dist.EstimatedJoinRows
	if resolved == model.SumAuto {
		basis = append(basis, "algorithm=auto")
		switch {
		case req.RelativeTolerance <= 1e-13 || dist.MagnitudeRatio >= 1e10:
			resolved = model.SumDoubleDoubleFix
			basis = append(basis, "precision_or_dynamic_range_requires_exact_fixed_point_merge")
		case req.RelativeTolerance <= 1e-9 || req.PartitionCount >= 8 || dist.MaxRecordShare > req.SkewThreshold:
			resolved = model.SumKahan
			basis = append(basis, "precision_partition_or_skew_requires_compensated_sum")
		default:
			resolved = model.SumNaive
			basis = append(basis, "tolerance_is_covered_by_fast_path")
		}
		if req.ComputeBudget > 0 && estimatedWork*5 > req.ComputeBudget && req.RelativeTolerance > 1e-8 && dist.MagnitudeRatio < 1e8 {
			resolved = model.SumNaive
			basis = append(basis, "compute_budget_favors_fast_path")
		}
	} else {
		basis = append(basis, "algorithm_explicit_no_silent_switch")
	}
	path := model.PathFast
	multiplier := int64(1)
	if resolved == model.SumKahan || resolved == model.SumDoubleDoubleFix {
		path = model.PathHigh
		multiplier = 5
		if resolved == model.SumDoubleDoubleFix {
			multiplier = 8
		}
	}
	fixedScaleBits := req.FixedScaleBits
	if resolved == model.SumDoubleDoubleFix && fixedScaleBits == 0 {
		fixedScaleBits = sum.DefaultFixedScaleBits
	}
	budgetSatisfied := req.ComputeBudget == 0 || estimatedWork*multiplier <= req.ComputeBudget
	basis = append(basis, fmt.Sprintf("partitions=%d", req.PartitionCount))
	basis = append(basis, fmt.Sprintf("max_partition_share=%.6f threshold=%.6f", dist.MaxRecordShare, req.SkewThreshold))
	basis = append(basis, fmt.Sprintf("magnitude_ratio=%.6g", dist.MagnitudeRatio))
	modelText, conclusion := errorModel(path, resolved)
	return model.RouteDecision{
		RequestedAlgorithm:   req.Algorithm,
		ResolvedAlgorithm:    string(resolved),
		Path:                 string(path),
		Basis:                basis,
		PrecisionTarget:      req.RelativeTolerance,
		ComputeBudget:        req.ComputeBudget,
		EstimatedWork:        estimatedWork,
		BudgetSatisfied:      budgetSatisfied,
		ErrorModel:           modelText,
		ApplicableConclusion: conclusion,
	}, fixedScaleBits
}

func errorModel(path model.PrecisionPath, algorithm model.SumAlgorithm) (string, string) {
	if path == model.PathFast {
		return "gamma_(n-1) * sum_abs, gamma_n=n*u/(1-n*u), u=2^-53",
			"Fast path is suitable only when the caller accepts the reported naive bound and the observed cross-partition deviation remains within tolerance; it is not suitable for severe cancellation."
	}
	switch algorithm {
	case model.SumKahan:
		return "(2*u + n*u^2) * sum_abs, u=2^-53",
			"High-precision compensated path is suitable for floating-point cancellation while maintaining stable merge by declared order."
	case model.SumDoubleDoubleFix:
		return "n*2^-(fixed_scale_bits+1) + u*|sum| quantization/final-rounding bound",
			"High-precision fixed-point path is suitable when strict partition-count invariance is required; the fixed scale is explicitly reported and never selected silently."
	default:
		return "declared high-precision error model", "High-precision path selected."
	}
}

func buildErrorDeclaration(route model.RouteDecision, fixedScaleBits int, result *engine.Result) model.ErrorDeclaration {
	declaration := model.ErrorDeclaration{
		Algorithm:            route.ResolvedAlgorithm,
		Path:                 route.Path,
		Model:                route.ErrorModel,
		ApplicableConclusion: route.ApplicableConclusion,
		CompensationDigest:   result.CompensationDigest,
	}
	if route.ResolvedAlgorithm == string(model.SumDoubleDoubleFix) {
		declaration.Parameter = map[string]any{"fixed_scale_bits": fixedScaleBits}
	}
	for _, group := range result.Groups {
		declaration.AbsoluteErrorBoundSum = finiteFloat(declaration.AbsoluteErrorBoundSum + group.AbsoluteErrorBound)
		declaration.MaxRelativeBound = math.Max(declaration.MaxRelativeBound, finiteFloat(group.RelativeErrorBound))
		declaration.MaxAverageRelativeBound = math.Max(declaration.MaxAverageRelativeBound, finiteFloat(group.AverageRelErrorBound))
	}
	return declaration
}

func finiteFloat(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return math.MaxFloat64
	}
	return value
}

func asBusinessError(err error) *model.BusinessError {
	if business, ok := err.(*model.BusinessError); ok {
		return business
	}
	return model.NewError(model.CodeInternal, "execution failed", "cause", err.Error())
}

func conflictError(requestID, existing, current string) *model.BusinessError {
	return model.NewError(model.CodeConflictingRequest,
		"request_id is already associated with different request content",
		"request_id", requestID,
		"existing_fingerprint", existing,
		"submitted_fingerprint", current)
}

func decodeStoredResponse(rec *model.RequestRecord) (*model.AggregateResponse, error) {
	if rec.ResponseJSON == "" {
		return nil, model.NewError(model.CodeInternal, "completed request has no stored response", "request_id", rec.RequestID)
	}
	var response model.AggregateResponse
	if err := json.Unmarshal([]byte(rec.ResponseJSON), &response); err != nil {
		return nil, model.NewError(model.CodeInternal, "stored response is corrupt", "request_id", rec.RequestID, "cause", err.Error())
	}
	return &response, nil
}

func decodeStoredBusinessError(rec *model.RequestRecord) error {
	if rec.ErrorJSON == "" {
		return model.NewError(model.CodeExecutionAborted, "request was not committed", "request_id", rec.RequestID, "status", rec.Status)
	}
	var business model.BusinessError
	if err := json.Unmarshal([]byte(rec.ErrorJSON), &business); err != nil {
		return model.NewError(model.CodeExecutionAborted, "stored failure context is unavailable", "request_id", rec.RequestID, "status", rec.Status)
	}
	return &business
}

func (s *Service) maybeCrashAfterIntent(requestID string) {
	if !s.crashEnabled || s.crashRequestID == "" || s.crashRequestID != requestID {
		return
	}
	marker := s.crashMarker
	if marker == "" {
		fmt.Fprintf(os.Stderr, "SHUFFLE_CRASH_REQUEST_ID matched %s but SHUFFLE_CRASH_MARKER is empty; refusing to crash without a one-shot marker\n", requestID)
		return
	}
	if err := os.MkdirAll(filepath.Dir(marker), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "cannot create crash marker directory: %v\n", err)
		return
	}
	file, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	_, _ = file.WriteString(requestID + "\n")
	_ = file.Sync()
	_ = file.Close()
	fmt.Fprintf(os.Stderr, "deterministic crash injection after durable intent for request %s\n", requestID)
	os.Exit(70)
}

package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"columnar-executor/internal/engine"
	"columnar-executor/internal/model"
	"columnar-executor/internal/snapshot"
	"columnar-executor/internal/store"
	"columnar-executor/internal/verify"
)

type Options struct {
	CrashHook func(executionID string, batchNo int)
}

type Service struct {
	store  *store.Store
	engine *engine.Engine

	snapshotMu sync.RWMutex
	snapshots  map[int64]model.Snapshot
	current    int64

	chainLocks sync.Map
	execSeq    atomic.Uint64

	startupRecovery []model.RecoveryReport
}

func New(st *store.Store, options Options) (*Service, error) {
	if st == nil {
		return nil, fmt.Errorf("store is required")
	}
	svc := &Service{
		store:     st,
		snapshots: map[int64]model.Snapshot{},
	}
	svc.engine = engine.New(st)
	svc.engine.CrashHook = options.CrashHook

	ctx := context.Background()
	saved, err := st.ListSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	for _, snap := range saved {
		if snap.Version > svc.current {
			svc.current = snap.Version
		}
		svc.snapshots[snap.Version] = snap
	}
	reports, err := st.RecoverIncomplete(ctx)
	if err != nil {
		return nil, fmt.Errorf("startup recovery: %w", err)
	}
	svc.startupRecovery = reports
	return svc, nil
}

func (s *Service) Store() *store.Store {
	return s.store
}

func (s *Service) GenerateSnapshot(ctx context.Context, request model.SnapshotGenerateRequest) (model.Snapshot, error) {
	snap, err := snapshot.Generate(request.Version, request.Rows, request.Seed)
	if err != nil {
		return model.Snapshot{}, err
	}
	return s.LoadSnapshot(ctx, snap)
}

func (s *Service) LoadSnapshot(ctx context.Context, snap model.Snapshot) (model.Snapshot, error) {
	if err := snapshot.Validate(snap); err != nil {
		return model.Snapshot{}, err
	}

	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	if s.current != 0 && snap.Version <= s.current {
		return model.Snapshot{}, model.NewBizError(model.CodeStaleVersion, "snapshot version is not newer than current version").
			With("submitted_version", snap.Version).With("current_version", s.current)
	}
	// Persist before publishing in memory. A failed write leaves the previous
	// snapshot fully usable.
	if err := s.store.SaveSnapshot(ctx, snap); err != nil {
		return model.Snapshot{}, model.NewBizError(model.CodeInternal, "failed to persist snapshot").With("cause", err.Error())
	}
	s.snapshots[snap.Version] = snap
	s.current = snap.Version
	return snap, nil
}

func (s *Service) CurrentSnapshot(ctx context.Context) (model.Snapshot, error) {
	s.snapshotMu.RLock()
	defer s.snapshotMu.RUnlock()
	if s.current == 0 {
		return model.Snapshot{}, model.NewBizError(model.CodeNotFound, "no snapshot has been loaded")
	}
	return s.snapshots[s.current], nil
}

func (s *Service) GetSnapshot(ctx context.Context, version int64) (model.Snapshot, error) {
	s.snapshotMu.RLock()
	snap, ok := s.snapshots[version]
	s.snapshotMu.RUnlock()
	if ok {
		return snap, nil
	}
	snap, err := s.store.GetSnapshot(ctx, version)
	if err != nil {
		return model.Snapshot{}, err
	}
	s.snapshotMu.Lock()
	if _, exists := s.snapshots[version]; !exists {
		s.snapshots[version] = snap
		if version > s.current {
			s.current = version
		}
	}
	s.snapshotMu.Unlock()
	return snap, nil
}

func (s *Service) SubmitChain(ctx context.Context, chain model.Chain) (model.Chain, error) {
	snap, err := s.GetSnapshot(ctx, chain.BaseSnapshotVersion)
	if err != nil {
		if biz, ok := model.AsBizError(err); ok && biz.Code == model.CodeNotFound {
			return model.Chain{}, model.NewBizError(model.CodeNotFound, "base snapshot version not found").
				With("base_snapshot_version", chain.BaseSnapshotVersion)
		}
		return model.Chain{}, err
	}
	if err := engine.ValidateChain(chain, snap); err != nil {
		return model.Chain{}, err
	}
	if chain.CreatedAt == "" {
		chain.CreatedAt = model.NowUTC()
	}
	if err := s.store.SubmitChain(ctx, chain); err != nil {
		return model.Chain{}, err
	}
	return chain, nil
}

func (s *Service) GetChain(ctx context.Context, chainID string, version int64) (model.Chain, error) {
	return s.store.GetChain(ctx, chainID, version)
}

func (s *Service) ExecuteChain(ctx context.Context, chainID string, request model.ExecuteRequest) (*model.ExecutionView, error) {
	if request.BitmapForm == "" {
		return nil, model.NewBizError(model.CodeInvalidRequest, "bitmap_form is required")
	}
	if !model.ValidBitmapForm(request.BitmapForm) {
		return nil, model.NewBizError(model.CodeInvalidRequest, "unsupported bitmap_form").
			With("bitmap_form", request.BitmapForm)
	}
	chain, err := s.store.GetChain(ctx, chainID, request.ChainVersion)
	if err != nil {
		return nil, err
	}
	snap, err := s.GetSnapshot(ctx, chain.BaseSnapshotVersion)
	if err != nil {
		return nil, err
	}
	selection, ok := snap.Selection(request.BitmapForm)
	if !ok {
		return nil, model.NewBizError(model.CodeBitmapMismatch, "selection bitmap form is not present in snapshot").
			With("bitmap_form", request.BitmapForm)
	}
	if selection.Rows != snap.RowCount {
		return nil, model.NewBizError(model.CodeBitmapMismatch, "selection bitmap length does not match snapshot rows").
			With("form", request.BitmapForm).With("bitmap_rows", selection.Rows).With("snapshot_rows", snap.RowCount)
	}

	executionID := strings.TrimSpace(request.ExecutionID)
	if executionID == "" {
		executionID = s.newExecutionID(chainID, request.ChainVersion)
	} else {
		if _, err := s.store.GetExecution(ctx, executionID); err == nil {
			return nil, model.NewBizError(model.CodeExecConflict, "execution_id already exists").With("execution_id", executionID)
		} else if biz, ok := model.AsBizError(err); !ok || biz.Code != model.CodeNotFound {
			return nil, err
		}
	}

	key := fmt.Sprintf("%s@%d", chainID, request.ChainVersion)
	lockValue, _ := s.chainLocks.LoadOrStore(key, &sync.Mutex{})
	lock := lockValue.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()

	return s.engine.Execute(ctx, snap, chain, request, selection, executionID)
}

func (s *Service) newExecutionID(chainID string, version int64) string {
	seq := s.execSeq.Add(1)
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return fmt.Sprintf("%s-v%d-%d", sanitize(chainID), version, seq)
	}
	return fmt.Sprintf("%s-v%d-%d-%s", sanitize(chainID), version, seq, hex.EncodeToString(random))
}

func sanitize(value string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")
	return replacer.Replace(value)
}

func (s *Service) GetExecution(ctx context.Context, executionID string) (model.ExecutionView, error) {
	return s.store.GetExecution(ctx, executionID)
}

func (s *Service) ListExecutions(ctx context.Context, chainID string, chainVersion int64) ([]model.ExecutionSummary, error) {
	return s.store.ListExecutions(ctx, chainID, chainVersion)
}

func (s *Service) ResolveExecution(ctx context.Context, chainID string, chainVersion int64, executionID string) (model.ExecutionView, error) {
	if executionID != "" {
		view, err := s.store.GetExecution(ctx, executionID)
		if err != nil {
			return model.ExecutionView{}, err
		}
		if view.ChainID != chainID || view.ChainVersion != chainVersion {
			return model.ExecutionView{}, model.NewBizError(model.CodeNotFound, "execution does not belong to chain version").
				With("execution_id", executionID).With("chain_id", chainID).With("chain_version", chainVersion)
		}
		return view, nil
	}
	summaries, err := s.store.ListExecutions(ctx, chainID, chainVersion)
	if err != nil {
		return model.ExecutionView{}, err
	}
	if len(summaries) == 0 {
		return model.ExecutionView{}, model.NewBizError(model.CodeNotFound, "no execution exists for chain version").
			With("chain_id", chainID).With("chain_version", chainVersion)
	}
	chosen := summaries[0].ExecutionID
	for _, summary := range summaries {
		if summary.Status == model.StatusCommitted {
			chosen = summary.ExecutionID
			break
		}
	}
	return s.store.GetExecution(ctx, chosen)
}

func (s *Service) RecoveryReports(ctx context.Context) ([]model.RecoveryReport, error) {
	return s.store.ListRecoveryReports(ctx)
}

func (s *Service) StartupRecoveryReports() []model.RecoveryReport {
	out := make([]model.RecoveryReport, len(s.startupRecovery))
	copy(out, s.startupRecovery)
	return out
}

func (s *Service) CompareExecutions(ctx context.Context, leftID, rightID string, tolerancePPM int64) (verify.Report, error) {
	left, err := s.store.GetExecution(ctx, leftID)
	if err != nil {
		return verify.Report{}, err
	}
	right, err := s.store.GetExecution(ctx, rightID)
	if err != nil {
		return verify.Report{}, err
	}
	if !left.Published || !right.Published {
		return verify.Report{}, model.NewBizError(model.CodeExecConflict, "only committed executions can be compared").
			With("left_published", left.Published).With("right_published", right.Published)
	}
	return verify.Compare(left, right, tolerancePPM), nil
}

type FaultProbeResult struct {
	Kind         string `json:"kind"`
	BusinessCode string `json:"business_code"`
	Description  string `json:"description"`
}

func ProbeFault(kind string) (FaultProbeResult, error) {
	code, ok := model.BusinessCodeForFault(kind)
	if !ok {
		return FaultProbeResult{}, model.NewBizError(model.CodeInvalidRequest, "unsupported fault kind").With("kind", kind)
	}
	description := map[string]string{
		model.CodeBitmapMismatch:  "selection/column bitmap length mismatch before batch results are published",
		model.CodeNumericOverflow: "checked int64 fixed-point arithmetic overflow with column and batch context",
		model.CodeDictIndex:       "dictionary index outside dictionary table",
		model.CodeDivideByZero:    "fixed-point division by zero",
		model.CodeExecAborted:     "mid-execution abort marker; provisional facts remain but no committed version exists",
	}[code]
	return FaultProbeResult{Kind: strings.ToLower(strings.TrimSpace(kind)), BusinessCode: code, Description: description}, nil
}

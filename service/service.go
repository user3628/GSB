package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"releasegate/domain"
	"releasegate/store"
)

const (
	IntentCreateRelease = "CREATE_RELEASE"
	IntentStartRelease  = "START_RELEASE"
	IntentIngestBatch   = "INGEST_BATCH"
	IntentRecompute     = "RECOMPUTE_STATS"
	IntentGateDecision  = "GATE_DECISION"
	IntentTransition    = "TRANSITION"
)

type Service struct {
	store   *store.Store
	clock   func() time.Time
	mu      sync.Mutex
	writers map[string]*sync.Mutex
}

func New(st *store.Store) *Service {
	return &Service{store: st, clock: time.Now, writers: map[string]*sync.Mutex{}}
}

func (s *Service) Store() *store.Store { return s.store }

func (s *Service) writer(releaseID string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.writers[releaseID]
	if m == nil {
		m = &sync.Mutex{}
		s.writers[releaseID] = m
	}
	return m
}

func (s *Service) nowMS() int64 { return s.clock().UnixMilli() }

type CreateReleaseRequest struct {
	ReleaseID  string                   `json:"release_id"`
	Definition domain.ReleaseDefinition `json:"definition"`
}

// UnmarshalJSON accepts both the documented wrapper
// {"release_id":...,"definition":{...}} and a bare release definition. The
// wrapper is canonical; the bare form keeps the domain component easy to embed.
func (r *CreateReleaseRequest) UnmarshalJSON(data []byte) error {
	var wrapper struct {
		ReleaseID  string                    `json:"release_id"`
		Definition *domain.ReleaseDefinition `json:"definition"`
	}
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return err
	}
	if wrapper.Definition != nil {
		r.ReleaseID = wrapper.ReleaseID
		r.Definition = *wrapper.Definition
		return nil
	}
	var direct domain.ReleaseDefinition
	if err := json.Unmarshal(data, &direct); err != nil {
		return err
	}
	r.ReleaseID = direct.ReleaseID
	r.Definition = direct
	return nil
}

func (s *Service) CreateRelease(ctx context.Context, req CreateReleaseRequest) (domain.Release, error) {
	def := req.Definition
	if def.ReleaseID == "" {
		def.ReleaseID = req.ReleaseID
	}
	if req.ReleaseID != "" && def.ReleaseID != req.ReleaseID {
		return domain.Release{}, domain.WithContext(domain.NewError(domain.CodeReleaseInvalid, "top-level release_id and definition.release_id disagree"), "top_level", req.ReleaseID)
	}
	if def.ReleaseID == "" {
		return domain.Release{}, domain.NewError(domain.CodeReleaseInvalid, "release_id is required")
	}
	if err := domain.ValidateDefinition(def); err != nil {
		return domain.Release{}, err
	}
	lock := s.writer(def.ReleaseID)
	lock.Lock()
	defer lock.Unlock()
	if _, err := s.store.GetRelease(ctx, s.store, def.ReleaseID); err == nil {
		return domain.Release{}, domain.WithContext(domain.NewError(domain.CodeReleaseInvalid, "release already exists"), "release_id", def.ReleaseID)
	} else if be, ok := domain.AsBusinessError(err); !ok || be.Code != domain.CodeNotFound {
		return domain.Release{}, err
	}
	now := s.nowMS()
	rel := domain.Release{
		ReleaseID: def.ReleaseID, Definition: def, State: domain.StatePrepared,
		CurrentStage: 0, TrafficPerMille: 0, CurrentStatsVersion: 0, CreatedAtMS: now, UpdatedAtMS: now,
	}
	intent := store.Intent{
		IntentID: newIntentID(), ReleaseID: def.ReleaseID, Kind: IntentCreateRelease,
		PayloadJSON: store.MarshalIntentPayload(map[string]any{"stage_count": len(def.Stages)}), FromState: domain.StatePrepared,
	}
	if err := s.store.BeginIntent(ctx, intent); err != nil {
		return domain.Release{}, err
	}
	tx, err := s.store.BeginTx(ctx, false)
	if err != nil {
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.Release{}, err
	}
	defer tx.Rollback()
	if err := s.store.CreateRelease(ctx, tx, rel); err != nil {
		_ = tx.Rollback()
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.Release{}, err
	}
	if err := s.store.CompleteIntent(ctx, tx, intent.IntentID, "release created in PREPARED state"); err != nil {
		_ = tx.Rollback()
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.Release{}, err
	}
	if err := tx.Commit(); err != nil {
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.Release{}, err
	}
	return rel, nil
}

func (s *Service) GetRelease(ctx context.Context, releaseID string) (domain.Release, error) {
	return s.store.GetRelease(ctx, s.store, releaseID)
}

func (s *Service) Start(ctx context.Context, releaseID string) (domain.Release, error) {
	lock := s.writer(releaseID)
	lock.Lock()
	defer lock.Unlock()
	rel, err := s.store.GetRelease(ctx, s.store, releaseID)
	if err != nil {
		return domain.Release{}, err
	}
	if err := domain.ValidateTransition(rel.State, domain.StateCanary); err != nil {
		return domain.Release{}, err
	}
	if len(rel.Definition.Stages) == 0 {
		return domain.Release{}, domain.NewError(domain.CodeReleaseInvalid, "release has no stages")
	}
	first := rel.Definition.Stages[0]
	now := s.nowMS()
	from := rel
	rel.State = domain.StateCanary
	rel.CurrentStage = first.Number
	rel.TrafficPerMille = first.TargetTrafficPerMille
	rel.UpdatedAtMS = now
	intent := store.Intent{IntentID: newIntentID(), ReleaseID: releaseID, Kind: IntentStartRelease,
		FromState: from.State, FromStage: from.CurrentStage, FromTrafficPerMille: from.TrafficPerMille, FromVersion: from.CurrentStatsVersion}
	if err := s.store.BeginIntent(ctx, intent); err != nil {
		return domain.Release{}, err
	}
	tx, err := s.store.BeginTx(ctx, false)
	if err != nil {
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.Release{}, err
	}
	defer tx.Rollback()
	if err := s.store.UpdateRelease(ctx, tx, rel); err != nil {
		_ = tx.Rollback()
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.Release{}, err
	}
	if err := s.store.CompleteIntent(ctx, tx, intent.IntentID, "release started at first canary stage"); err != nil {
		_ = tx.Rollback()
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.Release{}, err
	}
	if err := tx.Commit(); err != nil {
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.Release{}, err
	}
	return rel, nil
}

func (s *Service) IngestBatch(ctx context.Context, in domain.BatchInput) (domain.BatchResult, error) {
	rel, err := s.store.GetRelease(ctx, s.store, in.ReleaseID)
	if err != nil {
		return domain.BatchResult{}, err
	}
	batch, err := domain.NormalizeBatch(rel.Definition, in)
	if err != nil {
		return domain.BatchResult{}, err
	}
	hash, err := contentHash(batch)
	if err != nil {
		return domain.BatchResult{}, err
	}
	lock := s.writer(in.ReleaseID)
	lock.Lock()
	defer lock.Unlock()
	rel, err = s.store.GetRelease(ctx, s.store, in.ReleaseID)
	if err != nil {
		return domain.BatchResult{}, err
	}
	if existing, ok, err := s.store.GetBatch(ctx, s.store, in.ReleaseID, in.BatchID); err != nil {
		return domain.BatchResult{}, err
	} else if ok {
		return idempotentOrConflict(existing, hash)
	}
	if rel.State != domain.StateCanary && rel.State != domain.StateObserving && rel.State != domain.StatePromoting {
		return domain.BatchResult{}, domain.WithContext(domain.NewError(domain.CodeInvalidTransition, "observations cannot be ingested in current state"), "state", rel.State)
	}
	if batch.Stage != rel.CurrentStage {
		return domain.BatchResult{}, domain.WithContext(domain.NewError(domain.CodeReleaseInvalid, "batch stage is not the current release stage"), "expected_stage", rel.CurrentStage).ContextValue("actual_stage", batch.Stage)
	}
	maxTS, err := s.store.MaxCommittedTimestamp(ctx, s.store, in.ReleaseID, batch.Stage)
	if err != nil {
		return domain.BatchResult{}, err
	}
	if maxTS >= 0 && batch.Observations[0].TimestampMS < maxTS {
		return domain.BatchResult{}, domain.WithContext(domain.NewError(domain.CodeReleaseInvalid, "batch timestamps precede already committed observations"), "last_committed_timestamp_ms", maxTS)
	}
	now := s.nowMS()
	intent := store.Intent{IntentID: newIntentID(), ReleaseID: in.ReleaseID, Kind: IntentIngestBatch,
		FromState: rel.State, FromStage: rel.CurrentStage, FromTrafficPerMille: rel.TrafficPerMille, FromVersion: rel.CurrentStatsVersion,
		PayloadJSON: store.MarshalIntentPayload(map[string]any{"batch_id": batch.BatchID, "stage": batch.Stage, "observation_count": len(batch.Observations), "content_hash": hash})}
	if err := s.store.BeginIntent(ctx, intent); err != nil {
		return domain.BatchResult{}, err
	}
	if err := s.store.InsertStagingBatch(ctx, batch, hash, now); err != nil {
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.BatchResult{}, err
	}

	newVersion := rel.CurrentStatsVersion + 1
	tx, err := s.store.BeginTx(ctx, false)
	if err != nil {
		_ = s.store.MarkBatchAborted(ctx, in.ReleaseID, in.BatchID, err.Error())
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.BatchResult{}, err
	}
	txOpen := true
	defer func() {
		if txOpen {
			_ = tx.Rollback()
		}
	}()
	failIngest := func(cause error) (domain.BatchResult, error) {
		if txOpen {
			_ = tx.Rollback()
			txOpen = false
		}
		_ = s.store.MarkBatchAborted(ctx, in.ReleaseID, in.BatchID, cause.Error())
		_ = s.store.FailIntent(ctx, intent.IntentID, cause.Error())
		return domain.BatchResult{}, cause
	}
	if err := s.store.MarkBatchCommitted(ctx, tx, in.ReleaseID, in.BatchID, newVersion, now); err != nil {
		return failIngest(err)
	}
	observations, err := s.store.LoadCommittedObservations(ctx, tx, in.ReleaseID, rel.CurrentStage)
	if err != nil {
		return failIngest(err)
	}
	snapshot, err := ComputeSnapshot(rel.Definition, in.ReleaseID, newVersion, rel.CurrentStage, now, observations)
	if err != nil {
		return failIngest(err)
	}
	if err := s.store.InsertStatsVersion(ctx, tx, snapshot); err != nil {
		return failIngest(err)
	}
	if err := s.store.RetireOldVersions(ctx, tx, in.ReleaseID, newVersion); err != nil {
		return failIngest(err)
	}
	rel.CurrentStatsVersion = newVersion
	rel.UpdatedAtMS = now
	if err := s.store.UpdateRelease(ctx, tx, rel); err != nil {
		return failIngest(err)
	}
	if err := s.store.CompleteIntent(ctx, tx, intent.IntentID, "batch committed with immutable statistics snapshot"); err != nil {
		return failIngest(err)
	}
	if err := tx.Commit(); err != nil {
		txOpen = false
		_ = s.store.MarkBatchAborted(ctx, in.ReleaseID, in.BatchID, err.Error())
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.BatchResult{}, err
	}
	txOpen = false
	return domain.BatchResult{
		ReleaseID: in.ReleaseID, BatchID: in.BatchID, Status: store.BatchCommitted,
		StatsVersion: newVersion, ObservationCount: len(batch.Observations), ContentHash: hash,
	}, nil
}

func idempotentOrConflict(existing store.BatchRecord, hash string) (domain.BatchResult, error) {
	if existing.ContentHash != hash {
		return domain.BatchResult{}, domain.WithContext(domain.NewError(domain.CodeConflictingBatch, "batch_id was already used with different content"), "batch_id", existing.BatchID).ContextValue("existing_hash", existing.ContentHash).ContextValue("submitted_hash", hash)
	}
	if existing.Status != store.BatchCommitted {
		return domain.BatchResult{}, domain.WithContext(domain.NewError(domain.CodeConflictingBatch, "batch_id belongs to a non-committed batch"), "batch_id", existing.BatchID).ContextValue("status", existing.Status)
	}
	return domain.BatchResult{
		ReleaseID: existing.ReleaseID, BatchID: existing.BatchID, Status: existing.Status,
		IdempotentReuse: true, StatsVersion: existing.StatsVersion,
		ObservationCount: existing.ObservationCount, ContentHash: existing.ContentHash,
	}, nil
}

func (s *Service) Gate(ctx context.Context, releaseID string, boundVersion int64) (domain.GateResult, error) {
	lock := s.writer(releaseID)
	lock.Lock()
	defer lock.Unlock()
	rel, err := s.store.GetRelease(ctx, s.store, releaseID)
	if err != nil {
		return domain.GateResult{}, err
	}
	if rel.State != domain.StateCanary && rel.State != domain.StateObserving && rel.State != domain.StatePromoting {
		return domain.GateResult{}, domain.WithContext(domain.NewError(domain.CodeInvalidTransition, "gate cannot be evaluated in current state"), "state", rel.State)
	}
	if boundVersion != 0 && boundVersion != rel.CurrentStatsVersion {
		return domain.GateResult{}, domain.WithContext(domain.NewError(domain.CodeVersionRetired, "bound statistics version is no longer current"), "bound_version", boundVersion).ContextValue("current_version", rel.CurrentStatsVersion)
	}
	if rel.CurrentStatsVersion <= 0 {
		return domain.GateResult{}, domain.WithContext(domain.NewError(domain.CodeInsufficientSamples, "release has no committed observations"), "release_id", releaseID)
	}
	snap, err := s.store.SnapshotByVersion(ctx, s.store, releaseID, rel.CurrentStatsVersion)
	if err != nil {
		return domain.GateResult{}, err
	}
	stageDef, ok := rel.Definition.Stage(rel.CurrentStage)
	if !ok {
		return domain.GateResult{}, domain.WithContext(domain.NewError(domain.CodeReleaseInvalid, "current stage is absent from definition"), "stage", rel.CurrentStage)
	}
	evaluation := domain.EvaluateGate(stageDef, snap.StageMetrics, snap.StageEstimate, snap.StageErrorContract)
	result := domain.GateResult{StageMetrics: snap.StageMetrics, Estimate: snap.StageEstimate, ErrorContract: snap.StageErrorContract}
	now := s.nowMS()
	from := rel
	toState := domain.StateObserving
	toStage := rel.CurrentStage
	toTraffic := rel.TrafficPerMille
	if evaluation.Outcome == domain.OutcomePromote {
		toState = domain.StatePromoting
		next, hasNext := nextStage(rel.Definition, rel.CurrentStage)
		if hasNext {
			toStage = next.Number
			toTraffic = next.TargetTrafficPerMille
		} else {
			toTraffic = stageDef.TargetTrafficPerMille
		}
	}
	decision := domain.GateDecision{
		ReleaseID: releaseID, StatsVersion: rel.CurrentStatsVersion, Stage: rel.CurrentStage, Scope: "stage",
		Outcome: evaluation.Outcome, Code: evaluation.Code, TriggeredThreshold: evaluation.TriggeredThreshold,
		Measured: evaluation.Measured, FromState: from.State, ToState: toState,
		FromStage: from.CurrentStage, ToStage: toStage,
		FromTrafficPerMille: from.TrafficPerMille, ToTrafficPerMille: toTraffic, CreatedAtMS: now,
	}
	intent := store.Intent{IntentID: newIntentID(), ReleaseID: releaseID, Kind: IntentGateDecision,
		FromState: from.State, FromStage: from.CurrentStage, FromTrafficPerMille: from.TrafficPerMille, FromVersion: from.CurrentStatsVersion,
		PayloadJSON: store.MarshalIntentPayload(map[string]any{"outcome": evaluation.Outcome, "threshold": evaluation.TriggeredThreshold, "stats_version": rel.CurrentStatsVersion})}
	if err := s.store.BeginIntent(ctx, intent); err != nil {
		return domain.GateResult{}, err
	}
	tx, err := s.store.BeginTx(ctx, false)
	if err != nil {
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.GateResult{}, err
	}
	defer tx.Rollback()
	latest, err := s.store.GetRelease(ctx, tx, releaseID)
	if err != nil {
		_ = tx.Rollback()
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.GateResult{}, err
	}
	if latest.State != from.State || latest.CurrentStage != from.CurrentStage || latest.CurrentStatsVersion != from.CurrentStatsVersion || latest.TrafficPerMille != from.TrafficPerMille {
		_ = tx.Rollback()
		_ = s.store.FailIntent(ctx, intent.IntentID, "release changed concurrently")
		return domain.GateResult{}, domain.NewError(domain.CodeVersionRetired, "release changed while gate was being evaluated")
	}
	result.Decision = decision
	if err := s.store.InsertDecision(ctx, tx, &result.Decision); err != nil {
		_ = tx.Rollback()
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.GateResult{}, err
	}
	rel.State = toState
	rel.CurrentStage = toStage
	rel.TrafficPerMille = toTraffic
	rel.UpdatedAtMS = now
	if err := s.store.UpdateRelease(ctx, tx, rel); err != nil {
		_ = tx.Rollback()
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.GateResult{}, err
	}
	if err := s.store.CompleteIntent(ctx, tx, intent.IntentID, "decision, state and traffic committed atomically"); err != nil {
		_ = tx.Rollback()
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.GateResult{}, err
	}
	if err := tx.Commit(); err != nil {
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.GateResult{}, err
	}
	if evaluation.Code != "" {
		be := domain.WithContext(domain.NewError(evaluation.Code, "release gate did not promote"), "action", evaluation.Code)
		be.Context["gate_result"] = result
		return result, be
	}
	return result, nil
}

func (s *Service) Transition(ctx context.Context, releaseID, to, reason string) (domain.Release, error) {
	lock := s.writer(releaseID)
	lock.Lock()
	defer lock.Unlock()
	rel, err := s.store.GetRelease(ctx, s.store, releaseID)
	if err != nil {
		return domain.Release{}, err
	}
	if err := domain.ValidateTransition(rel.State, to); err != nil {
		return domain.Release{}, err
	}
	now := s.nowMS()
	from := rel
	rel.State = to
	if to == domain.StateCanary {
		first := rel.Definition.Stages[0]
		rel.CurrentStage = first.Number
		rel.TrafficPerMille = first.TargetTrafficPerMille
	}
	if to == domain.StateRolledBack || to == domain.StateAborted {
		rel.TrafficPerMille = 0
	}
	rel.UpdatedAtMS = now
	intent := store.Intent{IntentID: newIntentID(), ReleaseID: releaseID, Kind: IntentTransition,
		FromState: from.State, FromStage: from.CurrentStage, FromTrafficPerMille: from.TrafficPerMille, FromVersion: from.CurrentStatsVersion,
		PayloadJSON: store.MarshalIntentPayload(map[string]any{"to_state": to, "reason": reason})}
	if err := s.store.BeginIntent(ctx, intent); err != nil {
		return domain.Release{}, err
	}
	tx, err := s.store.BeginTx(ctx, false)
	if err != nil {
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.Release{}, err
	}
	defer tx.Rollback()
	latest, err := s.store.GetRelease(ctx, tx, releaseID)
	if err != nil {
		_ = tx.Rollback()
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.Release{}, err
	}
	if latest.State != from.State {
		_ = tx.Rollback()
		_ = s.store.FailIntent(ctx, intent.IntentID, "release changed concurrently")
		return domain.Release{}, domain.NewError(domain.CodeInvalidTransition, "release changed while transition was being prepared")
	}
	if err := s.store.UpdateRelease(ctx, tx, rel); err != nil {
		_ = tx.Rollback()
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.Release{}, err
	}
	if err := s.store.CompleteIntent(ctx, tx, intent.IntentID, "release header transition committed"); err != nil {
		_ = tx.Rollback()
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.Release{}, err
	}
	if err := tx.Commit(); err != nil {
		_ = s.store.FailIntent(ctx, intent.IntentID, err.Error())
		return domain.Release{}, err
	}
	return rel, nil
}

type StatsDetail struct {
	Release       domain.Release            `json:"release"`
	Version       int64                     `json:"version"`
	Scope         string                    `json:"scope"`
	Stage         int                       `json:"stage"`
	StageMetrics  domain.StageMetrics       `json:"stage_metrics"`
	Estimate      domain.DifferenceEstimate `json:"estimate"`
	ErrorContract domain.ErrorContract      `json:"error_contract"`
}

func (s *Service) StatsDetail(ctx context.Context, releaseID string, version int64, scope string, stage int) (StatsDetail, error) {
	if scope == "" {
		scope = "stage"
	}
	if scope != "stage" && scope != "cumulative" {
		return StatsDetail{}, domain.WithContext(domain.NewError(domain.CodeReleaseInvalid, "scope must be stage or cumulative"), "scope", scope)
	}
	rel, snap, err := s.store.ReadSnapshot(ctx, releaseID, version)
	if err != nil {
		return StatsDetail{}, err
	}
	if stage != 0 && stage != snap.Stage {
		return StatsDetail{}, domain.WithContext(domain.NewError(domain.CodeNotFound, "statistics version does not describe requested stage"), "requested_stage", stage).ContextValue("version_stage", snap.Stage)
	}
	detail := StatsDetail{Release: rel, Version: snap.Version, Scope: scope, Stage: snap.Stage}
	if scope == "cumulative" {
		detail.StageMetrics = snap.CumulativeMetrics
		detail.Estimate = snap.CumulativeEstimate
		detail.ErrorContract = snap.CumulativeErrorContract
	} else {
		detail.StageMetrics = snap.StageMetrics
		detail.Estimate = snap.StageEstimate
		detail.ErrorContract = snap.StageErrorContract
	}
	return detail, nil
}

func (s *Service) ErrorContract(ctx context.Context, releaseID string, version int64, scope string) (domain.ErrorContract, error) {
	d, err := s.StatsDetail(ctx, releaseID, version, scope, 0)
	return d.ErrorContract, err
}

func (s *Service) Decisions(ctx context.Context, releaseID string) ([]domain.GateDecision, error) {
	if _, err := s.store.GetRelease(ctx, s.store, releaseID); err != nil {
		return nil, err
	}
	return s.store.ListDecisions(ctx, releaseID)
}

func (s *Service) RecoveryReport(ctx context.Context, releaseID string) (store.RecoveryReport, error) {
	return s.store.LatestRecoveryReport(ctx, releaseID)
}

func (s *Service) ScanRecovery(ctx context.Context) (store.RecoveryReport, error) {
	return s.store.Recover(ctx)
}

// PrepareFaultIntent makes a durable intent and deliberately performs no
// domain mutation. A process-exit fault hook can then terminate the server.
func (s *Service) PrepareFaultIntent(ctx context.Context, releaseID, kind string) (store.Intent, error) {
	lock := s.writer(releaseID)
	lock.Lock()
	defer lock.Unlock()
	rel, err := s.store.GetRelease(ctx, s.store, releaseID)
	if err != nil {
		return store.Intent{}, err
	}
	if kind == "" {
		kind = IntentTransition
	}
	intent := store.Intent{IntentID: newIntentID(), ReleaseID: releaseID, Kind: kind,
		FromState: rel.State, FromStage: rel.CurrentStage, FromTrafficPerMille: rel.TrafficPerMille, FromVersion: rel.CurrentStatsVersion,
		PayloadJSON: store.MarshalIntentPayload(map[string]any{"fault": "forced-termination-after-durable-intent"})}
	if err := s.store.BeginIntent(ctx, intent); err != nil {
		return store.Intent{}, err
	}
	return intent, nil
}

func nextStage(def domain.ReleaseDefinition, current int) (domain.StageDefinition, bool) {
	for _, s := range def.Stages {
		if s.Number > current {
			return s, true
		}
	}
	return domain.StageDefinition{}, false
}

func contentHash(batch domain.Batch) (string, error) {
	type canonicalObservation struct {
		TimestampMS int64  `json:"timestamp_ms"`
		InstanceID  string `json:"instance_id"`
		Group       string `json:"group"`
		Success     bool   `json:"success"`
		LatencyUS   int64  `json:"latency_us"`
		Stage       int    `json:"stage"`
		Seq         int    `json:"seq"`
	}
	payload := struct {
		ReleaseID    string                 `json:"release_id"`
		BatchID      string                 `json:"batch_id"`
		Stage        int                    `json:"stage"`
		Observations []canonicalObservation `json:"observations"`
	}{ReleaseID: batch.ReleaseID, BatchID: batch.BatchID, Stage: batch.Stage}
	for _, o := range batch.Observations {
		payload.Observations = append(payload.Observations, canonicalObservation{
			TimestampMS: o.TimestampMS, InstanceID: o.InstanceID, Group: o.Group,
			Success: o.Success, LatencyUS: o.LatencyUS, Stage: o.Stage, Seq: o.Seq,
		})
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func newIntentID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err == nil {
		return "intent-" + hex.EncodeToString(b[:])
	}
	return fmt.Sprintf("intent-%d", time.Now().UnixNano())
}

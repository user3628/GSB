package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"recoveryengine/internal/fixed"
	"recoveryengine/internal/graph"
	"recoveryengine/internal/model"
	"recoveryengine/internal/probe"
	"recoveryengine/internal/spec"
	"recoveryengine/internal/store"
)

type Engine struct {
	store        *store.Store
	probe        *probe.Client
	testHooks    bool
	writeMu      sync.Mutex
	stateMu      sync.RWMutex
	state        *liveState
	reservations *reservationTracker
}

type liveState struct {
	view          StateView
	snapshot      model.Snapshot
	analysis      *graph.Analysis
	consistency   fixed.Result
	refs          []store.RefRecord
	derivedDigest string
}

type StateView struct {
	Version             int64                        `json:"version"`
	GeneratedAt         string                       `json:"generated_at"`
	ChecksumSHA256      string                       `json:"checksum_sha256"`
	Components          []model.Component            `json:"components"`
	Edges               []model.Edge                 `json:"edges"`
	Observations        map[string]model.Observation `json:"observations"`
	ConsistencyConfig   model.ConsistencyConfig      `json:"consistency_config"`
	DuplicateComponents []model.DuplicateComponent   `json:"duplicate_components,omitempty"`
	Derivation          DerivationView               `json:"derivation"`
	ReferenceCount      int                          `json:"reference_count"`
}

type DerivationView struct {
	TopologyBatches map[string]int                `json:"topology_batches"`
	BatchGroups     []graph.BatchGroup            `json:"batch_groups"`
	Blocked         map[string]graph.Block        `json:"blocked"`
	Cycles          []graph.Cycle                 `json:"cycles"`
	Diagnostics     []graph.Diagnostic            `json:"diagnostics"`
	Impacts         map[string]graph.ImpactResult `json:"impacts"`
	Consistency     fixed.Result                  `json:"consistency"`
	GraphDigest     string                        `json:"graph_digest"`
	DerivedDigest   string                        `json:"derived_digest"`
}

type CommitOptions struct {
	HoldAfterIntent time.Duration
}

type CommitResult struct {
	Version             int64                      `json:"version"`
	BaseVersion         int64                      `json:"base_version"`
	GeneratedAt         string                     `json:"generated_at"`
	ChecksumSHA256      string                     `json:"checksum_sha256"`
	DuplicateComponents []model.DuplicateComponent `json:"duplicate_components,omitempty"`
	Cascade             store.RebuildRecord        `json:"cascade"`
	Diagnostics         []graph.Diagnostic         `json:"diagnostics"`
	Consistency         fixed.Result               `json:"consistency"`
	GraphDigest         string                     `json:"graph_digest"`
	DerivedDigest       string                     `json:"derived_digest"`
	IntentID            string                     `json:"intent_id"`
	CommittedAt         string                     `json:"committed_at"`
	Replayed            bool                       `json:"replayed,omitempty"`
}

type storedBusinessError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
	Partial any            `json:"partial_result,omitempty"`
}

func New(ctx context.Context, databasePath string, testHooks bool) (*Engine, error) {
	database, err := store.Open(ctx, databasePath)
	if err != nil {
		return nil, err
	}
	engine := &Engine{
		store:        database,
		probe:        &probe.Client{},
		testHooks:    testHooks,
		reservations: newReservationTracker(),
	}
	if _, err := database.RecoverInterrupted(ctx, store.Now()); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("recover interrupted intents: %w", err)
	}
	snapshot, duplicates, storedDigest, exists, err := database.CurrentSnapshot(ctx)
	if err != nil {
		_ = database.Close()
		return nil, err
	}
	if !exists {
		snapshot = emptySnapshot()
		duplicates = nil
	}
	analysis := graph.Analyze(snapshot)
	consistency := fixed.Compute(snapshot)
	refs := buildDerivedRefs(snapshot, analysis, consistency)
	digest := derivedDigest(snapshot, analysis, consistency, refs)
	if exists && storedDigest != "" && storedDigest != digest {
		_ = database.Close()
		return nil, fmt.Errorf("persisted derived digest mismatch at version %d: stored=%s computed=%s", snapshot.Version, storedDigest, digest)
	}
	engine.state = &liveState{
		view:          makeStateView(snapshot, duplicates, analysis, consistency, refs, digest),
		snapshot:      snapshot,
		analysis:      analysis,
		consistency:   consistency,
		refs:          refs,
		derivedDigest: digest,
	}
	return engine, nil
}

func (e *Engine) Close() error {
	return e.store.Close()
}

func (e *Engine) State() StateView {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	return e.state.view
}

func (e *Engine) current() *liveState {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	return e.state
}

func (e *Engine) publish(snapshot model.Snapshot, duplicates []model.DuplicateComponent, analysis *graph.Analysis, consistency fixed.Result, refs []store.RefRecord, digest string) {
	next := &liveState{
		view:          makeStateView(snapshot, duplicates, analysis, consistency, refs, digest),
		snapshot:      snapshot,
		analysis:      analysis,
		consistency:   consistency,
		refs:          refs,
		derivedDigest: digest,
	}
	e.stateMu.Lock()
	e.state = next
	e.stateMu.Unlock()
}

func makeStateView(snapshot model.Snapshot, duplicates []model.DuplicateComponent, analysis *graph.Analysis, consistency fixed.Result, refs []store.RefRecord, digest string) StateView {
	return StateView{
		Version:             snapshot.Version,
		GeneratedAt:         snapshot.GeneratedAt,
		ChecksumSHA256:      snapshot.ChecksumSHA256,
		Components:          snapshot.SortedComponents(),
		Edges:               snapshot.SortedEdges(),
		Observations:        snapshot.Observations,
		ConsistencyConfig:   snapshot.ConsistencyConfig,
		DuplicateComponents: append([]model.DuplicateComponent(nil), duplicates...),
		ReferenceCount:      len(refs),
		Derivation: DerivationView{
			TopologyBatches: analysis.TopologyBatches,
			BatchGroups:     analysis.BatchGroups,
			Blocked:         analysis.Blocked,
			Cycles:          analysis.Cycles,
			Diagnostics:     analysis.Diagnostics,
			Impacts:         analysis.Impacts,
			Consistency:     consistency,
			GraphDigest:     analysis.GraphDigest,
			DerivedDigest:   digest,
		},
	}
}

func emptySnapshot() model.Snapshot {
	return model.Snapshot{
		Version:      0,
		GeneratedAt:  "1970-01-01T00:00:00Z",
		Components:   []model.Component{},
		Edges:        []model.Edge{},
		Observations: map[string]model.Observation{},
		ConsistencyConfig: model.ConsistencyConfig{
			ThresholdPermille: 0, RelativeTolerancePPM: 0,
		},
	}
}

func hashPayload(kind string, payload any) (string, error) {
	bytes, err := json.Marshal(struct {
		Kind    string `json:"kind"`
		Payload any    `json:"payload"`
	}{Kind: kind, Payload: payload})
	if err != nil {
		return "", err
	}
	sum := sha256Bytes(bytes)
	return "sha256:" + hex.EncodeToString(sum), nil
}

func sha256Bytes(value []byte) []byte {
	sum := sha256.Sum256(value)
	return sum[:]
}

func newIntentID() string {
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return fmt.Sprintf("intent-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("intent-%d-%s", time.Now().UnixNano(), hex.EncodeToString(random))
}

func newDrillID() string {
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return fmt.Sprintf("drill-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("drill-%d-%s", time.Now().UnixNano(), hex.EncodeToString(random))
}

func (e *Engine) prepareSubmission(ctx context.Context, key, kind string, payload any) (store.Submission, *model.BusinessError) {
	if key == "" {
		return store.Submission{}, model.NewError(model.CodeInvalidRequest, "idempotency_key is required", 422)
	}
	hash, err := hashPayload(kind, payload)
	if err != nil {
		return store.Submission{}, model.NewError(model.CodeInternal, "failed to hash request", 500)
	}
	submission, _, err := e.store.PrepareSubmission(ctx, key, kind, hash, store.Now())
	if errors.Is(err, store.ErrConflictingRequest) {
		return store.Submission{}, model.NewError(model.CodeConflictingRequest, "idempotency key was already used with different request content", 409).
			WithContext("idempotency_key", key)
	}
	if err != nil {
		return store.Submission{}, model.NewError(model.CodeInternal, "failed to prepare idempotent submission", 500).
			WithContext("cause", err.Error())
	}
	return submission, nil
}

func (e *Engine) beginIntent(ctx context.Context, submissionKey, kind string, baseVersion, targetVersion int64, payload any, options CommitOptions) (string, *model.BusinessError) {
	intentID := newIntentID()
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", model.NewError(model.CodeInternal, "failed to encode intent payload", 500)
	}
	if err := e.store.InsertIntent(ctx, store.Intent{
		ID: intentID, SubmissionKey: submissionKey, Kind: kind,
		BaseVersion: baseVersion, TargetVersion: targetVersion,
		PayloadJSON: payloadJSON, Phase: "awaiting_atomic_commit", State: "pending", CreatedAt: store.Now(),
	}); err != nil {
		return "", model.NewError(model.CodeInternal, "failed to persist intent", 500).WithContext("cause", err.Error())
	}
	if e.testHooks && options.HoldAfterIntent > 0 {
		time.Sleep(options.HoldAfterIntent)
	}
	return intentID, nil
}

func (e *Engine) rejectOperation(ctx context.Context, submissionKey, intentID string, status int, businessError *model.BusinessError) *model.BusinessError {
	record := storedBusinessError{
		Code: businessError.Code, Message: businessError.Message,
		Context: businessError.Context, Partial: businessError.Partial,
	}
	errorJSON, _ := json.Marshal(record)
	now := store.Now()
	if intentID != "" {
		if err := e.store.RejectIntent(ctx, intentID, string(errorJSON), now); err != nil {
			return model.NewError(model.CodeInternal, "failed to finalize rejected intent", 500).WithContext("cause", err.Error())
		}
	}
	if err := e.store.RejectSubmission(ctx, submissionKey, status, errorJSON, now); err != nil {
		return model.NewError(model.CodeInternal, "failed to finalize rejected submission", 500).WithContext("cause", err.Error())
	}
	return businessError
}

func (e *Engine) replayError(submission store.Submission) *model.BusinessError {
	var stored storedBusinessError
	if err := json.Unmarshal(submission.ErrorJSON, &stored); err != nil {
		return model.NewError(model.CodeInternal, "stored idempotent error is unreadable", 500)
	}
	status := submission.HTTPStatus
	if status == 0 {
		status = 409
	}
	return &model.BusinessError{Code: stored.Code, Message: stored.Message, Context: stored.Context, Status: status, Partial: stored.Partial}
}

func (e *Engine) ListIntents(ctx context.Context, state string, limit int) ([]store.IntentSummary, error) {
	return e.store.ListIntents(ctx, state, limit)
}

func (e *Engine) ListRecoveryReports(ctx context.Context, limit int) ([]store.RecoveryReport, error) {
	return e.store.ListRecoveryReports(ctx, limit)
}

func (e *Engine) Lineage(ctx context.Context, version int64) ([]store.LineageRef, error) {
	if version == 0 {
		version = e.current().snapshot.Version
	}
	return e.store.ListLineageRefs(ctx, version)
}

func (e *Engine) Rebuild(ctx context.Context, version int64) (json.RawMessage, error) {
	if version == 0 {
		version = e.current().snapshot.Version
	}
	return e.store.GetRebuild(ctx, version)
}

func (e *Engine) ListDrills(ctx context.Context, limit int) ([]store.DrillRecord, error) {
	return e.store.ListDrills(ctx, limit)
}

func (e *Engine) GetDrill(ctx context.Context, id string) (store.DrillRecord, error) {
	return e.store.GetDrill(ctx, id)
}

func (e *Engine) SnapshotCount(ctx context.Context) (int64, error) {
	return e.store.SnapshotCount(ctx)
}

func (e *Engine) ListSnapshots(ctx context.Context, limit int) ([]store.SnapshotSummary, error) {
	return e.store.ListSnapshots(ctx, limit)
}

func (e *Engine) RefCount() int {
	return len(e.current().refs)
}

func (e *Engine) specRefsChanged(previous []store.RefRecord, changed []spec.Ref) []string {
	return invalidatedViews(previous, changed)
}

func (e *Engine) conflict(err error) *model.BusinessError {
	if errors.Is(err, store.ErrVersionConflict) {
		return model.NewError(model.CodeStaleBase, "committed version changed before atomic publication", 409)
	}
	return model.NewError(model.CodeInternal, "atomic version commit failed", 500).WithContext("cause", err.Error())
}

// PollReservations exposes only operational reservation state. It never exposes a
// component as recovered: recovery success is published only by CompleteDrill.
func (e *Engine) PollReservations() []ReservationView {
	return e.reservations.Snapshot()
}

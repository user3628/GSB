// Package engine coordinates observation editing, imports, deterministic merge,
// state derivation and durable batch submission.
package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"replicationlag/internal/apperr"
	"replicationlag/internal/compat"
	"replicationlag/internal/domain"
	"replicationlag/internal/format"
	"replicationlag/internal/store"
)

type Engine struct {
	store    *store.Store
	matrix   compat.Matrix
	policyMu sync.RWMutex
	policy   domain.Policy
	now      func() int64
}

func New(database *store.Store, matrix compat.Matrix, policy domain.Policy) (*Engine, error) {
	if database == nil {
		return nil, fmt.Errorf("store is required")
	}
	if err := policy.Validate(); err != nil {
		return nil, fmt.Errorf("invalid policy: %w", err)
	}
	return &Engine{
		store:  database,
		matrix: matrix,
		policy: policy,
		now:    func() int64 { return time.Now().UnixMilli() },
	}, nil
}

func (e *Engine) Store() *store.Store { return e.store }

func (e *Engine) Policy() domain.Policy {
	e.policyMu.RLock()
	defer e.policyMu.RUnlock()
	return e.policy
}

func (e *Engine) SetPolicy(policy domain.Policy) error {
	if err := policy.Validate(); err != nil {
		return apperr.New(apperr.CodeBadRequest, "invalid lag policy", map[string]any{"detail": err.Error()})
	}
	e.policyMu.Lock()
	e.policy = policy
	e.policyMu.Unlock()
	return nil
}

type ImportRequest struct {
	BatchID     string
	BaseVersion *int64
	Format      string
	Data        []byte
}

type ImportOutcome struct {
	Record              int    `json:"record"`
	ReplicaID           string `json:"replica_id"`
	SampledAtMS         int64  `json:"sampled_at_ms"`
	Outcome             string `json:"outcome"`
	Reason              string `json:"reason"`
	ExistingFormat      int    `json:"existing_format_version,omitempty"`
	IncomingFormat      int    `json:"incoming_format_version"`
	ExistingIntervals   int    `json:"existing_interval_count,omitempty"`
	IncomingIntervals   int    `json:"incoming_interval_count"`
	ExistingLastWriteMS int64  `json:"existing_last_write_ms,omitempty"`
	IncomingLastWriteMS int64  `json:"incoming_last_write_ms"`
}

func (e *Engine) Import(ctx context.Context, request ImportRequest) (store.Result, error) {
	batchID := request.BatchID
	if batchID == "" {
		batchID = newBatchID()
	}
	hash := hashRequest("import", request.BaseVersion, []byte(strings.ToLower(request.Format)), request.Data)
	result, err := e.store.Submit(ctx, store.WriteRequest{
		BatchID: batchID, Operation: "import", RequestHash: hash, BaseVersion: request.BaseVersion,
	}, func(current []domain.Observation, currentVersion int64) (store.Mutation, error) {
		detectedFormat, detectedVersion, detectErr := format.Detect(request.Data)
		if detectErr != nil {
			return store.Mutation{}, detectErr
		}
		if request.Format != "" && !strings.EqualFold(request.Format, "auto") && !strings.EqualFold(request.Format, detectedFormat) {
			return store.Mutation{}, apperr.New(apperr.CodeFormatCorrupt, "declared input format does not match detected format", map[string]any{
				"declared_format": request.Format, "detected_format": detectedFormat, "detected_version": detectedVersion,
			})
		}
		parsed, parseErr := format.Import(request.Data, e.matrix, e.now())
		if parseErr != nil {
			return store.Mutation{}, parseErr
		}
		merged, outcomes, report, resolveErr := e.resolveImport(current, parsed.Observations)
		if resolveErr != nil {
			return store.Mutation{}, resolveErr
		}
		response := map[string]any{
			"operation":         "import",
			"batch_id":          batchID,
			"status":            "committed",
			"format":            parsed.Format,
			"format_version":    parsed.FormatVersion,
			"compatibility":     parsed.Decision,
			"degradations":      parsed.Degradations,
			"outcomes":          outcomes,
			"observation_count": len(merged),
			"report":            report,
		}
		return store.Mutation{Observations: merged, Report: report, Response: response}, nil
	})
	return result, err
}

func (e *Engine) resolveImport(current, incoming []domain.Observation) ([]domain.Observation, []ImportOutcome, domain.MergeReport, error) {
	byKey := make(map[string]domain.Observation, len(current)+len(incoming))
	for _, observation := range current {
		byKey[observationKey(observation)] = observation
	}
	seenIncoming := map[string]domain.Observation{}
	outcomes := make([]ImportOutcome, 0, len(incoming))
	for index, observation := range incoming {
		key := observationKey(observation)
		if previous, exists := seenIncoming[key]; exists {
			if previous.ContentFingerprint() != observation.ContentFingerprint() {
				return nil, nil, domain.MergeReport{}, apperr.New(apperr.CodeObservationConflict, "import batch contains conflicting observations for the same sample time", map[string]any{
					"replica_id": observation.ReplicaID, "sampled_at_ms": observation.SampledAtMS,
					"records": []int{index, index + 1},
				})
			}
			outcomes = append(outcomes, outcomeFor(index+1, observation, "SKIP", "identical duplicate within import batch", nil))
			continue
		}
		seenIncoming[key] = observation
		existing, exists := byKey[key]
		if !exists {
			byKey[key] = observation
			outcomes = append(outcomes, outcomeFor(index+1, observation, "OVERWRITE", "new observation accepted", nil))
			continue
		}
		if existing.ContentFingerprint() == observation.ContentFingerprint() {
			outcomes = append(outcomes, outcomeFor(index+1, observation, "SKIP", "identical observation already exists", &existing))
			continue
		}
		incomingWins, reject := compareImportCandidates(existing, observation)
		outcome := ImportOutcome{
			Record: index + 1, ReplicaID: observation.ReplicaID, SampledAtMS: observation.SampledAtMS,
			ExistingFormat: existing.FormatVersion, IncomingFormat: observation.FormatVersion,
			ExistingIntervals: existing.GTID.IntervalCount(), IncomingIntervals: observation.GTID.IntervalCount(),
			ExistingLastWriteMS: existing.LastWriteMS, IncomingLastWriteMS: observation.LastWriteMS,
		}
		switch {
		case reject:
			outcome.Outcome = "REJECT"
			outcome.Reason = "all deterministic tie-breakers are equal but content differs"
			outcomes = append(outcomes, outcome)
			return nil, outcomes, domain.MergeReport{}, apperr.New(apperr.CodeObservationConflict, "import conflict cannot be resolved deterministically", map[string]any{
				"replica_id": observation.ReplicaID, "sampled_at_ms": observation.SampledAtMS,
				"outcomes": outcomes,
			})
		case incomingWins:
			outcome.Outcome = "OVERWRITE"
			outcome.Reason = "incoming wins by format_version, interval size, then last_write_ms"
			byKey[key] = observation
		default:
			outcome.Outcome = "SKIP"
			outcome.Reason = "existing wins by format_version, interval size, then last_write_ms"
		}
		outcomes = append(outcomes, outcome)
	}
	merged := make([]domain.Observation, 0, len(byKey))
	for _, observation := range byKey {
		merged = append(merged, observation)
	}
	merged = domain.SortObservations(merged)
	report, err := domain.BuildSequences(merged, e.Policy())
	if err != nil {
		return nil, outcomes, domain.MergeReport{}, err
	}
	return merged, outcomes, report, nil
}

func compareImportCandidates(existing, incoming domain.Observation) (incomingWins bool, reject bool) {
	if incoming.FormatVersion != existing.FormatVersion {
		return incoming.FormatVersion > existing.FormatVersion, false
	}
	incomingSize := incoming.GTID.Cardinality()
	existingSize := existing.GTID.Cardinality()
	if incomingSize != existingSize {
		return incomingSize > existingSize, false
	}
	if incoming.LastWriteMS != existing.LastWriteMS {
		return incoming.LastWriteMS > existing.LastWriteMS, false
	}
	return false, true
}

func outcomeFor(record int, observation domain.Observation, outcome, reason string, existing *domain.Observation) ImportOutcome {
	result := ImportOutcome{
		Record: record, ReplicaID: observation.ReplicaID, SampledAtMS: observation.SampledAtMS,
		Outcome: outcome, Reason: reason, IncomingFormat: observation.FormatVersion,
		IncomingIntervals: observation.GTID.IntervalCount(), IncomingLastWriteMS: observation.LastWriteMS,
	}
	if existing != nil {
		result.ExistingFormat = existing.FormatVersion
		result.ExistingIntervals = existing.GTID.IntervalCount()
		result.ExistingLastWriteMS = existing.LastWriteMS
	}
	return result
}

type ObservationInput struct {
	ReplicaID        string   `json:"replica_id"`
	SampledAtMS      int64    `json:"sampled_at_ms"`
	GTID             string   `json:"gtid"`
	GTIDSegments     []string `json:"gtid_segments"`
	PositionName     string   `json:"position_name"`
	ExecutedPosition string   `json:"executed_position"`
	LagMS            int64    `json:"lag_ms"`
	ReportedVersion  string   `json:"reported_version"`
	FormatVersion    *int     `json:"format_version,omitempty"`
	LastWriteMS      *int64   `json:"last_write_ms,omitempty"`
	lagMissing       bool
}

// UnmarshalJSON tracks whether lag_ms was actually present. Zero is a valid
// lag value, so a missing field cannot be distinguished after decoding into an
// int64 without this marker.
func (o *ObservationInput) UnmarshalJSON(data []byte) error {
	type alias ObservationInput
	var decoded alias
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	raw, present := fields["lag_ms"]
	*o = ObservationInput(decoded)
	o.lagMissing = !present || string(raw) == "null"
	return nil
}

func (e *Engine) NormalizeObservation(input ObservationInput, lastWriteMS int64) (domain.Observation, error) {
	if input.lagMissing {
		return domain.Observation{}, apperr.New(apperr.CodeObservationInvalid, "observation field is invalid", map[string]any{
			"replica_id": input.ReplicaID, "sampled_at_ms": input.SampledAtMS, "field": "lag_ms", "detail": "lag_ms is required",
		})
	}
	gtidText := input.GTID
	if len(input.GTIDSegments) > 0 {
		gtidText = strings.Join(input.GTIDSegments, ",")
	}
	gtid, gtidErr := domain.ParseGTID(gtidText)
	if gtidErr != nil {
		return domain.Observation{}, domain.GTIDInvalidError(input.ReplicaID, input.SampledAtMS, "gtid", gtidErr)
	}
	position := input.PositionName
	if position == "" {
		position = input.ExecutedPosition
	}
	position, positionErr := domain.NormalizePosition(position, domain.PositionPolicy{})
	if positionErr != nil {
		return domain.Observation{}, apperr.New(apperr.CodeObservationInvalid, "observation field is invalid", map[string]any{
			"replica_id": input.ReplicaID, "sampled_at_ms": input.SampledAtMS, "field": "position_name", "detail": positionErr.Error(),
		})
	}
	version, versionErr := domain.ParseVersion(input.ReportedVersion)
	if versionErr != nil {
		return domain.Observation{}, apperr.New(apperr.CodeObservationInvalid, "observation field is invalid", map[string]any{
			"replica_id": input.ReplicaID, "sampled_at_ms": input.SampledAtMS, "field": "reported_version", "detail": versionErr.Error(),
		})
	}
	formatVersion := 1
	if input.FormatVersion != nil {
		formatVersion = *input.FormatVersion
	}
	if input.LastWriteMS != nil {
		lastWriteMS = *input.LastWriteMS
	}
	if lastWriteMS == 0 {
		lastWriteMS = e.now()
	}
	observation := domain.Observation{
		ReplicaID: input.ReplicaID, SampledAtMS: input.SampledAtMS, GTID: gtid.Normalized(),
		PositionName: position, LagMS: input.LagMS, ReportedVersion: version,
		FormatVersion: formatVersion, LastWriteMS: lastWriteMS,
	}
	if err := observation.Validate(); err != nil {
		return domain.Observation{}, err
	}
	return observation, nil
}

type UpsertRequest struct {
	BatchID     string
	BaseVersion *int64
	Observation ObservationInput
}

func (e *Engine) UpsertObservation(ctx context.Context, request UpsertRequest) (store.Result, error) {
	batchID := request.BatchID
	if batchID == "" {
		batchID = newBatchID()
	}
	payload, _ := json.Marshal(request.Observation)
	hash := hashRequest("edit_observation", request.BaseVersion, payload)
	return e.store.Submit(ctx, store.WriteRequest{
		BatchID: batchID, Operation: "edit_observation", RequestHash: hash, BaseVersion: request.BaseVersion,
	}, func(current []domain.Observation, currentVersion int64) (store.Mutation, error) {
		observation, err := e.NormalizeObservation(request.Observation, e.now())
		if err != nil {
			return store.Mutation{}, err
		}
		replaced := false
		next := make([]domain.Observation, 0, len(current)+1)
		for _, existing := range current {
			if existing.ReplicaID == observation.ReplicaID && existing.SampledAtMS == observation.SampledAtMS {
				next = append(next, observation)
				replaced = true
			} else {
				next = append(next, existing)
			}
		}
		if !replaced {
			next = append(next, observation)
		}
		next = domain.SortObservations(next)
		report, err := domain.BuildSequences(next, e.Policy())
		if err != nil {
			return store.Mutation{}, err
		}
		action := "added"
		if replaced {
			action = "replaced"
		}
		return store.Mutation{Observations: next, Report: report, Response: map[string]any{
			"operation": "edit_observation", "batch_id": batchID, "status": "committed",
			"edit_result": action, "observation": observation.View(), "report": report,
		}}, nil
	})
}

type DeleteRequest struct {
	BatchID     string
	BaseVersion *int64
	ReplicaID   string
	SampledAtMS int64
}

func (e *Engine) DeleteObservation(ctx context.Context, request DeleteRequest) (store.Result, error) {
	batchID := request.BatchID
	if batchID == "" {
		batchID = newBatchID()
	}
	hash := hashRequest("delete_observation", request.BaseVersion, []byte(fmt.Sprintf("%s/%d", request.ReplicaID, request.SampledAtMS)))
	return e.store.Submit(ctx, store.WriteRequest{
		BatchID: batchID, Operation: "delete_observation", RequestHash: hash, BaseVersion: request.BaseVersion,
	}, func(current []domain.Observation, currentVersion int64) (store.Mutation, error) {
		next := make([]domain.Observation, 0, len(current))
		deleted := false
		for _, observation := range current {
			if observation.ReplicaID == request.ReplicaID && observation.SampledAtMS == request.SampledAtMS {
				deleted = true
				continue
			}
			next = append(next, observation)
		}
		if !deleted {
			return store.Mutation{}, apperr.New(apperr.CodeNotFound, "observation not found", map[string]any{
				"replica_id": request.ReplicaID, "sampled_at_ms": request.SampledAtMS,
			})
		}
		report, err := domain.BuildSequences(next, e.Policy())
		if err != nil {
			return store.Mutation{}, err
		}
		return store.Mutation{Observations: next, Report: report, Response: map[string]any{
			"operation": "delete_observation", "batch_id": batchID, "status": "committed", "report": report,
		}}, nil
	})
}

type MergeRequest struct {
	BatchID     string
	BaseVersion *int64
}

func (e *Engine) Merge(ctx context.Context, request MergeRequest) (store.Result, error) {
	batchID := request.BatchID
	if batchID == "" {
		batchID = newBatchID()
	}
	hash := hashRequest("merge", request.BaseVersion)
	return e.store.Submit(ctx, store.WriteRequest{
		BatchID: batchID, Operation: "merge", RequestHash: hash, BaseVersion: request.BaseVersion,
	}, func(current []domain.Observation, currentVersion int64) (store.Mutation, error) {
		report, err := domain.BuildSequences(current, e.Policy())
		if err != nil {
			return store.Mutation{}, err
		}
		return store.Mutation{Observations: current, Report: report, Response: map[string]any{
			"operation": "merge", "batch_id": batchID, "status": "committed",
			"observation_count": len(current), "report": report,
		}}, nil
	})
}

func hashRequest(operation string, baseVersion *int64, parts ...[]byte) string {
	hash := sha256.New()
	hash.Write([]byte(operation))
	hash.Write([]byte{0})
	if baseVersion == nil {
		hash.Write([]byte("null"))
	} else {
		hash.Write([]byte(fmt.Sprintf("%d", *baseVersion)))
	}
	for _, part := range parts {
		hash.Write([]byte{0})
		hash.Write(part)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func newBatchID() string {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err == nil {
		return "b-" + hex.EncodeToString(buffer)
	}
	return fmt.Sprintf("b-%d", time.Now().UnixNano())
}

func observationKey(observation domain.Observation) string {
	return fmt.Sprintf("%s\x00%d", observation.ReplicaID, observation.SampledAtMS)
}

func (e *Engine) ListObservations(ctx context.Context) ([]domain.Observation, int64, error) {
	return e.store.ObservationSet(ctx)
}

func (e *Engine) GetSequence(ctx context.Context, replicaID string) (domain.EvolutionSequence, int64, error) {
	return e.store.GetSequence(ctx, replicaID)
}

func (e *Engine) ListSequences(ctx context.Context) ([]domain.EvolutionSequence, int64, error) {
	return e.store.ListSequences(ctx)
}

func (e *Engine) GetState(ctx context.Context, replicaID string) (store.StateView, int64, error) {
	return e.store.GetState(ctx, replicaID)
}

func (e *Engine) RecoveryReport(ctx context.Context) (store.RecoveryReport, int64, error) {
	return e.store.RecoveryReport(ctx)
}

func (e *Engine) Export(ctx context.Context, formatName string) ([]byte, int64, string, error) {
	observations, version, err := e.store.ObservationSet(ctx)
	if err != nil {
		return nil, 0, "", err
	}
	switch strings.ToLower(formatName) {
	case "", "jsonl", "json":
		data, err := format.EncodeJSONL(observations)
		return data, version, format.JSONLFormat, err
	case "binary", "bin", "compact":
		data, err := format.EncodeBinary(observations)
		return data, version, format.BinaryFormat, err
	default:
		return nil, version, "", apperr.New(apperr.CodeBadRequest, "unsupported export format", map[string]any{"format": formatName})
	}
}

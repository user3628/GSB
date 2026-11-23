package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"example.com/logicplan/editor/internal/bizerr"
	"example.com/logicplan/editor/internal/codec"
	"example.com/logicplan/editor/internal/fault"
	"example.com/logicplan/editor/internal/model"
	"example.com/logicplan/editor/internal/planstate"
	"example.com/logicplan/editor/internal/store"
)

type Engine struct {
	store    *store.Store
	state    atomic.Pointer[planstate.State]
	writeMu  sync.Mutex
	fault    *fault.Controller
	recovery store.RecoveryReport
}

type ImportResult struct {
	Imported      bool   `json:"imported"`
	Replayed      bool   `json:"replayed"`
	Version       uint64 `json:"version"`
	SnapshotSeq   uint64 `json:"snapshot_seq"`
	Format        string `json:"format"`
	FormatVersion int    `json:"format_version"`
	Decision      string `json:"decision"`
	Degradations  int    `json:"degradation_count"`
	Root          string `json:"root"`
	TreeHash      string `json:"tree_hash"`
}

type PatchRequest struct {
	CommitID    string                `json:"commit_id"`
	BaseVersion uint64                `json:"base_version"`
	Operations  []planstate.Operation `json:"operations"`
}

type PatchResult struct {
	Applied           bool     `json:"applied"`
	Replayed          bool     `json:"replayed"`
	CommitID          string   `json:"commit_id"`
	BaseVersion       uint64   `json:"base_version"`
	Version           uint64   `json:"version"`
	Root              string   `json:"root"`
	TreeHash          string   `json:"tree_hash"`
	AffectedNodes     []string `json:"affected_nodes"`
	RebuiltStructures []string `json:"rebuilt_structures"`
}

func Open(ctx context.Context, path string, controller *fault.Controller) (*Engine, error) {
	st, err := store.Open(path)
	if err != nil {
		return nil, bizerr.New(bizerr.CodeIO, "open sqlite store", map[string]any{"cause": err.Error(), "path": path})
	}
	e := &Engine{store: st, fault: controller}
	if e.fault == nil {
		e.fault = fault.New()
	}
	report, err := st.Recover(ctx)
	if err != nil {
		_ = st.Close()
		return nil, bizerr.New(bizerr.CodeIO, "recover persisted state", map[string]any{"cause": err.Error()})
	}
	e.recovery = report
	version, err := st.CurrentVersion(ctx)
	if err != nil {
		_ = st.Close()
		return nil, bizerr.New(bizerr.CodeIO, "read current snapshot version", map[string]any{"cause": err.Error()})
	}
	if version != 0 {
		state, err := st.LoadState(ctx, version)
		if err != nil {
			_ = st.Close()
			return nil, bizerr.New(bizerr.CodeInternal, "load committed snapshot", map[string]any{"cause": err.Error(), "version": version})
		}
		if state == nil {
			_ = st.Close()
			return nil, bizerr.New(bizerr.CodeInternal, "current snapshot version is missing", map[string]any{"version": version})
		}
		if err := planstate.DerivedInvariantError(state); err != nil {
			_ = st.Close()
			return nil, bizerr.New(bizerr.CodeInternal, "persisted derived structures are inconsistent", map[string]any{"cause": err.Error(), "version": version})
		}
		e.state.Store(state)
	}
	return e, nil
}

func (e *Engine) Close() error { return e.store.Close() }

func (e *Engine) Current() *planstate.State { return e.state.Load() }

func (e *Engine) Recovery() store.RecoveryReport { return e.recovery }

func (e *Engine) Faults() *fault.Controller { return e.fault }

func (e *Engine) Import(ctx context.Context, data []byte, requestID string) (ImportResult, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	requestHash := hashBytes("import\x00", data)
	if requestID != "" {
		if replay, err := e.replayImport(ctx, requestID, requestHash); err != nil || replay != nil {
			return valueOrZero(replay), err
		}
	}

	header, err := codec.Inspect(data)
	if err != nil {
		return ImportResult{}, err
	}
	if len(header.RequiredFeatures) > 0 {
		return ImportResult{}, codec.RejectUnknownRequired(header.Format, header.FormatVersion, header.RequiredFeatures)
	}
	compat, err := codec.Negotiate(header.Format, header.FormatVersion)
	if err != nil {
		if be, ok := bizerr.As(err); ok {
			be.Context["snapshot_seq"] = header.SnapshotSeq
			be.Context["generated_at"] = header.GeneratedAt
		}
		return ImportResult{}, err
	}
	if compat.Decision == codec.DecisionReject {
		return ImportResult{}, bizerr.New(bizerr.CodeFormatUnsupported,
			"snapshot format version was rejected by the compatibility matrix",
			map[string]any{"format": header.Format, "format_version": header.FormatVersion, "snapshot_seq": header.SnapshotSeq, "reason": compat.Reason})
	}
	var snapshot *model.Snapshot
	switch header.Format {
	case codec.FormatBinary:
		snapshot, err = codec.DecodeBinary(data)
	case codec.FormatJSONL:
		snapshot, err = codec.DecodeJSONL(data)
	default:
		err = bizerr.New(bizerr.CodeFormatCorrupt, "unsupported input format", map[string]any{"format": header.Format})
	}
	if err != nil {
		return ImportResult{}, err
	}
	if snapshot.SnapshotSeq != header.SnapshotSeq || snapshot.FormatVersion != header.FormatVersion {
		return ImportResult{}, bizerr.New(bizerr.CodeFormatCorrupt, "snapshot metadata changed between inspection and decoding", map[string]any{
			"inspected_snapshot_seq": header.SnapshotSeq, "decoded_snapshot_seq": snapshot.SnapshotSeq,
			"inspected_format_version": header.FormatVersion, "decoded_format_version": snapshot.FormatVersion,
		})
	}
	// Validate the entire batch before writing any intent or touching the current
	// snapshot. Derivation is deliberately performed only after the intent is durable.
	if _, err := model.Analyze(snapshot.Nodes); err != nil {
		return ImportResult{}, err
	}
	current := e.Current()
	if current != nil && snapshot.SnapshotSeq <= current.Version {
		return ImportResult{}, bizerr.New(bizerr.CodeSnapshotStale,
			"snapshot sequence is not greater than the currently committed version",
			map[string]any{"imported_snapshot_seq": snapshot.SnapshotSeq, "current_version": current.Version})
	}

	intentID := newID("intent")
	started := time.Now().UTC()
	intent := store.Intent{
		ID: intentID, Kind: "import", RequestHash: requestHash,
		BaseVersion: optionalVersion(current), Target: snapshot.SnapshotSeq,
		Detail: map[string]any{
			"format": header.Format, "format_version": header.FormatVersion,
			"decision": compat.Decision, "request_id": requestID,
		},
		StartedAt: started,
	}
	if err := e.store.InsertIntent(ctx, intent); err != nil {
		return ImportResult{}, bizerr.New(bizerr.CodeIO, "persist import intent", map[string]any{"cause": err.Error(), "intent_id": intentID})
	}
	e.fault.Checkpoint(fault.ImportAfterIntent)
	e.fault.Checkpoint(fault.ImportDuringRebuild)
	state, err := planstate.NewFromSnapshot(snapshot, string(header.Format), string(compat.Decision), time.Now().UTC())
	if err != nil {
		e.abortIntent(intentID, err)
		return ImportResult{}, err
	}
	if err := planstate.DerivedInvariantError(state); err != nil {
		e.abortIntent(intentID, err)
		return ImportResult{}, bizerr.New(bizerr.CodeInternal, "rebuilt derived structures are inconsistent", map[string]any{"cause": err.Error()})
	}
	result := ImportResult{
		Imported: true, Version: state.Version, SnapshotSeq: state.Version,
		Format: state.Format, FormatVersion: state.FormatVersion, Decision: state.Decision,
		Degradations: len(state.Degradations), Root: state.Root, TreeHash: state.Derived.RootHash,
	}
	var commit *store.CommitRecord
	if requestID != "" {
		payload, _ := json.Marshal(result)
		commit = &store.CommitRecord{CommitID: requestID, Kind: "import", RequestHash: requestHash, Version: state.Version, ResultJSON: string(payload), CreatedAt: time.Now().UTC()}
	}
	if err := e.store.SaveVersion(ctx, state, intentID, commit); err != nil {
		e.abortIntent(intentID, err)
		return ImportResult{}, bizerr.New(bizerr.CodeIO, "commit imported snapshot", map[string]any{"cause": err.Error(), "target_version": state.Version})
	}
	e.fault.Checkpoint(fault.ImportAfterCommit)
	e.state.Store(state)
	return result, nil
}

func (e *Engine) ApplyPatch(ctx context.Context, patch PatchRequest) (PatchResult, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if patch.CommitID == "" {
		return PatchResult{}, bizerr.New(bizerr.CodeBadRequest, "incremental fragment requires commit_id", nil)
	}
	canonical, err := json.Marshal(patch)
	if err != nil {
		return PatchResult{}, bizerr.New(bizerr.CodeBadRequest, "cannot canonicalize incremental fragment", map[string]any{"cause": err.Error()})
	}
	requestHash := hashBytes("patch\x00", canonical)
	if replay, err := e.replayPatch(ctx, patch.CommitID, requestHash); err != nil || replay != nil {
		return valueOrZeroPatch(replay), err
	}
	current := e.Current()
	if current == nil {
		return PatchResult{}, bizerr.New(bizerr.CodeSnapshotNotFound, "no committed snapshot is loaded", nil)
	}
	if patch.BaseVersion != current.Version {
		return PatchResult{}, bizerr.New(bizerr.CodeStaleBase, "incremental fragment base version is not current",
			map[string]any{"base_version": patch.BaseVersion, "current_version": current.Version, "commit_id": patch.CommitID})
	}
	prepared, err := planstate.PrepareEdit(current, patch.Operations)
	if err != nil {
		return PatchResult{}, err
	}
	targetVersion := current.Version + 1
	intentID := newID("intent")
	intent := store.Intent{
		ID: intentID, Kind: "patch", RequestHash: requestHash,
		BaseVersion: optionalVersion(current), Target: targetVersion,
		Detail:    map[string]any{"commit_id": patch.CommitID, "affected_nodes": prepared.Affected},
		StartedAt: time.Now().UTC(),
	}
	if err := e.store.InsertIntent(ctx, intent); err != nil {
		return PatchResult{}, bizerr.New(bizerr.CodeIO, "persist incremental intent", map[string]any{"cause": err.Error(), "intent_id": intentID})
	}
	e.fault.Checkpoint(fault.PatchAfterIntent)
	e.fault.Checkpoint(fault.PatchDuringRebuild)
	state, err := prepared.BuildState(current, targetVersion, time.Now().UTC())
	if err != nil {
		e.abortIntent(intentID, err)
		return PatchResult{}, err
	}
	if err := planstate.DerivedInvariantError(state); err != nil {
		e.abortIntent(intentID, err)
		return PatchResult{}, bizerr.New(bizerr.CodeInternal, "rebuilt derived structures are inconsistent", map[string]any{"cause": err.Error()})
	}
	result := PatchResult{
		Applied: true, CommitID: patch.CommitID, BaseVersion: patch.BaseVersion,
		Version: state.Version, Root: state.Root, TreeHash: state.Derived.RootHash,
		AffectedNodes: prepared.Affected, RebuiltStructures: prepared.Kinds.Names(),
	}
	payload, _ := json.Marshal(result)
	commit := &store.CommitRecord{
		CommitID: patch.CommitID, Kind: "patch", RequestHash: requestHash,
		Version: state.Version, ResultJSON: string(payload), CreatedAt: time.Now().UTC(),
	}
	if err := e.store.SaveVersion(ctx, state, intentID, commit); err != nil {
		e.abortIntent(intentID, err)
		return PatchResult{}, bizerr.New(bizerr.CodeIO, "commit incremental fragment", map[string]any{
			"cause": err.Error(), "target_version": targetVersion, "commit_id": patch.CommitID,
		})
	}
	e.fault.Checkpoint(fault.PatchAfterCommit)
	e.state.Store(state)
	return result, nil
}

func (e *Engine) replayImport(ctx context.Context, requestID, requestHash string) (*ImportResult, error) {
	record, err := e.store.FindCommit(ctx, requestID)
	if err != nil {
		return nil, bizerr.New(bizerr.CodeIO, "look up idempotent import", map[string]any{"cause": err.Error(), "request_id": requestID})
	}
	if record == nil {
		return nil, nil
	}
	if record.RequestHash != requestHash || record.Kind != "import" {
		return nil, bizerr.New(bizerr.CodeConflictingRequest, "request id was already used with different content",
			map[string]any{"request_id": requestID, "committed_version": record.Version})
	}
	var result ImportResult
	if err := json.Unmarshal([]byte(record.ResultJSON), &result); err != nil {
		return nil, bizerr.New(bizerr.CodeInternal, "persisted idempotent import result is unreadable", map[string]any{"cause": err.Error()})
	}
	result.Replayed = true
	return &result, nil
}

func (e *Engine) replayPatch(ctx context.Context, commitID, requestHash string) (*PatchResult, error) {
	record, err := e.store.FindCommit(ctx, commitID)
	if err != nil {
		return nil, bizerr.New(bizerr.CodeIO, "look up idempotent commit", map[string]any{"cause": err.Error(), "commit_id": commitID})
	}
	if record == nil {
		return nil, nil
	}
	if record.RequestHash != requestHash || record.Kind != "patch" {
		return nil, bizerr.New(bizerr.CodeConflictingRequest, "commit id was already used with different content",
			map[string]any{"commit_id": commitID, "committed_version": record.Version})
	}
	var result PatchResult
	if err := json.Unmarshal([]byte(record.ResultJSON), &result); err != nil {
		return nil, bizerr.New(bizerr.CodeInternal, "persisted idempotent patch result is unreadable", map[string]any{"cause": err.Error()})
	}
	result.Replayed = true
	return &result, nil
}

func (e *Engine) abortIntent(intentID string, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = e.store.FinishIntent(ctx, intentID, "aborted", cause.Error())
}

func hashBytes(prefix string, data []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte(prefix))
	_, _ = h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

func newID(prefix string) string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(raw[:])
}

func optionalVersion(state *planstate.State) *uint64 {
	if state == nil {
		return nil
	}
	version := state.Version
	return &version
}

func valueOrZero[T any](value *T) T {
	if value == nil {
		var zero T
		return zero
	}
	return *value
}

func valueOrZeroPatch(value *PatchResult) PatchResult {
	if value == nil {
		return PatchResult{}
	}
	return *value
}

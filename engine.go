package configimpact

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Engine exposes one serialized writer and concurrent version-bound readers.
type Engine struct {
	db      *sql.DB
	options Options

	writerMu sync.Mutex
	closeMu  sync.Mutex
	closed   bool

	recovery RecoveryReport

	// testHookAfterIntent is intentionally unexported. Tests use it to prove
	// that a process death after the durable intent leaves no partial snapshot.
	testHookAfterIntent func()
}

// Open opens or creates the local SQLite store, runs crash recovery, and
// returns a ready engine.
func Open(ctx context.Context, path string, options Options) (*Engine, error) {
	db, err := openDatabase(path)
	if err != nil {
		return nil, storageError(err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, storageError(err)
	}
	if err := migrate(ctx, db); err != nil {
		db.Close()
		return nil, storageError(err)
	}
	report, err := recoverDatabase(ctx, db)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Engine{db: db, options: normalizeOptions(options), recovery: report}, nil
}

// Close releases SQLite resources. It is safe to call more than once.
func (e *Engine) Close() error {
	e.closeMu.Lock()
	defer e.closeMu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	return e.db.Close()
}

// RecoveryReport returns the facts observed during startup recovery.
func (e *Engine) RecoveryReport() RecoveryReport {
	report := e.recovery
	report.AbortedIntents = append([]RecoveredIntent(nil), e.recovery.AbortedIntents...)
	report.CompleteDocumentVersions = append([]int64(nil), e.recovery.CompleteDocumentVersions...)
	return report
}

// CurrentVersion returns the last atomically published document version.
func (e *Engine) CurrentVersion(ctx context.Context) (int64, error) {
	if err := e.checkOpen(); err != nil {
		return 0, err
	}
	version, err := currentVersion(ctx, e.db)
	if err != nil {
		return 0, storageError(err)
	}
	return version, nil
}

// LoadJSON validates and atomically publishes a JSON document.
func (e *Engine) LoadJSON(ctx context.Context, payload []byte) (LoadResult, error) {
	doc, err := DecodeDocument(payload)
	if err != nil {
		return LoadResult{}, err
	}
	return e.LoadDocument(ctx, doc)
}

// LoadDocument atomically replaces the active document snapshot. Validation
// happens before the writer lock so a failed load cannot disturb old state.
func (e *Engine) LoadDocument(ctx context.Context, input *Document) (LoadResult, error) {
	if err := e.checkOpen(); err != nil {
		return LoadResult{}, err
	}
	cloned, err := cloneDocument(input)
	if err != nil {
		return LoadResult{}, invalidDocument("clone document failed: %v", err)
	}
	doc, err := validateDocument(cloned)
	if err != nil {
		return LoadResult{}, err
	}
	hash, payload, err := documentHash(doc)
	if err != nil {
		return LoadResult{}, invalidDocument("hash document failed: %v", err)
	}

	e.writerMu.Lock()
	defer e.writerMu.Unlock()

	baseVersion, err := currentVersion(ctx, e.db)
	if err != nil {
		return LoadResult{}, storageError(err)
	}
	if baseVersion > 0 {
		currentDoc, currentHash, _, err := loadDocumentAt(ctx, e.db, baseVersion)
		if err != nil {
			return LoadResult{}, storageError(err)
		}
		if currentDoc.Version == doc.Version && currentHash == hash {
			return LoadResult{
				Version:         doc.Version,
				PreviousVersion: baseVersion,
				Changed:         false,
				ComponentCount:  len(currentDoc.Components),
				DocumentHash:    currentHash,
			}, nil
		}
		if doc.Version <= baseVersion {
			return LoadResult{}, withDetails(invalidDocument("document version %d must be greater than current version %d", doc.Version, baseVersion), map[string]any{
				"document_version": doc.Version,
				"current_version":  baseVersion,
			})
		}
	}

	intentID, err := recordIntent(ctx, e.db, "load", "", baseVersion, doc.Version, hash, `{"phase":"document_rebuild"}`)
	if err != nil {
		return LoadResult{}, storageError(err)
	}
	if e.testHookAfterIntent != nil {
		e.testHookAfterIntent()
	}

	materializations := make([]MaterializedConfig, 0, len(doc.Components))
	impactEntries := make([]AffectedComponent, 0, len(doc.Components))
	ordered := sortedComponents(doc)
	for _, component := range ordered {
		config, err := materializeComponent(doc, component.ID, nil, component.Watches, e.options, true)
		if err != nil {
			e.abortIntent(ctx, intentID, "", "document materialization failed")
			return LoadResult{}, err
		}
		materializations = append(materializations, config)
		impactEntries = append(impactEntries, AffectedComponent{
			ComponentID:         component.ID,
			TriggerKeys:         []string{},
			CauseSeeds:          []string{},
			Downstream:          []string{},
			Path:                config.Decision.Path,
			Threshold:           config.Decision.Threshold,
			Ratio:               config.Decision.Ratio,
			EstimatedDurationNS: config.Decision.EstimatedDurationNS,
			ActualDurationNS:    config.Decision.ActualDurationNS,
		})
	}
	materializationMap := materializationMap(materializations)
	refs := buildReferences(doc, materializationMap)

	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		e.abortIntent(ctx, intentID, "", "begin load commit failed")
		return LoadResult{}, storageError(err)
	}
	defer tx.Rollback()
	if err := requireCurrentVersion(ctx, tx, baseVersion); err != nil {
		_ = tx.Rollback()
		e.abortIntent(ctx, intentID, "", "current version changed before load commit")
		return LoadResult{}, err
	}
	if err := persistSnapshotTx(ctx, tx, doc, payload, hash, materializations, impactEntries, refs); err != nil {
		_ = tx.Rollback()
		e.abortIntent(ctx, intentID, "", "load snapshot persistence failed")
		return LoadResult{}, storageError(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='complete', finished_at=?, detail_json=? WHERE id=?`, now, `{"phase":"snapshot_committed"}`, intentID); err != nil {
		_ = tx.Rollback()
		e.abortIntent(ctx, intentID, "", "load intent completion failed")
		return LoadResult{}, storageError(err)
	}
	if err := tx.Commit(); err != nil {
		e.abortIntent(ctx, intentID, "", "load commit result uncertain")
		return LoadResult{}, storageError(err)
	}
	return LoadResult{
		Version:         doc.Version,
		PreviousVersion: baseVersion,
		Changed:         true,
		ComponentCount:  len(doc.Components),
		DocumentHash:    hash,
	}, nil
}

// SubmitPatch validates a key-level patch, computes its impact frontier and
// atomically publishes materializations, impacts and references.
func (e *Engine) SubmitPatch(ctx context.Context, patch Patch) (PatchResult, error) {
	if err := e.checkOpen(); err != nil {
		return PatchResult{}, err
	}
	payloadHash, err := patchHash(&patch)
	if err != nil {
		return PatchResult{}, invalidPatch("hash patch failed: %v", err)
	}

	e.writerMu.Lock()
	defer e.writerMu.Unlock()

	if result, handled, err := e.lookupPatch(ctx, patch, payloadHash); handled || err != nil {
		return result, err
	}

	baseVersion, err := currentVersion(ctx, e.db)
	if err != nil {
		return PatchResult{}, storageError(err)
	}
	if baseVersion == 0 {
		return PatchResult{}, withDetails(businessError(ErrStaleBase, "no active document for patch base version %d", patch.BaseVersion), map[string]any{
			"base_version":    patch.BaseVersion,
			"current_version": 0,
		})
	}
	currentDoc, _, _, err := loadDocumentAt(ctx, e.db, baseVersion)
	if err != nil {
		return PatchResult{}, storageError(err)
	}
	if patch.BaseVersion != baseVersion {
		return PatchResult{}, withDetails(businessError(ErrStaleBase, "patch base version %d does not match current version %d", patch.BaseVersion, baseVersion), map[string]any{
			"base_version":    patch.BaseVersion,
			"current_version": baseVersion,
		})
	}

	nextDoc, err := ApplyPatch(currentDoc, patch)
	if err != nil {
		return PatchResult{}, err
	}
	targetVersion := nextDoc.Version
	intentID, err := recordIntent(ctx, e.db, "patch", patch.ID, baseVersion, targetVersion, payloadHash, `{"phase":"impact_rebuild"}`)
	if err != nil {
		return PatchResult{}, storageError(err)
	}
	if e.testHookAfterIntent != nil {
		e.testHookAfterIntent()
	}

	impact := computeImpact(currentDoc, nextDoc, patch.Changes)
	previous, err := loadMaterializations(ctx, e.db, baseVersion)
	if err != nil {
		e.abortIntent(ctx, intentID, patch.ID, "previous materializations unavailable")
		return PatchResult{}, storageError(err)
	}
	affected := make(map[string]AffectedComponent, len(impact.AffectedComponents))
	for _, entry := range impact.AffectedComponents {
		affected[entry.ComponentID] = entry
	}

	materializations := make([]MaterializedConfig, 0, len(nextDoc.Components))
	impactEntries := make([]AffectedComponent, 0, len(nextDoc.Components))
	for _, component := range sortedComponents(nextDoc) {
		entry, isAffected := affected[component.ID]
		var config MaterializedConfig
		if isAffected {
			config, err = materializeComponent(nextDoc, component.ID, materializationPointer(previous[component.ID]), entry.TriggerKeys, e.options, false)
			if err != nil {
				e.abortIntent(ctx, intentID, patch.ID, "component materialization failed")
				return PatchResult{}, err
			}
			entry.Path = config.Decision.Path
			entry.Threshold = config.Decision.Threshold
			entry.Ratio = config.Decision.Ratio
			entry.EstimatedDurationNS = config.Decision.EstimatedDurationNS
			entry.ActualDurationNS = config.Decision.ActualDurationNS
			affected[component.ID] = entry
		} else if prior, exists := previous[component.ID]; exists {
			config = reuseMaterialization(prior, nextDoc.Version, e.options)
			entry = AffectedComponent{
				ComponentID:         component.ID,
				TriggerKeys:         []string{},
				CauseSeeds:          []string{},
				Downstream:          []string{},
				Path:                config.Decision.Path,
				Threshold:           config.Decision.Threshold,
				Ratio:               config.Decision.Ratio,
				EstimatedDurationNS: 0,
				ActualDurationNS:    0,
			}
		} else {
			config, err = materializeComponent(nextDoc, component.ID, nil, component.Watches, e.options, true)
			if err != nil {
				e.abortIntent(ctx, intentID, patch.ID, "missing prior materialization recovery failed")
				return PatchResult{}, err
			}
			entry = AffectedComponent{
				ComponentID:         component.ID,
				TriggerKeys:         []string{},
				CauseSeeds:          []string{},
				Downstream:          []string{},
				Path:                config.Decision.Path,
				Threshold:           config.Decision.Threshold,
				Ratio:               config.Decision.Ratio,
				EstimatedDurationNS: config.Decision.EstimatedDurationNS,
				ActualDurationNS:    config.Decision.ActualDurationNS,
			}
		}
		materializations = append(materializations, config)
		impactEntries = append(impactEntries, entry)
	}
	updatedAffected := make([]AffectedComponent, 0, len(affected))
	for _, entry := range affected {
		updatedAffected = append(updatedAffected, entry)
	}
	sort.Slice(updatedAffected, func(i, j int) bool { return updatedAffected[i].ComponentID < updatedAffected[j].ComponentID })
	impact.AffectedComponents = updatedAffected

	nextHash, nextPayload, err := documentHash(nextDoc)
	if err != nil {
		e.abortIntent(ctx, intentID, patch.ID, "next document hash failed")
		return PatchResult{}, storageError(err)
	}
	result := PatchResult{
		PatchID:          patch.ID,
		BaseVersion:      baseVersion,
		DocumentVersion:  targetVersion,
		Impact:           impact,
		Materializations: materializations,
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		e.abortIntent(ctx, intentID, patch.ID, "result encoding failed")
		return PatchResult{}, storageError(err)
	}
	refs := buildReferences(nextDoc, materializationMap(materializations))

	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		e.abortIntent(ctx, intentID, patch.ID, "begin patch commit failed")
		return PatchResult{}, storageError(err)
	}
	defer tx.Rollback()
	if err := requireCurrentVersion(ctx, tx, baseVersion); err != nil {
		_ = tx.Rollback()
		e.abortIntent(ctx, intentID, patch.ID, "current version changed before patch commit")
		return PatchResult{}, err
	}
	if err := persistSnapshotTx(ctx, tx, nextDoc, nextPayload, nextHash, materializations, impactEntries, refs); err != nil {
		_ = tx.Rollback()
		e.abortIntent(ctx, intentID, patch.ID, "patch snapshot persistence failed")
		return PatchResult{}, storageError(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `
		UPDATE patch_log SET state='complete', document_version=?, result_json=?, updated_at=?
		WHERE patch_id=?`, targetVersion, string(resultJSON), now, patch.ID); err != nil {
		_ = tx.Rollback()
		e.abortIntent(ctx, intentID, patch.ID, "patch log completion failed")
		return PatchResult{}, storageError(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='complete', finished_at=?, detail_json=? WHERE id=?`, now, `{"phase":"snapshot_committed"}`, intentID); err != nil {
		_ = tx.Rollback()
		e.abortIntent(ctx, intentID, patch.ID, "patch intent completion failed")
		return PatchResult{}, storageError(err)
	}
	if err := tx.Commit(); err != nil {
		e.abortIntent(ctx, intentID, patch.ID, "patch commit result uncertain")
		return PatchResult{}, storageError(err)
	}
	return result, nil
}

// SubmitPatchJSON decodes and submits a strict-JSON patch payload.
func (e *Engine) SubmitPatchJSON(ctx context.Context, payload []byte) (PatchResult, error) {
	patch, err := DecodePatch(payload)
	if err != nil {
		return PatchResult{}, err
	}
	return e.SubmitPatch(ctx, *patch)
}

// ImpactAt returns the persisted impact record for one component in one
// committed snapshot. This is useful for audits after a restart.
func (e *Engine) ImpactAt(ctx context.Context, version int64, componentID string) (AffectedComponent, error) {
	if err := e.checkOpen(); err != nil {
		return AffectedComponent{}, err
	}
	var payload string
	err := e.db.QueryRowContext(ctx, `SELECT impact_json FROM impacts WHERE snapshot_version=? AND component_id=?`, version, componentID).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return AffectedComponent{}, withDetails(businessError(ErrNotFound, "impact record for %s at version %d not found", componentID, version), map[string]any{
			"component_id": componentID,
			"version":      version,
		})
	}
	if err != nil {
		return AffectedComponent{}, storageError(err)
	}
	var impact AffectedComponent
	if err := json.Unmarshal([]byte(payload), &impact); err != nil {
		return AffectedComponent{}, storageError(err)
	}
	return impact, nil
}

// ReferencesAt returns persisted key, group and upstream references for one
// component snapshot.
func (e *Engine) ReferencesAt(ctx context.Context, version int64, componentID string) ([]PatchRef, error) {
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	rows, err := e.db.QueryContext(ctx, `
		SELECT ref_kind, target_id, key_path FROM refs
		WHERE snapshot_version=? AND component_id=?
		ORDER BY ref_kind, target_id, key_path`, version, componentID)
	if err != nil {
		return nil, storageError(err)
	}
	defer rows.Close()
	refs := make([]PatchRef, 0)
	for rows.Next() {
		ref := PatchRef{ComponentID: componentID}
		if err := rows.Scan(&ref.Kind, &ref.TargetID, &ref.Key); err != nil {
			return nil, storageError(err)
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, storageError(err)
	}
	if len(refs) == 0 {
		return nil, withDetails(businessError(ErrNotFound, "references for %s at version %d not found", componentID, version), map[string]any{
			"component_id": componentID,
			"version":      version,
		})
	}
	return refs, nil
}

// DocumentAt returns the persisted document for a complete snapshot.
func (e *Engine) DocumentAt(ctx context.Context, version int64) (*Document, error) {
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	doc, _, _, err := loadDocumentAt(ctx, e.db, version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, withDetails(businessError(ErrNotFound, "document version %d not found", version), map[string]any{"version": version})
	}
	if err != nil {
		return nil, storageError(err)
	}
	return doc, nil
}

// Read returns the materialization visible at ObservedVersion. Historical
// snapshots remain readable after newer commits; future or incomplete
// observations are rejected rather than served with an older value.
func (e *Engine) Read(ctx context.Context, request ReadRequest) (ReadResult, error) {
	if err := e.checkOpen(); err != nil {
		return ReadResult{}, err
	}
	if strings.TrimSpace(request.ComponentID) == "" {
		return ReadResult{}, businessError(ErrNotFound, "component_id is required")
	}
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ReadResult{}, storageError(err)
	}
	defer tx.Rollback()

	committed, err := currentVersion(ctx, tx)
	if err != nil {
		return ReadResult{}, storageError(err)
	}
	observed := request.ObservedVersion
	if observed == 0 {
		observed = committed
	}
	if observed <= 0 || observed > committed {
		return ReadResult{}, e.staleReadDetails(ctx, tx, request.ComponentID, observed, committed)
	}

	var payload string
	var materializedVersion int64
	err = tx.QueryRowContext(ctx, `
		SELECT config_json, materialized_version FROM materializations
		WHERE snapshot_version=? AND component_id=?`, observed, request.ComponentID).Scan(&payload, &materializedVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return ReadResult{}, e.staleReadDetails(ctx, tx, request.ComponentID, observed, committed)
	}
	if err != nil {
		return ReadResult{}, storageError(err)
	}
	if materializedVersion > observed {
		return ReadResult{}, e.staleReadDetails(ctx, tx, request.ComponentID, observed, committed)
	}
	var config MaterializedConfig
	if err := json.Unmarshal([]byte(payload), &config); err != nil {
		return ReadResult{}, storageError(err)
	}
	return ReadResult{
		ComponentID:         request.ComponentID,
		ObservedVersion:     observed,
		SnapshotVersion:     observed,
		MaterializedVersion: materializedVersion,
		Config:              config,
	}, nil
}

func (e *Engine) staleReadDetails(ctx context.Context, tx *sql.Tx, componentID string, observed, committed int64) error {
	details := map[string]any{
		"component_id":      componentID,
		"observed_version":  observed,
		"committed_version": committed,
		"lagging_version":   committed,
	}
	var latest int64
	err := tx.QueryRowContext(ctx, `
		SELECT materialized_version FROM materializations
		WHERE component_id=? ORDER BY snapshot_version DESC LIMIT 1`, componentID).Scan(&latest)
	if err == nil {
		details["latest_materialized_version"] = latest
	}
	return withDetails(businessError(ErrStaleRead, "component %q cannot be served at observed version %d", componentID, observed), details)
}

func (e *Engine) lookupPatch(ctx context.Context, patch Patch, payloadHash string) (PatchResult, bool, error) {
	var storedHash, state string
	var resultJSON sql.NullString
	err := e.db.QueryRowContext(ctx, `
		SELECT payload_hash, state, result_json
		FROM patch_log WHERE patch_id=?`, patch.ID).Scan(&storedHash, &state, &resultJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return PatchResult{}, false, nil
	}
	if err != nil {
		return PatchResult{}, true, storageError(err)
	}
	if storedHash != payloadHash {
		return PatchResult{}, true, withDetails(businessError(ErrConflictingPatch, "patch id %q was already used with different content", patch.ID), map[string]any{
			"patch_id": patch.ID,
		})
	}
	switch state {
	case "complete":
		if !resultJSON.Valid {
			return PatchResult{}, true, storageError(fmt.Errorf("complete patch %q has no stored result", patch.ID))
		}
		var result PatchResult
		if err := json.Unmarshal([]byte(resultJSON.String), &result); err != nil {
			return PatchResult{}, true, storageError(err)
		}
		result.Duplicate = true
		return result, true, nil
	case "in_progress":
		return PatchResult{}, true, withDetails(businessError(ErrPatchInProgress, "patch %q is still in progress", patch.ID), map[string]any{"patch_id": patch.ID})
	case "aborted", "failed":
		return PatchResult{}, false, nil
	default:
		return PatchResult{}, true, storageError(fmt.Errorf("patch %q has unknown state %q", patch.ID, state))
	}
}

func (e *Engine) abortIntent(ctx context.Context, intentID int64, patchID, reason string) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	detail, _ := json.Marshal(map[string]any{"phase": "aborted", "reason": reason})
	_, _ = e.db.ExecContext(ctx, `UPDATE intents SET status='aborted', finished_at=?, detail_json=? WHERE id=? AND status <> 'complete'`, now, string(detail), intentID)
	if patchID != "" {
		_, _ = e.db.ExecContext(ctx, `
			UPDATE patch_log SET state='aborted', updated_at=? WHERE patch_id=? AND state <> 'complete'`, now, patchID)
	}
}

func (e *Engine) checkOpen() error {
	e.closeMu.Lock()
	defer e.closeMu.Unlock()
	if e.closed {
		return storageError(fmt.Errorf("engine is closed"))
	}
	return nil
}

func requireCurrentVersion(ctx context.Context, tx *sql.Tx, expected int64) error {
	actual, err := currentVersion(ctx, tx)
	if err != nil {
		return storageError(err)
	}
	if actual != expected {
		return withDetails(businessError(ErrStaleBase, "current version changed from %d to %d before commit", expected, actual), map[string]any{
			"expected_version": expected,
			"current_version":  actual,
		})
	}
	return nil
}

func materializationMap(configs []MaterializedConfig) map[string]MaterializedConfig {
	result := make(map[string]MaterializedConfig, len(configs))
	for _, config := range configs {
		result[config.ComponentID] = config
	}
	return result
}

func materializationPointer(config MaterializedConfig) *MaterializedConfig {
	if config.ComponentID == "" {
		return nil
	}
	return &config
}

func sortedComponents(doc *Document) []Component {
	components := append([]Component(nil), doc.Components...)
	sort.Slice(components, func(i, j int) bool { return components[i].ID < components[j].ID })
	return components
}

// VersionHash is a convenience hash for callers that need to compare document
// identity without persisting it themselves.
func (d *Document) VersionHash() string {
	hash, _, err := documentHash(d)
	if err != nil {
		return ""
	}
	return hash
}

// MustOpen is useful for the deterministic CLI and sample tools. It panics on
// setup failure; library callers should use Open.
func MustOpen(ctx context.Context, path string, options Options) *Engine {
	engine, err := Open(ctx, path, options)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		panic(err)
	}
	return engine
}

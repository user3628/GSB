package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// SubmitSnapshot commits a complete registration snapshot. The version check,
// validation, derived build and activation are all serialized by the writer.
func (e *Engine) SubmitSnapshot(ctx context.Context, req SnapshotRequest) (SnapshotResult, *Error) {
	var out SnapshotResult
	hash := snapshotHash(req)
	body, opErr := e.idempotent(ctx, req.RequestID, "snapshot", hash, func() ([]byte, *Error) {
		e.writeMu.Lock()
		defer e.writeMu.Unlock()
		return e.doSubmitSnapshot(ctx, req, hash)
	})
	if opErr != nil {
		return out, opErr
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return out, newError(ErrInternal, "decode stored snapshot response", "detail", err.Error())
	}
	return out, nil
}

func (e *Engine) doSubmitSnapshot(ctx context.Context, req SnapshotRequest, hash string) ([]byte, *Error) {
	if err := req.Validate(); err != nil {
		return nil, validationError(err)
	}
	if err := verifyDigest(req); err != nil {
		return nil, validationError(err)
	}

	e.stateMu.RLock()
	meta, existing, loadErr := e.loadState(ctx)
	e.stateMu.RUnlock()
	if loadErr != nil {
		return nil, loadErr
	}
	if req.Version <= meta.Version {
		return nil, newError(ErrStaleSnapshot, "snapshot version must be greater than current version",
			"currentVersion", meta.Version, "submittedVersion", req.Version)
	}

	existingByKey := runtimeMap(existing)
	instances := make([]RuntimeInstance, 0, len(req.Instances))
	newRevision := meta.DerivedRevision + 1
	for _, in := range SortedInstances(req.Instances) {
		key := in.ServiceName + "\x00" + in.InstanceID
		instances = append(instances, mergeRuntime(existingByKey[key], in, newRevision, true))
	}
	view := buildDerived(newRevision, instances, nowMillis())
	result := SnapshotResult{
		Version: req.Version, GeneratedAt: req.GeneratedAt, Digest: req.Digest,
		InstanceCount: len(instances), DerivedRevision: newRevision,
	}
	body, _ := json.Marshal(result)
	return e.commitProspective(ctx, prospectiveCommit{
		Version: req.Version, GeneratedAt: req.GeneratedAt, Digest: req.Digest,
		BaseVersion: meta.Version, Reason: "snapshot", Instances: instances, View: view,
		RequestID: req.RequestID, Operation: "snapshot", RequestHash: hash, Body: body,
	})
}

// SubmitDelta applies an affected-instance patch only if its base is exactly
// the current committed version. Concurrent same-base deltas are serialized, so
// exactly one can observe the matching base.
func (e *Engine) SubmitDelta(ctx context.Context, req DeltaRequest) (DeltaResult, *Error) {
	var out DeltaResult
	hash := deltaHash(req)
	body, opErr := e.idempotent(ctx, req.RequestID, "delta", hash, func() ([]byte, *Error) {
		e.writeMu.Lock()
		defer e.writeMu.Unlock()
		return e.doSubmitDelta(ctx, req, hash)
	})
	if opErr != nil {
		return out, opErr
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return out, newError(ErrInternal, "decode stored delta response", "detail", err.Error())
	}
	return out, nil
}

func (e *Engine) doSubmitDelta(ctx context.Context, req DeltaRequest, hash string) ([]byte, *Error) {
	if req.BaseVersion < 0 {
		return nil, validationError(fmt.Errorf("baseVersion must be non-negative"))
	}
	e.stateMu.RLock()
	meta, existing, loadErr := e.loadState(ctx)
	e.stateMu.RUnlock()
	if loadErr != nil {
		return nil, loadErr
	}
	if req.BaseVersion != meta.Version {
		return nil, newError(ErrStaleBase, "baseVersion is not the current committed version",
			"currentVersion", meta.Version, "baseVersion", req.BaseVersion)
	}
	version := req.Version
	if version == 0 {
		version = meta.Version + 1
	}
	if version <= meta.Version {
		return nil, newError(ErrStaleSnapshot, "delta version must be greater than current version",
			"currentVersion", meta.Version, "submittedVersion", version)
	}

	byKey := runtimeMap(existing)
	removeKeys := make(map[string]InstanceRef, len(req.Removes))
	for i, ref := range req.Removes {
		if !identityPattern.MatchString(ref.ServiceName) || !identityPattern.MatchString(ref.InstanceID) {
			return nil, validationError(fmt.Errorf("removes[%d]: invalid instance identity", i))
		}
		removeKeys[ref.ServiceName+"\x00"+ref.InstanceID] = ref
	}
	upsertKeys := make(map[string]struct{}, len(req.Upserts))
	for i, in := range req.Upserts {
		if err := in.Validate(); err != nil {
			return nil, validationError(fmt.Errorf("upserts[%d]: %w", i, err))
		}
		key := in.ServiceName + "\x00" + in.InstanceID
		if _, duplicate := upsertKeys[key]; duplicate {
			return nil, validationError(fmt.Errorf("upserts[%d]: duplicate %s/%s", i, in.ServiceName, in.InstanceID))
		}
		if _, both := removeKeys[key]; both {
			return nil, validationError(fmt.Errorf("upserts[%d]: instance is both upserted and removed", i))
		}
		upsertKeys[key] = struct{}{}
	}

	newRevision := meta.DerivedRevision + 1
	for _, ref := range removeKeys {
		delete(byKey, ref.ServiceName+"\x00"+ref.InstanceID)
	}
	for _, in := range SortedInstances(req.Upserts) {
		key := in.ServiceName + "\x00" + in.InstanceID
		byKey[key] = mergeRuntime(byKey[key], in, newRevision, true)
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	instances := make([]RuntimeInstance, 0, len(keys))
	for _, key := range keys {
		instances = append(instances, byKey[key])
	}
	generatedAt := time.Now().UTC().Format(time.RFC3339Nano)
	digest := ComputeDigest(version, generatedAt, runtimeRegistrations(instances))
	view := buildDerived(newRevision, instances, nowMillis())
	result := DeltaResult{
		BaseVersion: req.BaseVersion, Version: version, Digest: digest,
		InstanceCount: len(instances), DerivedRevision: newRevision,
	}
	body, _ := json.Marshal(result)
	return e.commitProspective(ctx, prospectiveCommit{
		Version: version, GeneratedAt: generatedAt, Digest: digest,
		BaseVersion: req.BaseVersion, Reason: "delta", Instances: instances, View: view,
		RequestID: req.RequestID, Operation: "delta", RequestHash: hash, Body: body,
	})
}

type prospectiveCommit struct {
	Version     int64
	GeneratedAt string
	Digest      string
	BaseVersion int64
	Reason      string
	Instances   []RuntimeInstance
	View        DerivedView
	RequestID   string
	Operation   string
	RequestHash string
	Body        []byte
}

func (e *Engine) commitProspective(ctx context.Context, p prospectiveCommit) ([]byte, *Error) {
	intentID := newID()
	// All writes begin with a durable intent record.
	err := writeTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		if err := insertIntent(ctx, tx, intentID, p.Reason, p.RequestID, p.RequestHash, p.Version, p.View.Revision, "", nil); err != nil {
			return err
		}
		if p.RequestID != "" {
			return putRequestRunning(ctx, tx, p.RequestID, p.Operation, p.RequestHash)
		}
		return nil
	})
	if err != nil {
		return nil, newError(ErrInternal, "write intent failed", "detail", err.Error())
	}
	e.crash("after_intent")

	// Stage a complete prospective cascade. The active pointers still expose
	// the prior version and all staged rows have status STAGING.
	err = writeTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO snapshots(version,generated_at,digest,instance_count,status,request_id,base_version,created_at)
			VALUES(?,?,?,?,?,?,?,?)`, p.Version, p.GeneratedAt, p.Digest, len(p.Instances), "STAGING", p.RequestID, p.BaseVersion, nowText()); err != nil {
			return err
		}
		for _, in := range p.Instances {
			if _, err := tx.ExecContext(ctx, `INSERT INTO snapshot_instances(
				snapshot_version,service_name,instance_id,endpoint,weight,lease_ttl_ms,probe_kind,
				probe_path,expected_status,expected_marker,probe_timeout_ms,priority,success_threshold,
				failure_threshold,state,config_fingerprint) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
				p.Version, in.ServiceName, in.InstanceID, in.Endpoint, in.Weight, in.LeaseTTLMS,
				string(in.Probe.Kind), in.Probe.Path, in.Probe.ExpectedStatus, in.Probe.ExpectedMarker,
				in.Probe.TimeoutMS, in.Priority, in.SuccessThreshold, in.FailureThreshold,
				string(in.State), in.ConfigFingerprint); err != nil {
				return err
			}
		}
		if err := stageDerived(ctx, tx, p.View, p.Version, p.Reason); err != nil {
			return err
		}
		e.crash("mid_cascade")
		return nil
	})
	if err != nil {
		e.abortIntent(ctx, intentID, p)
		return nil, newError(ErrInternal, "stage prospective cascade failed", "detail", err.Error())
	}
	e.crash("after_stage")

	// Activate all visible state in one transaction. No reader can observe an
	// old health summary paired with a new unhealthy state.
	err = writeTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM instances`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM leases`); err != nil {
			return err
		}
		for _, in := range p.Instances {
			if err := upsertRuntimeInstance(ctx, tx, in); err != nil {
				return err
			}
			if err := upsertLease(ctx, tx, in, p.Version); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE snapshots SET status='COMPLETE' WHERE version=?`, p.Version); err != nil {
			return err
		}
		if err := activateDerived(ctx, tx, p.View, p.Version); err != nil {
			return err
		}
		if err := completeIntent(ctx, tx, intentID, "COMPLETED"); err != nil {
			return err
		}
		if p.RequestID != "" {
			return finishRequest(ctx, tx, p.RequestID, "COMPLETED", p.Body, 200, p.Version, "")
		}
		return nil
	})
	if err != nil {
		e.abortIntent(ctx, intentID, p)
		return nil, newError(ErrInternal, "activate prospective cascade failed", "detail", err.Error())
	}
	e.crash("after_activation")
	return p.Body, nil
}

func (e *Engine) abortIntent(ctx context.Context, intentID string, p prospectiveCommit) {
	_ = writeTx(context.Background(), e.db.sql, func(tx *sql.Tx) error {
		_, _ = tx.ExecContext(ctx, `DELETE FROM health_summary WHERE revision=?`, p.View.Revision)
		_, _ = tx.ExecContext(ctx, `DELETE FROM weight_allocations WHERE revision=?`, p.View.Revision)
		_, _ = tx.ExecContext(ctx, `DELETE FROM probe_plans WHERE revision=?`, p.View.Revision)
		_, _ = tx.ExecContext(ctx, `DELETE FROM derived_revisions WHERE revision=? AND status='STAGING'`, p.View.Revision)
		_, _ = tx.ExecContext(ctx, `DELETE FROM snapshot_instances WHERE snapshot_version=?`, p.Version)
		_, _ = tx.ExecContext(ctx, `DELETE FROM snapshots WHERE version=? AND status='STAGING'`, p.Version)
		_, _ = tx.ExecContext(ctx, `UPDATE intents SET status='ABORTED',updated_at=? WHERE id=?`, nowText(), intentID)
		return nil
	})
}

func (e *Engine) loadState(ctx context.Context) (currentMeta, []RuntimeInstance, *Error) {
	var meta currentMeta
	var instances []RuntimeInstance
	err := readTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		var err error
		meta, err = readCurrentMeta(ctx, tx)
		if err != nil {
			return err
		}
		instances, err = loadRuntimeInstances(ctx, tx)
		return err
	})
	if err != nil {
		return meta, nil, newError(ErrInternal, "read current state failed", "detail", err.Error())
	}
	return meta, instances, nil
}

func runtimeMap(instances []RuntimeInstance) map[string]RuntimeInstance {
	out := make(map[string]RuntimeInstance, len(instances))
	for _, in := range instances {
		out[in.ServiceName+"\x00"+in.InstanceID] = in
	}
	return out
}

func runtimeRegistrations(instances []RuntimeInstance) []Instance {
	out := make([]Instance, 0, len(instances))
	for _, in := range instances {
		out = append(out, in.Instance)
	}
	return out
}

func mergeRuntime(existing RuntimeInstance, in Instance, revision int64, resetOnChange bool) RuntimeInstance {
	fingerprint := ConfigFingerprint(in)
	if existing.ServiceName != "" && existing.ConfigFingerprint == fingerprint {
		existing.Instance = in
		existing.UpdatedRevision = revision
		return existing
	}
	out := RuntimeInstance{Instance: in, ConfigFingerprint: fingerprint, State: StateUnknown, UpdatedRevision: revision}
	if !resetOnChange && existing.ServiceName != "" {
		return existing
	}
	return out
}

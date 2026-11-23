package mfa

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"
)

func (e *Engine) Rebind(ctx context.Context, request RebindRequest) (RebindResult, error) {
	result := RebindResult{
		TenantID:    request.TenantID,
		UserID:      request.UserID,
		FactorType:  request.FactorType,
		NewFactorID: request.NewFactorID,
		AttemptID:   request.AttemptID,
	}
	if request.TenantID == "" || request.UserID == "" || request.NewFactorID == "" || request.AttemptID == "" {
		result.Code = CodePolicyMalformed
		return result, policyError("rebind", "tenant_id, user_id, new_factor_id and attempt_id are required")
	}
	if !request.FactorType.Supported() {
		result.Code = CodePolicyMalformed
		return result, policyError("rebind.factor_type", "unsupported factor type")
	}
	if !request.Sensitivity.Supported() {
		result.Code = CodePolicyMalformed
		return result, policyError("rebind.sensitivity", "unsupported sensitivity label")
	}
	if request.ExpiresAtUnix < 0 {
		result.Code = CodePolicyMalformed
		return result, policyError("rebind.expires_at_unix", "must not be negative")
	}
	if request.HoldMillis < 0 {
		result.Code = CodePolicyMalformed
		return result, policyError("rebind.hold_millis", "must not be negative")
	}
	if request.FactorType == FactorRecovery {
		if request.RecoveryCodeCount == 0 {
			request.RecoveryCodeCount = DefaultRecoveryCodes
		}
		if request.RecoveryCodeCount < 1 || request.RecoveryCodeCount > 64 {
			result.Code = CodePolicyMalformed
			return result, policyError("rebind.recovery_code_count", "must be between 1 and 64")
		}
	} else if request.RecoveryCodeCount != 0 {
		result.Code = CodePolicyMalformed
		return result, policyError("rebind.recovery_code_count", "is only valid for recovery factors")
	}

	lockKey := rebindKey(request.TenantID, request.UserID, request.FactorType)
	value, _ := e.rebindLocks.LoadOrStore(lockKey, &sync.Mutex{})
	mutex := value.(*sync.Mutex)
	if !mutex.TryLock() {
		result.Code = CodeRebindContention
		e.recordRebindContention(ctx, request, &result)
		result.Message = "another rebind is in flight for this user and factor type"
		return result, bizError(CodeRebindContention, result.Message)
	}
	defer mutex.Unlock()
	e.rebindInFlight.Store(lockKey, true)
	defer e.rebindInFlight.Delete(lockKey)

	if request.HoldMillis > 0 {
		timer := time.NewTimer(time.Duration(request.HoldMillis) * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			result.Code = CodeInternal
			return result, ctx.Err()
		}
	}

	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()

	oldFactor, err := findActiveFactor(ctx, tx, request.TenantID, request.UserID, request.FactorType)
	if err != nil {
		if err == sql.ErrNoRows {
			result.Code = CodeNoViableFactor
			return result, bizError(CodeNoViableFactor, "no active factor exists for rebind")
		}
		return result, err
	}
	result.OldFactorID = oldFactor.ID
	if !oldFactor.Rebindable {
		result.Code = CodePolicyMalformed
		return result, policyError("rebind.old_factor_id", "active factor is not marked rebindable")
	}
	if request.NewFactorID == oldFactor.ID {
		result.Code = CodePolicyMalformed
		return result, policyError("rebind.new_factor_id", "must differ from the active factor identifier")
	}
	var duplicate int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM factors WHERE factor_id = ?`, request.NewFactorID).Scan(&duplicate); err != nil {
		return result, err
	}
	if duplicate != 0 {
		result.Code = CodePolicyMalformed
		return result, policyError("rebind.new_factor_id", "factor identifier already exists")
	}
	var beforeCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM factors WHERE tenant_id = ? AND user_id = ?`, request.TenantID, request.UserID).Scan(&beforeCount); err != nil {
		return result, err
	}
	secretSeed := strings.TrimSpace(request.SecretSeed)
	if secretSeed == "" {
		secretSeed = "deterministic-seed:" + request.NewFactorID
	}
	sequence, err := nextSequence(ctx, tx, "factor_seq")
	if err != nil {
		return result, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO factors(
    factor_id, tenant_id, user_id, factor_type, sensitivity, strength, status,
    rebindable, last_activated, expires_at_unix, secret_seed, recovery_code_count,
    success_count, failure_count, consecutive_failures, degraded_reason, created_seq
) VALUES(?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?, ?, 0, 0, 0, '', ?)
`, request.NewFactorID, request.TenantID, request.UserID, string(request.FactorType),
		string(request.Sensitivity), request.Sensitivity.Strength(), string(StatusActive),
		request.ExpiresAtUnix, secretSeed, request.RecoveryCodeCount, sequence); err != nil {
		return result, err
	}
	if request.FactorType == FactorRecovery {
		for index := 0; index < request.RecoveryCodeCount; index++ {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO recovery_codes(factor_id, code_index, code_hash, consumed, consumed_attempt)
VALUES(?, ?, ?, 0, '')
`, request.NewFactorID, index, recoveryCodeHash(secretSeed, index)); err != nil {
				return result, err
			}
		}
	}
	// Rebind replaces one factor slot atomically. The old identifier remains
	// auditable in prior snapshots and in rebind_traces, while the live factor
	// count for the user remains unchanged.
	if _, err := tx.ExecContext(ctx, `DELETE FROM factors WHERE factor_id = ?`, oldFactor.ID); err != nil {
		return result, err
	}
	var afterCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM factors WHERE tenant_id = ? AND user_id = ?`, request.TenantID, request.UserID).Scan(&afterCount); err != nil {
		return result, err
	}
	if beforeCount != afterCount {
		return result, fmt.Errorf("rebind factor count invariant violated: before=%d after=%d", beforeCount, afterCount)
	}
	version, err := writeSnapshot(ctx, tx, "factor_rebind", request.AttemptID)
	if err != nil {
		return result, err
	}
	rebindSequence, err := nextSequence(ctx, tx, "rebind_seq")
	if err != nil {
		return result, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO rebind_traces(trace_id, sequence, tenant_id, user_id, factor_type, old_factor_id, new_factor_id, attempt_id, status, code, snapshot_version)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'succeeded', ?, ?)
`, stableID("rebind", rebindSequence), rebindSequence, request.TenantID, request.UserID,
		string(request.FactorType), oldFactor.ID, request.NewFactorID, request.AttemptID, string(CodeOK), version); err != nil {
		return result, err
	}
	if err := commitWithHooks(tx, "rebind", e.options); err != nil {
		return result, err
	}
	result.Code = CodeOK
	result.SnapshotVersion = version
	return result, nil
}

func (e *Engine) RebindInFlight(tenantID, userID string, factorType FactorType) bool {
	_, ok := e.rebindInFlight.Load(rebindKey(tenantID, userID, factorType))
	return ok
}

func findActiveFactor(ctx context.Context, tx *sql.Tx, tenantID, userID string, factorType FactorType) (*SnapshotFactor, error) {
	row := tx.QueryRowContext(ctx, `
SELECT factor_id, tenant_id, user_id, factor_type, sensitivity, strength, status,
       rebindable, last_activated, expires_at_unix, secret_seed, recovery_code_count,
       success_count, failure_count, consecutive_failures, degraded_reason
FROM factors
WHERE tenant_id = ? AND user_id = ? AND factor_type = ? AND status = ? AND last_activated = 1
ORDER BY factor_id LIMIT 1
`, tenantID, userID, string(factorType), string(StatusActive))
	return scanFactor(row)
}

func (e *Engine) recordRebindContention(ctx context.Context, request RebindRequest, result *RebindResult) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer tx.Rollback()
	var oldFactorID string
	_ = tx.QueryRowContext(ctx, `
SELECT factor_id FROM factors
WHERE tenant_id = ? AND user_id = ? AND factor_type = ? AND status = ? AND last_activated = 1
ORDER BY factor_id LIMIT 1
`, request.TenantID, request.UserID, string(request.FactorType), string(StatusActive)).Scan(&oldFactorID)
	result.OldFactorID = oldFactorID
	var version int64
	if err := tx.QueryRowContext(ctx, `SELECT head_version FROM snapshot_meta WHERE singleton = 1`).Scan(&version); err != nil {
		return
	}
	result.SnapshotVersion = version
	sequence, err := nextSequence(ctx, tx, "rebind_seq")
	if err != nil {
		return
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO rebind_traces(trace_id, sequence, tenant_id, user_id, factor_type, old_factor_id, new_factor_id, attempt_id, status, code, snapshot_version)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'contended', ?, ?)
`, stableID("rebind", sequence), sequence, request.TenantID, request.UserID,
		string(request.FactorType), oldFactorID, request.NewFactorID, request.AttemptID, string(CodeRebindContention), version); err != nil {
		return
	}
	_ = tx.Commit()
}

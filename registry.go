package mfa

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

func decodeRequiredFactors(encoded string, target *[]RequiredFactor) error {
	return json.Unmarshal([]byte(encoded), target)
}

func (e *Engine) Register(ctx context.Context, cfg Config) error {
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return err
	}

	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var existing int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM tenants`).Scan(&existing); err != nil {
		return err
	}
	if existing != 0 {
		return policyError("registry", "registry is not empty")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM snapshot_seals; DELETE FROM snapshots; UPDATE snapshot_meta SET head_version = 0, head_hash = '' WHERE singleton = 1;`); err != nil {
		return err
	}

	for _, tenant := range normalized.Tenants {
		if _, err := tx.ExecContext(ctx, `INSERT INTO tenants(id, name) VALUES(?, ?)`, tenant.ID, tenant.Name); err != nil {
			return fmt.Errorf("insert tenant %s: %w", tenant.ID, err)
		}
	}
	for _, user := range normalized.Users {
		if _, err := tx.ExecContext(ctx, `INSERT INTO users(id, tenant_id, name) VALUES(?, ?, ?)`, user.ID, user.TenantID, user.Name); err != nil {
			return fmt.Errorf("insert user %s: %w", user.ID, err)
		}
	}
	for _, policy := range normalized.Policies {
		requiredJSON, err := json.Marshal(policy.RequiredFactors)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO policies(tenant_id, min_strength, required_json) VALUES(?, ?, ?)
`, policy.TenantID, policy.MinStrength, string(requiredJSON)); err != nil {
			return fmt.Errorf("insert policy %s: %w", policy.TenantID, err)
		}
	}
	codeByFactor := make(map[string][]RecoveryCode)
	for _, code := range normalized.Codes {
		codeByFactor[code.FactorID] = append(codeByFactor[code.FactorID], code)
	}
	for _, factor := range normalized.Factors {
		sequence, err := nextSequence(ctx, tx, "factor_seq")
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO factors(
    factor_id, tenant_id, user_id, factor_type, sensitivity, strength, status,
    rebindable, last_activated, expires_at_unix, secret_seed, recovery_code_count,
    success_count, failure_count, consecutive_failures, degraded_reason, created_seq
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, '', ?)
`,
			factor.ID,
			factor.TenantID,
			factor.UserID,
			string(factor.Type),
			string(factor.Sensitivity),
			factor.Strength,
			string(factor.Status),
			boolInt(factor.Rebindable),
			boolInt(factor.LastActivated),
			factor.ExpiresAtUnix,
			factor.SecretSeed,
			factor.RecoveryCodeCount,
			sequence,
		); err != nil {
			return fmt.Errorf("insert factor %s: %w", factor.ID, err)
		}
		for _, code := range codeByFactor[factor.ID] {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO recovery_codes(factor_id, code_index, code_hash, consumed, consumed_attempt)
VALUES(?, ?, ?, 0, '')
`, code.FactorID, code.Index, code.CodeHash); err != nil {
				return fmt.Errorf("insert recovery code: %w", err)
			}
		}
	}
	if err := e.saveEngineMeta(ctx, tx, normalized.ClockUnix, normalized.Limits); err != nil {
		return err
	}
	if _, err := writeSnapshot(ctx, tx, "register", ""); err != nil {
		return err
	}
	if err := commitWithHooks(tx, "register", e.options); err != nil {
		return err
	}
	e.limits = normalized.Limits
	e.clockUnix = normalized.ClockUnix
	e.sem = make(chan struct{}, e.limits.MaxConcurrency)
	return nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (e *Engine) ActivateFactor(ctx context.Context, request ActivationRequest) (ActivationResult, error) {
	result := ActivationResult{
		TenantID: request.TenantID,
		UserID:   request.UserID,
		FactorID: request.FactorID,
	}
	if request.TenantID == "" || request.UserID == "" || request.FactorID == "" || request.AttemptID == "" {
		return result, policyError("activation", "tenant_id, user_id, factor_id and attempt_id are required")
	}

	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()

	factor, err := loadFactor(ctx, tx, request.FactorID)
	if err != nil {
		if err == sql.ErrNoRows {
			return result, bizError(CodeNoViableFactor, "factor identifier is missing")
		}
		return result, err
	}
	if factor.TenantID != request.TenantID || factor.UserID != request.UserID {
		return result, policyError("activation.factor_id", "factor does not belong to the declared tenant and user")
	}
	if factor.Status == StatusExpired || (factor.ExpiresAtUnix > 0 && factor.ExpiresAtUnix <= e.ClockUnix()) {
		return result, bizError(CodeNoViableFactor, "factor is expired")
	}
	if factor.Status == StatusActive {
		var version int64
		if err := tx.QueryRowContext(ctx, `SELECT head_version FROM snapshot_meta WHERE singleton = 1`).Scan(&version); err != nil {
			return result, err
		}
		result.Code = CodeOK
		result.SnapshotVersion = version
		return result, tx.Commit()
	}
	if factor.Status != StatusPending && factor.Status != StatusRevoked {
		return result, policyError("activation.status", "only pending or revoked factors may be activated")
	}
	var activeCount int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM factors
WHERE tenant_id = ? AND user_id = ? AND factor_type = ? AND status = ? AND factor_id <> ?
`, factor.TenantID, factor.UserID, string(factor.Type), string(StatusActive), factor.ID).Scan(&activeCount); err != nil {
		return result, err
	}
	if activeCount != 0 {
		return result, policyError("activation.factor_id", "another active factor already occupies this user and factor type")
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE factors SET status = ?, last_activated = 1, degraded_reason = '', consecutive_failures = 0
WHERE factor_id = ?
`, string(StatusActive), factor.ID); err != nil {
		return result, err
	}
	version, err := writeSnapshot(ctx, tx, "factor_activation", request.AttemptID)
	if err != nil {
		return result, err
	}
	if err := commitWithHooks(tx, "factor_activation", e.options); err != nil {
		return result, err
	}
	result.Code = CodeOK
	result.SnapshotVersion = version
	return result, nil
}

func (e *Engine) RestoreFactor(ctx context.Context, request RestoreRequest) (RestoreResult, error) {
	result := RestoreResult{
		TenantID: request.TenantID,
		UserID:   request.UserID,
		FactorID: request.FactorID,
	}
	if request.TenantID == "" || request.UserID == "" || request.FactorID == "" || request.AttemptID == "" {
		return result, policyError("restore", "tenant_id, user_id, factor_id and attempt_id are required")
	}
	if request.Reason == "" {
		return result, policyError("restore.reason", "reason is required")
	}
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	factor, err := loadFactor(ctx, tx, request.FactorID)
	if err != nil {
		if err == sql.ErrNoRows {
			return result, bizError(CodeNoViableFactor, "factor identifier is missing")
		}
		return result, err
	}
	if factor.TenantID != request.TenantID || factor.UserID != request.UserID {
		return result, policyError("restore.factor_id", "factor does not belong to the declared tenant and user")
	}
	if factor.Status != StatusDegraded {
		return result, policyError("restore.status", "only degraded factors may be restored")
	}
	if factor.ExpiresAtUnix > 0 && factor.ExpiresAtUnix <= e.ClockUnix() {
		return result, bizError(CodeNoViableFactor, "factor is expired")
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE factors SET status = ?, consecutive_failures = 0, degraded_reason = '' WHERE factor_id = ?
`, string(StatusActive), factor.ID); err != nil {
		return result, err
	}
	sequence, err := nextSequence(ctx, tx, "risk_seq")
	if err != nil {
		return result, err
	}
	traceID := stableID("risk", sequence)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO risk_traces(trace_id, sequence, tenant_id, user_id, factor_id, attempt_id, kind, reason, consecutive_failures, snapshot_version)
VALUES(?, ?, ?, ?, ?, ?, 'restored', ?, 0, 0)
`, traceID, sequence, factor.TenantID, factor.UserID, factor.ID, request.AttemptID, request.Reason); err != nil {
		return result, err
	}
	version, err := writeSnapshot(ctx, tx, "factor_restore", request.AttemptID)
	if err != nil {
		return result, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE risk_traces SET snapshot_version = ? WHERE trace_id = ?`, version, traceID); err != nil {
		return result, err
	}
	if err := commitWithHooks(tx, "factor_restore", e.options); err != nil {
		return result, err
	}
	result.Code = CodeOK
	result.SnapshotVersion = version
	return result, nil
}

func (e *Engine) ListFactors(ctx context.Context) ([]Factor, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	snapshotFactors, err := listFactorsTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	factors := make([]Factor, 0, len(snapshotFactors))
	for _, factor := range snapshotFactors {
		factors = append(factors, factor.Factor)
	}
	return factors, nil
}

func (e *Engine) UsableFactor(ctx context.Context, tenantID, userID, factorID string) (bool, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	factor, err := loadFactor(ctx, tx, factorID)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return factor.TenantID == tenantID && factor.UserID == userID && factor.Status == StatusActive &&
		factor.LastActivated && (factor.ExpiresAtUnix == 0 || factor.ExpiresAtUnix > e.ClockUnix()), nil
}

func (e *Engine) ListPolicies(ctx context.Context) ([]Policy, error) {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	_, _, policies, _, err := readState(ctx, tx)
	return policies, err
}

func (e *Engine) ExpireFactors(ctx context.Context, tenantID, userID, attemptID string) (ExpireResult, error) {
	result := ExpireResult{TenantID: tenantID, UserID: userID, FactorIDs: make([]string, 0)}
	if tenantID == "" || userID == "" || attemptID == "" {
		return result, policyError("expire", "tenant_id, user_id and attempt_id are required")
	}
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `
SELECT factor_id FROM factors
WHERE tenant_id = ? AND user_id = ? AND status = ? AND expires_at_unix > 0 AND expires_at_unix <= ?
ORDER BY factor_id
`, tenantID, userID, string(StatusActive), e.ClockUnix())
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var factorID string
		if err := rows.Scan(&factorID); err != nil {
			_ = rows.Close()
			return result, err
		}
		result.FactorIDs = append(result.FactorIDs, factorID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return result, err
	}
	_ = rows.Close()
	if len(result.FactorIDs) == 0 {
		result.Code = CodeOK
		result.SnapshotVersion, err = currentSnapshotVersion(ctx, tx)
		return result, err
	}
	for _, factorID := range result.FactorIDs {
		if _, err := tx.ExecContext(ctx, `
UPDATE factors SET status = ?, last_activated = 0, degraded_reason = 'expired' WHERE factor_id = ?
`, string(StatusExpired), factorID); err != nil {
			return result, err
		}
		sequence, err := nextSequence(ctx, tx, "risk_seq")
		if err != nil {
			return result, err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO risk_traces(trace_id, sequence, tenant_id, user_id, factor_id, attempt_id, kind, reason, consecutive_failures, snapshot_version)
VALUES(?, ?, ?, ?, ?, ?, 'expired', 'factor expiration time reached', 0, 0)
`, stableID("risk", sequence), sequence, tenantID, userID, factorID, attemptID); err != nil {
			return result, err
		}
	}
	version, err := writeSnapshot(ctx, tx, "factor_expiration", attemptID)
	if err != nil {
		return result, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE risk_traces SET snapshot_version = ? WHERE attempt_id = ? AND snapshot_version = 0`, version, attemptID); err != nil {
		return result, err
	}
	if err := commitWithHooks(tx, "factor_expiration", e.options); err != nil {
		return result, err
	}
	result.Code = CodeOK
	result.SnapshotVersion = version
	return result, nil
}

package authquota

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

type tenantRow struct {
	TenantID        string
	Quota           Fixed
	LedgerVersion   int64
	SnapshotVersion int64
	UnitCost        Fixed
	RateNum         int64
	RateDen         int64
	CreatedAt       int64
}

func loadTenantTx(ctx context.Context, tx *sql.Tx, tenantID string) (tenantRow, error) {
	var tenant tenantRow
	err := tx.QueryRowContext(ctx, `
SELECT id, quota_remaining, ledger_version, snapshot_version, unit_cost, rate_num, rate_den, created_at
FROM tenants WHERE id=?`, tenantID).Scan(
		&tenant.TenantID, &tenant.Quota, &tenant.LedgerVersion, &tenant.SnapshotVersion,
		&tenant.UnitCost, &tenant.RateNum, &tenant.RateDen, &tenant.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return tenantRow{}, bizError(CodeNotFound, "tenant not found", nil)
	}
	if err != nil {
		return tenantRow{}, err
	}
	return tenant, nil
}

func buildSessionState(ctx context.Context, tx *sql.Tx, tenant tenantRow) (SessionState, error) {
	state := SessionState{
		SchemaVersion:  StateSchemaVersion,
		TenantID:       tenant.TenantID,
		Version:        tenant.SnapshotVersion,
		LedgerVersion:  tenant.LedgerVersion,
		QuotaRemaining: tenant.Quota,
		Billing: BillingState{
			UnitCost: tenant.UnitCost,
			RateNum:  tenant.RateNum,
			RateDen:  tenant.RateDen,
		},
		Families:      make([]FamilyState, 0),
		RefreshTokens: make([]TokenState, 0),
		AccessTokens:  make([]TokenState, 0),
	}

	rows, err := tx.QueryContext(ctx, `
SELECT id, tenant_id, client_id, created_at, revoked_at, revoke_reason
FROM token_families WHERE tenant_id=? ORDER BY id`, tenant.TenantID)
	if err != nil {
		return SessionState{}, err
	}
	for rows.Next() {
		var family FamilyState
		if err := rows.Scan(&family.FamilyID, &family.TenantID, &family.ClientID, &family.CreatedAt, &family.RevokedAt, &family.RevokeReason); err != nil {
			rows.Close()
			return SessionState{}, err
		}
		state.Families = append(state.Families, family)
	}
	if err := rows.Close(); err != nil {
		return SessionState{}, err
	}
	if err := rows.Err(); err != nil {
		return SessionState{}, err
	}

	refreshRows, err := tx.QueryContext(ctx, `
SELECT token_hash, family_id, sequence, tenant_id, client_id, issued_at, expires_at, revoked_at, revoke_reason, replaced_by_hash
FROM refresh_tokens WHERE tenant_id=? ORDER BY token_hash`, tenant.TenantID)
	if err != nil {
		return SessionState{}, err
	}
	for refreshRows.Next() {
		var token TokenState
		if err := refreshRows.Scan(&token.TokenHash, &token.FamilyID, &token.Sequence, &token.TenantID, &token.ClientID,
			&token.IssuedAt, &token.ExpiresAt, &token.RevokedAt, &token.RevokeReason, &token.ReplacedBy); err != nil {
			refreshRows.Close()
			return SessionState{}, err
		}
		state.RefreshTokens = append(state.RefreshTokens, token)
	}
	if err := refreshRows.Close(); err != nil {
		return SessionState{}, err
	}
	if err := refreshRows.Err(); err != nil {
		return SessionState{}, err
	}

	accessRows, err := tx.QueryContext(ctx, `
SELECT token_hash, family_id, sequence, tenant_id, client_id, issued_at, expires_at, revoked_at, revoke_reason, ''
FROM access_tokens WHERE tenant_id=? ORDER BY token_hash`, tenant.TenantID)
	if err != nil {
		return SessionState{}, err
	}
	for accessRows.Next() {
		var token TokenState
		if err := accessRows.Scan(&token.TokenHash, &token.FamilyID, &token.Sequence, &token.TenantID, &token.ClientID,
			&token.IssuedAt, &token.ExpiresAt, &token.RevokedAt, &token.RevokeReason, &token.ReplacedBy); err != nil {
			accessRows.Close()
			return SessionState{}, err
		}
		state.AccessTokens = append(state.AccessTokens, token)
	}
	if err := accessRows.Close(); err != nil {
		return SessionState{}, err
	}
	return state, accessRows.Err()
}

func appendSnapshot(ctx context.Context, e *Engine, tx *sql.Tx, tenantID, kind string) (int64, error) {
	tenant, err := loadTenantTx(ctx, tx, tenantID)
	if err != nil {
		return 0, err
	}
	nextVersion := tenant.SnapshotVersion + 1
	if e.cfg.Fault == FaultSnapshotJump && kind == "token_rotation" && e.consumeSnapshotJumpFault() {
		// Deliberately leave one version absent. This fault can only be
		// requested through configuration, never by editing source code.
		nextVersion++
	}
	tenant.SnapshotVersion = nextVersion
	if _, err := tx.ExecContext(ctx, `UPDATE tenants SET snapshot_version=? WHERE id=?`, nextVersion, tenantID); err != nil {
		return 0, err
	}
	state, err := buildSessionState(ctx, tx, tenant)
	if err != nil {
		return 0, err
	}
	body, err := CanonicalJSON(state)
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO tenant_snapshots(tenant_id, version, kind, state_json, state_hash, created_at)
VALUES(?,?,?,?,?,?)`, tenantID, nextVersion, kind, body, hashBytes(body), nowNano(e.cfg)); err != nil {
		return 0, err
	}
	return nextVersion, nil
}

func validateSnapshots(ctx context.Context, db *sql.DB, cfg Config, tenantID string) (SnapshotView, error) {
	// The tenant row and its snapshot chain are read in one statement so a
	// concurrent commit cannot create a transient false gap between queries.
	rows, err := db.QueryContext(ctx, `
SELECT t.snapshot_version, s.version, s.state_json, s.state_hash
FROM tenants t
LEFT JOIN tenant_snapshots s ON s.tenant_id=t.id
WHERE t.id=?
ORDER BY s.version`, tenantID)
	if err != nil {
		return SnapshotView{}, err
	}
	defer rows.Close()

	view := SnapshotView{TenantID: tenantID}
	expected := int64(1)
	var lastGood *SessionState
	gap := false
	foundTenant := false
	for rows.Next() {
		var latest, version sql.NullInt64
		var body []byte
		var storedHash sql.NullString
		if err := rows.Scan(&latest, &version, &body, &storedHash); err != nil {
			return SnapshotView{}, err
		}
		if !foundTenant {
			foundTenant = true
			view.LatestVersion = latest.Int64
		}
		if !version.Valid {
			continue
		}
		if gap {
			continue
		}
		if version.Int64 != expected || hashBytes(body) != storedHash.String {
			gap = true
			continue
		}
		var state SessionState
		if err := json.Unmarshal(body, &state); err != nil || state.SchemaVersion != StateSchemaVersion || state.Version != version.Int64 || state.TenantID != tenantID {
			gap = true
			continue
		}
		copyState := state
		lastGood = &copyState
		expected++
	}
	if err := rows.Err(); err != nil {
		return SnapshotView{}, err
	}
	if !foundTenant {
		return SnapshotView{}, bizError(CodeNotFound, "tenant not found", nil)
	}
	if !gap && expected-1 != view.LatestVersion {
		gap = true
	}
	if !gap && lastGood != nil && lastGood.Version == view.LatestVersion {
		view.Version = lastGood.Version
		view.State = cloneSessionState(*lastGood)
		return view, nil
	}

	view.ReadOnly = true
	view.Recovered = lastGood != nil
	if lastGood != nil {
		view.Version = lastGood.Version
		view.State = cloneSessionState(*lastGood)
	}
	detail := fmt.Sprintf("snapshot chain is discontinuous: latest=%d consistent=%d", view.LatestVersion, view.Version)
	if cfg.RecoveryStrategy == RecoveryConsistent {
		detail += "; consistent-strategy returned the last contiguous read-only view"
	} else {
		detail += "; strict-strategy refuses authoritative reads"
	}
	return view, bizError(CodeSnapshotVersion, detail, nil)
}

func (e *Engine) consumeSnapshotJumpFault() bool {
	e.faultMu.Lock()
	defer e.faultMu.Unlock()
	if e.snapshotJumpUsed {
		return false
	}
	e.snapshotJumpUsed = true
	return true
}

func (e *Engine) requireHealthySnapshots(ctx context.Context, tenantID string) error {
	_, err := validateSnapshots(ctx, e.db, e.cfg, tenantID)
	if err != nil {
		return err
	}
	return nil
}

func isBusinessCode(err error, code string) bool {
	var target *BusinessError
	return errors.As(err, &target) && target.Code == code
}

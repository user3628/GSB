package mfa

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
)

type SnapshotFactor struct {
	Factor
	SecretSeed    string         `json:"secret_seed"`
	RecoveryCodes []RecoveryCode `json:"recovery_codes,omitempty"`
}

type SnapshotPayload struct {
	Version       int64            `json:"version"`
	ParentVersion int64            `json:"parent_version"`
	Reason        string           `json:"reason"`
	AttemptID     string           `json:"attempt_id,omitempty"`
	Tenants       []Tenant         `json:"tenants"`
	Users         []User           `json:"users"`
	Policies      []Policy         `json:"policies"`
	Factors       []SnapshotFactor `json:"factors"`
}

func readState(ctx context.Context, tx *sql.Tx) ([]Tenant, []User, []Policy, []SnapshotFactor, error) {
	tenants := make([]Tenant, 0)
	tenantRows, err := tx.QueryContext(ctx, `SELECT id, name FROM tenants ORDER BY id`)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for tenantRows.Next() {
		var tenant Tenant
		if err := tenantRows.Scan(&tenant.ID, &tenant.Name); err != nil {
			_ = tenantRows.Close()
			return nil, nil, nil, nil, err
		}
		tenants = append(tenants, tenant)
	}
	if err := tenantRows.Err(); err != nil {
		_ = tenantRows.Close()
		return nil, nil, nil, nil, err
	}
	_ = tenantRows.Close()

	users := make([]User, 0)
	userRows, err := tx.QueryContext(ctx, `SELECT id, tenant_id, name FROM users ORDER BY tenant_id, id`)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for userRows.Next() {
		var user User
		if err := userRows.Scan(&user.ID, &user.TenantID, &user.Name); err != nil {
			_ = userRows.Close()
			return nil, nil, nil, nil, err
		}
		users = append(users, user)
	}
	if err := userRows.Err(); err != nil {
		_ = userRows.Close()
		return nil, nil, nil, nil, err
	}
	_ = userRows.Close()

	policies := make([]Policy, 0)
	policyRows, err := tx.QueryContext(ctx, `SELECT tenant_id, min_strength, required_json FROM policies ORDER BY tenant_id`)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for policyRows.Next() {
		var policy Policy
		var requiredJSON string
		if err := policyRows.Scan(&policy.TenantID, &policy.MinStrength, &requiredJSON); err != nil {
			_ = policyRows.Close()
			return nil, nil, nil, nil, err
		}
		if err := decodeRequiredFactors(requiredJSON, &policy.RequiredFactors); err != nil {
			_ = policyRows.Close()
			return nil, nil, nil, nil, err
		}
		sort.Slice(policy.RequiredFactors, func(i, j int) bool {
			return policy.RequiredFactors[i].Type < policy.RequiredFactors[j].Type
		})
		policies = append(policies, policy)
	}
	if err := policyRows.Err(); err != nil {
		_ = policyRows.Close()
		return nil, nil, nil, nil, err
	}
	_ = policyRows.Close()

	factors := make([]SnapshotFactor, 0)
	factorRows, err := tx.QueryContext(ctx, `
SELECT factor_id, tenant_id, user_id, factor_type, sensitivity, strength, status,
       rebindable, last_activated, expires_at_unix, secret_seed, recovery_code_count,
       success_count, failure_count, consecutive_failures, degraded_reason
FROM factors
ORDER BY tenant_id, user_id, factor_id
`)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for factorRows.Next() {
		var factor SnapshotFactor
		var factorType, sensitivity, status string
		var rebindable, lastActivated int
		if err := factorRows.Scan(
			&factor.ID,
			&factor.TenantID,
			&factor.UserID,
			&factorType,
			&sensitivity,
			&factor.Strength,
			&status,
			&rebindable,
			&lastActivated,
			&factor.ExpiresAtUnix,
			&factor.SecretSeed,
			&factor.RecoveryCodeCount,
			&factor.SuccessCount,
			&factor.FailureCount,
			&factor.ConsecutiveFailures,
			&factor.DegradedReason,
		); err != nil {
			_ = factorRows.Close()
			return nil, nil, nil, nil, err
		}
		factor.Type = FactorType(factorType)
		factor.Sensitivity = Sensitivity(sensitivity)
		factor.Status = FactorStatus(status)
		factor.Rebindable = rebindable != 0
		factor.LastActivated = lastActivated != 0
		factors = append(factors, factor)
	}
	if err := factorRows.Err(); err != nil {
		_ = factorRows.Close()
		return nil, nil, nil, nil, err
	}
	_ = factorRows.Close()

	codeRows, err := tx.QueryContext(ctx, `SELECT factor_id, code_index, code_hash, consumed FROM recovery_codes ORDER BY factor_id, code_index`)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	codesByFactor := make(map[string][]RecoveryCode)
	for codeRows.Next() {
		var code RecoveryCode
		var consumed int
		if err := codeRows.Scan(&code.FactorID, &code.Index, &code.CodeHash, &consumed); err != nil {
			_ = codeRows.Close()
			return nil, nil, nil, nil, err
		}
		code.Consumed = consumed != 0
		codesByFactor[code.FactorID] = append(codesByFactor[code.FactorID], code)
	}
	if err := codeRows.Err(); err != nil {
		_ = codeRows.Close()
		return nil, nil, nil, nil, err
	}
	_ = codeRows.Close()
	for index := range factors {
		factors[index].RecoveryCodes = codesByFactor[factors[index].ID]
	}

	return tenants, users, policies, factors, nil
}

func loadFactor(ctx context.Context, tx *sql.Tx, factorID string) (*SnapshotFactor, error) {
	row := tx.QueryRowContext(ctx, `
SELECT factor_id, tenant_id, user_id, factor_type, sensitivity, strength, status,
       rebindable, last_activated, expires_at_unix, secret_seed, recovery_code_count,
       success_count, failure_count, consecutive_failures, degraded_reason
FROM factors WHERE factor_id = ?
`, factorID)
	var factor SnapshotFactor
	var factorType, sensitivity, status string
	var rebindable, lastActivated int
	if err := row.Scan(
		&factor.ID, &factor.TenantID, &factor.UserID, &factorType, &sensitivity,
		&factor.Strength, &status, &rebindable, &lastActivated, &factor.ExpiresAtUnix,
		&factor.SecretSeed, &factor.RecoveryCodeCount, &factor.SuccessCount,
		&factor.FailureCount, &factor.ConsecutiveFailures, &factor.DegradedReason,
	); err != nil {
		return nil, err
	}
	factor.Type = FactorType(factorType)
	factor.Sensitivity = Sensitivity(sensitivity)
	factor.Status = FactorStatus(status)
	factor.Rebindable = rebindable != 0
	factor.LastActivated = lastActivated != 0
	codeRows, err := tx.QueryContext(ctx, `SELECT factor_id, code_index, code_hash, consumed FROM recovery_codes WHERE factor_id = ? ORDER BY code_index`, factorID)
	if err != nil {
		return nil, err
	}
	defer codeRows.Close()
	for codeRows.Next() {
		var code RecoveryCode
		var consumed int
		if err := codeRows.Scan(&code.FactorID, &code.Index, &code.CodeHash, &consumed); err != nil {
			return nil, err
		}
		code.Consumed = consumed != 0
		factor.RecoveryCodes = append(factor.RecoveryCodes, code)
	}
	if err := codeRows.Err(); err != nil {
		return nil, err
	}
	return &factor, nil
}

func scanFactor(scanner interface{ Scan(...any) error }) (*SnapshotFactor, error) {
	var factor SnapshotFactor
	var factorType, sensitivity, status string
	var rebindable, lastActivated int
	if err := scanner.Scan(
		&factor.ID, &factor.TenantID, &factor.UserID, &factorType, &sensitivity,
		&factor.Strength, &status, &rebindable, &lastActivated, &factor.ExpiresAtUnix,
		&factor.SecretSeed, &factor.RecoveryCodeCount, &factor.SuccessCount,
		&factor.FailureCount, &factor.ConsecutiveFailures, &factor.DegradedReason,
	); err != nil {
		return nil, err
	}
	factor.Type = FactorType(factorType)
	factor.Sensitivity = Sensitivity(sensitivity)
	factor.Status = FactorStatus(status)
	factor.Rebindable = rebindable != 0
	factor.LastActivated = lastActivated != 0
	return &factor, nil
}

func listFactorsTx(ctx context.Context, tx *sql.Tx) ([]SnapshotFactor, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT factor_id, tenant_id, user_id, factor_type, sensitivity, strength, status,
       rebindable, last_activated, expires_at_unix, secret_seed, recovery_code_count,
       success_count, failure_count, consecutive_failures, degraded_reason
FROM factors ORDER BY tenant_id, user_id, factor_id
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	factors := make([]SnapshotFactor, 0)
	for rows.Next() {
		factor, err := scanFactor(rows)
		if err != nil {
			return nil, err
		}
		factors = append(factors, *factor)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for index := range factors {
		codes, err := loadRecoveryCodes(ctx, tx, factors[index].ID)
		if err != nil {
			return nil, err
		}
		factors[index].RecoveryCodes = codes
	}
	return factors, nil
}

func userKey(tenantID, userID string) string {
	return tenantID + "\x00" + userID
}

func rebindKey(tenantID, userID string, factorType FactorType) string {
	return tenantID + "\x00" + userID + "\x00" + string(factorType)
}

func assertRowsAffected(result sql.Result, expected int64, description string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != expected {
		return fmt.Errorf("%s: expected %d affected rows, got %d", description, expected, affected)
	}
	return nil
}

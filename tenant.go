package tokenforge

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const (
	familyStatusActive  = "active"
	familyStatusRevoked = "revoked"

	tokenStatusActive        = "active"
	tokenStatusConsumed      = "consumed"
	tokenStatusRevokedSingle = "revoked_single"
	tokenStatusRevokedFamily = "revoked_family"

	revokeReasonReuse  = "token_reuse"
	revokeReasonManual = "manual_family_revoke"
)

// TenantSpec creates a tenant quota account. UnitCostFixed and UsageFixed are
// scale=1e9 fixed point values. A refresh charge is
// floor(UnitCostFixed*UsageFixed/Scale), with the remainder recorded.
type TenantSpec struct {
	TenantID          string
	DefaultUserID     string
	InitialQuotaFixed int64
	UnitCostFixed     int64
	UsageFixed        int64
}

type Tenant struct {
	TenantID      string     `json:"tenant_id"`
	DefaultUserID string     `json:"default_user_id"`
	Quota         QuotaState `json:"quota"`
}

type QuotaState struct {
	BalanceFixed  int64     `json:"balance_fixed"`
	SpentFixed    int64     `json:"spent_fixed"`
	UnitCostFixed int64     `json:"unit_cost_fixed"`
	UsageFixed    int64     `json:"usage_fixed"`
	ErrorBook     ErrorBook `json:"error_book"`
}

type ErrorBook struct {
	EntryCount          int64 `json:"entry_count"`
	TruncatingCount     int64 `json:"truncating_count"`
	RemainderUnitsTotal int64 `json:"remainder_units_total"`
	UpperBoundFixed     int64 `json:"upper_bound_fixed"`
}

type tenantRow struct {
	TenantID            string
	UserID              string
	UnitCostFixed       int64
	UsageFixed          int64
	BalanceFixed        int64
	SpentFixed          int64
	ErrorEntryCount     int64
	TruncatingCount     int64
	RemainderUnitsTotal int64
}

func (t tenantRow) quotaState() (QuotaState, error) {
	bound, err := ErrorBoundFixedUpper(t.RemainderUnitsTotal)
	if err != nil {
		return QuotaState{}, err
	}
	return QuotaState{
		BalanceFixed:  t.BalanceFixed,
		SpentFixed:    t.SpentFixed,
		UnitCostFixed: t.UnitCostFixed,
		UsageFixed:    t.UsageFixed,
		ErrorBook: ErrorBook{
			EntryCount:          t.ErrorEntryCount,
			TruncatingCount:     t.TruncatingCount,
			RemainderUnitsTotal: t.RemainderUnitsTotal,
			UpperBoundFixed:     bound,
		},
	}, nil
}

// CreateTenant initializes a quota account at snapshot version 0. The first
// issue/refesh/revoke operation creates snapshot version 1.
func (s *Service) CreateTenant(ctx context.Context, spec TenantSpec) error {
	if strings.TrimSpace(spec.TenantID) == "" || strings.TrimSpace(spec.DefaultUserID) == "" {
		return codeError(CodeInvalidArgument, "tenant_id and default_user_id are required")
	}
	for name, v := range map[string]int64{
		"initial_quota_fixed": spec.InitialQuotaFixed,
		"unit_cost_fixed":     spec.UnitCostFixed,
		"usage_fixed":         spec.UsageFixed,
	} {
		if err := validateFixedNonNegative(name, v); err != nil {
			return err
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin create tenant: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `INSERT INTO tenants
		(tenant_id, user_id, unit_cost_fixed, usage_fixed, balance_fixed, spent_fixed,
		 error_entry_count, truncating_count, remainder_units_total)
		VALUES (?, ?, ?, ?, ?, 0, 0, 0, 0)`,
		spec.TenantID, spec.DefaultUserID, spec.UnitCostFixed, spec.UsageFixed, spec.InitialQuotaFixed)
	if err != nil {
		if isUniqueConstraint(err) {
			return codeError(CodeTenantExists, "tenant already exists")
		}
		return fmt.Errorf("insert tenant: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO tenant_state
		(tenant_id, version, content_hash) VALUES (?, 0, '')`, spec.TenantID); err != nil {
		return fmt.Errorf("initialize tenant state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit create tenant: %w", err)
	}
	return nil
}

func (s *Service) loadTenant(ctx context.Context, q queryRower, tenantID string) (tenantRow, error) {
	return loadTenantRow(ctx, q, tenantID)
}

func loadTenantRow(ctx context.Context, q queryRower, tenantID string) (tenantRow, error) {
	var t tenantRow
	err := q.QueryRowContext(ctx, `SELECT tenant_id, user_id, unit_cost_fixed, usage_fixed,
		balance_fixed, spent_fixed, error_entry_count, truncating_count, remainder_units_total
		FROM tenants WHERE tenant_id = ?`, tenantID).Scan(
		&t.TenantID, &t.UserID, &t.UnitCostFixed, &t.UsageFixed,
		&t.BalanceFixed, &t.SpentFixed, &t.ErrorEntryCount, &t.TruncatingCount, &t.RemainderUnitsTotal)
	if err == sql.ErrNoRows {
		return t, codeError(CodeTenantNotFound, "tenant not found")
	}
	if err != nil {
		return t, fmt.Errorf("load tenant: %w", err)
	}
	return t, nil
}

func (s *Service) GetTenant(ctx context.Context, tenantID string) (Tenant, error) {
	t, err := s.loadTenant(ctx, s.db, tenantID)
	if err != nil {
		return Tenant{}, err
	}
	quota, err := t.quotaState()
	if err != nil {
		return Tenant{}, err
	}
	return Tenant{TenantID: t.TenantID, DefaultUserID: t.UserID, Quota: quota}, nil
}

func isUniqueConstraint(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique constraint") || strings.Contains(msg, "constraint failed")
}

type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

package tokenforge

import (
	"context"
	"fmt"
)

type ChargeRecord struct {
	EntryID         string `json:"entry_id"`
	TenantID        string `json:"tenant_id"`
	FamilyID        string `json:"family_id"`
	RefreshTokenID  string `json:"refresh_token_id"`
	Sequence        int64  `json:"sequence"`
	UnitCostFixed   int64  `json:"unit_cost_fixed"`
	UsageFixed      int64  `json:"usage_fixed"`
	ProductFixed    int64  `json:"product_fixed"`
	ChargedFixed    int64  `json:"charged_fixed"`
	RemainderUnits  int64  `json:"remainder_units"`
	SnapshotVersion int64  `json:"snapshot_version"`
	CreatedAt       int64  `json:"created_at_unix_nano"`
}

type chargeOutcome struct {
	ProductFixed       int64
	ChargedFixed       int64
	RemainderUnits     int64
	NewBalanceFixed    int64
	NewSpentFixed      int64
	NewEntryCount      int64
	NewTruncatingCount int64
	NewRemainderTotal  int64
}

// computeRefreshCharge validates every multiplication/add before returning a
// value. The caller writes the ledger and quota rows only after this function
// succeeds, so E_QUOTA_OVERFLOW/E_QUOTA_EXHAUSTED always leave the ledger
// untouched.
func computeRefreshCharge(t tenantRow) (chargeOutcome, error) {
	var out chargeOutcome
	product, charged, remainder, err := MulDivScale(t.UnitCostFixed, t.UsageFixed)
	if err != nil {
		return out, err
	}
	if charged > t.BalanceFixed {
		return out, codeError(CodeQuotaExhausted, "tenant quota balance is insufficient")
	}
	spent, err := CheckedAdd(t.SpentFixed, charged)
	if err != nil {
		return out, err
	}
	balance, err := CheckedAdd(t.BalanceFixed, -charged)
	if err != nil {
		return out, err
	}
	remainderTotal, err := CheckedAdd(t.RemainderUnitsTotal, remainder)
	if err != nil {
		return out, err
	}
	entryCount, err := CheckedAdd(t.ErrorEntryCount, 1)
	if err != nil {
		return out, err
	}
	truncatingCount := t.TruncatingCount
	if remainder > 0 {
		truncatingCount, err = CheckedAdd(t.TruncatingCount, 1)
		if err != nil {
			return out, err
		}
	}
	out = chargeOutcome{
		ProductFixed:       product,
		ChargedFixed:       charged,
		RemainderUnits:     remainder,
		NewBalanceFixed:    balance,
		NewSpentFixed:      spent,
		NewEntryCount:      entryCount,
		NewTruncatingCount: truncatingCount,
		NewRemainderTotal:  remainderTotal,
	}
	return out, nil
}

func insertCharge(ctx context.Context, q familyQuerier, record ChargeRecord) error {
	_, err := q.ExecContext(ctx, `INSERT INTO charge_ledger
		(entry_id, tenant_id, family_id, refresh_token_id, sequence, unit_cost_fixed,
		 usage_fixed, product_fixed, charged_fixed, remainder_units, snapshot_version, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.EntryID, record.TenantID, record.FamilyID, record.RefreshTokenID, record.Sequence,
		record.UnitCostFixed, record.UsageFixed, record.ProductFixed, record.ChargedFixed,
		record.RemainderUnits, record.SnapshotVersion, record.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert charge ledger: %w", err)
	}
	return nil
}

func loadCharges(ctx context.Context, q familyQuerier, tenantID string) ([]ChargeRecord, error) {
	rows, err := q.QueryContext(ctx, `SELECT entry_id, tenant_id, family_id, refresh_token_id,
		sequence, unit_cost_fixed, usage_fixed, product_fixed, charged_fixed, remainder_units,
		snapshot_version, created_at FROM charge_ledger WHERE tenant_id = ? ORDER BY entry_id`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("load charge ledger: %w", err)
	}
	defer rows.Close()
	out := make([]ChargeRecord, 0)
	for rows.Next() {
		var r ChargeRecord
		if err := rows.Scan(&r.EntryID, &r.TenantID, &r.FamilyID, &r.RefreshTokenID,
			&r.Sequence, &r.UnitCostFixed, &r.UsageFixed, &r.ProductFixed, &r.ChargedFixed,
			&r.RemainderUnits, &r.SnapshotVersion, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan charge ledger: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load charge ledger: %w", err)
	}
	return out, nil
}

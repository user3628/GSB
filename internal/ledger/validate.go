package ledger

import (
	"fmt"
	"regexp"
)

const (
	MaxSharesMilli = int64(1_000_000_000_000)
	MaxRatePPM     = int64(1_000_000)
	MaxTimestampMS = int64(253_402_300_799_999)
)

var settleSeqPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,96}$`)
var idPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,96}$`)

func ValidateSnapshot(s Snapshot) error {
	if s.Version <= 0 {
		return Biz(CodeLedgerInvalid, "snapshot version must be positive", map[string]any{"field": "version"})
	}
	if s.GeneratedAtMS < 0 || s.GeneratedAtMS > MaxTimestampMS {
		return Biz(CodeLedgerInvalid, "generated_at_ms is outside the supported epoch range", map[string]any{"field": "generated_at_ms", "maximum": MaxTimestampMS})
	}
	if len(s.Pools) == 0 {
		return Biz(CodeLedgerInvalid, "snapshot must declare at least one pool", nil)
	}
	if len(s.Tenants) == 0 {
		return Biz(CodeLedgerInvalid, "snapshot must declare at least one tenant", nil)
	}
	if len(s.RateTiers) == 0 {
		return Biz(CodeLedgerInvalid, "snapshot must declare at least one rate tier", nil)
	}

	pools := make(map[string]Pool, len(s.Pools))
	for i, p := range s.Pools {
		ctx := map[string]any{"pool_index": i, "pool_id": p.ID}
		if !idPattern.MatchString(p.ID) {
			return Biz(CodeLedgerInvalid, "pool id is empty or has invalid characters", ctx)
		}
		if _, exists := pools[p.ID]; exists {
			return Biz(CodeLedgerInvalid, "duplicate pool id", ctx)
		}
		if p.TotalSharesMilli <= 0 || p.TotalSharesMilli > MaxSharesMilli {
			return Biz(CodeLedgerInvalid, "pool total_shares_milli is outside the supported range", ctx)
		}
		if p.ReserveSharesMilli < 0 || p.ReserveSharesMilli >= p.TotalSharesMilli {
			return Biz(CodeLedgerInvalid, "pool reserve_shares_milli must be >= 0 and < total_shares_milli", ctx)
		}
		if p.OvercommitThresholdPPM <= 0 || p.OvercommitThresholdPPM > 1_000_000 {
			return Biz(CodeLedgerInvalid, "overcommit_threshold_ppm must be in (0,1000000]", ctx)
		}
		if p.RelativeTolerancePPM < 0 || p.RelativeTolerancePPM > 1_000_000 {
			return Biz(CodeLedgerInvalid, "relative_tolerance_ppm must be in [0,1000000]", ctx)
		}
		pools[p.ID] = p
	}

	for tier, rt := range s.RateTiers {
		if !idPattern.MatchString(tier) {
			return Biz(CodeLedgerInvalid, "rate tier id is empty or has invalid characters", map[string]any{"rate_tier": tier})
		}
		if rt.RatePPM < 0 || rt.RatePPM > MaxRatePPM {
			return Biz(CodeLedgerInvalid, "rate_ppm_per_minute is outside the supported range", map[string]any{"rate_tier": tier, "rate_ppm_per_minute": rt.RatePPM})
		}
	}

	tenants := make(map[string]Tenant, len(s.Tenants))
	baseByPool := make(map[string]int64)
	for i, t := range s.Tenants {
		ctx := map[string]any{"tenant_index": i, "tenant_id": t.ID}
		if !idPattern.MatchString(t.ID) {
			return Biz(CodeLedgerInvalid, "tenant id is empty or has invalid characters", ctx)
		}
		if _, exists := tenants[t.ID]; exists {
			return Biz(CodeLedgerInvalid, "duplicate tenant id", ctx)
		}
		if _, ok := pools[t.PoolID]; !ok {
			return Biz(CodeLedgerInvalid, "tenant references an undeclared pool", ctx)
		}
		if t.BaseSharesMilli < 0 || t.BaseSharesMilli > MaxSharesMilli {
			return Biz(CodeLedgerInvalid, "base_shares_milli is outside the supported range", ctx)
		}
		if t.CreditLimitMilli < 0 || t.CreditLimitMilli > MaxSharesMilli {
			return Biz(CodeLedgerInvalid, "credit_limit_milli is outside the supported range", ctx)
		}
		if _, ok := s.RateTiers[t.RateTier]; !ok {
			return Biz(CodeLedgerInvalid, "tenant references an undeclared rate tier", ctx)
		}
		if t.RepaymentPriority <= 0 {
			return Biz(CodeLedgerInvalid, "repayment_priority must be positive (lower number means higher priority)", ctx)
		}
		next, ok := safeAdd(baseByPool[t.PoolID], t.BaseSharesMilli)
		if !ok {
			return Biz(CodeLedgerInvalid, "tenant base share total overflows", ctx)
		}
		baseByPool[t.PoolID] = next
		tenants[t.ID] = t
	}
	for poolID, total := range baseByPool {
		if total > pools[poolID].TotalSharesMilli {
			return Biz(CodeLedgerInvalid, "tenant base shares exceed pool total shares", map[string]any{
				"pool_id":                 poolID,
				"base_shares_milli":       total,
				"pool_total_shares_milli": pools[poolID].TotalSharesMilli,
			})
		}
	}
	return nil
}

func ValidateEventShape(e Event) error {
	if !settleSeqPattern.MatchString(e.SettleSeq) {
		return Biz(CodeEventInvalid, "settle_seq is empty or has invalid characters", map[string]any{"settle_seq": e.SettleSeq})
	}
	if !idPattern.MatchString(e.TenantID) {
		return Biz(CodeEventInvalid, "tenant_id is empty or has invalid characters", map[string]any{"settle_seq": e.SettleSeq, "tenant_id": e.TenantID})
	}
	if e.Type != "borrow" && e.Type != "return" {
		return Biz(CodeEventInvalid, "event type must be borrow or return", map[string]any{"settle_seq": e.SettleSeq, "type": e.Type})
	}
	if e.SharesMilli <= 0 || e.SharesMilli > MaxSharesMilli {
		return Biz(CodeEventInvalid, "shares_milli must be positive and within the supported range", map[string]any{"settle_seq": e.SettleSeq, "shares_milli": e.SharesMilli})
	}
	if e.TimestampMS < 0 || e.TimestampMS > MaxTimestampMS {
		return Biz(CodeEventInvalid, "timestamp_ms is outside the supported epoch range", map[string]any{"settle_seq": e.SettleSeq, "timestamp_ms": e.TimestampMS, "maximum": MaxTimestampMS})
	}
	return nil
}

func safeAdd(a, b int64) (int64, bool) {
	if b > 0 && a > MaxInt64-b {
		return 0, false
	}
	if b < 0 && a < -MaxInt64-1-b {
		return 0, false
	}
	return a + b, true
}

func safeMul(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	if a > 0 && b > 0 && a > MaxInt64/b {
		return 0, false
	}
	if a < 0 && b < 0 && a < MaxInt64/b {
		return 0, false
	}
	if a > 0 && b < 0 && b < -MaxInt64/a {
		return 0, false
	}
	if a < 0 && b > 0 && a < -MaxInt64/b {
		return 0, false
	}
	return a * b, true
}

func checkedAddInt64(a, b int64, what string) (int64, error) {
	v, ok := safeAdd(a, b)
	if !ok {
		return 0, Biz(CodeLedgerInvalid, "integer overflow", map[string]any{"operation": what})
	}
	return v, nil
}

func checkedMulInt64(a, b int64, what string) (int64, error) {
	v, ok := safeMul(a, b)
	if !ok {
		return 0, Biz(CodeLedgerInvalid, "integer overflow", map[string]any{"operation": what})
	}
	return v, nil
}

func checkedSharesToMicros(sharesMilli int64, what string) (int64, error) {
	return checkedMulInt64(sharesMilli, MicroPerMilli, what)
}

func internalErrorf(format string, args ...any) error {
	return NewBizError(CodeInternal, fmt.Sprintf(format, args...), 500, nil)
}

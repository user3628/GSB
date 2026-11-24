package tokenforge

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// SampleTenant describes a deterministic acceptance tenant. Every quota value
// is scale=1e9 int64 fixed point.
type SampleTenant struct {
	TenantID          string `json:"tenant_id"`
	UserID            string `json:"user_id"`
	InitialQuotaFixed int64  `json:"initial_quota_fixed"`
	UnitCostFixed     int64  `json:"unit_cost_fixed"`
	UsageFixed        int64  `json:"usage_fixed"`
	FamilyCount       int    `json:"family_count"`
	Role              string `json:"role"`
}

type SampleQuotaStep struct {
	Ordinal      int    `json:"ordinal"`
	TenantID     string `json:"tenant_id"`
	Operation    string `json:"operation"`
	RefreshCount int    `json:"refresh_count"`
}

// SampleSpec is produced entirely in-process. It contains no credentials and
// no external identifiers.
type SampleSpec struct {
	Name            string            `json:"name"`
	Seed            int64             `json:"seed"`
	KeyDigest       string            `json:"key_digest"`
	ClockUnixNano   int64             `json:"clock_unix_nano"`
	AccessTTLNanos  int64             `json:"access_ttl_nanos"`
	RefreshTTLNanos int64             `json:"refresh_ttl_nanos"`
	Tenants         []SampleTenant    `json:"tenants"`
	QuotaScript     []SampleQuotaStep `json:"quota_script"`
}

func DefaultSampleSpec() SampleSpec {
	key := deterministicSampleKey(20261124)
	sum := sha256.Sum256(key)
	return SampleSpec{
		Name:            "windows-local-deterministic-v1",
		Seed:            20261124,
		KeyDigest:       hex.EncodeToString(sum[:]),
		ClockUnixNano:   time.Date(2026, 1, 1, 3, 4, 5, 0, time.UTC).UnixNano(),
		AccessTTLNanos:  int64(30 * 60 * 1_000_000_000),
		RefreshTTLNanos: int64(24 * 60 * 60 * 1_000_000_000),
		Tenants: []SampleTenant{
			{TenantID: "tenant_small", UserID: "user_small", InitialQuotaFixed: 100 * Scale, UnitCostFixed: 333_333_333, UsageFixed: 1_500_000_000, FamilyCount: 3, Role: "small_precise"},
			{TenantID: "tenant_large", UserID: "user_large", InitialQuotaFixed: 100 * Scale, UnitCostFixed: 333_333_333, UsageFixed: 1_500_000_000, FamilyCount: 6, Role: "large_stream"},
			{TenantID: "tenant_overflow", UserID: "user_overflow", InitialQuotaFixed: 1 << 62, UnitCostFixed: 1 << 62, UsageFixed: Scale, FamilyCount: 1, Role: "overflow_fault"},
			{TenantID: "tenant_parity", UserID: "user_parity", InitialQuotaFixed: 100 * Scale, UnitCostFixed: 333_333_333, UsageFixed: 1_500_000_000, FamilyCount: 5, Role: "path_parity"},
			{TenantID: "tenant_concurrent", UserID: "user_concurrent", InitialQuotaFixed: 100 * Scale, UnitCostFixed: 333_333_333, UsageFixed: 1_500_000_000, FamilyCount: 2, Role: "concurrency"},
			{TenantID: "tenant_gap", UserID: "user_gap", InitialQuotaFixed: 100 * Scale, UnitCostFixed: 333_333_333, UsageFixed: 1_500_000_000, FamilyCount: 1, Role: "snapshot_gap_fault"},
			{TenantID: "tenant_rollback", UserID: "user_rollback", InitialQuotaFixed: 100 * Scale, UnitCostFixed: 333_333_333, UsageFixed: 1_500_000_000, FamilyCount: 1, Role: "rollback_recovery"},
		},
		QuotaScript: []SampleQuotaStep{
			{Ordinal: 1, TenantID: "tenant_small", Operation: "rotate_small_family", RefreshCount: 2},
			{Ordinal: 2, TenantID: "tenant_large", Operation: "rotate_large_family", RefreshCount: 1},
			{Ordinal: 3, TenantID: "tenant_overflow", Operation: "overflow_rejected", RefreshCount: 1},
			{Ordinal: 4, TenantID: "tenant_parity", Operation: "precise_vs_stream", RefreshCount: 5},
			{Ordinal: 5, TenantID: "tenant_concurrent", Operation: "same_token_race", RefreshCount: 8},
		},
	}
}

func deterministicSampleKey(seed int64) []byte {
	sum := sha256.Sum256([]byte(fmt.Sprintf("tokenforge/local-sample-key/%d", seed)))
	return append([]byte(nil), sum[:]...)
}

// SigningKey derives the local HMAC-SHA256 key from the built-in seed. No
// network, account, environment secret or external file is consulted.
func (s SampleSpec) SigningKey() []byte { return deterministicSampleKey(s.Seed) }

// SequentialIDSource emits deterministic family/token IDs while remaining safe
// for concurrent refresh calls.
type SequentialIDSource struct {
	mu       sync.Mutex
	families int64
}

func NewSequentialIDSource() *SequentialIDSource { return &SequentialIDSource{} }

func (s *SequentialIDSource) NewFamilyID(tenantID, _ string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.families++
	return fmt.Sprintf("fam_%s_%04d", tenantID, s.families), nil
}

func (s *SequentialIDSource) NewTokenID(_ string, familyID string, sequence int64, kind TokenKind) (string, error) {
	return fmt.Sprintf("tok_%s_%02d_%s", familyID, sequence, kind), nil
}

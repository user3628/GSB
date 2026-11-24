package ledger

import "math"

const (
	MilliPerShare = int64(1000)
	MicroPerMilli = int64(1000)
	MicroPerShare = MilliPerShare * MicroPerMilli
	NanoPerMicro  = int64(1000)
	MinuteMS      = int64(60000)

	InterestScale = "1e-6 share (micro-share); one minute uses exact nano-share numerator with floor-to-micro and residual carry"
)

type RateTier struct {
	RatePPM int64 `json:"rate_ppm_per_minute"`
}

type Pool struct {
	ID                     string `json:"id"`
	TotalSharesMilli       int64  `json:"total_shares_milli"`
	ReserveSharesMilli     int64  `json:"reserve_shares_milli"`
	OvercommitThresholdPPM int64  `json:"overcommit_threshold_ppm"`
	RelativeTolerancePPM   int64  `json:"relative_tolerance_ppm"`
}

type Tenant struct {
	ID                string `json:"id"`
	PoolID            string `json:"pool_id"`
	BaseSharesMilli   int64  `json:"base_shares_milli"`
	CreditLimitMilli  int64  `json:"credit_limit_milli"`
	RateTier          string `json:"rate_tier"`
	RepaymentPriority int64  `json:"repayment_priority"`
}

type Snapshot struct {
	Version       int64               `json:"version"`
	GeneratedAtMS int64               `json:"generated_at_ms"`
	Digest        string              `json:"digest,omitempty"`
	Pools         []Pool              `json:"pools"`
	Tenants       []Tenant            `json:"tenants"`
	RateTiers     map[string]RateTier `json:"rate_tiers"`
}

type Event struct {
	SettleSeq   string `json:"settle_seq"`
	TenantID    string `json:"tenant_id"`
	Type        string `json:"type"`
	SharesMilli int64  `json:"shares_milli"`
	TimestampMS int64  `json:"timestamp_ms"`
}

type SnapshotSubmission struct {
	RequestID string   `json:"request_id"`
	Snapshot  Snapshot `json:"snapshot"`
}

type EventSubmission struct {
	RequestID string  `json:"request_id"`
	Events    []Event `json:"events"`
}

type SettlementRequest struct {
	RequestID string `json:"request_id"`
	ThroughMS int64  `json:"through_ms"`
}

type BorrowRequest struct {
	RequestID   string `json:"request_id"`
	SettleSeq   string `json:"settle_seq"`
	TenantID    string `json:"tenant_id"`
	SharesMilli int64  `json:"shares_milli"`
	TimestampMS int64  `json:"timestamp_ms"`
}

type CrashRequest struct {
	RequestID string `json:"request_id"`
	ThroughMS int64  `json:"through_ms"`
}

type FactSnapshot struct {
	Snapshot      Snapshot
	CommitVersion int64
}

type FactEvent struct {
	Event         Event
	ContentDigest string
	CommitVersion int64
}

type LotState struct {
	LotID                     string `json:"lot_id"`
	OpenedMinute              int64  `json:"opened_minute"`
	OriginalPrincipalMilli    int64  `json:"original_principal_milli"`
	OutstandingPrincipalMilli int64  `json:"outstanding_principal_milli"`
	ReturnedPrincipalMilli    int64  `json:"returned_principal_milli"`
	InterestMicros            int64  `json:"interest_micros"`
	RepaymentPriority         int64  `json:"repayment_priority"`
	RoundingErrorNanos        int64  `json:"rounding_error_nanos"`
}

type RateSegment struct {
	RateTier              string `json:"rate_tier"`
	RatePPM               int64  `json:"rate_ppm_per_minute"`
	StartMinute           int64  `json:"start_minute"`
	EndMinuteExclusive    int64  `json:"end_minute_exclusive"`
	Minutes               int64  `json:"minutes"`
	PrincipalMilliMinutes int64  `json:"principal_milli_minutes"`
	ExactInterestNanos    int64  `json:"exact_interest_nanos"`
	InterestMicros        int64  `json:"interest_micros"`
	RoundingErrorNanos    int64  `json:"rounding_error_nanos"`
}

type LotPayment struct {
	LotID              string `json:"lot_id"`
	PrincipalPaidMilli int64  `json:"principal_paid_milli"`
	InterestPaidMicros int64  `json:"interest_paid_micros"`
}

type ReturnAllocation struct {
	EventSettleSeq       string       `json:"event_settle_seq"`
	AtMinute             int64        `json:"at_minute"`
	RequestedAmountMilli int64        `json:"requested_amount_milli"`
	PrincipalPaidMilli   int64        `json:"principal_paid_milli"`
	InterestPaidMicros   int64        `json:"interest_paid_micros"`
	Payments             []LotPayment `json:"payments"`
}

type InterestStep struct {
	TenantID       string `json:"tenant_id"`
	LotID          string `json:"lot_id"`
	Minute         int64  `json:"minute"`
	RateTier       string `json:"rate_tier"`
	RatePPM        int64  `json:"rate_ppm_per_minute"`
	PrincipalMilli int64  `json:"principal_milli"`
	ExactNanos     int64  `json:"exact_interest_nanos"`
	DeltaMicros    int64  `json:"delta_interest_micros"`
	CarryNanos     int64  `json:"carry_nanos"`
	Rounding       string `json:"rounding"`
}

type TenantState struct {
	TenantID                   string             `json:"tenant_id"`
	PoolID                     string             `json:"pool_id"`
	DeclarationSnapshotVersion int64              `json:"declaration_snapshot_version"`
	BaseSharesMilli            int64              `json:"base_shares_milli"`
	CreditLimitMilli           int64              `json:"credit_limit_milli"`
	RateTier                   string             `json:"rate_tier"`
	RepaymentPriority          int64              `json:"repayment_priority"`
	PrincipalOutstandingMilli  int64              `json:"principal_outstanding_milli"`
	UnsettledInterestMicros    int64              `json:"unsettled_interest_micros"`
	AvailableCreditMicros      int64              `json:"available_credit_micros"`
	BorrowedTotalMilli         int64              `json:"borrowed_total_milli"`
	ReturnedTotalMilli         int64              `json:"returned_total_milli"`
	RoundingErrorNanos         int64              `json:"rounding_error_nanos"`
	InterestRoundingMethod     string             `json:"interest_rounding_method"`
	OutstandingLots            []LotState         `json:"outstanding_lots"`
	RateSegments               []RateSegment      `json:"rate_segments"`
	ReturnAllocations          []ReturnAllocation `json:"return_allocations"`
}

type CompensatedSum struct {
	Method                   string `json:"method"`
	ValueMicros              string `json:"value_micros"`
	NaiveValueMicros         string `json:"naive_value_micros"`
	CompensationMicros       string `json:"compensation_micros"`
	AbsoluteErrorBoundMicros string `json:"absolute_error_bound_micros"`
	RelativeErrorBoundPPM    string `json:"relative_error_bound_ppm"`
}

type PoolState struct {
	PoolID                    string         `json:"pool_id"`
	TotalSharesMilli          int64          `json:"total_shares_milli"`
	ReserveSharesMilli        int64          `json:"reserve_shares_milli"`
	UsableCapacityMilli       int64          `json:"usable_capacity_milli"`
	TenantCount               int            `json:"tenant_count"`
	PrincipalOutstandingMilli int64          `json:"principal_outstanding_milli"`
	UnsettledInterestMicros   int64          `json:"unsettled_interest_micros"`
	UsedMicros                int64          `json:"used_micros"`
	UsedRatioFixed            string         `json:"used_ratio_fixed"`
	UsedRatioPPM              int64          `json:"used_ratio_ppm"`
	RatioRounding             string         `json:"ratio_rounding"`
	OvercommitThresholdPPM    int64          `json:"overcommit_threshold_ppm"`
	RelativeTolerancePPM      int64          `json:"relative_tolerance_ppm"`
	ThresholdLimitMicros      int64          `json:"threshold_limit_micros"`
	ToleranceLimitMicros      int64          `json:"tolerance_limit_micros"`
	ToleranceRule             string         `json:"tolerance_rule"`
	OvercommitRisk            bool           `json:"overcommit_risk"`
	ExcessOverThresholdMicros int64          `json:"excess_over_threshold_micros"`
	TriggerTenantID           string         `json:"trigger_tenant_id,omitempty"`
	CrossTenantExposureMicros int64          `json:"cross_tenant_exposure_micros"`
	AvailableCreditMicros     int64          `json:"available_credit_micros"`
	CompensatedSum            CompensatedSum `json:"compensated_sum"`
}

type LedgerState struct {
	LedgerVersion        int64         `json:"ledger_version"`
	AsOfMinute           int64         `json:"as_of_minute"`
	AsOfMS               int64         `json:"as_of_ms"`
	LastSettledThroughMS int64         `json:"last_settled_through_ms"`
	LastSnapshotVersion  int64         `json:"last_snapshot_version"`
	SnapshotCount        int           `json:"snapshot_count"`
	EventCount           int           `json:"event_count"`
	SnapshotDigest       string        `json:"snapshot_digest,omitempty"`
	Tenants              []TenantState `json:"tenants"`
	Pools                []PoolState   `json:"pools"`
	StateDigest          string        `json:"state_digest,omitempty"`
}

type TenantSettlementDetail struct {
	TenantID                  string         `json:"tenant_id"`
	PrincipalOutstandingMilli int64          `json:"principal_outstanding_milli"`
	UnsettledInterestMicros   int64          `json:"unsettled_interest_micros"`
	AvailableCreditMicros     int64          `json:"available_credit_micros"`
	CumulativeRoundingNanos   int64          `json:"cumulative_rounding_error_nanos"`
	RateSegments              []RateSegment  `json:"rate_segments"`
	InterestSteps             []InterestStep `json:"interest_steps"`
}

type SettlementReport struct {
	RequestID          string                   `json:"request_id"`
	ThroughMS          int64                    `json:"through_ms"`
	ThroughMinute      int64                    `json:"through_minute"`
	LedgerVersion      int64                    `json:"ledger_version"`
	StateDigest        string                   `json:"state_digest"`
	CalculationDigest  string                   `json:"calculation_digest"`
	MinuteBoundaryRule string                   `json:"minute_boundary_rule"`
	InterestScale      string                   `json:"interest_scale"`
	Segments           []TenantSettlementDetail `json:"tenant_segments"`
	Pools              []PoolState              `json:"pools"`
}

type BorrowDecision struct {
	Accepted                  bool         `json:"accepted"`
	Code                      string       `json:"code,omitempty"`
	TenantID                  string       `json:"tenant_id"`
	RequestedSharesMilli      int64        `json:"requested_shares_milli"`
	RemainingCreditMicros     int64        `json:"remaining_credit_micros"`
	ProjectedUsedMicros       int64        `json:"projected_used_micros"`
	ProjectedRatioFixed       string       `json:"projected_used_ratio_fixed"`
	PoolOvercommit            bool         `json:"pool_overcommit"`
	PriorityConflict          bool         `json:"priority_conflict"`
	TriggerTenantID           string       `json:"trigger_tenant_id,omitempty"`
	ExcessOverThresholdMicros int64        `json:"excess_over_threshold_micros,omitempty"`
	State                     *LedgerState `json:"state,omitempty"`
}

const MaxInt64 = int64(math.MaxInt64)

func MinuteOf(timestampMS int64) int64 {
	if timestampMS <= 0 {
		return 0
	}
	return (timestampMS + MinuteMS - 1) / MinuteMS
}

func MinuteStartMS(minute int64) int64 {
	return minute * MinuteMS
}

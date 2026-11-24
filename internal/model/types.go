package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	PrecisionFP16 = "FP16"
	PrecisionFP32 = "FP32"

	PoolVRAM      = "VRAM"
	PoolPersisted = "PERSISTED"

	WritePending = "PENDING"
	WriteWritten = "WRITTEN"

	RoundingRNE = "IEEE754_BINARY16_RNE"

	WritePreferenceFP32First = "FP32_FIRST"
	WritePreferenceFP16First = "FP16_FIRST"
)

type ResidencyPlan struct {
	VRAM      []string `json:"vram"`
	Persisted []string `json:"persisted"`
}

type SnapshotRequest struct {
	Version                  int64           `json:"version"`
	GeneratedAt              string          `json:"generated_at"`
	VRAMLimitBytes           json.RawMessage `json:"vram_limit_bytes"`
	WriteBudgetBytesPerRound json.RawMessage `json:"write_budget_bytes_per_round"`
	TimeBudgetMS             json.RawMessage `json:"time_budget_ms_per_round"`
	WriteQueueCapacity       *int            `json:"write_queue_capacity"`
	LossScaleInitial         json.RawMessage `json:"loss_scale_initial"`
	LossScaleGrowth          json.RawMessage `json:"loss_scale_growth"`
	PrecisionAbsLimitNanos   json.RawMessage `json:"precision_abs_limit_nanos"`
	PrecisionRelLimitPPM     json.RawMessage `json:"precision_rel_limit_ppm"`
	WritePreference          string          `json:"write_preference"`
	Residency                *ResidencyPlan  `json:"residency"`
	VRAMResidency            []string        `json:"vram_residency"`
	PersistedResidency       []string        `json:"persisted_residency"`
	Checksum                 string          `json:"checksum"`
}

type Snapshot struct {
	Version                  int64    `json:"version"`
	GeneratedAt              string   `json:"generated_at"`
	VRAMLimitBytes           int64    `json:"vram_limit_bytes"`
	WriteBudgetBytesPerRound int64    `json:"write_budget_bytes_per_round"`
	TimeBudgetMS             int64    `json:"time_budget_ms_per_round"`
	WriteQueueCapacity       int      `json:"write_queue_capacity"`
	LossScaleInitial         string   `json:"loss_scale_initial"`
	LossScaleGrowth          string   `json:"loss_scale_growth"`
	PrecisionAbsLimitNanos   int64    `json:"precision_abs_limit_nanos"`
	PrecisionRelLimitPPM     int64    `json:"precision_rel_limit_ppm"`
	WritePreference          string   `json:"write_preference"`
	VRAMResidency            []string `json:"vram_residency"`
	PersistedResidency       []string `json:"persisted_residency"`
	Checksum                 string   `json:"checksum"`
	StateRevision            int64    `json:"state_revision,omitempty"`
}

// CanonicalSnapshotChecksum hashes a normalized policy object. It is exported
// so the sample generator, clients, and tests use exactly the engine rule.
func CanonicalSnapshotChecksum(s Snapshot) (string, error) {
	normalized := struct {
		Version                  int64    `json:"version"`
		GeneratedAt              string   `json:"generated_at"`
		VRAMLimitBytes           int64    `json:"vram_limit_bytes"`
		WriteBudgetBytesPerRound int64    `json:"write_budget_bytes_per_round"`
		TimeBudgetMS             int64    `json:"time_budget_ms_per_round"`
		WriteQueueCapacity       int      `json:"write_queue_capacity"`
		LossScaleInitial         string   `json:"loss_scale_initial"`
		LossScaleGrowth          string   `json:"loss_scale_growth"`
		PrecisionAbsLimitNanos   int64    `json:"precision_abs_limit_nanos"`
		PrecisionRelLimitPPM     int64    `json:"precision_rel_limit_ppm"`
		WritePreference          string   `json:"write_preference"`
		VRAMResidency            []string `json:"vram_residency"`
		PersistedResidency       []string `json:"persisted_residency"`
	}{
		Version:                  s.Version,
		GeneratedAt:              s.GeneratedAt,
		VRAMLimitBytes:           s.VRAMLimitBytes,
		WriteBudgetBytesPerRound: s.WriteBudgetBytesPerRound,
		TimeBudgetMS:             s.TimeBudgetMS,
		WriteQueueCapacity:       s.WriteQueueCapacity,
		LossScaleInitial:         s.LossScaleInitial,
		LossScaleGrowth:          s.LossScaleGrowth,
		PrecisionAbsLimitNanos:   s.PrecisionAbsLimitNanos,
		PrecisionRelLimitPPM:     s.PrecisionRelLimitPPM,
		WritePreference:          s.WritePreference,
		VRAMResidency:            sortedUnique(s.VRAMResidency),
		PersistedResidency:       sortedUnique(s.PersistedResidency),
	}
	b, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

type ShardInput struct {
	ShardID            string          `json:"shard_id"`
	ParameterID        string          `json:"parameter_id"`
	Precision          string          `json:"precision"`
	Bytes              int64           `json:"bytes"`
	Digest             string          `json:"digest"`
	MomentumDigest     string          `json:"momentum_digest"`
	RestoredFromDigest string          `json:"restored_from_digest"`
	TargetPool         string          `json:"target_pool"`
	LastAccessMS       *int64          `json:"last_access_ms"`
	LastAccessAt       string          `json:"last_access_at"`
	Priority           int             `json:"priority"`
	WriteMS            int64           `json:"write_ms"`
	Pinned             bool            `json:"pinned"`
	Quantization       string          `json:"quantization"`
	FP16Bits           []uint16        `json:"fp16_bits"`
	FP32Nanos          []int64         `json:"fp32_nanos"`
	PayloadHex         string          `json:"payload_hex"`
	Round              int             `json:"round,omitempty"`
	Extra              json.RawMessage `json:"-"`
}

type Shard struct {
	Round              int      `json:"round"`
	ShardID            string   `json:"shard_id"`
	ParameterID        string   `json:"parameter_id"`
	Precision          string   `json:"precision"`
	Bytes              int64    `json:"bytes"`
	Digest             string   `json:"digest"`
	MomentumDigest     string   `json:"momentum_digest,omitempty"`
	RestoredFromDigest string   `json:"restored_from_digest,omitempty"`
	Location           string   `json:"location"`
	DesiredPool        string   `json:"desired_pool"`
	WriteState         string   `json:"write_state"`
	LastAccessMS       int64    `json:"last_access_ms"`
	Priority           int      `json:"priority"`
	WriteMS            int64    `json:"write_ms"`
	Pinned             bool     `json:"pinned"`
	Quantization       string   `json:"quantization,omitempty"`
	FP16Bits           []uint16 `json:"fp16_bits,omitempty"`
	FP32Nanos          []int64  `json:"fp32_nanos,omitempty"`
	PayloadHex         string   `json:"payload_hex,omitempty"`
	Revision           int64    `json:"revision"`
	UpdatedAt          string   `json:"updated_at"`
}

// ContentMetadataEqual compares the fields whose change invalidates derived
// results. Access time and current placement are intentionally excluded.
func (s Shard) ContentMetadataEqual(other Shard) bool {
	return s.Round == other.Round &&
		s.ShardID == other.ShardID &&
		s.ParameterID == other.ParameterID &&
		s.Precision == other.Precision &&
		s.Bytes == other.Bytes &&
		s.Digest == other.Digest &&
		s.MomentumDigest == other.MomentumDigest &&
		s.RestoredFromDigest == other.RestoredFromDigest &&
		s.DesiredPool == other.DesiredPool &&
		s.Priority == other.Priority &&
		s.WriteMS == other.WriteMS &&
		s.Pinned == other.Pinned &&
		s.Quantization == other.Quantization &&
		equalUint16Slices(s.FP16Bits, other.FP16Bits) &&
		equalInt64Slices(s.FP32Nanos, other.FP32Nanos) &&
		s.PayloadHex == other.PayloadHex
}

func equalUint16Slices(a, b []uint16) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalInt64Slices(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type IncrementRequest struct {
	SubmissionID    string          `json:"submission_id"`
	CheckpointID    string          `json:"checkpoint_id"`
	BaseVersion     int64           `json:"base_version"`
	Round           int             `json:"round"`
	Shards          []ShardInput    `json:"shards"`
	Fragments       []ShardInput    `json:"fragments"`
	LossScale       json.RawMessage `json:"loss_scale"`
	LossScaleGrowth json.RawMessage `json:"loss_scale_growth"`
}

func (r *IncrementRequest) Normalize() {
	if r.SubmissionID == "" {
		r.SubmissionID = r.CheckpointID
	}
	shards := make([]ShardInput, 0, len(r.Shards)+len(r.Fragments))
	shards = append(shards, r.Shards...)
	shards = append(shards, r.Fragments...)
	r.Shards = shards
	r.Fragments = nil
	for i := range r.Shards {
		if r.Shards[i].Round == 0 {
			r.Shards[i].Round = r.Round
		}
	}
}

type SubmissionResponse struct {
	SubmissionID     string            `json:"submission_id"`
	Revision         int64             `json:"revision"`
	BaseVersion      int64             `json:"base_version"`
	Round            int               `json:"round"`
	PolicyVersion    int64             `json:"policy_version"`
	LossScale        string            `json:"loss_scale"`
	ShardsUpserted   int               `json:"shards_upserted"`
	ShardsNoop       int               `json:"shards_noop"`
	Evictions        []Eviction        `json:"evictions"`
	Writes           []WriteTask       `json:"writes"`
	Deferred         []WriteTask       `json:"deferred"`
	QueueDepth       int               `json:"queue_depth"`
	WriteOrderBasis  string            `json:"write_order_basis"`
	RemainingBytes   int64             `json:"remaining_write_bytes"`
	RemainingTimeMS  int64             `json:"remaining_time_ms"`
	DerivedRebuilt   []string          `json:"derived_rebuilt"`
	IdempotentReplay bool              `json:"idempotent_replay,omitempty"`
	Precision        []PrecisionResult `json:"precision,omitempty"`
}

type Eviction struct {
	ShardID      string `json:"shard_id"`
	Round        int    `json:"round"`
	Bytes        int64  `json:"bytes"`
	Precision    string `json:"precision"`
	LastAccessMS int64  `json:"last_access_ms"`
	Reason       string `json:"reason"`
}

type WriteTask struct {
	Round       int    `json:"round"`
	ShardID     string `json:"shard_id"`
	Precision   string `json:"precision"`
	Digest      string `json:"digest"`
	Bytes       int64  `json:"bytes"`
	Priority    int    `json:"priority"`
	WriteMS     int64  `json:"write_ms"`
	Status      string `json:"status"`
	QueueReason string `json:"queue_reason,omitempty"`
}

type PoolUsage struct {
	Pool       string `json:"pool"`
	UsedBytes  int64  `json:"used_bytes"`
	ShardCount int    `json:"shard_count"`
	LimitBytes int64  `json:"limit_bytes,omitempty"`
	Revision   int64  `json:"revision"`
	UpdatedAt  string `json:"updated_at"`
}

type PrecisionElement struct {
	Index            int    `json:"index"`
	FP16Bits         uint16 `json:"fp16_bits"`
	FP32Nanos        int64  `json:"fp32_nanos"`
	RestoredNanos    int64  `json:"restored_nanos"`
	SignedErrorNanos int64  `json:"signed_error_nanos"`
	AbsErrorNanos    int64  `json:"abs_error_nanos"`
	RelativePPM      int64  `json:"relative_ppm"`
}

type PrecisionReport struct {
	Round                   int               `json:"round,omitempty"`
	LossScale               string            `json:"loss_scale"`
	RoundingMode            string            `json:"rounding_mode"`
	Results                 []PrecisionResult `json:"results"`
	VerifiedPairs           int               `json:"verified_pairs"`
	DigestOnlyPairs         int               `json:"digest_only_pairs"`
	FailedPairs             int               `json:"failed_pairs"`
	Passed                  bool              `json:"passed"`
	MaxAbsDeviationNanos    int64             `json:"max_abs_deviation_nanos"`
	MaxRelativePPM          int64             `json:"max_relative_ppm"`
	CumulativeAbsDeviation  int64             `json:"cumulative_abs_deviation_nanos"`
	CumulativeRoundingNanos int64             `json:"cumulative_rounding_nanos"`
	ResultDigest            string            `json:"result_digest"`
}

type VerifyPrecisionRequest struct {
	Round      int             `json:"round"`
	LossScale  json.RawMessage `json:"loss_scale"`
	Submission string          `json:"submission_id,omitempty"`
}

type PrecisionResult struct {
	Round                   int                `json:"round"`
	ShardID                 string             `json:"shard_id"`
	ParameterID             string             `json:"parameter_id"`
	FP16ShardID             string             `json:"fp16_shard_id"`
	FP32ShardID             string             `json:"fp32_shard_id"`
	FP16Digest              string             `json:"fp16_digest"`
	FP32Digest              string             `json:"fp32_digest"`
	LossScale               string             `json:"loss_scale"`
	RoundingMode            string             `json:"rounding_mode"`
	Elements                int                `json:"elements"`
	MaxAbsDeviationNanos    int64              `json:"max_abs_deviation_nanos"`
	MaxRelativePPM          int64              `json:"max_relative_ppm"`
	SumAbsDeviationNanos    int64              `json:"sum_abs_deviation_nanos"`
	CumulativeRoundingNanos int64              `json:"cumulative_rounding_nanos"`
	AbsLimitNanos           int64              `json:"abs_limit_nanos"`
	RelLimitPPM             int64              `json:"rel_limit_ppm"`
	CheckMode               string             `json:"check_mode"`
	Passed                  bool               `json:"passed"`
	DriftReason             string             `json:"drift_reason,omitempty"`
	CheckedAt               string             `json:"checked_at"`
	ResultDigest            string             `json:"result_digest"`
	Details                 []PrecisionElement `json:"details,omitempty"`
}

type SchedulerDecision struct {
	Shards           []Shard
	Evictions        []Eviction
	Writes           []WriteTask
	Deferred         []WriteTask
	Precision        []PrecisionResult
	QueueDepth       int
	OrderBasis       string
	RemainingBytes   int64
	RemainingTimeMS  int64
	ChangedShards    []string
	ChangedRounds    []int
	LossScaleChanged bool
}

type Intent struct {
	ID           int64  `json:"id"`
	Kind         string `json:"kind"`
	SubmissionID string `json:"submission_id"`
	State        string `json:"state"`
	BeforeJSON   string `json:"before_json,omitempty"`
	AfterJSON    string `json:"after_json,omitempty"`
	CreatedAt    string `json:"created_at"`
	AppliedAt    string `json:"applied_at,omitempty"`
}

type RecoveryReport struct {
	ReportID            int64          `json:"report_id"`
	CreatedAt           string         `json:"created_at"`
	DBRevision          int64          `json:"db_revision"`
	FactsBasis          string         `json:"facts_basis"`
	InterruptedIntents  int            `json:"interrupted_intents"`
	RolledBackWrites    int            `json:"rolled_back_writes"`
	RolledBackEvictions int            `json:"rolled_back_evictions"`
	AbortedSubmissions  int            `json:"aborted_submissions"`
	OrphanBlobsRemoved  int            `json:"orphan_blobs_removed"`
	PoolRebuilt         bool           `json:"pool_view_rebuilt"`
	Pools               []PoolUsage    `json:"pools"`
	Consistency         []RoundSummary `json:"consistency"`
	DurableShards       int            `json:"durable_shards"`
	PendingShards       int            `json:"pending_shards"`
	Details             map[string]any `json:"details,omitempty"`
}

type RoundSummary struct {
	Round             int    `json:"round"`
	Status            string `json:"status"`
	ShardCount        int    `json:"shard_count"`
	WrittenShards     int    `json:"written_shards"`
	PendingShards     int    `json:"pending_shards"`
	FailedPrecision   int    `json:"failed_precision"`
	UnverifiedPairs   int    `json:"unverified_pairs"`
	SourceFingerprint string `json:"source_fingerprint"`
	DecidedRevision   int64  `json:"decided_revision"`
	RebuiltAt         string `json:"rebuilt_at"`
}

type StateView struct {
	CurrentPolicyVersion int64       `json:"current_policy_version"`
	Revision             int64       `json:"revision"`
	LossScale            string      `json:"loss_scale"`
	LossScaleGrowth      string      `json:"loss_scale_growth"`
	Policy               *Snapshot   `json:"policy,omitempty"`
	Pools                []PoolUsage `json:"pools"`
}

type ShardQuery struct {
	Round    int     `json:"round,omitempty"`
	Pool     string  `json:"pool,omitempty"`
	Revision int64   `json:"revision"`
	Shards   []Shard `json:"shards"`
}

type WriteRecord struct {
	RecordID     int64  `json:"record_id"`
	SubmissionID string `json:"submission_id"`
	Round        int    `json:"round"`
	ShardID      string `json:"shard_id"`
	Digest       string `json:"digest"`
	Bytes        int64  `json:"bytes"`
	Pool         string `json:"pool"`
	CommittedAt  string `json:"committed_at"`
	BlobPath     string `json:"blob_path"`
}

type Derivative struct {
	Round             int    `json:"round"`
	ShardID           string `json:"shard_id"`
	MasterDigest      string `json:"master_digest"`
	ConsistencyStatus string `json:"consistency_status"`
	ConsistencyDigest string `json:"consistency_digest"`
	VRAMEstimateBytes int64  `json:"vram_estimate_bytes"`
	RecoveryBaseline  string `json:"recovery_baseline"`
	SourceFingerprint string `json:"source_fingerprint"`
	RebuiltAt         string `json:"rebuilt_at"`
}

func NormalizePrecision(v string) string {
	return strings.ToUpper(strings.TrimSpace(v))
}

func NormalizePool(v string) string {
	switch strings.ToUpper(strings.TrimSpace(v)) {
	case "VRAM", "GPU", "HIGH_SPEED":
		return PoolVRAM
	case "PERSISTED", "DISK", "PERSISTENT", "LOW_SPEED":
		return PoolPersisted
	default:
		return strings.ToUpper(strings.TrimSpace(v))
	}
}

func ValidPrecision(v string) bool {
	return v == PrecisionFP16 || v == PrecisionFP32
}

func ValidPool(v string) bool {
	return v == PoolVRAM || v == PoolPersisted
}

func DefaultTarget(precision string) string {
	if precision == PrecisionFP16 {
		return PoolVRAM
	}
	return PoolPersisted
}

func sortedUnique(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func NewDigest(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func FormatMS(ms int64) string {
	return time.UnixMilli(ms).UTC().Format(time.RFC3339Nano)
}

func ParseTimeMS(text string) (int64, error) {
	if strings.TrimSpace(text) == "" {
		return 0, nil
	}
	t, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}

func StableTaskKey(t WriteTask) string {
	return strconv.Itoa(t.Round) + "\x00" + t.ShardID
}

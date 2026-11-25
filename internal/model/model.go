// Package model defines the stable data model shared by every pipeline stage.
//
// All on-disk representations use canonical JSON (see internal/determ) so that
// the same inputs and configuration always produce byte-identical artifacts.
package model

// RawTicket is one customer service ticket read from a deterministic JSONL
// shard. Agreement is a pointer so that a missing value is distinguishable
// from an explicit zero.
type RawTicket struct {
	TicketID    string   `json:"ticket_id"`
	Text        string   `json:"text"`
	Channel     string   `json:"channel"`
	Language    string   `json:"language"`
	IntentLabel string   `json:"intent_label"`
	Priority    int      `json:"priority"`
	Agreement   *float64 `json:"agreement,omitempty"`
}

// ShardMeta is the sidecar metadata describing a JSONL shard. The declared
// row count is checked against the actual count so a mismatch injection can
// be reported with a stable business code instead of generic I/O failure.
type ShardMeta struct {
	ShardID       string `json:"shard_id"`
	DeclaredRows  int    `json:"declared_rows"`
	PolicyVersion int    `json:"policy_version"`
}

// Shard groups a metadata file with the tickets parsed from its JSONL body.
type Shard struct {
	Meta    ShardMeta
	Tickets []RawTicket
}

// Policy is the monotonic strategy snapshot. Version must strictly increase
// whenever any field changes; the engine derives per-stage fingerprints from
// the relevant subsets so unaffected upstream stages can be reused.
type Policy struct {
	Version        int                `json:"version"`
	IntentWeights  map[string]float64 `json:"intent_weights"`
	PriorityThresh []float64          `json:"priority_thresholds"`
	Smoothing      float64            `json:"smoothing"`
	HashDim        int                `json:"hash_dim"`
	SampleRatio    float64            `json:"sample_ratio"`
	Injection      Injection          `json:"injection"`
}

// Injection is the explicit anomaly injection switch block carried inside the
// policy snapshot. Every switch maps to one stable business error code.
type Injection struct {
	EmptyText          bool `json:"empty_text"`
	LabelOutOfRange    bool `json:"label_out_of_range"`
	AgreementMissing   bool `json:"agreement_missing"`
	ZeroSmoothing      bool `json:"zero_smoothing"`
	ZeroHashDim        bool `json:"zero_hash_dim"`
	ShardCountMismatch bool `json:"shard_count_mismatch"`
}

// Stage names. These strings are part of the persisted contract and must not
// be renamed without a migration.
const (
	StageNormalize = "normalize"
	StageIntent    = "intent"
	StagePriority  = "priority"
	StageValidate  = "validate"
)

// Stages is the fixed execution order of the pipeline.
var Stages = []string{StageNormalize, StageIntent, StagePriority, StageValidate}

// Business error codes. They are persisted alongside stage records so the
// verification tool can replay and assert them across versions.
const (
	CodeEmptyText       = "E_EMPTY_TEXT"
	CodeLabelRange      = "E_LABEL_RANGE"
	CodeConfig          = "E_CONFIG"
	CodeNumeric         = "E_NUMERIC"
	CodeShardMismatch   = "E_SHARD_MISMATCH"
	CodeStaleCheckpoint = "E_STALE_CHECKPOINT"
	CodeUnknown         = "E_UNKNOWN"
)

// Downgrade reasons recorded on priority decisions. They explain why a
// sample was downgraded instead of being silently dropped.
const (
	DowngradeLabelRange    = "D_LABEL_OUT_OF_RANGE"
	DowngradeAgreementMiss = "D_AGREEMENT_MISSING"
	DowngradeAgreementLow  = "D_AGREEMENT_LOW"
)

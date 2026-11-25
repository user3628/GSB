package prefixcache

import "time"

// Stage names are stable identifiers used by invalidation records.
const (
	StageNormalization Stage = "normalization"
	StageFingerprint   Stage = "fingerprint"
	StageSlotting      Stage = "slotting"
	StageHit           Stage = "hit"
)

// Decision is the result of a cache hit adjudication.
type Decision string

const (
	DecisionExact       Decision = "exact_hit"
	DecisionMerged      Decision = "merged_hit"
	DecisionNotReusable Decision = "not_reusable"
)

// Stage is one of the four deterministic pipeline stages.
type Stage string

// Message is one prompt segment. Turn is supplied by the caller and is
// fingerprinted so insertion or removal of a turn is observable.
type Message struct {
	Turn     int    `json:"turn"`
	Role     string `json:"role"`
	Language string `json:"language"`
	Text     string `json:"text"`
}

// Request is a prompt request record.
type Request struct {
	ID         string    `json:"id"`
	TemplateID string    `json:"template_id"`
	Messages   []Message `json:"messages"`
}

// Policy is one immutable prompt-cache policy snapshot. Version is the
// effective cache revision. SlotCount and OccupancyBudget are explicit because
// a bounded cache needs both a hash address space and an occupancy budget.
type Policy struct {
	Version               int64   `json:"version"`
	Granularity           int     `json:"granularity"`
	MinPrefixLen          int     `json:"min_prefix_len"`
	MergeThreshold        float64 `json:"merge_threshold"`
	MaxPrefixesPerRequest int     `json:"max_prefixes_per_request"`
	SlotCount             int     `json:"slot_count"`
	OccupancyBudget       int     `json:"occupancy_budget"`
}

// PolicyPatch changes exactly one policy stage field. Version advances by one
// when the patch is applied and is therefore not a patch field.
type PolicyPatch struct {
	Granularity           *int     `json:"granularity,omitempty"`
	MinPrefixLen          *int     `json:"min_prefix_len,omitempty"`
	MergeThreshold        *float64 `json:"merge_threshold,omitempty"`
	MaxPrefixesPerRequest *int     `json:"max_prefixes_per_request,omitempty"`
}

// PolicySubmission imports a complete policy snapshot. Mode can be "auto",
// "advance", or "replace". In auto mode, a matching BaseDigest is a normal
// advance; a missing or mismatching digest is a whole replacement.
type PolicySubmission struct {
	Policy          Policy `json:"policy"`
	Mode            string `json:"mode,omitempty"`
	ExpectedVersion *int64 `json:"expected_version,omitempty"`
	BaseDigest      string `json:"base_digest,omitempty"`
}

// StageDigests are content-addressed fingerprints for each pipeline stage.
type StageDigests struct {
	Normalization string `json:"normalization"`
	Fingerprint   string `json:"fingerprint"`
	Slotting      string `json:"slotting"`
	Hit           string `json:"hit"`
	Version       string `json:"version"`
}

// Block is one normalized prefix chunk. StartChar and EndChar are rune offsets
// within the source message. PrefixStartChar and PrefixEndChar are rune offsets
// in the concatenated normalized prompt.
type Block struct {
	Index              int    `json:"index"`
	SourceMessageIndex int    `json:"source_message_index"`
	Turn               int    `json:"turn"`
	Role               string `json:"role"`
	Language           string `json:"language"`
	ChunkIndex         int    `json:"chunk_index"`
	Text               string `json:"text"`
	StartChar          int    `json:"start_char"`
	EndChar            int    `json:"end_char"`
	CharSpan           int    `json:"char_span"`
	PrefixStartChar    int    `json:"prefix_start_char"`
	PrefixEndChar      int    `json:"prefix_end_char"`
	Fingerprint        string `json:"fingerprint"`
	Cacheable          bool   `json:"cacheable"`
	InvalidReason      string `json:"invalid_reason,omitempty"`
}

// PrefixCandidate is a cumulative prefix ending at a block boundary.
type PrefixCandidate struct {
	BlockCount   int      `json:"block_count"`
	Fingerprints []string `json:"fingerprints"`
	PrefixKey    string   `json:"prefix_key"`
	PrefixChars  int      `json:"prefix_chars"`
	Cacheable    bool     `json:"cacheable"`
	RejectReason string   `json:"reject_reason,omitempty"`
}

// RequestPlan is the deterministic output of normalization and fingerprinting.
type RequestPlan struct {
	Version      int64             `json:"version"`
	RequestID    string            `json:"request_id"`
	TemplateID   string            `json:"template_id"`
	StageDigests StageDigests      `json:"stage_digests"`
	Blocks       []Block           `json:"blocks"`
	Prefixes     []PrefixCandidate `json:"prefixes"`
	PlanDigest   string            `json:"plan_digest"`
}

// Slot is one occupied direct-mapped cache slot.
type Slot struct {
	Version           int64    `json:"version"`
	SlotNo            int      `json:"slot_no"`
	PrefixKey         string   `json:"prefix_key"`
	BlockFingerprints []string `json:"block_fingerprints"`
	BlockCount        int      `json:"block_count"`
	PrefixChars       int      `json:"prefix_chars"`
	OccupiedAt        int64    `json:"occupied_at"`
	ReusedFrom        int64    `json:"reused_from,omitempty"`
	Owners            []string `json:"owners"`
}

// PrefixAllocation records what happened to one cacheable candidate for one
// request. SlotNo is nil when the candidate was not placed.
type PrefixAllocation struct {
	PrefixKey   string `json:"prefix_key"`
	BlockCount  int    `json:"block_count"`
	PrefixChars int    `json:"prefix_chars"`
	SlotNo      *int   `json:"slot_no,omitempty"`
	Status      string `json:"status"`
	Reason      string `json:"reason,omitempty"`
	ReusedFrom  int64  `json:"reused_from,omitempty"`
}

// RequestState is the single effective state record for a request at a version.
type RequestState struct {
	Version     int64              `json:"version"`
	Request     Request            `json:"request"`
	Plan        RequestPlan        `json:"plan"`
	Allocations []PrefixAllocation `json:"allocations"`
	Hit         HitResult          `json:"hit"`
}

// SimilarityEvidence preserves the numbers used for a decision. Similarity is
// common leading blocks divided by max(query blocks, cached blocks).
type SimilarityEvidence struct {
	Exact              bool    `json:"exact"`
	CommonPrefixBlocks int     `json:"common_prefix_blocks"`
	QueryBlocks        int     `json:"query_blocks"`
	CachedBlocks       int     `json:"cached_blocks"`
	Similarity         float64 `json:"similarity"`
	Threshold          float64 `json:"threshold"`
	ThresholdMargin    float64 `json:"threshold_margin"`
}

// HitResult is either a reusable decision or a fully evidenced miss.
type HitResult struct {
	Version         int64              `json:"version"`
	ObservedVersion int64              `json:"observed_version"`
	RequestID       string             `json:"request_id"`
	Decision        Decision           `json:"decision"`
	QueryPrefixKey  string             `json:"query_prefix_key,omitempty"`
	SlotNo          *int               `json:"slot_no,omitempty"`
	SlotPrefixKey   string             `json:"slot_prefix_key,omitempty"`
	SlotRequestID   string             `json:"slot_request_id,omitempty"`
	QueryChars      int                `json:"query_chars"`
	CachedChars     int                `json:"cached_chars"`
	Evidence        SimilarityEvidence `json:"evidence"`
	Reasons         []string           `json:"reasons,omitempty"`
}

// Invalidation identifies the earliest changed stage and the affected request.
type Invalidation struct {
	Version   int64  `json:"version"`
	OfVersion int64  `json:"of_version"`
	Field     string `json:"field"`
	Stage     Stage  `json:"stage"`
	RequestID string `json:"request_id"`
	Reason    string `json:"reason"`
	OldValue  string `json:"old_value,omitempty"`
	NewValue  string `json:"new_value,omitempty"`
}

// Eviction records both slot collisions and budget/limit evictions. SlotNo is
// -1 when the item was rejected before a physical slot was assigned.
type Eviction struct {
	Version          int64   `json:"version"`
	RequestID        string  `json:"request_id"`
	SlotNo           int     `json:"slot_no"`
	PrefixKey        string  `json:"prefix_key"`
	PrefixChars      int     `json:"prefix_chars"`
	Reason           string  `json:"reason"`
	WinnerPrefixKey  string  `json:"winner_prefix_key,omitempty"`
	WinnerRequestID  string  `json:"winner_request_id,omitempty"`
	Similarity       float64 `json:"similarity"`
	Threshold        float64 `json:"threshold"`
	ThresholdMargin  float64 `json:"threshold_margin"`
	OccupiedAt       int64   `json:"occupied_at"`
	WinnerOccupiedAt int64   `json:"winner_occupied_at"`
	Note             string  `json:"note"`
}

// CommitResult summarizes an atomic revision.
type CommitResult struct {
	Version            int64          `json:"version"`
	Kind               string         `json:"kind"`
	PolicyDigest       string         `json:"policy_digest"`
	VersionFingerprint string         `json:"version_fingerprint"`
	StageDigests       StageDigests   `json:"stage_digests"`
	SlotDigest         string         `json:"slot_digest"`
	AllocationSequence int64          `json:"allocation_sequence"`
	InvalidatedStage   Stage          `json:"invalidated_stage,omitempty"`
	Invalidations      []Invalidation `json:"invalidations"`
	Evictions          []Eviction     `json:"evictions"`
	SlotCount          int            `json:"slot_count"`
	ReusedSlotCount    int            `json:"reused_slot_count"`
}

// Status is a read-only snapshot of the effective cache head.
type Status struct {
	CurrentVersion     int64        `json:"current_version"`
	Policy             Policy       `json:"policy"`
	PolicyDigest       string       `json:"policy_digest"`
	VersionFingerprint string       `json:"version_fingerprint"`
	StageDigests       StageDigests `json:"stage_digests"`
	SlotDigest         string       `json:"slot_digest"`
	AllocationSequence int64        `json:"allocation_sequence"`
	SlotOccupancy      int          `json:"slot_occupancy"`
	OccupancyBudget    int          `json:"occupancy_budget"`
	SlotCount          int          `json:"slot_count"`
	RequestCount       int          `json:"request_count"`
	InvalidationCount  int          `json:"invalidation_count"`
	EvictionCount      int          `json:"eviction_count"`
	DatabasePath       string       `json:"database_path,omitempty"`
}

// Snapshot exposes immutable versioned state.
type Snapshot struct {
	Version            int64          `json:"version"`
	Kind               string         `json:"kind"`
	Policy             Policy         `json:"policy"`
	PolicyDigest       string         `json:"policy_digest"`
	ParentVersion      int64          `json:"parent_version"`
	ParentDigest       string         `json:"parent_digest"`
	VersionFingerprint string         `json:"version_fingerprint"`
	StageDigests       StageDigests   `json:"stage_digests"`
	SlotDigest         string         `json:"slot_digest"`
	AllocationSequence int64          `json:"allocation_sequence"`
	Slots              []Slot         `json:"slots"`
	States             []RequestState `json:"states"`
	Invalidations      []Invalidation `json:"invalidations"`
	Evictions          []Eviction     `json:"evictions"`
	CreatedAt          time.Time      `json:"created_at"`
}

// Dataset is deterministic demo input.
type Dataset struct {
	Seed     int64     `json:"seed"`
	Policy   Policy    `json:"policy"`
	Requests []Request `json:"requests"`
}

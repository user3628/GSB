package domain

// Declared process/service record from the JSON-lines manifest.
type ProcessDecl struct {
	PID           int64    `json:"pid"`
	ParentPID     int64    `json:"parent_pid"`
	ExePath       string   `json:"exe_path"`
	Account       string   `json:"account"`
	InheritedFrom int64    `json:"inherited_from"`
	InheritedMask uint64   `json:"inherited_mask"`
	ServiceBin    string   `json:"service_bin"`
	ServiceWrite  int      `json:"service_write"`
	SourceOffset  int64    `json:"-"`
	HandleIDs     []string `json:"handle_ids"`
}

// Suspicion event from the length-prefixed event stream.
type Event struct {
	Seq          int64  `json:"seq"`
	SubjectPID   int64  `json:"subject_pid"`
	TargetPID    int64  `json:"target_pid"`
	TargetObject string `json:"target_object"`
	Op           string `json:"op"`
	Mask         uint64 `json:"mask"`
	OffsetNS     int64  `json:"offset_ns"`
	SourceOffset int64  `json:"-"`
}

// Bundle is the normalized in-memory representation of one sample batch.
type Bundle struct {
	Version   string
	Processes []ProcessDecl
	Events    []Event
}

// LedgerItem records one truncation remainder at the finer 1e-12 scale.
type LedgerItem struct {
	ProcPID   int64  `json:"proc_pid"`
	EventSeq  int64  `json:"event_seq"`
	Kind      string `json:"kind"`
	Remainder int64  `json:"remainder"`
}

// ProcLedger aggregates truncation error for a single process.
type ProcLedger struct {
	PID          int64 `json:"pid"`
	RemainderSum int64 `json:"remainder_sum"`
	ItemCount    int   `json:"item_count"`
	BoundMicro   int64 `json:"bound_micro"`
}

// Range is an integer fixed-point score interval at scale 1e-6.
type Range struct {
	Low  int64 `json:"low_micro"`
	High int64 `json:"high_micro"`
}

// Reason records one deterministic scoring contribution.
type Reason struct {
	Kind   string `json:"kind"`
	Weight int64  `json:"weight_micro"`
	Detail string `json:"detail"`
}

// ProcConclusion is the per-process verdict of one detection round.
type ProcConclusion struct {
	PID        int64      `json:"pid"`
	ScoreMicro int64      `json:"score_micro"`
	BoundMicro int64      `json:"bound_micro"`
	Range      Range      `json:"range"`
	Code       BizCode    `json:"code"`
	Reasons    []Reason   `json:"reasons"`
	Ledger     ProcLedger `json:"ledger"`
	Trace      []TraceHop `json:"trace"`
}

// TraceHop is one edge in the handle/process provenance chain.
type TraceHop struct {
	FromPID int64  `json:"from_pid"`
	ToPID   int64  `json:"to_pid"`
	Edge    string `json:"edge"`
	Detail  string `json:"detail"`
}

// RoundResult is the complete deterministic output of one detection round.
type RoundResult struct {
	SampleVersion string           `json:"sample_version"`
	SampleHash    string           `json:"sample_hash"`
	Conclusions   []ProcConclusion `json:"conclusions"`
	Orphans       []int64          `json:"orphans"`
	SampledAt     string           `json:"sampled_at"`
	ElapsedMicros int64            `json:"elapsed_micros"`
}

// VirtualRootPID is the synthetic parent of orphan processes.
const VirtualRootPID int64 = -1

// FixedSampledAt is the only time marker ever emitted in reports.
const FixedSampledAt = "T0+SAMPLE_MARK"

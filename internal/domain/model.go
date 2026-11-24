package domain

import "time"

const (
	FormatBinary = "binary"
	FormatJSONL  = "jsonl"

	ActionAccept   = "accepted"
	ActionDegraded = "degraded_accepted"
	ActionReject   = "rejected"

	PolicyModeSPKI = "spki_sha256"
	PolicyModeRoot = "trusted_root_sha256"

	EventHandshakeStart      = "handshake_start"
	EventCertificateReceived = "certificate_received"
	EventPinVerification     = "pin_verification"
	EventHandshakeComplete   = "handshake_complete"
	EventAbnormalTermination = "abnormal_termination"

	StateEmpty        = ""
	StateStarted      = "HANDSHAKE_STARTED"
	StateCertReceived = "CERTIFICATE_RECEIVED"
	StateChecked      = "PIN_CHECKED"
	StateCompleted    = "HANDSHAKE_COMPLETED"
	StateAborted      = "ABNORMAL_TERMINATED"

	ConclusionTrusted      = "trusted"
	ConclusionMITM         = "suspected_mitm"
	ConclusionInsufficient = "insufficient_evidence"
)

type Event struct {
	Seq            int    `json:"seq"`
	Type           string `json:"type"`
	OffsetNS       int64  `json:"offset_ns,omitempty"`
	TimeRFC3339    string `json:"time,omitempty"`
	PolicyVersion  string `json:"policy_version,omitempty"`
	ObservedResult string `json:"observed_result,omitempty"`
}

type Policy struct {
	Version      string   `json:"version"`
	Mode         string   `json:"mode"`
	SPKIPins     []string `json:"spki_pins,omitempty"`
	TrustedRoots []string `json:"trusted_roots,omitempty"`
}

type PolicyTimelineEntry struct {
	Policy
	EffectiveOffsetNS int64  `json:"effective_offset_ns,omitempty"`
	EffectiveTime     string `json:"effective_time,omitempty"`
	EffectiveAtMS     int64  `json:"effective_at_ms,omitempty"`
}

type CertEntry struct {
	PEM        string `json:"pem"`
	Role       string `json:"role,omitempty"`
	SerialHint string `json:"serial_hint,omitempty"`
}

type Record struct {
	Format              string                `json:"format,omitempty"`
	FormatVersion       int                   `json:"format_version"`
	ConnectionID        string                `json:"connection_id"`
	SNI                 string                `json:"sni"`
	ProtocolVersion     string                `json:"protocol_version,omitempty"`
	ObservedStartMS     int64                 `json:"observed_start_unix_ms,omitempty"`
	CertChainPEM        string                `json:"cert_chain_pem,omitempty"`
	Chain               []CertEntry           `json:"chain,omitempty"`
	Policy              Policy                `json:"policy"`
	PolicyTimeline      []PolicyTimelineEntry `json:"policy_timeline,omitempty"`
	Events              []Event               `json:"events"`
	RequiredFutureField any                   `json:"future_required,omitempty"`
}

type Degradation struct {
	RecordIndex  int    `json:"record_index,omitempty"`
	ConnectionID string `json:"connection_id,omitempty"`
	Field        string `json:"field"`
	Rule         string `json:"rule"`
	Supplied     string `json:"supplied,omitempty"`
	Filled       string `json:"filled"`
	Reason       string `json:"reason"`
}

type Negotiation struct {
	Format           string        `json:"format"`
	RequestedVersion int           `json:"requested_version"`
	EffectiveVersion int           `json:"effective_version"`
	Action           string        `json:"action"`
	Code             string        `json:"code,omitempty"`
	Message          string        `json:"message,omitempty"`
	Degradations     []Degradation `json:"degradations,omitempty"`
	RequiredFields   []string      `json:"required_fields,omitempty"`
}

type RoundingInfo struct {
	EventSeq                  int    `json:"event_seq"`
	EventType                 string `json:"event_type"`
	RawUnixNS                 int64  `json:"raw_unix_ns"`
	TimestampMS               int64  `json:"timestamp_ms"`
	RemainderNS               int64  `json:"remainder_ns"`
	RoundingBit               int    `json:"rounding_bit"`
	RoundedUp                 bool   `json:"rounded_up"`
	AppliedDeltaNS            int64  `json:"applied_delta_ns"`
	CumulativeRoundingErrorNS int64  `json:"cumulative_rounding_error_ns"`
}

type NormalizedEvent struct {
	Event
	RawUnixNS     int64        `json:"raw_unix_ns"`
	TimestampMS   int64        `json:"timestamp_ms"`
	Rounding      RoundingInfo `json:"rounding"`
	PolicyVersion string       `json:"active_policy_version"`
}

type PolicyEntryResult struct {
	Value   string `json:"value"`
	Matched bool   `json:"matched"`
}

type CertValidity struct {
	Subject             string `json:"subject"`
	Issuer              string `json:"issuer"`
	Serial              string `json:"serial"`
	SPKISHA256          string `json:"spki_sha256"`
	CertSHA256          string `json:"cert_sha256"`
	NotBeforeMS         int64  `json:"not_before_ms"`
	NotAfterMS          int64  `json:"not_after_ms"`
	ValidityRemainingMS int64  `json:"validity_remaining_ms"`
	SupportedSignature  bool   `json:"supported_signature"`
	SignatureAlgorithm  string `json:"signature_algorithm"`
	IsCA                bool   `json:"is_ca"`
}

type ChainReport struct {
	Valid          bool                `json:"valid"`
	OrderedSerials []string            `json:"ordered_serials"`
	Certificates   []CertValidity      `json:"certificates"`
	RootSHA256     string              `json:"root_sha256,omitempty"`
	RootTrusted    bool                `json:"root_trusted"`
	SNIMatched     bool                `json:"sni_matched"`
	PolicyMode     string              `json:"policy_mode"`
	PolicyMatched  bool                `json:"policy_matched"`
	EntryResults   []PolicyEntryResult `json:"entry_results"`
	HitEntries     []string            `json:"hit_entries"`
	MissedEntries  []string            `json:"missed_entries"`
	Findings       []Evidence          `json:"findings,omitempty"`
	ErrorCode      string              `json:"error_code,omitempty"`
}

type Evidence struct {
	Seq           int            `json:"seq"`
	EventSeq      int            `json:"event_seq,omitempty"`
	PolicyVersion string         `json:"policy_version,omitempty"`
	Kind          string         `json:"kind"`
	Severity      string         `json:"severity"`
	Code          string         `json:"code,omitempty"`
	Message       string         `json:"message"`
	Details       map[string]any `json:"details,omitempty"`
	ObservedAtMS  int64          `json:"observed_at_ms,omitempty"`
}

type EventJudgment struct {
	EventSeq      int          `json:"event_seq"`
	EventType     string       `json:"event_type"`
	StateBefore   string       `json:"state_before"`
	StateAfter    string       `json:"state_after"`
	TimestampMS   int64        `json:"timestamp_ms"`
	PolicyVersion string       `json:"policy_version,omitempty"`
	Result        string       `json:"result,omitempty"`
	Chain         *ChainReport `json:"chain,omitempty"`
	Error         *Error       `json:"error,omitempty"`
}

type Conclusion struct {
	ConnectionID          string            `json:"connection_id"`
	CanonicalConnectionID string            `json:"canonical_connection_id"`
	SNI                   string            `json:"sni"`
	ProtocolVersion       string            `json:"protocol_version"`
	State                 string            `json:"state"`
	Conclusion            string            `json:"conclusion"`
	ReasonCode            string            `json:"reason_code,omitempty"`
	PolicyVersionSequence []string          `json:"policy_version_sequence"`
	PolicyVersions        []string          `json:"policy_versions"`
	PolicyDocuments       map[string]Policy `json:"policy_documents,omitempty"`
	EvidenceChain         []Evidence        `json:"evidence_chain"`
	EventJudgments        []EventJudgment   `json:"event_judgments"`
	Rounding              []RoundingInfo    `json:"rounding"`
	CommitVersion         int64             `json:"commit_version"`
	Negotiation           *Negotiation      `json:"negotiation,omitempty"`
}

type StoredPolicyVersion struct {
	Policy
	Status          string `json:"status"`
	AvailableFromMS int64  `json:"available_from_ms"`
	RetiredAtMS     int64  `json:"retired_at_ms,omitempty"`
	ReplacedBy      string `json:"replaced_by,omitempty"`
	CreatedCommit   int64  `json:"created_commit"`
}

type RecoveryReport struct {
	StartedAtMS          int64          `json:"started_at_ms"`
	FinishedAtMS         int64          `json:"finished_at_ms"`
	LastCompleteCommit   int64          `json:"last_complete_commit"`
	ScannedIntents       int            `json:"scanned_intents"`
	AbortedIntents       []IntentRecord `json:"aborted_intents"`
	AlreadyComplete      []IntentRecord `json:"already_complete"`
	VisibleConnectionIDs []string       `json:"visible_connection_ids"`
	Facts                []string       `json:"facts"`
}

type IntentRecord struct {
	IntentID      string `json:"intent_id"`
	CommitVersion int64  `json:"commit_version"`
	Kind          string `json:"kind"`
	Status        string `json:"status"`
	BatchID       string `json:"batch_id,omitempty"`
	PayloadJSON   string `json:"payload_json,omitempty"`
	CreatedAtMS   int64  `json:"created_at_ms"`
	UpdatedAtMS   int64  `json:"updated_at_ms"`
}

func NowMS() int64 { return time.Now().UnixMilli() }

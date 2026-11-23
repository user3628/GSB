// Package codes defines stable business error codes and the canonical
// intermediate event model shared by every parser, correlator and reporter.
package codes

// Business codes. These strings are part of the report contract and must stay
// stable across releases; do not renumber or rename.
const (
	// EOK marks a fully healthy record.
	EOK = "E_OK"
	// EUnknownEvent: event type enumeration not understood by this parser.
	EUnknownEvent = "E_UNKNOWN_EVENT"
	// ETimeOverflow: timestamp conversion overflows int64 or loses precision.
	ETimeOverflow = "E_TIME_OVERFLOW"
	// EOrphanRef: referenced parent/session process has no creation event.
	EOrphanRef = "E_ORPHAN_REF"
	// ESnapshotVersionIncompat: snapshot format versions are not loadable.
	ESnapshotVersionIncompat = "E_SNAPSHOT_VERSION_INCOMPAT"
	// ETruncatedRecord: a length-prefixed record runs past the available bytes.
	ETruncatedRecord = "E_TRUNCATED_RECORD"
	// EJSONFieldType: a JSON line field has the wrong primitive type.
	EJSONFieldType = "E_JSON_FIELD_TYPE"
	// ELengthOverrun: text block length prefix exceeds remaining bytes.
	ELengthOverrun = "E_LENGTH_OVERRUN"
	// EUnknownBinaryVersion: binary header version is not supported.
	EUnknownBinaryVersion = "E_UNKNOWN_BINARY_VERSION"
	// EEventBudget: per-session event budget exhausted.
	EEventBudget = "E_EVENT_BUDGET"
	// EChecksum: tail check block does not match the body.
	EChecksum = "E_CHECKSUM"
	// EBadHeader: magic/header is unrecognised.
	EBadHeader = "E_BAD_HEADER"
)

// Source format identifiers.
const (
	SourceBinary = "binary"
	SourceJSONL  = "jsonl"
	SourceText   = "text"
)

// Format versions understood by this build.
const (
	FmtBinaryV1 = "binary/v1"
	FmtBinaryV2 = "binary/v2"
	FmtJSONLV1  = "jsonl/v1"
	FmtTextV1   = "text/v1"
)

// EventType is the normalized, format-independent event family.
type EventType string

const (
	EvProcessCreate EventType = "process_create"
	EvProcessExit   EventType = "process_exit"
	EvFileAccess    EventType = "file_access"
	EvRegistry      EventType = "registry_write"
	EvNetwork       EventType = "network_connect"
	EvModuleLoad    EventType = "module_load" // v2 only
	EvUnknown       EventType = "unknown"
)

// Timestamp encoding families used by the raw sources.
type TimeEncoding string

const (
	TimeEpochMS  TimeEncoding = "epoch_ms"
	TimeRFC3339  TimeEncoding = "rfc3339"
	TimeFiletime TimeEncoding = "filetime"
)

// Event is the canonical intermediate record produced by every parser.
type Event struct {
	// ID is a deterministic identifier assigned after parsing (see report pkg).
	ID string `json:"id,omitempty"`
	// Source is one of the Source* constants.
	Source string `json:"source"`
	// FormatVersion is the exact parser format version that decoded this event.
	FormatVersion string `json:"format_version"`
	// SourcePath is the absolute or workspace-relative input path.
	SourcePath string `json:"source_path"`
	// Offset is the byte offset at which the record begins in its source.
	Offset int64 `json:"offset"`
	// EndOffset is the byte immediately after the record (skip range end).
	EndOffset int64 `json:"end_offset"`
	// Seq is the zero-based ordinal of the record inside its source.
	Seq int `json:"seq"`
	// Type is the normalized event family; EvUnknown when unrecognised.
	Type EventType `json:"type"`
	// RawType is the original, format-specific type token.
	RawType string `json:"raw_type"`
	// Subject is the acting principal (user/account).
	Subject string `json:"subject"`
	// Object is the target (path/key/address/command line).
	Object string `json:"object"`
	// SessionID groups process lineage; never cross-linked between sessions.
	SessionID string `json:"session_id"`
	// PID / PPID carry process lineage when present, else 0.
	PID  int64 `json:"pid"`
	PPID int64 `json:"ppid"`
	// TimeEncoding records how RawTime should be interpreted.
	TimeEncoding TimeEncoding `json:"time_encoding"`
	// RawTime is the untouched original timestamp value.
	RawTime string `json:"raw_time"`
	// TimeUnixNano is UTC nanoseconds since the Unix epoch; nil on failure.
	TimeUnixNano *int64 `json:"time_unix_nano,omitempty"`
	// Payload is the verbatim raw record bytes, always retained.
	Payload []byte `json:"payload_b64"`
	// Code is the per-event business code (EOK when healthy).
	Code string `json:"code"`
}

// ErrorRecord describes one malformed/failed decode with a stable code and the
// exact byte range that was skipped.
type ErrorRecord struct {
	SourcePath string `json:"source_path"`
	Source     string `json:"source"`
	Code       string `json:"code"`
	Message    string `json:"message"`
	Offset     int64  `json:"offset"`
	EndOffset  int64  `json:"end_offset"`
}

// Key returns the aggregation key used by the scheduler/report summaries.
func (e ErrorRecord) Key() string { return e.SourcePath + "|" + e.Code }

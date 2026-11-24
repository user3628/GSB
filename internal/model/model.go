package model

import (
	"fmt"
	"sort"
	"strings"
)

const (
	CodeFormatVersion     = "E_FORMAT_VERSION"
	CodeTruncated         = "E_TRUNCATED"
	CodeDigestInvalid     = "E_DIGEST_INVALID"
	CodeDigestConflict    = "E_DIGEST_CONFLICT"
	CodeVersionDowngrade  = "E_VERSION_DOWNGRADE"
	CodeWeakDigest        = "E_WEAK_DIGEST"
	CodeComponentBudget   = "E_COMPONENT_BUDGET"
	CodeSnapshotVersion   = "E_SNAPSHOT_VERSION"
	CodeRecordInvalid     = "E_RECORD_INVALID"
	CodeSourceIO          = "E_SOURCE_IO"
	CodeInvalidConfig     = "E_INVALID_CONFIG"
	CodeConsistencyFailed = "E_CONSISTENCY"
)

const (
	FormatBinary    SourceFormat = "binary"
	FormatJSONL     SourceFormat = "jsonl"
	FormatTextBlock SourceFormat = "textblock"
)

// SourceFormat is the stable on-disk format family name.
type SourceFormat string

func (f SourceFormat) Valid() bool {
	return f == FormatBinary || f == FormatJSONL || f == FormatTextBlock
}

func (f SourceFormat) Label() string {
	switch f {
	case FormatBinary:
		return "binary-collector"
	case FormatJSONL:
		return "json-export"
	case FormatTextBlock:
		return "textblock-legacy"
	default:
		return string(f)
	}
}

// CodeError carries the machine-readable business code required by the CLI and
// reports. BytesRead is meaningful for truncation diagnostics.
type CodeError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Offset    int64  `json:"offset,omitempty"`
	BytesRead int64  `json:"bytes_read,omitempty"`
}

func (e *CodeError) Error() string {
	if e == nil {
		return ""
	}
	if e.Offset != 0 || e.BytesRead != 0 {
		return fmt.Sprintf("%s at offset %d (bytes_read=%d): %s", e.Code, e.Offset, e.BytesRead, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewCodeError(code, message string, offset, bytesRead int64) *CodeError {
	return &CodeError{Code: code, Message: message, Offset: offset, BytesRead: bytesRead}
}

func CodeOf(err error) string {
	if ce, ok := err.(*CodeError); ok {
		return ce.Code
	}
	return ""
}

// SourceSpec describes one isolated parse session input.
type SourceSpec struct {
	ID         string       `json:"id"`
	Format     SourceFormat `json:"format"`
	Path       string       `json:"-"`
	ReportPath string       `json:"path"`
}

// PreReleaseIdentifier preserves the original SemVer identifier while carrying
// a comparable representation.
type PreReleaseIdentifier struct {
	Raw     string `json:"raw"`
	Numeric bool   `json:"numeric"`
	Number  uint64 `json:"number,omitempty"`
}

// VersionView normalizes SemVer and four-part integer versions. Canonical does
// not contain build metadata because SemVer precedence ignores it. The original
// spelling remains in Raw.
type VersionView struct {
	Raw        string                 `json:"raw"`
	Canonical  string                 `json:"canonical"`
	Core       [4]uint64              `json:"core"`
	Prerelease []PreReleaseIdentifier `json:"prerelease"`
	Build      string                 `json:"build,omitempty"`
}

// DigestView retains both raw input and a canonical algorithm + bytes view.
type DigestView struct {
	Raw           string `json:"raw"`
	Algorithm     string `json:"algorithm"`
	Hex           string `json:"hex"`
	Base64        string `json:"base64"`
	ByteLength    int    `json:"byte_length"`
	NominalBits   int    `json:"nominal_bits"`
	EffectiveBits int    `json:"effective_bits"`
	Truncated     bool   `json:"truncated"`
}

func (d DigestView) Equal(other DigestView) bool {
	return d.Algorithm == other.Algorithm && d.Hex == other.Hex
}

// Declaration is the normalized record model emitted by every parser.
type Declaration struct {
	ID           string       `json:"id"`
	SourceID     string       `json:"source_id"`
	SourcePath   string       `json:"source_path"`
	SourceFormat SourceFormat `json:"source_format"`
	Origin       string       `json:"origin"`
	Offset       int64        `json:"offset"`
	RecordIndex  int          `json:"record_index"`
	ComponentKey string       `json:"component_key"`
	RawName      string       `json:"raw_name"`
	Version      VersionView  `json:"version"`
	Digest       DigestView   `json:"digest"`
	RawDigest    string       `json:"raw_digest"`
	Signature    string       `json:"signature,omitempty"`
	Codes        []string     `json:"codes"`
}

// ParseIssue is a source- or record-level diagnostic. Parsers keep valid
// records when the format still permits forward progress.
type ParseIssue struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Offset    int64  `json:"offset"`
	BytesRead int64  `json:"bytes_read,omitempty"`
	Record    int    `json:"record_index,omitempty"`
}

// SourceResult is the isolated outcome of one parse session.
type SourceResult struct {
	Spec             SourceSpec    `json:"spec"`
	FormatVersion    string        `json:"format_version"`
	Declarations     []Declaration `json:"declarations"`
	Issues           []ParseIssue  `json:"issues"`
	BytesRead        int64         `json:"bytes_read"`
	ParsedCount      int           `json:"parsed_count"`
	BudgetStopped    bool          `json:"budget_stopped"`
	DigestAlgorithms []string      `json:"digest_algorithms"`
}

func (r *SourceResult) AddIssue(code, message string, offset, bytesRead int64, record int) {
	r.Issues = append(r.Issues, ParseIssue{
		Code:      code,
		Message:   message,
		Offset:    offset,
		BytesRead: bytesRead,
		Record:    record,
	})
}

func (r *SourceResult) HasCode(code string) bool {
	for _, issue := range r.Issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}

func (r *SourceResult) Finalize() {
	r.ParsedCount = len(r.Declarations)
	set := make(map[string]struct{})
	for _, decl := range r.Declarations {
		set[decl.Digest.Algorithm] = struct{}{}
	}
	r.DigestAlgorithms = r.DigestAlgorithms[:0]
	for algorithm := range set {
		r.DigestAlgorithms = append(r.DigestAlgorithms, algorithm)
	}
	sort.Strings(r.DigestAlgorithms)
	sort.SliceStable(r.Issues, func(i, j int) bool {
		if r.Issues[i].Offset != r.Issues[j].Offset {
			return r.Issues[i].Offset < r.Issues[j].Offset
		}
		if r.Issues[i].Code != r.Issues[j].Code {
			return r.Issues[i].Code < r.Issues[j].Code
		}
		return r.Issues[i].Message < r.Issues[j].Message
	})
}

// Evidence identifies one declaration in a finding.
type Evidence struct {
	DeclarationID string       `json:"declaration_id"`
	SourceID      string       `json:"source_id"`
	SourcePath    string       `json:"source_path"`
	SourceFormat  SourceFormat `json:"source_format"`
	Offset        int64        `json:"offset"`
	ComponentKey  string       `json:"component_key"`
	Version       string       `json:"version"`
	Algorithm     string       `json:"algorithm,omitempty"`
	DigestHex     string       `json:"digest_hex,omitempty"`
	RawDigest     string       `json:"raw_digest,omitempty"`
}

// Finding is a business-code conclusion with a deterministic evidence pair.
type Finding struct {
	Code         string    `json:"code"`
	ComponentKey string    `json:"component_key"`
	Version      string    `json:"version"`
	Message      string    `json:"message"`
	Primary      Evidence  `json:"primary"`
	Secondary    *Evidence `json:"secondary,omitempty"`
}

func FindingsHaveCode(findings []Finding, code string) bool {
	for _, finding := range findings {
		if finding.Code == code {
			return true
		}
	}
	return false
}

func HasCode(codes []string, code string) bool {
	for _, candidate := range codes {
		if candidate == code {
			return true
		}
	}
	return false
}

func AppendUniqueCode(codes []string, code string) []string {
	if HasCode(codes, code) {
		return codes
	}
	codes = append(codes, code)
	sort.Strings(codes)
	return codes
}

func NormalizeCodes(codes []string) []string {
	if codes == nil {
		return []string{}
	}
	seen := make(map[string]struct{}, len(codes))
	out := make([]string, 0, len(codes))
	for _, code := range codes {
		if code == "" {
			continue
		}
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		out = append(out, code)
	}
	sort.Strings(out)
	return out
}

func CanonicalJoin(parts ...string) string {
	return strings.Join(parts, "\x00")
}

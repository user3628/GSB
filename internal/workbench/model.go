package workbench

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	SchemaVersion = 1
	SampleClock   = "fixed-sample-clock"
	ToolName      = "dnswatch"

	FormatBinary = "binary"
	FormatJSON   = "json"
	FormatText   = "text"

	KindQuery    = "query"
	KindResponse = "response"
	KindCache    = "cache"
)

const (
	CodeMagicInvalid       = "E_MAGIC_INVALID"
	CodeTruncated          = "E_TRUNCATED"
	CodeVersionUnsupported = "E_VER_UNSUPPORTED"
	CodeNameConflict       = "E_NAME_CONFLICT"
	CodeUnsolicited        = "E_UNSOLICITED"
	CodeAdditionalInject   = "E_ADDITIONAL_INJECT"
	CodeTTLMismatch        = "E_TTL_MISMATCH"
	CodeFlipFlop           = "E_FLIP_FLOP"
	CodeEventBudget        = "E_EVENT_BUDGET"
	CodeLengthOverflow     = "E_LENGTH_OVERFLOW"
	CodeParse              = "E_PARSE"
	CodeSnapshotVersion    = "E_SNAPSHOT_VERSION"
	CodeSourceOpen         = "E_SOURCE_OPEN"
)

// BusinessError carries the stable machine code, source location, and human
// readable context required by the report and CLI.
type BusinessError struct {
	Code          string `json:"code"`
	Source        string `json:"source,omitempty"`
	Offset        int64  `json:"offset,omitempty"`
	Detail        string `json:"detail,omitempty"`
	RelatedSource string `json:"related_source,omitempty"`
	RelatedOffset int64  `json:"related_offset,omitempty"`
}

func (e *BusinessError) Error() string {
	if e.Source != "" {
		return fmt.Sprintf("%s at %s:%d: %s", e.Code, e.Source, e.Offset, e.Detail)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Detail)
}

func NewBusinessError(code, source string, offset int64, detail string) *BusinessError {
	return &BusinessError{Code: code, Source: source, Offset: offset, Detail: detail}
}

type Issue struct {
	Code          string `json:"code"`
	Source        string `json:"source"`
	Offset        int64  `json:"offset"`
	Time          string `json:"time,omitempty"`
	TxID          uint32 `json:"txid,omitempty"`
	Name          string `json:"name,omitempty"`
	Detail        string `json:"detail,omitempty"`
	RelatedSource string `json:"related_source,omitempty"`
	RelatedOffset int64  `json:"related_offset,omitempty"`
}

type Record struct {
	Name     string `json:"name"`
	NameWire string `json:"name_wire,omitempty"`
	Type     string `json:"type"`
	Class    uint16 `json:"class"`
	TTL      uint32 `json:"ttl"`
	Address  string `json:"address,omitempty"`
	RDataHex string `json:"rdata_hex,omitempty"`
	Offset   int64  `json:"offset"`
	Section  string `json:"section,omitempty"`
}

type Event struct {
	Source        string   `json:"source"`
	Format        string   `json:"format"`
	ParserVersion string   `json:"parser_version"`
	Kind          string   `json:"kind"`
	TxID          uint32   `json:"txid"`
	Flags         uint16   `json:"flags"`
	Name          string   `json:"name"`
	NameWire      string   `json:"name_wire"`
	Tick          uint64   `json:"tick"`
	Time          string   `json:"time"`
	Offset        int64    `json:"offset"`
	Answers       []Record `json:"answers,omitempty"`
	Additionals   []Record `json:"additionals,omitempty"`
}

func (e Event) Reference() EventRef {
	addresses := make([]string, 0)
	for _, record := range append(append([]Record{}, e.Answers...), e.Additionals...) {
		if record.Address != "" {
			addresses = append(addresses, record.Address)
		}
	}
	sort.Strings(addresses)
	return EventRef{
		Kind:          e.Kind,
		Source:        e.Source,
		Offset:        e.Offset,
		Time:          e.Time,
		Format:        e.Format,
		ParserVersion: e.ParserVersion,
		Addresses:     addresses,
	}
}

type EventRef struct {
	Kind          string   `json:"kind"`
	Source        string   `json:"source"`
	Offset        int64    `json:"offset"`
	Time          string   `json:"time"`
	Format        string   `json:"format"`
	ParserVersion string   `json:"parser_version"`
	Addresses     []string `json:"addresses,omitempty"`
}

type SourceSummary struct {
	Path           string   `json:"path"`
	Format         string   `json:"format"`
	ParserVersions []string `json:"parser_versions"`
	Bytes          int64    `json:"bytes"`
	Events         int      `json:"events"`
	Skipped        int      `json:"skipped"`
	Issues         []Issue  `json:"issues,omitempty"`
}

type TransactionReport struct {
	TxID      uint32     `json:"txid"`
	Name      string     `json:"name"`
	NameWire  string     `json:"name_wire"`
	Queries   []EventRef `json:"queries,omitempty"`
	Responses []EventRef `json:"responses,omitempty"`
	Caches    []EventRef `json:"caches,omitempty"`
	Verdicts  []Issue    `json:"verdicts,omitempty"`
}

type Compatibility struct {
	CurrentParserVersion  string   `json:"current_parser_version"`
	RequiredParserVersion string   `json:"required_parser_version"`
	Compatible            bool     `json:"compatible"`
	Code                  string   `json:"code,omitempty"`
	FormatVersions        []string `json:"format_versions"`
	Detail                string   `json:"detail,omitempty"`
}

type ChainEntry struct {
	Version               int      `json:"version"`
	ParentVersion         int      `json:"parent_version"`
	ParentChainHash       string   `json:"parent_chain_hash"`
	ContentHash           string   `json:"content_hash"`
	ChainHash             string   `json:"chain_hash"`
	RequiredParserVersion string   `json:"required_parser_version"`
	FormatVersions        []string `json:"format_versions"`
	TargetParserVersion   string   `json:"target_parser_version"`
	SampleClock           string   `json:"sample_clock"`
}

type SnapshotMetadata struct {
	Version               int          `json:"version"`
	ParentVersion         int          `json:"parent_version"`
	ParentChainHash       string       `json:"parent_chain_hash"`
	ContentHash           string       `json:"content_hash"`
	ChainHash             string       `json:"chain_hash"`
	RequiredParserVersion string       `json:"required_parser_version"`
	FormatVersions        []string     `json:"format_versions"`
	TargetParserVersion   string       `json:"target_parser_version"`
	SampleClock           string       `json:"sample_clock"`
	Chain                 []ChainEntry `json:"chain"`
}

type Report struct {
	SchemaVersion       int                 `json:"schema_version"`
	Tool                string              `json:"tool"`
	SampleClock         string              `json:"sample_clock"`
	TargetParserVersion string              `json:"target_parser_version"`
	Compatibility       Compatibility       `json:"compatibility"`
	Sources             []SourceSummary     `json:"sources"`
	Transactions        []TransactionReport `json:"transactions"`
	Findings            []Issue             `json:"findings"`
	Snapshot            *SnapshotMetadata   `json:"snapshot,omitempty"`
}

// SessionResult is produced by one isolated source parser.
type SessionResult struct {
	Source         string
	Format         string
	ParserVersions []string
	Bytes          int64
	Events         []Event
	Issues         []Issue
	Skipped        int
}

// Summary converts a session into the stable report summary.
func (s SessionResult) Summary() SourceSummary {
	versions := append([]string(nil), s.ParserVersions...)
	sort.Strings(versions)
	issues := append([]Issue(nil), s.Issues...)
	SortIssues(issues)
	return SourceSummary{
		Path:           s.Source,
		Format:         s.Format,
		ParserVersions: versions,
		Bytes:          s.Bytes,
		Events:         len(s.Events),
		Skipped:        s.Skipped,
		Issues:         issues,
	}
}

func SortIssues(issues []Issue) {
	sort.SliceStable(issues, func(i, j int) bool {
		a, b := issues[i], issues[j]
		if a.Source != b.Source {
			return a.Source < b.Source
		}
		if a.TxID != b.TxID {
			return a.TxID < b.TxID
		}
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		if a.Offset != b.Offset {
			return a.Offset < b.Offset
		}
		return a.RelatedSource < b.RelatedSource
	})
}

func SortSourceSummaries(sources []SourceSummary) {
	sort.SliceStable(sources, func(i, j int) bool { return sources[i].Path < sources[j].Path })
}

func SortTransactions(txs []TransactionReport) {
	sort.SliceStable(txs, func(i, j int) bool {
		if txs[i].TxID != txs[j].TxID {
			return txs[i].TxID < txs[j].TxID
		}
		return txs[i].Name < txs[j].Name
	})
}

func SortEventRefs(refs []EventRef) {
	sort.SliceStable(refs, func(i, j int) bool {
		if refs[i].Time != refs[j].Time {
			return refs[i].Time < refs[j].Time
		}
		if refs[i].Source != refs[j].Source {
			return refs[i].Source < refs[j].Source
		}
		return refs[i].Offset < refs[j].Offset
	})
}

// NormalizeName lowercases labels, removes the optional trailing root dot, and
// validates labels. The returned wire form is the length-prefixed canonical
// label sequence used as the cross-format identity.
func NormalizeName(input string) (string, []byte, error) {
	name := strings.TrimSpace(input)
	if name == "" {
		return "", nil, fmt.Errorf("empty question name")
	}
	name = strings.TrimSuffix(name, ".")
	if name == "" {
		return "", nil, fmt.Errorf("root-only names are not supported")
	}
	labels := strings.Split(name, ".")
	wire := make([]byte, 0, len(name)+2)
	lowered := make([]string, 0, len(labels))
	for _, label := range labels {
		if label == "" {
			return "", nil, fmt.Errorf("empty label in %q", input)
		}
		if len(label) > 63 {
			return "", nil, fmt.Errorf("label exceeds 63 bytes in %q", input)
		}
		label = strings.ToLower(label)
		if len(wire)+1+len(label)+1 > 255 {
			return "", nil, fmt.Errorf("name exceeds 255 wire bytes in %q", input)
		}
		wire = append(wire, byte(len(label)))
		wire = append(wire, []byte(label)...)
		lowered = append(lowered, label)
	}
	wire = append(wire, 0)
	return strings.Join(lowered, "."), wire, nil
}

func CanonicalNameKey(name string) (string, error) {
	_, wire, err := NormalizeName(name)
	if err != nil {
		return "", err
	}
	return WireNameKey(wire), nil
}

func WireNameKey(wire []byte) string {
	var b strings.Builder
	pos := 0
	for pos < len(wire) {
		size := int(wire[pos])
		pos++
		if size == 0 {
			break
		}
		fmt.Fprintf(&b, "%d:", size)
		b.WriteString(string(wire[pos : pos+size]))
		b.WriteByte('|')
		pos += size
	}
	return b.String()
}

func ParseWireName(data []byte) (name string, wire []byte, next int, err error) {
	labels := make([]string, 0, 8)
	wire = make([]byte, 0, 32)
	pos := 0
	for {
		if pos >= len(data) {
			return "", nil, 0, fmt.Errorf("wire name truncated at byte %d", pos)
		}
		size := int(data[pos])
		wire = append(wire, data[pos])
		pos++
		if size == 0 {
			break
		}
		if size > 63 || pos+size > len(data) {
			return "", nil, 0, fmt.Errorf("invalid wire label length %d at byte %d", size, pos-1)
		}
		labels = append(labels, strings.ToLower(string(data[pos:pos+size])))
		wire = append(wire, data[pos:pos+size]...)
		pos += size
		if len(wire)+1 > 255 {
			return "", nil, 0, fmt.Errorf("wire name exceeds 255 bytes")
		}
	}
	if len(labels) == 0 {
		return "", nil, 0, fmt.Errorf("root-only names are not supported")
	}
	name = strings.Join(labels, ".")
	if _, _, normErr := NormalizeName(name); normErr != nil {
		return "", nil, 0, normErr
	}
	return name, wire, pos, nil
}

func ParseTxID(raw string) (uint32, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, fmt.Errorf("empty transaction id")
	}
	base := 10
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		base = 16
		s = s[2:]
	}
	value, err := strconv.ParseUint(s, base, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid 32-bit transaction id %q", raw)
	}
	return uint32(value), nil
}

func ParseUint32Raw(raw json.RawMessage) (uint32, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("missing value")
	}
	var number uint64
	if err := json.Unmarshal(raw, &number); err == nil {
		if number > 0xffffffff {
			return 0, fmt.Errorf("value exceeds uint32")
		}
		return uint32(number), nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return ParseTxID(text)
	}
	return 0, fmt.Errorf("unsupported numeric representation")
}

func ParseUint16Raw(raw json.RawMessage) (uint16, error) {
	value, err := ParseUint32Raw(raw)
	if err != nil {
		return 0, err
	}
	if value > 0xffff {
		return 0, fmt.Errorf("value exceeds uint16")
	}
	return uint16(value), nil
}

func ParseSampleMarker(raw string) (uint64, string, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, "", fmt.Errorf("empty sample marker")
	}
	digits := s
	if strings.HasPrefix(s, "S") || strings.HasPrefix(s, "s") {
		digits = s[1:]
	} else if strings.HasPrefix(s, "sample-") {
		digits = strings.TrimPrefix(s, "sample-")
	}
	tick, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("invalid sample marker %q", raw)
	}
	return tick, SampleLabel(tick), nil
}

func ParseSampleRaw(raw json.RawMessage) (uint64, string, error) {
	if len(raw) == 0 {
		return 0, SampleLabel(0), nil
	}
	var number uint64
	if err := json.Unmarshal(raw, &number); err == nil {
		return number, SampleLabel(number), nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return ParseSampleMarker(text)
	}
	return 0, "", fmt.Errorf("unsupported sample marker")
}

func SampleLabel(tick uint64) string {
	return fmt.Sprintf("S%06d", tick)
}

func TypeName(value uint16) string {
	switch value {
	case 1:
		return "A"
	case 2:
		return "NS"
	case 5:
		return "CNAME"
	case 6:
		return "SOA"
	case 12:
		return "PTR"
	case 15:
		return "MX"
	case 16:
		return "TXT"
	case 28:
		return "AAAA"
	case 33:
		return "SRV"
	case 41:
		return "OPT"
	default:
		return fmt.Sprintf("TYPE%d", value)
	}
}

func TypeCode(name string) (uint16, error) {
	s := strings.ToUpper(strings.TrimSpace(name))
	switch s {
	case "A":
		return 1, nil
	case "NS":
		return 2, nil
	case "CNAME":
		return 5, nil
	case "SOA":
		return 6, nil
	case "PTR":
		return 12, nil
	case "MX":
		return 15, nil
	case "TXT":
		return 16, nil
	case "AAAA":
		return 28, nil
	case "SRV":
		return 33, nil
	case "OPT":
		return 41, nil
	}
	if strings.HasPrefix(s, "TYPE") {
		return ParseTypeNumber(s[4:])
	}
	return ParseTypeNumber(s)
}

func ParseTypeNumber(s string) (uint16, error) {
	value, err := strconv.ParseUint(strings.TrimSpace(s), 10, 16)
	if err != nil {
		return 0, fmt.Errorf("invalid DNS type %q", s)
	}
	return uint16(value), nil
}

func ParseKind(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "query", "request", "q":
		return KindQuery, nil
	case "response", "answer", "reply", "r":
		return KindResponse, nil
	case "cache", "cached", "cache_lookup", "cache-observation":
		return KindCache, nil
	default:
		return "", fmt.Errorf("unknown event kind %q", raw)
	}
}

func HexBytes(value []byte) string {
	return hex.EncodeToString(value)
}

func RelPath(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(rel)
}

func AddressFromRData(typeCode uint16, data []byte) string {
	switch typeCode {
	case 1:
		if len(data) == 4 {
			addr, ok := netip.AddrFromSlice(data)
			if ok {
				return addr.String()
			}
		}
	case 28:
		if len(data) == 16 {
			addr, ok := netip.AddrFromSlice(data)
			if ok {
				return addr.String()
			}
		}
	}
	return ""
}

func IsAddressType(t string) bool {
	return t == "A" || t == "AAAA"
}

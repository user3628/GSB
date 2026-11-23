package domain

import "strings"

const (
	ReportMarker          = "DETERMINISTIC_SAMPLE_V1"
	ReportSchemaVersion   = "service-adjudication-report-v1"
	SnapshotSchemaVersion = "service-adjudication-snapshot-v1"
	GenesisHash           = "GENESIS"
)

type Code string

const (
	CodeFormatVersion       Code = "E_FORMAT_VERSION"
	CodePathMalformed       Code = "E_PATH_MALFORMED"
	CodePrivilegeExposure   Code = "E_PRIVILEGE_EXPOSURE"
	CodeAccountUnknown      Code = "E_ACCOUNT_UNKNOWN"
	CodeDeclarationConflict Code = "E_DECLARATION_CONFLICT"
	CodeSnapshotVersion     Code = "E_SNAPSHOT_VERSION"
	CodeServiceBudget       Code = "E_SERVICE_BUDGET"
	CodeSourceMalformed     Code = "E_SOURCE_MALFORMED"
)

var BlockingCodes = map[Code]bool{
	CodeFormatVersion:       true,
	CodePathMalformed:       true,
	CodeAccountUnknown:      true,
	CodeDeclarationConflict: true,
	CodeSnapshotVersion:     true,
	CodeServiceBudget:       true,
	CodeSourceMalformed:     true,
}

type SourceKind string

const (
	SourceBinaryPolicy SourceKind = "binary_policy"
	SourceJSONManifest SourceKind = "json_manifest"
	SourceLegacyText   SourceKind = "legacy_text"
)

var DefaultSourcePriorities = []SourceKind{
	SourceBinaryPolicy,
	SourceJSONManifest,
	SourceLegacyText,
}

func ParseSourceKind(value string) (SourceKind, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "binary", "binary_policy", "policy":
		return SourceBinaryPolicy, true
	case "json", "json_manifest", "manifest", "jsonl":
		return SourceJSONManifest, true
	case "text", "legacy", "legacy_text", "textblock":
		return SourceLegacyText, true
	default:
		return "", false
	}
}

type StartType string

const (
	StartBoot      StartType = "boot"
	StartSystem    StartType = "system"
	StartAutomatic StartType = "automatic"
	StartManual    StartType = "manual"
	StartDisabled  StartType = "disabled"
	StartUnknown   StartType = "unknown"
)

func NormalizeStartType(value string) (StartType, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "boot", "boot_start":
		return StartBoot, true
	case "system", "system_start":
		return StartSystem, true
	case "auto", "automatic", "auto_start", "autostart":
		return StartAutomatic, true
	case "manual", "demand", "demand_start":
		return StartManual, true
	case "disabled", "disable":
		return StartDisabled, true
	default:
		return StartUnknown, false
	}
}

type AccountKind string

const (
	AccountUnknown        AccountKind = "Unknown"
	AccountCustom         AccountKind = "Custom"
	AccountLocalService   AccountKind = "LocalService"
	AccountNetworkService AccountKind = "NetworkService"
	AccountLocalSystem    AccountKind = "LocalSystem"
)

func AccountLevel(kind AccountKind) int {
	switch kind {
	case AccountLocalSystem:
		return 4
	case AccountNetworkService:
		return 3
	case AccountLocalService:
		return 2
	case AccountCustom:
		return 1
	default:
		return 0
	}
}

func AccountKindFromEnum(value uint8) AccountKind {
	switch value {
	case 1:
		return AccountLocalSystem
	case 2:
		return AccountNetworkService
	case 3:
		return AccountLocalService
	case 4:
		return AccountCustom
	default:
		return AccountUnknown
	}
}

func ClassifyAccount(raw string) AccountKind {
	normalized := NormalizeWindowsToken(raw)
	switch normalized {
	case "", "unknown", "n/a", "none", "???", "?":
		return AccountUnknown
	case "localsystem", "local system", "nt authority\\system", "system":
		return AccountLocalSystem
	case "networkservice", "network service", "nt authority\\networkservice":
		return AccountNetworkService
	case "localservice", "local service", "nt authority\\localservice":
		return AccountLocalService
	}
	if strings.HasPrefix(normalized, "nt authority\\") {
		return AccountUnknown
	}
	return AccountCustom
}

func NormalizeWindowsToken(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.ReplaceAll(value, "\\", "/")
	for strings.Contains(value, "  ") {
		value = strings.ReplaceAll(value, "  ", " ")
	}
	return value
}

func NormalizeServiceName(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

type LogonCapability string

const (
	LogonInteractive LogonCapability = "interactive"
	LogonNetwork     LogonCapability = "network"
	LogonService     LogonCapability = "service"
	LogonBatch       LogonCapability = "batch"
	LogonDenied      LogonCapability = "denied"
)

var logonOrder = []LogonCapability{
	LogonInteractive,
	LogonNetwork,
	LogonService,
	LogonBatch,
	LogonDenied,
}

func NormalizeLogonCapability(value string) (LogonCapability, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "interactive":
		return LogonInteractive, true
	case "network":
		return LogonNetwork, true
	case "service", "servicesvc":
		return LogonService, true
	case "batch":
		return LogonBatch, true
	case "denied", "none":
		return LogonDenied, true
	default:
		return "", false
	}
}

func SortLogonCapabilities(values []LogonCapability) []LogonCapability {
	seen := make(map[LogonCapability]bool, len(values))
	out := make([]LogonCapability, 0, len(values))
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	ordered := make([]LogonCapability, 0, len(out))
	for _, candidate := range logonOrder {
		if seen[candidate] {
			ordered = append(ordered, candidate)
		}
	}
	return ordered
}

func LogonCapabilitiesFromMask(mask uint16) ([]LogonCapability, bool) {
	values := make([]LogonCapability, 0, 5)
	known := uint16(0x1f)
	if mask&^known != 0 {
		return nil, false
	}
	pairs := []struct {
		bit uint16
		cap LogonCapability
	}{
		{1, LogonInteractive},
		{2, LogonNetwork},
		{4, LogonService},
		{8, LogonBatch},
		{16, LogonDenied},
	}
	for _, pair := range pairs {
		if mask&pair.bit != 0 {
			values = append(values, pair.cap)
		}
	}
	return values, true
}

type ByteRange struct {
	Offset int64 `json:"offset"`
	End    int64 `json:"end"`
}

type SourceRef struct {
	SourceID      string     `json:"source_id"`
	Kind          SourceKind `json:"kind"`
	FormatVersion string     `json:"format_version"`
	Offset        int64      `json:"offset"`
	End           int64      `json:"end"`
	RecordIndex   int        `json:"record_index"`
	Fingerprint   string     `json:"fingerprint"`
}

type Issue struct {
	Code        Code       `json:"code"`
	SourceID    string     `json:"source_id,omitempty"`
	SourceKind  SourceKind `json:"source_kind,omitempty"`
	Offset      int64      `json:"offset"`
	End         int64      `json:"end"`
	ServiceName string     `json:"service_name,omitempty"`
	Field       string     `json:"field,omitempty"`
	Detail      string     `json:"detail"`
}

type PathAssessment struct {
	Original             string   `json:"original"`
	Normalized           string   `json:"normalized"`
	Arguments            string   `json:"arguments,omitempty"`
	ImagePathKey         string   `json:"image_path_key"`
	Quoted               bool     `json:"quoted"`
	HasSpace             bool     `json:"has_space"`
	Unquoted             bool     `json:"unquoted"`
	ExecutableCandidates []string `json:"executable_candidates,omitempty"`
	WritableDirectory    bool     `json:"writable_directory"`
	WritableReasons      []string `json:"writable_reasons,omitempty"`
	DeclaredWritable     bool     `json:"declared_writable"`
	Malformed            bool     `json:"malformed"`
	MalformedReason      string   `json:"malformed_reason,omitempty"`
}

type PrivilegeProfile struct {
	AccountKind       AccountKind       `json:"account_kind"`
	AccountLevel      int               `json:"account_level"`
	StartType         StartType         `json:"start_type"`
	StartupLevel      int               `json:"startup_level"`
	LogonCapabilities []LogonCapability `json:"logon_capabilities,omitempty"`
	LogonLevel        int               `json:"logon_level"`
	EffectiveScore    int               `json:"effective_score"`
	EffectiveTier     string            `json:"effective_tier"`
	HighPrivilege     bool              `json:"high_privilege"`
	Active            bool              `json:"active"`
}

type Declaration struct {
	ServiceName           string            `json:"service_name"`
	NormalizedServiceName string            `json:"normalized_service_name"`
	DisplayName           string            `json:"display_name"`
	StartType             StartType         `json:"start_type"`
	ImagePath             string            `json:"image_path"`
	AccountRaw            string            `json:"account_raw"`
	AccountKind           AccountKind       `json:"account_kind"`
	AccountPresent        bool              `json:"account_present"`
	LogonCapabilities     []LogonCapability `json:"logon_capabilities,omitempty"`
	LogonPresent          bool              `json:"logon_present"`
	Path                  PathAssessment    `json:"path"`
	Privilege             PrivilegeProfile  `json:"privilege"`
	DeclaredWritable      bool              `json:"declared_writable"`
	Usable                bool              `json:"usable"`
	Codes                 []string          `json:"codes,omitempty"`
	Source                SourceRef         `json:"source"`
}

type SourceResult struct {
	SourceID           string        `json:"source_id"`
	Kind               SourceKind    `json:"kind"`
	FormatVersion      string        `json:"format_version"`
	Fingerprint        string        `json:"fingerprint"`
	Bytes              int64         `json:"bytes"`
	RecordsSeen        int           `json:"records_seen"`
	RecordsAccepted    int           `json:"records_accepted"`
	RecordsSkipped     int           `json:"records_skipped"`
	UsableDeclarations int           `json:"usable_declarations"`
	Partial            bool          `json:"partial"`
	Declarations       []Declaration `json:"declarations,omitempty"`
	Issues             []Issue       `json:"issues,omitempty"`
	SkippableRanges    []ByteRange   `json:"skippable_ranges,omitempty"`
}

type SourceSummary struct {
	SourceID           string      `json:"source_id"`
	Kind               SourceKind  `json:"kind"`
	FormatVersion      string      `json:"format_version"`
	Fingerprint        string      `json:"fingerprint"`
	Bytes              int64       `json:"bytes"`
	RecordsSeen        int         `json:"records_seen"`
	RecordsAccepted    int         `json:"records_accepted"`
	RecordsSkipped     int         `json:"records_skipped"`
	UsableDeclarations int         `json:"usable_declarations"`
	Partial            bool        `json:"partial"`
	Codes              []string    `json:"codes,omitempty"`
	Issues             []Issue     `json:"issues,omitempty"`
	SkippableRanges    []ByteRange `json:"skippable_ranges,omitempty"`
}

type DeclarationView struct {
	Source            SourceRef         `json:"source"`
	DisplayName       string            `json:"display_name"`
	StartType         StartType         `json:"start_type"`
	ImagePath         string            `json:"image_path"`
	Path              PathAssessment    `json:"path"`
	AccountRaw        string            `json:"account_raw"`
	AccountKind       AccountKind       `json:"account_kind"`
	AccountPresent    bool              `json:"account_present"`
	LogonCapabilities []LogonCapability `json:"logon_capabilities,omitempty"`
	LogonPresent      bool              `json:"logon_present"`
	Privilege         PrivilegeProfile  `json:"privilege"`
	Usable            bool              `json:"usable"`
	Codes             []string          `json:"codes,omitempty"`
}

type ConflictSide struct {
	Source     SourceRef `json:"source"`
	Raw        string    `json:"raw"`
	Normalized string    `json:"normalized"`
}

type Conflict struct {
	Field      string       `json:"field"`
	Authority  ConflictSide `json:"authority"`
	Challenger ConflictSide `json:"challenger"`
}

type FieldSource struct {
	Field    string    `json:"field"`
	Source   SourceRef `json:"source"`
	Value    string    `json:"value"`
	Implicit bool      `json:"implicit"`
}

type MergeSummary struct {
	Authority    SourceRef     `json:"authority"`
	Considered   []SourceRef   `json:"considered"`
	FieldSources []FieldSource `json:"field_sources"`
	Conflicts    []Conflict    `json:"conflicts,omitempty"`
}

type ServiceVerdict struct {
	ServiceName  string            `json:"service_name"`
	DisplayName  string            `json:"display_name"`
	Authority    SourceRef         `json:"authority"`
	Declarations []DeclarationView `json:"declarations"`
	Merge        MergeSummary      `json:"merge"`
	ImagePath    PathAssessment    `json:"image_path"`
	Privilege    PrivilegeProfile  `json:"privilege"`
	RiskFlags    []string          `json:"risk_flags,omitempty"`
	Codes        []string          `json:"codes,omitempty"`
	Issues       []Issue           `json:"issues,omitempty"`
}

type SnapshotSummary struct {
	Committed        bool              `json:"committed"`
	RequestedVersion int               `json:"requested_version,omitempty"`
	ActiveVersion    int               `json:"active_version"`
	FallbackVersion  int               `json:"fallback_version,omitempty"`
	HeadVersion      int               `json:"head_version"`
	ChainVersions    []int             `json:"chain_versions"`
	ChainID          string            `json:"chain_id,omitempty"`
	ContentHash      string            `json:"content_hash,omitempty"`
	TargetFormat     string            `json:"target_format"`
	FormatVersions   map[string]string `json:"format_versions"`
	Priorities       []string          `json:"priorities"`
	Codes            []string          `json:"codes,omitempty"`
	Detail           string            `json:"detail,omitempty"`
}

type Metrics struct {
	MaxConcurrency      int    `json:"max_concurrency"`
	ObservedMaxInFlight int    `json:"observed_max_in_flight"`
	QueuedSources       int    `json:"queued_sources"`
	CompletedSources    int    `json:"completed_sources"`
	MergePartitions     int    `json:"merge_partitions"`
	MergeMode           string `json:"merge_mode"`
}

type Report struct {
	Marker          string           `json:"marker"`
	SchemaVersion   string           `json:"schema_version"`
	TargetFormat    string           `json:"target_format"`
	Priorities      []string         `json:"priorities"`
	SourceSummaries []SourceSummary  `json:"source_summaries"`
	Services        []ServiceVerdict `json:"services"`
	Issues          []Issue          `json:"issues"`
	BusinessCodes   []string         `json:"business_codes"`
	Metrics         Metrics          `json:"metrics"`
	Snapshot        *SnapshotSummary `json:"snapshot,omitempty"`
}

type TraceOutput struct {
	Marker          string          `json:"marker"`
	SchemaVersion   string          `json:"schema_version"`
	ServiceName     string          `json:"service_name"`
	SnapshotVersion int             `json:"snapshot_version"`
	ChainVersions   []int           `json:"chain_versions"`
	Service         *ServiceVerdict `json:"service,omitempty"`
	Codes           []string        `json:"codes,omitempty"`
	Detail          string          `json:"detail,omitempty"`
}

func SortedStrings(values []string) []string {
	seen := make(map[string]bool, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	sortStrings(out)
	return out
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

func HasBlockingCode(codes []string) bool {
	for _, value := range codes {
		if BlockingCodes[Code(value)] {
			return true
		}
	}
	return false
}

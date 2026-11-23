package model

import (
	"sort"
	"strings"
)

const (
	SchemaVersion       = "pki-audit/v1"
	FixedSampleMarker   = "deterministic-sample-v1"
	DefaultValidationAt = "2030-01-01T00:00:00Z"
)

const (
	CodeChainUntrusted          = "E_CHAIN_UNTRUSTED"
	CodeHostnameMismatch        = "E_HOSTNAME_MISMATCH"
	CodeExpired                 = "E_EXPIRED"
	CodeExtKeyUsage             = "E_EXT_KEY_USAGE"
	CodeSampleBudgetExceeded    = "E_SAMPLE_BUDGET_EXCEEDED"
	CodeSnapshotGap             = "E_SNAPSHOT_GAP"
	CodeChainBroken             = "E_CHAIN_BROKEN"
	CodeUntrustedSelfSigned     = "E_UNTRUSTED_SELF_SIGNED"
	CodePathLenExceeded         = "E_PATH_LEN_EXCEEDED"
	CodeGlobalBudgetExhausted   = "E_GLOBAL_BUDGET_EXHAUSTED"
	CodeTLSVersionMismatch      = "E_TLS_VERSION_MISMATCH"
	CodeSignatureAlgMismatch    = "E_SIGNATURE_ALGORITHM_MISMATCH"
	CodeSampleInvalid           = "E_SAMPLE_INVALID"
	CodeUnhandledCriticalExt    = "E_UNHANDLED_CRITICAL_EXTENSION"
	CodeSnapshotInvalid         = "E_SNAPSHOT_INVALID"
	CodeReportNotFullyAccounted = "E_REPORT_NOT_FULLY_ACCOUNTED"
)

const (
	StatePending          = "pending"
	StateVerifying        = "verifying"
	StateCompleted        = "completed"
	StateFailed           = "failed"
	StateBudgetTerminated = "budget_terminated"
)

const (
	OutcomePassed   = "passed"
	OutcomeFailed   = "failed"
	OutcomeDegraded = "degraded"
)

// Meta is the deterministic per-sample input from meta.json.
type Meta struct {
	SNI                string   `json:"sni"`
	TLSVersion         string   `json:"tls_version"`
	ServerTLSVersions  []string `json:"server_tls_versions"`
	ExpectedResult     string   `json:"expected_result,omitempty"`
	Description        string   `json:"description,omitempty"`
	FixtureSchema      string   `json:"fixture_schema,omitempty"`
	CertificateProfile string   `json:"certificate_profile,omitempty"`
}

// CertificateIdentity is retained for failures so operators can correlate a
// business code with the exact certificate that triggered it.
type CertificateIdentity struct {
	Subject string `json:"subject"`
	Serial  string `json:"serial"`
}

// CertificateSummary is a deterministic, non-secret view of one chain entry.
type CertificateSummary struct {
	Position             int      `json:"position"`
	Role                 string   `json:"role"`
	Subject              string   `json:"subject"`
	Issuer               string   `json:"issuer"`
	Serial               string   `json:"serial"`
	NotBefore            string   `json:"not_before"`
	NotAfter             string   `json:"not_after"`
	IsCA                 bool     `json:"is_ca"`
	SelfSigned           bool     `json:"self_signed"`
	PublicKeyAlgorithm   string   `json:"public_key_algorithm"`
	SignatureAlgorithm   string   `json:"signature_algorithm"`
	DNSNames             []string `json:"dns_names,omitempty"`
	IPAddresses          []string `json:"ip_addresses,omitempty"`
	KeyUsage             int      `json:"key_usage"`
	ExtKeyUsage          []string `json:"ext_key_usage,omitempty"`
	BasicConstraintsOK   bool     `json:"basic_constraints_valid"`
	MaxPathLen           int      `json:"max_path_len"`
	MaxPathLenZero       bool     `json:"max_path_len_zero"`
	UnhandledCriticalExt []string `json:"unhandled_critical_extensions,omitempty"`
}

// ChainLink records the explicit issuer/subject and signature relationship
// between adjacent certificates in the leaf-to-root path.
type ChainLink struct {
	From                   int    `json:"from"`
	To                     int    `json:"to"`
	IssuerSubjectConnected bool   `json:"issuer_subject_connected"`
	SignatureValid         bool   `json:"signature_valid"`
	Error                  string `json:"error,omitempty"`
}

// Finding is a structured compatibility or validation deviation. Duplicate
// findings are removed before serialization so a deviation is counted once.
type Finding struct {
	SamplePath  string               `json:"sample_path,omitempty"`
	Code        string               `json:"code"`
	Field       string               `json:"field,omitempty"`
	Expected    string               `json:"expected,omitempty"`
	Actual      string               `json:"actual,omitempty"`
	Detail      string               `json:"detail,omitempty"`
	Certificate *CertificateIdentity `json:"certificate,omitempty"`
}

// BudgetMetrics uses deterministic accounting units. One CPU unit is modeled
// as one microsecond-equivalent of deterministic audit work; wall-clock time is
// deliberately excluded from the report.
type BudgetMetrics struct {
	QueuePosition int   `json:"queue_position"`
	WaitTicks     int64 `json:"wait_ticks"`
	CPUUnits      int64 `json:"cpu_units"`
	MemoryBytes   int64 `json:"memory_bytes"`
	CPUCharged    bool  `json:"cpu_charged"`
	MemoryCharged bool  `json:"memory_charged"`
}

// SampleResult is the complete result for one relative sample path.
type SampleResult struct {
	Path               string               `json:"path"`
	ChainDepth         int                  `json:"chain_depth"`
	State              string               `json:"state"`
	Outcome            string               `json:"outcome"`
	BusinessCode       string               `json:"business_code,omitempty"`
	ExpectedResult     string               `json:"expected_result"`
	ActualResult       string               `json:"actual_result"`
	ExpectationMatched bool                 `json:"expectation_matched"`
	Findings           []Finding            `json:"findings,omitempty"`
	Chain              []CertificateSummary `json:"chain,omitempty"`
	Links              []ChainLink          `json:"links,omitempty"`
	FailureCertificate *CertificateIdentity `json:"failure_certificate,omitempty"`
	Metrics            BudgetMetrics        `json:"metrics"`
}

// Diagnostic is a workbench-level issue, such as a checkpoint gap.
type Diagnostic struct {
	Code    string `json:"code"`
	Path    string `json:"path,omitempty"`
	Message string `json:"message"`
}

type ReportSummary struct {
	Total          int  `json:"total"`
	Completed      int  `json:"completed"`
	Failed         int  `json:"failed"`
	Degraded       int  `json:"degraded"`
	Findings       int  `json:"findings"`
	FullyAccounted bool `json:"fully_accounted"`
}

// Report is intentionally free of absolute paths, wall-clock timestamps, and
// concurrency values so identical inputs serialize to identical bytes.
type Report struct {
	SchemaVersion string         `json:"schema_version"`
	SampleMarker  string         `json:"sample_marker"`
	Summary       ReportSummary  `json:"summary"`
	Samples       []SampleResult `json:"samples"`
	Diagnostics   []Diagnostic   `json:"diagnostics,omitempty"`
}

var codePriority = []string{
	CodeChainBroken,
	CodeUntrustedSelfSigned,
	CodePathLenExceeded,
	CodeChainUntrusted,
	CodeExpired,
	CodeExtKeyUsage,
	CodeUnhandledCriticalExt,
	CodeHostnameMismatch,
	CodeTLSVersionMismatch,
	CodeSignatureAlgMismatch,
	CodeSampleInvalid,
	CodeSampleBudgetExceeded,
	CodeGlobalBudgetExhausted,
}

// PrimaryCode returns one stable business code for a sample. Findings remain
// available for the full audit trail.
func PrimaryCode(findings []Finding) string {
	if len(findings) == 0 {
		return ""
	}
	best := findings[0].Code
	bestRank := codeRank(best)
	for _, finding := range findings[1:] {
		rank := codeRank(finding.Code)
		if rank < bestRank || (rank == bestRank && finding.Code < best) {
			best = finding.Code
			bestRank = rank
		}
	}
	return best
}

func codeRank(code string) int {
	for i, candidate := range codePriority {
		if candidate == code {
			return i
		}
	}
	return len(codePriority)
}

func normalizeExpected(expected string) string {
	expected = strings.TrimSpace(expected)
	if expected == "" || strings.EqualFold(expected, "OK") || strings.EqualFold(expected, "PASS") {
		return "OK"
	}
	return strings.ToUpper(expected)
}

// Finalize establishes canonical ordering, removes duplicate findings, derives
// the top-level business code, and enforces report count accounting.
func (r *Report) Finalize() {
	for i := range r.Samples {
		result := &r.Samples[i]
		result.Findings = dedupeFindings(result.Findings)
		for j := range result.Findings {
			if result.Findings[j].SamplePath == "" {
				result.Findings[j].SamplePath = result.Path
			}
		}
		actual := "OK"
		if len(result.Findings) > 0 {
			actual = PrimaryCode(result.Findings)
		}
		if result.BusinessCode == "" && len(result.Findings) > 0 {
			result.BusinessCode = actual
		}
		if result.ActualResult == "" {
			result.ActualResult = actual
		}
		if result.ExpectedResult == "" {
			result.ExpectedResult = "OK"
		}
		expected := normalizeExpected(result.ExpectedResult)
		result.ExpectedResult = expected
		result.ExpectationMatched = normalizeExpected(result.ActualResult) == expected
		if result.Outcome == OutcomePassed {
			result.State = StateCompleted
		} else if result.Outcome == OutcomeFailed && result.State == "" {
			result.State = StateFailed
		}
	}
	sort.SliceStable(r.Samples, func(i, j int) bool {
		if r.Samples[i].Path == r.Samples[j].Path {
			return r.Samples[i].BusinessCode < r.Samples[j].BusinessCode
		}
		return r.Samples[i].Path < r.Samples[j].Path
	})
	sort.SliceStable(r.Diagnostics, func(i, j int) bool {
		if r.Diagnostics[i].Path != r.Diagnostics[j].Path {
			return r.Diagnostics[i].Path < r.Diagnostics[j].Path
		}
		if r.Diagnostics[i].Code != r.Diagnostics[j].Code {
			return r.Diagnostics[i].Code < r.Diagnostics[j].Code
		}
		return r.Diagnostics[i].Message < r.Diagnostics[j].Message
	})
	r.Summary = ReportSummary{Total: len(r.Samples)}
	for _, result := range r.Samples {
		switch result.Outcome {
		case OutcomePassed:
			r.Summary.Completed++
		case OutcomeDegraded:
			r.Summary.Degraded++
		default:
			r.Summary.Failed++
		}
		r.Summary.Findings += len(result.Findings)
	}
	r.Summary.FullyAccounted = r.Summary.Completed+r.Summary.Failed+r.Summary.Degraded == r.Summary.Total
}

func dedupeFindings(findings []Finding) []Finding {
	if len(findings) == 0 {
		return nil
	}
	sort.SliceStable(findings, func(i, j int) bool {
		a, b := findings[i], findings[j]
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		if a.Field != b.Field {
			return a.Field < b.Field
		}
		if a.Expected != b.Expected {
			return a.Expected < b.Expected
		}
		if a.Actual != b.Actual {
			return a.Actual < b.Actual
		}
		if a.Detail != b.Detail {
			return a.Detail < b.Detail
		}
		if a.Certificate == nil || b.Certificate == nil {
			return a.Certificate == nil && b.Certificate != nil
		}
		if a.Certificate.Subject != b.Certificate.Subject {
			return a.Certificate.Subject < b.Certificate.Subject
		}
		return a.Certificate.Serial < b.Certificate.Serial
	})
	seen := make(map[string]int, len(findings))
	out := make([]Finding, 0, len(findings))
	for _, finding := range findings {
		key := findingKey(finding)
		index, ok := seen[key]
		if ok {
			if findingQuality(finding) > findingQuality(out[index]) {
				out[index] = finding
			}
			continue
		}
		seen[key] = len(out)
		out = append(out, finding)
	}
	return out
}

func findingQuality(finding Finding) int {
	score := 0
	if finding.Expected != "" {
		score++
	}
	if finding.Actual != "" {
		score++
	}
	if finding.Detail != "" {
		score++
	}
	if finding.Certificate != nil {
		score++
	}
	return score
}

func findingKey(finding Finding) string {
	subject, serial := "", ""
	if finding.Certificate != nil {
		subject = finding.Certificate.Subject
		serial = finding.Certificate.Serial
	}
	return strings.Join([]string{finding.Code, finding.Field, subject, serial}, "\x00")
}

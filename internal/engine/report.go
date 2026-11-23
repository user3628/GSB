package engine

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// ReportSchema identifies the deterministic report format.
const ReportSchema = "iotrust-report/v1"

// TimeMark is the fixed sampling mark used for all report time fields.
const TimeMark = "T_FIXED_SAMPLE"

// RegisterSummary is the registration digest of one device.
type RegisterSummary struct {
	DeviceID    string `json:"device_id"`
	Fingerprint string `json:"fingerprint"`
	Caps        int    `json:"caps"`
	Events      int    `json:"events"`
	Registered  bool   `json:"registered"`
}

// AuthDecision records the authorization adjudication inputs and result.
type AuthDecision struct {
	Threshold   int64 `json:"threshold"`
	Score       int64 `json:"score"`
	ScorePoints int64 `json:"score_points"`
	Decision    Code  `json:"decision"`
}

// CredStatus records the issued credential and its continuous-check code.
type CredStatus struct {
	Digest       string `json:"digest"`
	ScoreVersion int64  `json:"score_version"`
	IssuedAt     int64  `json:"issued_at"`
	ExpiresAt    int64  `json:"expires_at"`
	VerifyAt     int64  `json:"verify_at"`
	VerifyCode   Code   `json:"verify_code"`
}

// DeviceReport is the per-device report entry.
type DeviceReport struct {
	TenantID   string          `json:"tenant_id"`
	DeviceID   string          `json:"device_id"`
	Code       Code            `json:"code"`
	Register   RegisterSummary `json:"register"`
	ScoreChain []ScoreStep     `json:"score_chain"`
	Truncation TruncationEntry `json:"truncation"`
	Auth       AuthDecision    `json:"auth"`
	Cred       *CredStatus     `json:"cred,omitempty"`
	Schedule   SchedEntry      `json:"schedule"`
}

// SchedulingReport summarizes queueing and budget usage.
type SchedulingReport struct {
	Concurrency  int   `json:"concurrency"`
	MaxInFlight  int   `json:"max_in_flight"`
	WaitQueueMax int   `json:"wait_queue_max"`
	PeakInFlight int   `json:"peak_in_flight_planned"`
	Runnable     int   `json:"runnable"`
	Waiting      int   `json:"waiting"`
	Degraded     int   `json:"degraded"`
	CPUUsed      int64 `json:"cpu_used"`
	CPUBudget    int64 `json:"cpu_budget"`
	MemUsed      int64 `json:"mem_used"`
	MemBudget    int64 `json:"mem_budget"`
}

// FaultRecord reports a load-time fault such as a snapshot version gap.
type FaultRecord struct {
	Code             Code   `json:"code"`
	LatestContinuous int64  `json:"latest_continuous"`
	Detail           string `json:"detail"`
}

// CodeCount pairs a business code with its device count.
type CodeCount struct {
	Code  Code `json:"code"`
	Count int  `json:"count"`
}

// Totals aggregates per-code device counts.
type Totals struct {
	Devices int         `json:"devices"`
	ByCode  []CodeCount `json:"by_code"`
}

// Report is the deterministic JSON authorization report.
type Report struct {
	Schema          string            `json:"schema"`
	TimeMark        string            `json:"time_mark"`
	Round           int               `json:"round"`
	Now             int64             `json:"now"`
	Params          Params            `json:"params"`
	SnapshotVersion int64             `json:"snapshot_version"`
	Fault           *FaultRecord      `json:"fault,omitempty"`
	Contention      *ContentionRecord `json:"contention,omitempty"`
	Devices         []DeviceReport    `json:"devices"`
	Scheduling      SchedulingReport  `json:"scheduling"`
	Snapshots       []SnapshotRecord  `json:"snapshot_chain"`
	Totals          Totals            `json:"totals"`
}

// Bytes renders the report with stable ordering so output is byte-level
// deterministic across worker counts and repeated runs.
func (r *Report) Bytes() []byte {
	sort.SliceStable(r.Devices, func(i, j int) bool {
		a, b := r.Devices[i], r.Devices[j]
		if a.TenantID != b.TenantID {
			return a.TenantID < b.TenantID
		}
		if a.DeviceID != b.DeviceID {
			return a.DeviceID < b.DeviceID
		}
		return a.Code < b.Code
	})
	sort.SliceStable(r.Snapshots, func(i, j int) bool { return r.Snapshots[i].Version < r.Snapshots[j].Version })
	sort.SliceStable(r.Totals.ByCode, func(i, j int) bool { return r.Totals.ByCode[i].Code < r.Totals.ByCode[j].Code })
	out, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		panic(err)
	}
	return append(out, '\n')
}

// CodeSummary renders per-code counts as a compact stable string.
func (r *Report) CodeSummary() string {
	counts := map[Code]int{}
	for _, d := range r.Devices {
		counts[d.Code]++
	}
	codes := make([]string, 0, len(counts))
	for c := range counts {
		codes = append(codes, string(c))
	}
	sort.Strings(codes)
	parts := make([]string, 0, len(codes))
	for _, c := range codes {
		parts = append(parts, fmt.Sprintf("%s=%d", c, counts[Code(c)]))
	}
	return strings.Join(parts, ",")
}

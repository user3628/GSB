// Package report builds the deterministic JSON report. All entries are
// stably sorted and the only time field is a fixed sampling marker, so
// repeated runs over unchanged input produce byte-identical output.
package report

import (
	"encoding/json"
	"sort"

	"fwadjudge/internal/codes"
)

// TimeMark is the fixed sampling marker used instead of wall-clock time.
const TimeMark = "SAMPLE@FIXED-0"

type RuleSummary struct {
	Index    int    `json:"index"`
	Action   string `json:"action"`
	Priority int    `json:"priority"`
	Hash     string `json:"hash"`
}

type SnapshotReport struct {
	SnapshotID  string        `json:"snapshot_id"`
	Version     int           `json:"version"`
	ContentHash string        `json:"content_hash"`
	Replaced    bool          `json:"replaced"`
	RuleCount   int           `json:"rule_count"`
	LedgerBound string        `json:"ledger_bound"`
	Rules       []RuleSummary `json:"rules"`
	Codes       []string      `json:"codes"`
}

type DivergenceReport struct {
	SnapA     string    `json:"snap_a"`
	SnapB     string    `json:"snap_b"`
	ProbeID   string    `json:"probe_id"`
	RuleA     int       `json:"rule_a"`
	RuleB     int       `json:"rule_b"`
	Field     string    `json:"field"`
	Class     string    `json:"class"`
	IntervalA [2]string `json:"interval_a"`
	IntervalB [2]string `json:"interval_b"`
	Code      string    `json:"code"`
}

type VersionChainReport struct {
	SnapshotID       string `json:"snapshot_id"`
	Versions         []int  `json:"versions"`
	LatestContinuous int    `json:"latest_continuous"`
	Code             string `json:"code"`
}

type Report struct {
	TimeMark    string               `json:"time_mark"`
	Snapshots   []SnapshotReport     `json:"snapshots"`
	Divergences []DivergenceReport   `json:"divergences"`
	Chains      []VersionChainReport `json:"version_chains"`
	Codes       []string             `json:"codes"`
}

func New() *Report {
	return &Report{
		TimeMark:    TimeMark,
		Snapshots:   []SnapshotReport{},
		Divergences: []DivergenceReport{},
		Chains:      []VersionChainReport{},
		Codes:       []string{},
	}
}

// Sort orders every collection deterministically: snapshots by id,
// rules by index, divergences by (snap_a, snap_b, probe), codes
// lexicographically.
func (r *Report) Sort() {
	sort.Slice(r.Snapshots, func(i, j int) bool { return r.Snapshots[i].SnapshotID < r.Snapshots[j].SnapshotID })
	for i := range r.Snapshots {
		rules := r.Snapshots[i].Rules
		sort.Slice(rules, func(a, b int) bool { return rules[a].Index < rules[b].Index })
		sort.Strings(r.Snapshots[i].Codes)
	}
	sort.Slice(r.Divergences, func(i, j int) bool {
		a, b := r.Divergences[i], r.Divergences[j]
		if a.SnapA != b.SnapA {
			return a.SnapA < b.SnapA
		}
		if a.SnapB != b.SnapB {
			return a.SnapB < b.SnapB
		}
		if a.ProbeID != b.ProbeID {
			return a.ProbeID < b.ProbeID
		}
		return a.Code < b.Code
	})
	sort.Slice(r.Chains, func(i, j int) bool { return r.Chains[i].SnapshotID < r.Chains[j].SnapshotID })
	sort.Strings(r.Codes)
}

// AddCode records a business code in the deduplicated top-level set.
func (r *Report) AddCode(code string) {
	for _, c := range r.Codes {
		if c == code {
			return
		}
	}
	r.Codes = append(r.Codes, code)
}

// Bytes renders the report as deterministic, indented JSON.
func (r *Report) Bytes() ([]byte, error) {
	r.Sort()
	out, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(out, '\n'), nil
}

// CodeOf extracts the business code string from an error.
func CodeOf(err error) string {
	if ce, ok := err.(*codes.Error); ok {
		return ce.Code
	}
	if err != nil {
		return "E_INTERNAL"
	}
	return codes.OK
}

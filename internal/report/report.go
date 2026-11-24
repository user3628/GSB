// Package report renders deterministic JSON reports. Rows are ordered by
// process id and business code; time fields only carry the fixed sample mark.
package report

import (
	"bytes"
	"encoding/json"
	"sort"

	"privescworkbench/internal/domain"
)

// ReportRow is the flat, stable per-process record emitted in JSON reports.
type ReportRow struct {
	PID        int64             `json:"pid"`
	Code       domain.BizCode    `json:"code"`
	ScoreMicro int64             `json:"score_micro"`
	BoundMicro int64             `json:"bound_micro"`
	Range      domain.Range      `json:"range"`
	Ledger     domain.ProcLedger `json:"ledger"`
	SampledAt  string            `json:"sampled_at"`
	TraceLen   int               `json:"trace_len"`
}

// Report is the top-level deterministic document.
type Report struct {
	SampleVersion string      `json:"sample_version"`
	SampleHash    string      `json:"sample_hash"`
	Version       int64       `json:"snapshot_version"`
	SampledAt     string      `json:"sampled_at"`
	ElapsedMicros int64       `json:"elapsed_micros"`
	Rows          []ReportRow `json:"rows"`
	Orphans       []int64     `json:"orphans"`
}

// FromRound builds a stable report from a detection result. When
// normalize is true the measured elapsed value and the mutable snapshot
// version are replaced by fixed markers so byte-identical reports can be
// reproduced across runs and processes.
func FromRound(round *domain.RoundResult, snapshotVersion int64, normalize bool) *Report {
	elapsed := round.ElapsedMicros
	version := snapshotVersion
	if normalize {
		elapsed = 0
		version = 0
	}
	r := &Report{
		SampleVersion: round.SampleVersion,
		SampleHash:    round.SampleHash,
		Version:       version,
		SampledAt:     domain.FixedSampledAt,
		ElapsedMicros: elapsed,
		Orphans:       append([]int64(nil), round.Orphans...),
		Rows:          make([]ReportRow, 0, len(round.Conclusions)),
	}
	for _, c := range round.Conclusions {
		r.Rows = append(r.Rows, ReportRow{
			PID: c.PID, Code: c.Code, ScoreMicro: c.ScoreMicro,
			BoundMicro: c.BoundMicro, Range: c.Range, Ledger: c.Ledger,
			SampledAt: domain.FixedSampledAt, TraceLen: len(c.Trace),
		})
	}
	sort.Slice(r.Rows, func(i, j int) bool {
		if r.Rows[i].PID != r.Rows[j].PID {
			return r.Rows[i].PID < r.Rows[j].PID
		}
		return r.Rows[i].Code < r.Rows[j].Code
	})
	return r
}

// JSON renders the report with a trailing newline and stable key order.
func (r *Report) JSON() ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

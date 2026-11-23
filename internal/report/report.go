// Package report renders deterministic JSON adjudication reports.
package report

import (
	"bytes"
	"encoding/json"
	"sort"

	"injectctl/internal/metrics"
	"injectctl/internal/model"
	"injectctl/internal/scheduler"
	"injectctl/internal/store"
)

// ParseErrorView is the serializable form of a parse error.
type ParseErrorView struct {
	Code         string `json:"code"`
	SourceOffset int    `json:"source_offset"`
	SampleID     string `json:"sample_id,omitempty"`
	Message      string `json:"message"`
}

// SampleView is one per-sample report row.
type SampleView struct {
	Seq          int    `json:"seq"`
	SampleID     string `json:"sample_id"`
	Surface      string `json:"surface"`
	Landing      string `json:"landing"`
	Code         string `json:"code"`
	RuleID       string `json:"rule_id,omitempty"`
	Marker       string `json:"marker,omitempty"`
	RawOffset    int    `json:"raw_offset"`
	LandedOffset int    `json:"landed_offset"`
	RawLength    int    `json:"raw_length"`
	LandedLength int    `json:"landed_length"`
	Normalized   string `json:"normalized,omitempty"`
}

// BatchView groups one batch's rows and statistics.
type BatchView struct {
	BatchID     string          `json:"batch_id"`
	Code        string          `json:"code"`
	Version     int             `json:"reason_version"`
	Samples     []SampleView    `json:"samples"`
	Corrections []any           `json:"corrections,omitempty"`
	Latency     []metrics.Stats `json:"latency_stats"`
}

// SnapshotView is one snapshot chain entry.
type SnapshotView struct {
	Version        int    `json:"version"`
	BatchID        string `json:"batch_id"`
	ContentHash    string `json:"content_hash"`
	SummaryHash    string `json:"summary_hash"`
	RuleSetVersion string `json:"rule_set_version"`
	ContextVersion string `json:"context_version"`
}

// Document is the full export. No wall-clock fields appear anywhere.
type Document struct {
	SchemaVersion       string           `json:"schema_version"`
	RuleSetVersion      string           `json:"rule_set_version"`
	ContextVersion      string           `json:"context_version"`
	Batches             []BatchView      `json:"batches"`
	ParseErrors         []ParseErrorView `json:"parse_errors"`
	SnapshotChain       []SnapshotView   `json:"snapshot_chain"`
	SnapshotChainStatus string           `json:"snapshot_chain_status"`
	BusinessCodes       []string         `json:"business_codes"`
}

// Build assembles a deterministic report document.
func Build(contextVersion string, results []scheduler.BatchResult,
	parseErrs []ParseErrorView, chain store.ChainReport) Document {

	doc := Document{
		SchemaVersion:  "report-1.0.0",
		RuleSetVersion: model.RuleSetVersion,
		ContextVersion: contextVersion,
		BusinessCodes:  collectCodes(results, parseErrs, chain),
	}

	for _, res := range results {
		bv := BatchView{
			BatchID: res.BatchID, Code: res.Code, Version: res.Version,
			Latency: res.Stats,
		}
		for _, v := range res.Verdicts {
			bv.Samples = append(bv.Samples, SampleView{
				Seq: v.Seq, SampleID: v.SampleID, Surface: v.Surface,
				Landing: v.Landing, Code: v.Code, RuleID: v.RuleID,
				Marker: v.Marker, RawOffset: v.RawOffset,
				LandedOffset: v.LandedOffset, RawLength: v.RawLength,
				LandedLength: v.LandedLength, Normalized: v.Normalized,
			})
		}
		for _, c := range res.Corrections {
			bv.Corrections = append(bv.Corrections, c)
		}
		sort.Slice(bv.Samples, func(i, j int) bool { return bv.Samples[i].Seq < bv.Samples[j].Seq })
		doc.Batches = append(doc.Batches, bv)
	}
	doc.ParseErrors = parseErrs

	for _, snap := range chain.Snapshots {
		doc.SnapshotChain = append(doc.SnapshotChain, SnapshotView{
			Version: snap.Version, BatchID: snap.BatchID,
			ContentHash: snap.ContentHash, SummaryHash: snap.SummaryHash,
			RuleSetVersion: snap.RuleSetVersion, ContextVersion: snap.ContextVersion,
		})
	}
	if chain.ErrorCode != "" {
		doc.SnapshotChainStatus = chain.ErrorCode
	} else {
		doc.SnapshotChainStatus = model.CodeOK
	}

	sort.Slice(doc.Batches, func(i, j int) bool { return doc.Batches[i].BatchID < doc.Batches[j].BatchID })
	sort.Strings(doc.BusinessCodes)
	return doc
}

// Render serializes the document with stable key ordering and a trailing newline.
func Render(doc Document) ([]byte, error) {
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

// EqualBytes compares two rendered reports byte for byte.
func EqualBytes(a, b []byte) bool { return bytes.Equal(a, b) }

func collectCodes(results []scheduler.BatchResult, parseErrs []ParseErrorView, chain store.ChainReport) []string {
	seen := map[string]bool{}
	add := func(c string) {
		if c != "" {
			seen[c] = true
		}
	}
	for _, r := range results {
		add(r.Code)
		for _, v := range r.Verdicts {
			add(v.Code)
		}
	}
	for _, e := range parseErrs {
		add(e.Code)
	}
	add(chain.ErrorCode)
	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}
	return out
}

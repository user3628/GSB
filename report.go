package dnsdetect

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const ReportSchema = "dnsdetect.report/v1"

type ReportConfig struct {
	AssociationWindowTicks int64            `json:"association_window_ticks"`
	WindowSpanTicks        int64            `json:"window_span_ticks"`
	WindowAnchorTicks      int64            `json:"window_anchor_ticks"`
	MaxConcurrency         int              `json:"max_concurrency"`
	MaxEventsPerSession    int              `json:"max_events_per_session"`
	MaxSpanTicksPerSession int64            `json:"max_span_ticks_per_session"`
	StrictTXIDQName        bool             `json:"strict_txid_qname"`
	SourcePriorities       []SourcePriority `json:"source_priorities"`
}

type SourceSummary struct {
	Path                 string     `json:"path"`
	Kind                 SourceKind `json:"kind"`
	Priority             int        `json:"priority"`
	FormatVersion        int        `json:"format_version"`
	EventCount           int        `json:"event_count"`
	MinTimeTicks         int64      `json:"min_time_ticks"`
	MaxTimeTicks         int64      `json:"max_time_ticks"`
	MaxTruncationNS      int64      `json:"max_truncation_ns"`
	UnionErrorBoundTicks int64      `json:"union_error_bound_ticks"`
}

type SnapshotInfo struct {
	Version           int64  `json:"version"`
	CreatedTicks      int64  `json:"created_ticks"`
	ContentDigest     string `json:"content_digest"`
	PrevContentDigest string `json:"prev_content_digest"`
	LedgerDigest      string `json:"ledger_digest"`
}

// Report is the stable host-facing artifact. It contains no wall-clock
// strings: every time is an int64 100ns Unix tick and every report declares the
// fixed sampling marker.
type Report struct {
	Schema           string                 `json:"schema"`
	TimeScale        string                 `json:"time_scale"`
	Sampling         string                 `json:"sampling"`
	Config           ReportConfig           `json:"config"`
	FormatVersions   []int                  `json:"format_versions"`
	SourcePriorities []SourcePriority       `json:"source_priorities"`
	Sources          []SourceSummary        `json:"sources"`
	Windows          []WindowProgress       `json:"windows"`
	Pairs            []Pair                 `json:"pairs"`
	Findings         []Finding              `json:"findings"`
	Uncertain        []UncertainAssociation `json:"uncertain_associations"`
	Ledger           []LedgerAggregate      `json:"time_error_ledger"`
	Snapshot         *SnapshotInfo          `json:"snapshot,omitempty"`
}

func buildReport(config Config, sessions []*SourceSession, ledger ErrorLedger, adjudication *Adjudication, formatVersions []int) *Report {
	config = config.WithDefaults()
	priorities := make([]SourcePriority, 0, len(config.SourcePriorities))
	for kind, priority := range config.SourcePriorities {
		priorities = append(priorities, SourcePriority{Kind: kind, Priority: priority})
	}
	sort.Slice(priorities, func(i, j int) bool { return priorities[i].Kind < priorities[j].Kind })
	sources := make([]SourceSummary, 0, len(sessions))
	for _, session := range sessions {
		summary := SourceSummary{Path: session.Path, Kind: session.Kind, Priority: session.Priority, FormatVersion: session.FormatVersion, EventCount: len(session.Events)}
		if len(session.Events) > 0 {
			summary.MinTimeTicks = session.Events[0].TimeTicks
			summary.MaxTimeTicks = session.Events[0].TimeTicks
			for _, event := range session.Events[1:] {
				if event.TimeTicks < summary.MinTimeTicks {
					summary.MinTimeTicks = event.TimeTicks
				}
				if event.TimeTicks > summary.MaxTimeTicks {
					summary.MaxTimeTicks = event.TimeTicks
				}
			}
		}
		for _, entry := range ledger.Entries {
			if entry.SourcePath != session.Path {
				continue
			}
			if entry.TruncationNS > summary.MaxTruncationNS {
				summary.MaxTruncationNS = entry.TruncationNS
			}
			summary.UnionErrorBoundTicks += TimeErrorTicks(entry.TruncationNS)
		}
		sources = append(sources, summary)
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].Path < sources[j].Path })
	report := &Report{
		Schema:    ReportSchema,
		TimeScale: TimeScale,
		Sampling:  SamplingMarker,
		Config: ReportConfig{
			AssociationWindowTicks: config.AssociationWindowTicks,
			WindowSpanTicks:        config.WindowSpanTicks,
			WindowAnchorTicks:      config.WindowAnchorTicks,
			MaxConcurrency:         config.MaxConcurrency,
			MaxEventsPerSession:    config.MaxEventsPerSession,
			MaxSpanTicksPerSession: config.MaxSpanTicksPerSession,
			StrictTXIDQName:        config.StrictTXIDQName,
			SourcePriorities:       priorities,
		},
		FormatVersions:   append([]int(nil), formatVersions...),
		SourcePriorities: priorities,
		Sources:          sources,
		Windows:          append([]WindowProgress(nil), adjudication.Windows...),
		Pairs:            append([]Pair(nil), adjudication.Pairs...),
		Findings:         append([]Finding(nil), adjudication.Findings...),
		Uncertain:        append([]UncertainAssociation(nil), adjudication.Uncertain...),
		Ledger:           ledger.Aggregate(),
	}
	report.FormatVersions = normalizeFormatVersions(report.FormatVersions)
	ensureReportSlices(report)
	return report
}

func ensureReportSlices(report *Report) {
	if report.FormatVersions == nil {
		report.FormatVersions = []int{}
	}
	if report.SourcePriorities == nil {
		report.SourcePriorities = []SourcePriority{}
	}
	if report.Config.SourcePriorities == nil {
		report.Config.SourcePriorities = []SourcePriority{}
	}
	if report.Sources == nil {
		report.Sources = []SourceSummary{}
	}
	if report.Windows == nil {
		report.Windows = []WindowProgress{}
	}
	if report.Pairs == nil {
		report.Pairs = []Pair{}
	}
	if report.Findings == nil {
		report.Findings = []Finding{}
	}
	if report.Uncertain == nil {
		report.Uncertain = []UncertainAssociation{}
	}
	if report.Ledger == nil {
		report.Ledger = []LedgerAggregate{}
	}
}

// CanonicalBytes returns byte-stable indented JSON with exactly one trailing
// newline. Map-backed inputs are normalized into sorted slices before use.
func (r *Report) CanonicalBytes() ([]byte, error) {
	if r == nil {
		return nil, fmt.Errorf("nil report")
	}
	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func WriteReport(writer io.Writer, report *Report) error {
	if writer == nil {
		return newBusinessError(CodeIO, "", -1, "nil report writer", nil)
	}
	encoded, err := report.CanonicalBytes()
	if err != nil {
		return newBusinessError(CodeSnapshotFormat, "", -1, "cannot encode report", err)
	}
	written, err := writer.Write(encoded)
	if err != nil {
		return newBusinessError(CodeIO, "", -1, "cannot write report", err)
	}
	if written != len(encoded) {
		return newBusinessError(CodeIO, "", -1, "short report write", nil)
	}
	return nil
}

func ExportReport(path string, report *Report) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return newBusinessError(CodeIO, path, -1, "cannot create report directory", err)
	}
	encoded, err := report.CanonicalBytes()
	if err != nil {
		return newBusinessError(CodeSnapshotFormat, path, -1, "cannot encode report", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".dnsdetect-report-*")
	if err != nil {
		return newBusinessError(CodeIO, path, -1, "cannot create temporary report", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.Write(encoded); err != nil {
		temporary.Close()
		return newBusinessError(CodeIO, path, -1, "cannot write temporary report", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return newBusinessError(CodeIO, path, -1, "cannot sync temporary report", err)
	}
	if err := temporary.Close(); err != nil {
		return newBusinessError(CodeIO, path, -1, "cannot close temporary report", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		// Windows does not replace existing files with os.Rename.
		if removeErr := os.Remove(path); removeErr == nil {
			err = os.Rename(temporaryPath, path)
		}
		if err != nil {
			return newBusinessError(CodeIO, path, -1, "cannot replace report", err)
		}
	}
	return nil
}

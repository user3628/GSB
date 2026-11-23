// Package report turns normalized events, correlation graphs and a committed
// snapshot into a deterministic JSON forensic report. Equal inputs always
// produce byte-identical bytes: IDs are content-derived and every collection is
// explicitly sorted before encoding.
package report

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"forensicworkbench/internal/codes"
	"forensicworkbench/internal/correlate"
	"forensicworkbench/internal/scheduler"
	"forensicworkbench/internal/snapshot"
)

// Strategy controls malformed-input handling reported in the summary.
type Strategy string

const (
	StrategyContinue Strategy = "continue"
	StrategyAbort    Strategy = "abort"
)

// SourceSummary summarizes one source's parse outcome.
type SourceSummary struct {
	SourcePath    string `json:"source_path"`
	Source        string `json:"source"`
	FormatVersion string `json:"format_version"`
	Accepted      int    `json:"accepted_events"`
	Errors        int    `json:"error_records"`
	FatalCode     string `json:"fatal_code,omitempty"`
}

// TimelineEvent is the sorted, view-ready projection of a normalized event.
type TimelineEvent struct {
	EventID       string          `json:"event_id"`
	TimeUnixNano  *int64          `json:"time_unix_nano"`
	Source        string          `json:"source"`
	FormatVersion string          `json:"format_version"`
	SourcePath    string          `json:"source_path"`
	Offset        int64           `json:"offset"`
	EndOffset     int64           `json:"end_offset"`
	Type          codes.EventType `json:"type"`
	Subject       string          `json:"subject"`
	Object        string          `json:"object"`
	SessionID     string          `json:"session_id"`
	PID           int64           `json:"pid"`
	Code          string          `json:"code"`
}

// Report is the top-level serialized document.
type Report struct {
	SchemaVersion     string               `json:"schema_version"`
	SnapshotVersion   int64                `json:"snapshot_version"`
	SnapshotHash      string               `json:"snapshot_hash"`
	FormatVersions    []string             `json:"format_versions"`
	MalformedStrategy Strategy             `json:"malformed_strategy"`
	SourceSummaries   []SourceSummary      `json:"source_summaries"`
	Timeline          []TimelineEvent      `json:"timeline"`
	LineageEdges      []correlate.Edge     `json:"lineage_edges"`
	Errors            []codes.ErrorRecord  `json:"errors"`
	Aggregates        []scheduler.AggCount `json:"error_aggregates"`
	Scheduling        SchedulingView       `json:"scheduling"`
	NormalEventCount  int                  `json:"normal_event_count"`
	AbandonedErrors   int                  `json:"abandoned_error_records"`
}

// SchedulingView surfaces resource control telemetry.
type SchedulingView struct {
	Concurrency int                   `json:"configured_concurrency"`
	EventBudget int64                 `json:"configured_event_budget"`
	MaxParallel int                   `json:"effective_concurrency"`
	WaitQueue   []scheduler.WaitEntry `json:"wait_queue"`
}

const schemaVersion = "forensic-report/v1"

// eventID derives a stable, content-based identifier.
func eventID(ev codes.Event) string {
	h := sha256.New()
	h.Write([]byte(ev.SourcePath))
	h.Write([]byte{0})
	h.Write([]byte(ev.Source))
	h.Write([]byte{0})
	var buf [8]byte
	v := uint64(ev.Offset)
	for i := 7; i >= 0; i-- {
		buf[i] = byte(v)
		v >>= 8
	}
	h.Write(buf[:])
	h.Write([]byte(ev.RawType))
	return "evt_" + hex.EncodeToString(h.Sum(nil))[:16]
}

// Build assembles the report from scheduler output, the correlation graph and
// the actually committed snapshot (the version is never pre-estimated).
func Build(outcome *scheduler.Outcome, graph *correlate.Graph, sn *snapshot.Snapshot, cfg scheduler.Config, strategy Strategy) *Report {
	r := &Report{
		SchemaVersion:     schemaVersion,
		SnapshotVersion:   sn.Version,
		SnapshotHash:      sn.ContentHash,
		FormatVersions:    append([]string(nil), sn.FormatVersions...),
		MalformedStrategy: strategy,
		Scheduling: SchedulingView{
			Concurrency: cfg.Concurrency, EventBudget: cfg.EventBudget, MaxParallel: cfg.Concurrency,
			WaitQueue: outcome.WaitQueue,
		},
	}

	var allEvents []codes.Event
	var allErrors []codes.ErrorRecord
	formatSet := map[string]bool{}
	abandoned := 0
	for _, res := range outcome.Results {
		if res == nil {
			continue
		}
		abandoned += res.Abandoned
		sum := SourceSummary{
			SourcePath: res.SourcePath, Source: res.Source,
			FormatVersion: res.FormatVersion, Accepted: len(res.Events), Errors: len(res.Errors),
		}
		if res.Fatal != nil {
			sum.FatalCode = res.Fatal.Code
			allErrors = append(allErrors, *res.Fatal)
		}
		allErrors = append(allErrors, res.Errors...)
		r.SourceSummaries = append(r.SourceSummaries, sum)
		if res.FormatVersion != "" {
			formatSet[res.FormatVersion] = true
		}
		allEvents = append(allEvents, res.Events...)
	}

	// Assign deterministic IDs and build the timeline projection.
	for i := range allEvents {
		allEvents[i].ID = eventID(allEvents[i])
	}
	for _, ev := range allEvents {
		r.Timeline = append(r.Timeline, TimelineEvent{
			EventID: ev.ID, TimeUnixNano: ev.TimeUnixNano, Source: ev.Source,
			FormatVersion: ev.FormatVersion, SourcePath: ev.SourcePath,
			Offset: ev.Offset, EndOffset: ev.EndOffset, Type: ev.Type,
			Subject: ev.Subject, Object: ev.Object, SessionID: ev.SessionID,
			PID: ev.PID, Code: ev.Code,
		})
		if ev.Code == codes.EOK && ev.TimeUnixNano != nil {
			r.NormalEventCount++
		}
	}
	sortTimeline(r.Timeline)

	// Flatten lineage edges in session order.
	for _, sid := range graph.Order {
		r.LineageEdges = append(r.LineageEdges, graph.Sessions[sid].Edges...)
	}

	// Deterministic error ordering and aggregates copied from the scheduler.
	sort.SliceStable(allErrors, func(i, j int) bool {
		if allErrors[i].SourcePath != allErrors[j].SourcePath {
			return allErrors[i].SourcePath < allErrors[j].SourcePath
		}
		if allErrors[i].Offset != allErrors[j].Offset {
			return allErrors[i].Offset < allErrors[j].Offset
		}
		return allErrors[i].Code < allErrors[j].Code
	})
	r.Errors = allErrors
	r.Aggregates = outcome.Aggregates

	sort.Slice(r.SourceSummaries, func(i, j int) bool {
		return r.SourceSummaries[i].SourcePath < r.SourceSummaries[j].SourcePath
	})
	sort.Strings(r.FormatVersions)
	r.AbandonedErrors = abandoned
	return r
}

// sortTimeline orders by normalized time, then source path, then byte offset.
// Events whose time could not be normalized sort last but remain present.
func sortTimeline(t []TimelineEvent) {
	sort.SliceStable(t, func(i, j int) bool {
		a, b := t[i], t[j]
		an, bn := a.TimeUnixNano != nil, b.TimeUnixNano != nil
		if an != bn {
			return an // timed events first
		}
		if an && *a.TimeUnixNano != *b.TimeUnixNano {
			return *a.TimeUnixNano < *b.TimeUnixNano
		}
		if a.SourcePath != b.SourcePath {
			return a.SourcePath < b.SourcePath
		}
		if a.Offset != b.Offset {
			return a.Offset < b.Offset
		}
		return a.EventID < b.EventID
	})
}

// Marshal emits canonical JSON with a trailing newline for stable diffing.
func Marshal(r *Report) ([]byte, error) {
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

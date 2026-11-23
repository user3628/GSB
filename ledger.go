package dnsdetect

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

// LedgerEntry records the exact integer conversion applied to one event.
// TruncationNS is in [0,99] for relative-nanosecond and RFC3339 inputs and is
// zero for exact FILETIME inputs.
type LedgerEntry struct {
	SourcePath   string     `json:"source_path"`
	SourceKind   SourceKind `json:"source_kind"`
	EventOffset  int64      `json:"event_offset"`
	Conversion   string     `json:"conversion"`
	TimeTicks    int64      `json:"time_ticks"`
	TruncationNS int64      `json:"truncation_ns"`
	WindowIndex  int64      `json:"window_index"`
}

// ErrorLedger is session-local. It is never shared by concurrently parsed
// sources, which keeps accounting and failure attribution isolated.
type ErrorLedger struct {
	Entries []LedgerEntry `json:"entries"`
}

func (l *ErrorLedger) Add(sourcePath string, kind SourceKind, offset int64, conversion string, ticks, truncationNS, windowIndex int64) {
	l.Entries = append(l.Entries, LedgerEntry{
		SourcePath:   sourcePath,
		SourceKind:   kind,
		EventOffset:  offset,
		Conversion:   conversion,
		TimeTicks:    ticks,
		TruncationNS: truncationNS,
		WindowIndex:  windowIndex,
	})
}

type LedgerAggregate struct {
	SourcePath        string     `json:"source_path"`
	SourceKind        SourceKind `json:"source_kind"`
	WindowIndex       int64      `json:"window_index"`
	EventCount        int        `json:"event_count"`
	AffectedEvents    int        `json:"affected_events"`
	MaxTruncationNS   int64      `json:"max_truncation_ns"`
	TotalTruncationNS int64      `json:"total_truncation_ns"`
	UnionBoundTicks   int64      `json:"union_bound_ticks"`
}

// Aggregate groups the ledger by source and window. UnionBoundTicks is the
// conservative total radius obtained by summing each affected event's
// one-tick-or-less uncertainty interval.
func (l ErrorLedger) Aggregate() []LedgerAggregate {
	type key struct {
		path   string
		kind   SourceKind
		window int64
	}
	groups := make(map[key]*LedgerAggregate)
	for _, entry := range l.Entries {
		k := key{path: entry.SourcePath, kind: entry.SourceKind, window: entry.WindowIndex}
		group, ok := groups[k]
		if !ok {
			group = &LedgerAggregate{SourcePath: entry.SourcePath, SourceKind: entry.SourceKind, WindowIndex: entry.WindowIndex}
			groups[k] = group
		}
		group.EventCount++
		if entry.TruncationNS != 0 {
			group.AffectedEvents++
			group.TotalTruncationNS += entry.TruncationNS
			group.UnionBoundTicks += TimeErrorTicks(entry.TruncationNS)
		}
		if entry.TruncationNS > group.MaxTruncationNS {
			group.MaxTruncationNS = entry.TruncationNS
		}
	}
	out := make([]LedgerAggregate, 0, len(groups))
	for _, group := range groups {
		out = append(out, *group)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourcePath != out[j].SourcePath {
			return out[i].SourcePath < out[j].SourcePath
		}
		if out[i].WindowIndex != out[j].WindowIndex {
			return out[i].WindowIndex < out[j].WindowIndex
		}
		return out[i].SourceKind < out[j].SourceKind
	})
	return out
}

// BoundTicks returns the maximum per-event uncertainty radius for a source and
// window. It is at most one tick for all supported integer conversions.
func (l ErrorLedger) BoundTicks(sourcePath string, windowIndex int64) int64 {
	var bound int64
	for _, entry := range l.Entries {
		if entry.SourcePath == sourcePath && entry.WindowIndex == windowIndex {
			if candidate := TimeErrorTicks(entry.TruncationNS); candidate > bound {
				bound = candidate
			}
		}
	}
	return bound
}

// MergeLedgers merges session ledgers in deterministic order.
func MergeLedgers(ledgers []ErrorLedger) ErrorLedger {
	var merged ErrorLedger
	for _, ledger := range ledgers {
		merged.Entries = append(merged.Entries, ledger.Entries...)
	}
	sort.SliceStable(merged.Entries, func(i, j int) bool {
		a, b := merged.Entries[i], merged.Entries[j]
		if a.SourcePath != b.SourcePath {
			return a.SourcePath < b.SourcePath
		}
		if a.EventOffset != b.EventOffset {
			return a.EventOffset < b.EventOffset
		}
		return a.TimeTicks < b.TimeTicks
	})
	return merged
}

// LedgerDigest is the stable SHA-256 digest stored in snapshots.
func (l ErrorLedger) Digest() (string, error) {
	type digestEntry struct {
		SourcePath   string     `json:"source_path"`
		SourceKind   SourceKind `json:"source_kind"`
		EventOffset  int64      `json:"event_offset"`
		Conversion   string     `json:"conversion"`
		TimeTicks    int64      `json:"time_ticks"`
		TruncationNS int64      `json:"truncation_ns"`
		WindowIndex  int64      `json:"window_index"`
	}
	entries := make([]digestEntry, 0, len(l.Entries))
	for _, entry := range l.Entries {
		entries = append(entries, digestEntry(entry))
	}
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]
		if a.SourcePath != b.SourcePath {
			return a.SourcePath < b.SourcePath
		}
		if a.EventOffset != b.EventOffset {
			return a.EventOffset < b.EventOffset
		}
		if a.TimeTicks != b.TimeTicks {
			return a.TimeTicks < b.TimeTicks
		}
		return a.TruncationNS < b.TruncationNS
	})
	encoded, err := json.Marshal(entries)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

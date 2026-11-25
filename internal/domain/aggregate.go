package domain

import (
	"sort"
)

func AggregateWindow(window Window, events []Event) (Aggregate, *Error) {
	deduped := make(map[int64]Event)
	for _, event := range events {
		if event.TenantID != window.TenantID || event.GroupKey != window.GroupKey {
			continue
		}
		if event.TimestampMS < window.StartMS || event.TimestampMS >= window.EndMS {
			continue
		}
		current, exists := deduped[event.Seq]
		if !exists || preferEvent(event, current) {
			deduped[event.Seq] = event
		}
	}
	selected := make([]Event, 0, len(deduped))
	for _, event := range deduped {
		selected = append(selected, event)
	}
	sort.Slice(selected, func(i, j int) bool {
		if selected[i].TimestampMS != selected[j].TimestampMS {
			return selected[i].TimestampMS < selected[j].TimestampMS
		}
		if selected[i].Seq != selected[j].Seq {
			return selected[i].Seq < selected[j].Seq
		}
		return selected[i].EventID < selected[j].EventID
	})

	aggregate := Aggregate{
		SnapshotID: window.SnapshotID,
		WindowID:   window.WindowID,
		TenantID:   window.TenantID,
		SpecID:     window.SpecID,
		GroupKey:   window.GroupKey,
		WindowType: window.WindowType,
		StartMS:    window.StartMS,
		EndMS:      window.EndMS,
		Boundary:   window.Boundary,
		Count:      int64(len(selected)),
		Rounding:   "NONE",
	}
	for _, event := range selected {
		if !event.Measure.Present {
			continue
		}
		aggregate.SampleSize++
		sum, ok := CheckedAddInt64(aggregate.SumScaled, event.Measure.Scaled)
		if !ok {
			return Aggregate{}, NewError(CodeNumericOverflow, "int64 fixed-point sum overflow", map[string]any{
				"tenant_id":       window.TenantID,
				"window_id":       window.WindowID,
				"window_start_ms": window.StartMS,
				"window_end_ms":   window.EndMS,
				"group_key":       window.GroupKey,
				"running_sum":     aggregate.SumScaled,
				"next_value":      event.Measure.Scaled,
				"event_id":        event.EventID,
				"scale":           Scale,
			})
		}
		aggregate.SumScaled = sum
		if aggregate.MinScaled == nil || event.Measure.Scaled < *aggregate.MinScaled {
			value := event.Measure.Scaled
			aggregate.MinScaled = &value
		}
		if aggregate.MaxScaled == nil || event.Measure.Scaled > *aggregate.MaxScaled {
			value := event.Measure.Scaled
			aggregate.MaxScaled = &value
		}
	}
	return aggregate, nil
}

// Duplicate sequence numbers are deterministic: the greatest timestamp wins,
// then a present measure over a missing measure, then the greatest scaled
// measure, then the lexicographically greatest event_id.
func preferEvent(candidate, current Event) bool {
	if candidate.TimestampMS != current.TimestampMS {
		return candidate.TimestampMS > current.TimestampMS
	}
	if candidate.Measure.Present != current.Measure.Present {
		return candidate.Measure.Present
	}
	if candidate.Measure.Present && candidate.Measure.Scaled != current.Measure.Scaled {
		return candidate.Measure.Scaled > current.Measure.Scaled
	}
	return candidate.EventID > current.EventID
}

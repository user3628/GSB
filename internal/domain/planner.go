package domain

import (
	"math"
	"sort"
)

type windowKey struct {
	tenantID string
	specID   string
	groupKey string
	startMS  int64
	endMS    int64
	count    int
}

func PlanWindows(snapshot *Snapshot) ([]Window, *Error) {
	specByID := make(map[string]WindowSpec, len(snapshot.WindowSpecs))
	for _, spec := range snapshot.WindowSpecs {
		specByID[spec.ID] = spec
	}
	eventsByTenantGroup := make(map[string]map[string][]Event)
	for _, event := range snapshot.Events {
		tenantGroups := eventsByTenantGroup[event.TenantID]
		if tenantGroups == nil {
			tenantGroups = make(map[string][]Event)
			eventsByTenantGroup[event.TenantID] = tenantGroups
		}
		tenantGroups[event.GroupKey] = append(tenantGroups[event.GroupKey], event)
	}

	windows := make([]Window, 0)
	for _, spec := range snapshot.WindowSpecs {
		for tenantID, groups := range eventsByTenantGroup {
			groupKeys := make([]string, 0, len(groups))
			for groupKey := range groups {
				groupKeys = append(groupKeys, groupKey)
			}
			sort.Strings(groupKeys)
			for _, groupKey := range groupKeys {
				events := groups[groupKey]
				var planned []Window
				var planErr *Error
				switch spec.Type {
				case WindowTumbling:
					planned, planErr = planTumbling(snapshot.SnapshotID, tenantID, groupKey, spec, events)
				case WindowSliding:
					planned, planErr = planSliding(snapshot.SnapshotID, tenantID, groupKey, spec, events)
				case WindowSession:
					planned, planErr = planSessions(snapshot.SnapshotID, tenantID, groupKey, spec, events)
				default:
					planErr = NewError(CodeSnapshotInvalid, "unsupported window type while planning", map[string]any{
						"spec_id": spec.ID, "window_type": spec.Type,
					})
				}
				if planErr != nil {
					return nil, planErr
				}
				windows = append(windows, planned...)
			}
		}
	}

	sort.Slice(windows, func(i, j int) bool {
		if windows[i].TenantID != windows[j].TenantID {
			return windows[i].TenantID < windows[j].TenantID
		}
		if windows[i].SpecID != windows[j].SpecID {
			return windows[i].SpecID < windows[j].SpecID
		}
		if windows[i].GroupKey != windows[j].GroupKey {
			return windows[i].GroupKey < windows[j].GroupKey
		}
		if windows[i].StartMS != windows[j].StartMS {
			return windows[i].StartMS < windows[j].StartMS
		}
		return windows[i].EndMS < windows[j].EndMS
	})
	return windows, nil
}

func planTumbling(snapshotID, tenantID, groupKey string, spec WindowSpec, events []Event) ([]Window, *Error) {
	byStart := make(map[int64]int)
	for _, event := range events {
		start := floorDiv(event.TimestampMS, spec.WidthMS) * spec.WidthMS
		if _, ok := checkedWindowEnd(start, spec.WidthMS); !ok {
			return nil, windowRangeError(tenantID, groupKey, spec, start)
		}
		byStart[start]++
	}
	starts := make([]int64, 0, len(byStart))
	for start := range byStart {
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	windows := make([]Window, 0, len(starts))
	for _, start := range starts {
		end, ok := checkedWindowEnd(start, spec.WidthMS)
		if !ok {
			return nil, windowRangeError(tenantID, groupKey, spec, start)
		}
		windows = append(windows, makeWindow(snapshotID, tenantID, groupKey, spec, start, end, byStart[start]))
	}
	return windows, nil
}

func planSliding(snapshotID, tenantID, groupKey string, spec WindowSpec, events []Event) ([]Window, *Error) {
	byStart := make(map[int64]int)
	for _, event := range events {
		if _, ok := checkedWindowEnd(event.TimestampMS, spec.WidthMS); !ok {
			return nil, windowRangeError(tenantID, groupKey, spec, event.TimestampMS)
		}
		firstK := floorDiv(event.TimestampMS-spec.WidthMS, spec.SlideMS) + 1
		lastK := floorDiv(event.TimestampMS, spec.SlideMS)
		for k := firstK; k <= lastK; k++ {
			if k > math.MaxInt64/spec.SlideMS {
				return nil, windowRangeError(tenantID, groupKey, spec, k)
			}
			start := k * spec.SlideMS
			byStart[start]++
		}
	}
	starts := make([]int64, 0, len(byStart))
	for start := range byStart {
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	windows := make([]Window, 0, len(starts))
	for _, start := range starts {
		end, ok := checkedWindowEnd(start, spec.WidthMS)
		if !ok {
			return nil, windowRangeError(tenantID, groupKey, spec, start)
		}
		windows = append(windows, makeWindow(snapshotID, tenantID, groupKey, spec, start, end, byStart[start]))
	}
	return windows, nil
}

func planSessions(snapshotID, tenantID, groupKey string, spec WindowSpec, events []Event) ([]Window, *Error) {
	sortedEvents := append([]Event(nil), events...)
	sort.Slice(sortedEvents, func(i, j int) bool {
		if sortedEvents[i].TimestampMS != sortedEvents[j].TimestampMS {
			return sortedEvents[i].TimestampMS < sortedEvents[j].TimestampMS
		}
		if sortedEvents[i].Seq != sortedEvents[j].Seq {
			return sortedEvents[i].Seq < sortedEvents[j].Seq
		}
		return sortedEvents[i].EventID < sortedEvents[j].EventID
	})

	windows := make([]Window, 0)
	if len(sortedEvents) == 0 {
		return windows, nil
	}
	sessionStart := sortedEvents[0].TimestampMS
	sessionLast := sortedEvents[0].TimestampMS
	sessionCount := 1
	flush := func() *Error {
		gapEnd, ok := checkedWindowEnd(sessionLast, spec.SessionGapMS)
		if !ok {
			return windowRangeError(tenantID, groupKey, spec, sessionLast)
		}
		end := gapEnd
		if spec.MaxSessionMS > 0 {
			widthEnd, ok := checkedWindowEnd(sessionStart, spec.MaxSessionMS)
			if !ok {
				return windowRangeError(tenantID, groupKey, spec, sessionStart)
			}
			if widthEnd < end {
				end = widthEnd
			}
		}
		if end <= sessionStart {
			return NewError(CodeZeroDurationWindow, "window planner produced a zero-duration session", map[string]any{
				"tenant_id": tenantID, "group_key": groupKey, "spec_id": spec.ID,
				"window_start_ms": sessionStart, "window_end_ms": end,
			})
		}
		windows = append(windows, makeWindow(snapshotID, tenantID, groupKey, spec, sessionStart, end, sessionCount))
		return nil
	}

	for _, event := range sortedEvents[1:] {
		gapSplit := event.TimestampMS-sessionLast >= spec.SessionGapMS
		widthSplit := spec.MaxSessionMS > 0 && event.TimestampMS-sessionStart >= spec.MaxSessionMS
		if gapSplit || widthSplit {
			if err := flush(); err != nil {
				return nil, err
			}
			sessionStart = event.TimestampMS
			sessionLast = event.TimestampMS
			sessionCount = 1
			continue
		}
		sessionLast = event.TimestampMS
		sessionCount++
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return windows, nil
}

func makeWindow(snapshotID, tenantID, groupKey string, spec WindowSpec, start, end int64, eventCount int) Window {
	return Window{
		SnapshotID: snapshotID,
		WindowID:   WindowID(snapshotID, tenantID, spec.ID, groupKey, start, end, spec.Boundary),
		TenantID:   tenantID,
		SpecID:     spec.ID,
		GroupKey:   groupKey,
		StartMS:    start,
		EndMS:      end,
		Boundary:   spec.Boundary,
		WindowType: spec.Type,
		EventCount: eventCount,
		ZeroLength: start >= end,
	}
}

func checkedWindowEnd(start, duration int64) (int64, bool) {
	if duration <= 0 || start > math.MaxInt64-duration {
		return 0, false
	}
	return start + duration, true
}

func windowRangeError(tenantID, groupKey string, spec WindowSpec, start int64) *Error {
	return NewError(CodeWindowRange, "window range exceeds int64 millisecond domain", map[string]any{
		"tenant_id": tenantID, "group_key": groupKey, "spec_id": spec.ID, "window_start_ms": start,
	})
}

func floorDiv(value, divisor int64) int64 {
	quotient := value / divisor
	remainder := value % divisor
	if remainder != 0 && (remainder < 0) != (divisor < 0) {
		quotient--
	}
	return quotient
}

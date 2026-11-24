package cockpitaudio

import (
	"fmt"
	"sort"
	"sync"
)

type TimelineInterval struct {
	ID string `json:"id"`
	Interval
}

type OverlapMarker struct {
	ID          string   `json:"id"`
	StartMS     int      `json:"start_ms"`
	EndMS       int      `json:"end_ms"`
	ZoneIDs     []string `json:"zone_ids"`
	IntervalIDs []string `json:"interval_ids"`
}

type Timeline struct {
	StartMS      int                `json:"start_ms"`
	EndMS        int                `json:"end_ms"`
	Intervals    []TimelineInterval `json:"intervals"`
	Overlaps     []OverlapMarker    `json:"overlaps"`
	Attributions []Attribution      `json:"attributions"`
}

type EditOp struct {
	Type        string        `json:"type"`
	Operator    string        `json:"operator"`
	Reason      string        `json:"reason"`
	IntervalID  string        `json:"interval_id,omitempty"`
	SplitAtMS   int           `json:"split_at_ms,omitempty"`
	IntervalIDs []string      `json:"interval_ids,omitempty"`
	ZoneID      string        `json:"zone_id,omitempty"`
	Priority    float64       `json:"priority,omitempty"`
	CommandID   string        `json:"command_id,omitempty"`
	Hit         *CandidateHit `json:"hit,omitempty"`
}

type EditRecord struct {
	Version     int               `json:"version"`
	Op          EditOp            `json:"op"`
	AffectedIDs []string          `json:"affected_interval_ids"`
	Basis       map[string]string `json:"basis"`
}

type EditState struct {
	mu                sync.RWMutex
	Version           int                                `json:"version"`
	Intervals         []TimelineInterval                 `json:"intervals"`
	Priorities        map[string]float64                 `json:"priorities"`
	Hits              map[string]map[string]CandidateHit `json:"hits"`
	Log               []EditRecord                       `json:"log"`
	priorityOverrides map[string]float64
}

func NewEditState(intervals []Interval, priorities map[string]float64) *EditState {
	tis := make([]TimelineInterval, 0, len(intervals))
	for _, in := range intervals {
		tis = append(tis, TimelineInterval{ID: stableIntervalID(in), Interval: in})
	}
	sortTimelineIntervals(tis)
	prio := DefaultPriorities()
	for k, v := range priorities {
		prio[k] = v
	}
	return &EditState{
		Version: 0, Intervals: tis, Priorities: prio,
		Hits: map[string]map[string]CandidateHit{}, Log: []EditRecord{},
		priorityOverrides: map[string]float64{},
	}
}

func stableIntervalID(in Interval) string {
	return fmt.Sprintf("iv-%s-%06d-%06d", in.ZoneID, in.StartMS, in.EndMS)
}

func sortTimelineIntervals(in []TimelineInterval) {
	sort.SliceStable(in, func(i, j int) bool {
		if in[i].StartMS != in[j].StartMS {
			return in[i].StartMS < in[j].StartMS
		}
		if in[i].ZoneID != in[j].ZoneID {
			return in[i].ZoneID < in[j].ZoneID
		}
		return in[i].ID < in[j].ID
	})
}

func (e *EditState) Clone() *EditState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	cp := &EditState{
		Version: e.Version, Intervals: append([]TimelineInterval(nil), e.Intervals...),
		Priorities: map[string]float64{}, Hits: map[string]map[string]CandidateHit{},
		Log: append([]EditRecord(nil), e.Log...), priorityOverrides: map[string]float64{},
	}
	for k, v := range e.Priorities {
		cp.Priorities[k] = v
	}
	for k, v := range e.priorityOverrides {
		cp.priorityOverrides[k] = v
	}
	for cmd, byZone := range e.Hits {
		cp.Hits[cmd] = map[string]CandidateHit{}
		for z, h := range byZone {
			cp.Hits[cmd][z] = h
		}
	}
	return cp
}

func (e *EditState) CurrentVersion() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.Version
}

func (e *EditState) LogCopy() []EditRecord {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return append([]EditRecord(nil), e.Log...)
}

func (e *EditState) EffectiveIntervals() []Interval {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]Interval, 0, len(e.Intervals))
	for _, in := range e.Intervals {
		out = append(out, in.Interval)
	}
	return out
}

func (e *EditState) TimelineIntervals() []TimelineInterval {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return append([]TimelineInterval(nil), e.Intervals...)
}

func (e *EditState) PriorityMap() map[string]float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make(map[string]float64, len(e.Priorities))
	for k, v := range e.Priorities {
		out[k] = v
	}
	return out
}

func (e *EditState) PriorityOverrideMap() map[string]float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make(map[string]float64, len(e.priorityOverrides))
	for k, v := range e.priorityOverrides {
		out[k] = v
	}
	return out
}

func (e *EditState) HitOverrides() map[string]map[string]CandidateHit {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make(map[string]map[string]CandidateHit, len(e.Hits))
	for cmd, byZone := range e.Hits {
		out[cmd] = make(map[string]CandidateHit, len(byZone))
		for z, h := range byZone {
			out[cmd][z] = h
		}
	}
	return out
}

func (e *EditState) Apply(op EditOp) (EditRecord, *Error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if op.Operator == "" {
		op.Operator = "anonymous"
	}
	if op.Reason == "" {
		op.Reason = "unspecified"
	}

	var affected []string
	basis := map[string]string{"op_type": op.Type}
	var verr *Error

	switch op.Type {
	case "split_interval":
		affected, verr = e.splitLocked(op, basis)
	case "merge_intervals", "merge_adjacent":
		affected, verr = e.mergeLocked(op, basis)
	case "set_priority":
		affected, verr = e.priorityLocked(op, basis)
	case "upsert_hit":
		affected, verr = e.hitLocked(op, basis)
	default:
		verr = NewError(CodeInvalidParam, StageTimeline, "", "unknown edit op type %q", op.Type)
	}
	if verr != nil {
		return EditRecord{}, verr
	}

	e.Version++
	record := EditRecord{Version: e.Version, Op: op, AffectedIDs: affected, Basis: basis}
	e.Log = append(e.Log, record)
	return record, nil
}

func (e *EditState) indexOf(id string) int {
	for i := range e.Intervals {
		if e.Intervals[i].ID == id {
			return i
		}
	}
	return -1
}

func (e *EditState) splitLocked(op EditOp, basis map[string]string) ([]string, *Error) {
	idx := e.indexOf(op.IntervalID)
	if idx < 0 {
		return nil, NewError(CodeNotFound, StageTimeline, "", "interval %s not found", op.IntervalID)
	}
	old := e.Intervals[idx]
	if op.SplitAtMS <= old.StartMS || op.SplitAtMS >= old.EndMS {
		return nil, NewError(CodeInvalidParam, StageTimeline, "", "split point %d is outside (%d,%d)", op.SplitAtMS, old.StartMS, old.EndMS)
	}
	left := old
	right := old
	left.ID = fmt.Sprintf("%s@v%d-left", old.ID, e.Version+1)
	left.EndMS = op.SplitAtMS
	left.Source = "edit_split"
	right.ID = fmt.Sprintf("%s@v%d-right", old.ID, e.Version+1)
	right.StartMS = op.SplitAtMS
	right.Source = "edit_split"

	rest := append([]TimelineInterval{}, e.Intervals[:idx]...)
	rest = append(rest, left, right)
	rest = append(rest, e.Intervals[idx+1:]...)
	e.Intervals = rest
	sortTimelineIntervals(e.Intervals)
	basis["split_at_ms"] = fmt.Sprint(op.SplitAtMS)
	basis["original_start_ms"] = fmt.Sprint(old.StartMS)
	basis["original_end_ms"] = fmt.Sprint(old.EndMS)
	return []string{old.ID, left.ID, right.ID}, nil
}

func (e *EditState) mergeLocked(op EditOp, basis map[string]string) ([]string, *Error) {
	ids := op.IntervalIDs
	if op.Type == "merge_adjacent" && op.ZoneID != "" && len(ids) == 0 {
		for _, in := range e.Intervals {
			if in.ZoneID == op.ZoneID && in.Kind == "speech" {
				if len(ids) == 0 || e.areConsecutiveSpeech(ids[len(ids)-1], in.ID) {
					ids = append(ids, in.ID)
				}
			}
		}
	}
	if len(ids) < 2 {
		return nil, NewError(CodeInvalidParam, StageTimeline, "", "merge requires at least two interval IDs")
	}
	selected := make([]TimelineInterval, 0, len(ids))
	for _, id := range ids {
		idx := e.indexOf(id)
		if idx < 0 {
			return nil, NewError(CodeNotFound, StageTimeline, "", "interval %s not found", id)
		}
		selected = append(selected, e.Intervals[idx])
	}
	sortTimelineIntervals(selected)
	zone := selected[0].ZoneID
	kind := selected[0].Kind
	for _, in := range selected[1:] {
		if in.ZoneID != zone || in.Kind != kind {
			return nil, NewError(CodeInvalidParam, StageTimeline, "", "merge requires same zone and kind")
		}
	}
	for i := 1; i < len(selected); i++ {
		if selected[i].StartMS < selected[i-1].EndMS {
			return nil, NewError(CodeInvalidParam, StageTimeline, "", "merge targets overlap unexpectedly")
		}
	}

	merged := selected[0]
	merged.EndMS = selected[len(selected)-1].EndMS
	merged.Score = 0
	for _, in := range selected {
		merged.Score += in.Score
	}
	merged.Score = Round(merged.Score/float64(len(selected)), 6)
	merged.Source = "edit_merge"
	merged.ID = fmt.Sprintf("merged-v%d-%s-%d-%d", e.Version+1, zone, merged.StartMS, merged.EndMS)

	idSet := map[string]bool{}
	for _, id := range ids {
		idSet[id] = true
	}
	kept := make([]TimelineInterval, 0, len(e.Intervals)-len(ids)+1)
	for _, in := range e.Intervals {
		if !idSet[in.ID] {
			kept = append(kept, in)
		}
	}
	kept = append(kept, merged)
	e.Intervals = kept
	sortTimelineIntervals(e.Intervals)
	basis["merged_count"] = fmt.Sprint(len(ids))
	basis["span_start_ms"] = fmt.Sprint(merged.StartMS)
	basis["span_end_ms"] = fmt.Sprint(merged.EndMS)
	return append(append([]string{}, ids...), merged.ID), nil
}

func (e *EditState) areConsecutiveSpeech(prevID, nextID string) bool {
	pi, ni := e.indexOf(prevID), e.indexOf(nextID)
	if pi < 0 || ni < 0 {
		return false
	}
	prev, next := e.Intervals[pi], e.Intervals[ni]
	if prev.ZoneID != next.ZoneID || prev.Kind != next.Kind {
		return false
	}
	for i := minInt(pi, ni) + 1; i < maxInt(pi, ni); i++ {
		if e.Intervals[i].ZoneID == prev.ZoneID {
			return false
		}
	}
	return true
}

func (e *EditState) priorityLocked(op EditOp, basis map[string]string) ([]string, *Error) {
	if op.ZoneID == "" {
		return nil, NewError(CodeInvalidParam, StageTimeline, "", "zone_id is required")
	}
	if op.Priority <= 0 {
		return nil, NewError(CodeInvalidParam, StageTimeline, "", "priority must be positive")
	}
	old := e.Priorities[op.ZoneID]
	e.Priorities[op.ZoneID] = op.Priority
	if e.priorityOverrides == nil {
		e.priorityOverrides = map[string]float64{}
	}
	e.priorityOverrides[op.ZoneID] = op.Priority
	basis["zone_id"] = op.ZoneID
	basis["old_priority"] = fmt.Sprint(old)
	basis["new_priority"] = fmt.Sprint(op.Priority)
	return []string{"priority:" + op.ZoneID}, nil
}

func (e *EditState) hitLocked(op EditOp, basis map[string]string) ([]string, *Error) {
	if op.CommandID == "" || op.Hit == nil || op.Hit.ZoneID == "" {
		return nil, NewError(CodeInvalidParam, StageTimeline, "", "command_id and hit.zone_id are required")
	}
	if _, ok := e.Hits[op.CommandID]; !ok {
		e.Hits[op.CommandID] = map[string]CandidateHit{}
	}
	old := e.Hits[op.CommandID][op.Hit.ZoneID]
	e.Hits[op.CommandID][op.Hit.ZoneID] = *op.Hit
	basis["command_id"] = op.CommandID
	basis["zone_id"] = op.Hit.ZoneID
	basis["old_strength"] = fmt.Sprint(old.Strength)
	basis["new_strength"] = fmt.Sprint(op.Hit.Strength)
	return []string{"hit:" + op.CommandID + ":" + op.Hit.ZoneID}, nil
}

func BuildTimeline(intervals []TimelineInterval, attributions []Attribution) Timeline {
	sorted := append([]TimelineInterval(nil), intervals...)
	sortTimelineIntervals(sorted)
	attrs := append([]Attribution(nil), attributions...)
	sort.SliceStable(attrs, func(i, j int) bool { return attrs[i].CommandID < attrs[j].CommandID })
	tl := Timeline{Intervals: sorted, Overlaps: []OverlapMarker{}, Attributions: attrs}
	if len(sorted) > 0 {
		tl.StartMS = sorted[0].StartMS
		tl.EndMS = sorted[len(sorted)-1].EndMS
	}
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			a, b := sorted[i], sorted[j]
			if a.ZoneID == b.ZoneID {
				continue
			}
			start := maxInt(a.StartMS, b.StartMS)
			end := minInt(a.EndMS, b.EndMS)
			if start >= end {
				continue
			}
			zones := []string{a.ZoneID, b.ZoneID}
			sort.Strings(zones)
			ids := []string{a.ID, b.ID}
			sort.Strings(ids)
			tl.Overlaps = append(tl.Overlaps, OverlapMarker{
				ID:      fmt.Sprintf("ovl-%06d-%06d-%s-%s", start, end, zones[0], zones[1]),
				StartMS: start, EndMS: end, ZoneIDs: zones, IntervalIDs: ids,
			})
		}
	}
	sort.SliceStable(tl.Overlaps, func(i, j int) bool {
		if tl.Overlaps[i].StartMS != tl.Overlaps[j].StartMS {
			return tl.Overlaps[i].StartMS < tl.Overlaps[j].StartMS
		}
		return tl.Overlaps[i].ID < tl.Overlaps[j].ID
	})
	return tl
}

func ActiveZonesAt(tl Timeline, atMS int) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, in := range tl.Intervals {
		if in.StartMS <= atMS && atMS < in.EndMS && !seen[in.ZoneID] {
			seen[in.ZoneID] = true
			out = append(out, in.ZoneID)
		}
	}
	sort.Strings(out)
	return out
}

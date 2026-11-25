package feedbackstream

import (
	"fmt"
	"math"
	"sort"
)

func floorDiv(a, b int64) int64 {
	q := a / b
	r := a % b
	if r != 0 && ((r < 0) != (b < 0)) {
		q--
	}
	return q
}

func WindowStart(tsMS int64, window WindowSpec) int64 {
	size := window.SizeMS
	if size <= 0 {
		size = 1000
	}
	return window.OriginMS + floorDiv(tsMS-window.OriginMS, size)*size
}

func WindowEnd(startMS int64, window WindowSpec) int64 {
	return startMS + window.SizeMS
}

func WindowContains(tsMS, startMS int64, window WindowSpec) bool {
	switch window.BoundaryRule {
	case "", BoundaryStartInclEndExcl:
		return tsMS >= startMS && tsMS < startMS+window.SizeMS
	default:
		return false
	}
}

func AssignWindow(event Event, window WindowSpec) (start, end int64) {
	start = WindowStart(event.TimestampMS, window)
	return start, WindowEnd(start, window)
}

func NewBatch(id string, events []Event) (*Batch, error) {
	if len(events) == 0 {
		return nil, SpecInvalid("batch contains no events").With("batch_id", id)
	}
	if err := ValidateEventStream(events); err != nil {
		return nil, err
	}
	partition := events[0].PartitionID
	start := WindowStart(events[0].TimestampMS, WindowSpec{SizeMS: 1000, OriginMS: 0, BoundaryRule: BoundaryStartInclEndExcl})
	for _, event := range events {
		if event.PartitionID != partition {
			return nil, SpecInvalid("batch spans multiple partitions").With("batch_id", id)
		}
		eventStart := WindowStart(event.TimestampMS, WindowSpec{SizeMS: 1000, OriginMS: 0})
		if eventStart != start {
			return nil, SpecInvalid("batch spans multiple windows").With("batch_id", id)
		}
	}
	payload, err := CanonicalJSON(events)
	if err != nil {
		return nil, err
	}
	return &Batch{
		ID: id, PartitionID: partition, WindowStart: start, WindowEnd: start + 1000,
		Events: events, EventCount: len(events), ByteSize: int64(len(payload)),
		PayloadHash: SHA256Hex(payload),
	}, nil
}

// BuildWindowBatches creates one deterministic batch per partition/window.
func BuildWindowBatches(events []Event, window WindowSpec) ([]*Batch, error) {
	type key struct {
		partition int
		start     int64
	}
	grouped := map[key][]Event{}
	keys := make([]key, 0)
	for _, event := range events {
		start := WindowStart(event.TimestampMS, window)
		k := key{event.PartitionID, start}
		if _, ok := grouped[k]; !ok {
			keys = append(keys, k)
		}
		grouped[k] = append(grouped[k], event)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].partition != keys[j].partition {
			return keys[i].partition < keys[j].partition
		}
		return keys[i].start < keys[j].start
	})
	batches := make([]*Batch, 0, len(keys))
	for _, k := range keys {
		groupEvents := grouped[k]
		sort.SliceStable(groupEvents, func(i, j int) bool { return groupEvents[i].Sequence < groupEvents[j].Sequence })
		windowIndex := int64(0)
		if window.SizeMS > 0 {
			windowIndex = (k.start - window.OriginMS) / window.SizeMS
		}
		id := fmt.Sprintf("batch-p%d-w%d", k.partition, windowIndex)
		payload, err := CanonicalJSON(groupEvents)
		if err != nil {
			return nil, err
		}
		batches = append(batches, &Batch{
			ID: id, PartitionID: k.partition, WindowStart: k.start,
			WindowEnd: WindowEnd(k.start, window), Events: groupEvents,
			EventCount: len(groupEvents), ByteSize: int64(len(payload)),
			PayloadHash: SHA256Hex(payload),
		})
	}
	return batches, nil
}

func EstimateCost(batch Candidate, model CostModel) int64 {
	return model.BaseCostMS + model.PerEventCostMS*int64(batch.EventCount)
}

func BatchCost(batch *Batch, model CostModel) int64 {
	return model.BaseCostMS + model.PerEventCostMS*int64(batch.EventCount)
}

func overflowError(groupID, key string, start, end int64) *Error {
	return NewError(CodeNumericOverflow, "int64 fixed-point aggregation overflow").
		With("fusion_group_id", groupID).
		With("group_key", key).
		With("window_start_ms", start).
		With("window_end_ms", end)
}

func addChecked(sum, metric int64, groupID, key string, start, end int64) (int64, error) {
	if metric > 0 && sum > math.MaxInt64-metric {
		return 0, overflowError(groupID, key, start, end)
	}
	if metric < 0 && sum < math.MinInt64-metric {
		return 0, overflowError(groupID, key, start, end)
	}
	return sum + metric, nil
}

func AggregateRows(rows []ProjectedEvent, groupID, signatureHash string, partitionID int) ([]WindowAggregate, error) {
	type aggKey struct {
		start int64
		end   int64
		key   string
	}
	type acc struct {
		agg WindowAggregate
	}
	byKey := map[aggKey]*acc{}
	keys := make([]aggKey, 0)
	for _, row := range rows {
		k := aggKey{row.WindowStartMS, row.WindowEndMS, row.GroupKey}
		item := byKey[k]
		if item == nil {
			item = &acc{agg: WindowAggregate{
				PartitionID: partitionID, WindowStartMS: k.start, WindowEndMS: k.end, GroupKey: k.key,
				Count: 0, Sum: 0, Min: row.MetricFixed, Max: row.MetricFixed,
			}}
			byKey[k] = item
			keys = append(keys, k)
		}
		next, err := addChecked(item.agg.Sum, row.MetricFixed, groupID, k.key, k.start, k.end)
		if err != nil {
			return nil, err
		}
		item.agg.Sum = next
		item.agg.Count++
		if row.MetricFixed < item.agg.Min {
			item.agg.Min = row.MetricFixed
		}
		if row.MetricFixed > item.agg.Max {
			item.agg.Max = row.MetricFixed
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].start != keys[j].start {
			return keys[i].start < keys[j].start
		}
		return keys[i].key < keys[j].key
	})
	out := make([]WindowAggregate, 0, len(keys))
	for _, k := range keys {
		out = append(out, byKey[k].agg)
	}
	return out, nil
}

// ExecuteGroup evaluates the operators contained in one fusion group.
// projectedInput is required for a group whose first operator is aggregate and
// was produced by an upstream fused group.
func ExecuteGroup(group FusionGroup, policy *PolicySnapshot, batch *Batch, projectedInput []ProjectedEvent) (GroupOutput, error) {
	opByID := map[string]OperatorSpec{}
	for _, op := range policy.Pipeline.Operators {
		opByID[op.ID] = op
	}
	rows := make([]ProjectedEvent, 0)
	if len(group.Kinds) > 0 && group.Kinds[0] == "aggregate" {
		rows = append(rows, projectedInput...)
	} else {
		for _, event := range batch.Events {
			start, end := AssignWindow(event, policy.Window)
			rows = append(rows, ProjectedEvent{Event: event, WindowStartMS: start, WindowEndMS: end})
		}
	}

	projected := false
	for _, opID := range group.OperatorIDs {
		op := opByID[opID]
		switch op.Kind {
		case "scan":
			continue
		case "filter":
			filtered := rows[:0]
			for _, row := range rows {
				if op.Config.MetricGTE == nil || row.MetricFixed >= *op.Config.MetricGTE {
					filtered = append(filtered, row)
				}
			}
			rows = filtered
		case "project":
			projected = true
			for i := range rows {
				rows[i].TimestampMS = 0
				rows[i].Sequence = 0
			}
		case "aggregate":
			aggs, err := AggregateRows(rows, group.ID, group.SignatureHash, batch.PartitionID)
			if err != nil {
				return GroupOutput{}, err
			}
			return GroupOutput{GroupID: group.ID, SignatureHash: group.SignatureHash, Aggregates: aggs}, nil
		default:
			return GroupOutput{}, PipelineInvalid("unsupported operator kind: "+op.Kind).With("operator_id", op.ID)
		}
	}

	out := GroupOutput{GroupID: group.ID, SignatureHash: group.SignatureHash}
	if projected || (len(group.Kinds) > 0 && group.Kinds[len(group.Kinds)-1] == "project") {
		out.Projected = append(out.Projected, rows...)
	}
	return out, nil
}

func CandidateFromBatch(batch *Batch, policy *PolicySnapshot) Candidate {
	return Candidate{
		BatchID: batch.ID, PartitionID: batch.PartitionID,
		WindowStart: batch.WindowStart, EventCount: batch.EventCount,
		CostMS:   BatchCost(batch, policy.CostModel),
		ByteSize: batch.ByteSize,
	}
}

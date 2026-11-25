package store

import "example.com/gsb-router/internal/model"

const (
	InjectMappingMissing   = "mapping_missing"
	InjectRangeOverlap     = "range_overlap"
	InjectBaseRegression   = "base_regression"
	InjectObservedAhead    = "observed_ahead"
	InjectRebuildInterrupt = "rebuild_interrupt"
)

func applySnapshotFault(snapshot *model.Snapshot, inject string) bool {
	switch inject {
	case "", InjectBaseRegression, InjectObservedAhead, InjectRebuildInterrupt:
		return false
	case InjectMappingMissing:
		if len(snapshot.LogicalTables) == 0 || len(snapshot.LogicalTables[0].Ranges) == 0 {
			return false
		}
		snapshot.LogicalTables[0].Ranges[0].PhysicalTable = "db9.__injected_missing__"
		return true
	case InjectRangeOverlap:
		for i := range snapshot.LogicalTables {
			table := &snapshot.LogicalTables[i]
			if len(table.Ranges) == 0 {
				continue
			}
			if len(table.Ranges) == 1 {
				first := table.Ranges[0]
				table.Ranges = append(table.Ranges, model.ShardRange{
					ID:             first.ID + "-injected-overlap",
					Lower:          first.Lower,
					Upper:          first.Upper,
					LowerInclusive: true,
					UpperInclusive: true,
					PhysicalTable:  first.PhysicalTable,
					Weight:         first.Weight,
				})
				return true
			}
			first, second := &table.Ranges[0], &table.Ranges[1]
			first.UpperInclusive = true
			second.Lower = first.Upper
			second.LowerInclusive = true
			return true
		}
	}
	return false
}

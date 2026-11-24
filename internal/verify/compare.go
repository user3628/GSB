package verify

import (
	"fmt"
	"reflect"

	"columnar-executor/internal/fixed"
	"columnar-executor/internal/model"
)

type Mismatch struct {
	Kind   string `json:"kind"`
	Name   string `json:"name,omitempty"`
	Left   string `json:"left"`
	Right  string `json:"right"`
	Detail string `json:"detail,omitempty"`
}

type Report struct {
	LeftExecution        string     `json:"left_execution"`
	RightExecution       string     `json:"right_execution"`
	LeftPath             string     `json:"left_path"`
	RightPath            string     `json:"right_path"`
	RowsConsistent       bool       `json:"rows_consistent"`
	AggregatesConsistent bool       `json:"aggregates_consistent"`
	TolerancePPM         int64      `json:"tolerance_ppm"`
	Mismatches           []Mismatch `json:"mismatches,omitempty"`
}

func Compare(left, right model.ExecutionView, tolerancePPM int64) Report {
	report := Report{
		LeftExecution:        left.ExecutionID,
		RightExecution:       right.ExecutionID,
		LeftPath:             left.Path.Path,
		RightPath:            right.Path.Path,
		TolerancePPM:         tolerancePPM,
		RowsConsistent:       true,
		AggregatesConsistent: true,
	}
	leftRows := flattenRows(left.Batches)
	rightRows := flattenRows(right.Batches)
	if len(leftRows) != len(rightRows) {
		report.RowsConsistent = false
		report.Mismatches = append(report.Mismatches, Mismatch{
			Kind: "rows", Left: fmt.Sprintf("%d rows", len(leftRows)), Right: fmt.Sprintf("%d rows", len(rightRows)),
			Detail: "filtered row count differs",
		})
	} else {
		for i := range leftRows {
			if !reflect.DeepEqual(leftRows[i], rightRows[i]) {
				report.RowsConsistent = false
				report.Mismatches = append(report.Mismatches, Mismatch{
					Kind: "row", Name: fmt.Sprintf("row_index_%d", i),
					Left: fmt.Sprintf("%+v", leftRows[i]), Right: fmt.Sprintf("%+v", rightRows[i]),
					Detail: "projected row differs",
				})
				if len(report.Mismatches) >= 20 {
					break
				}
			}
		}
	}
	leftAggs := map[string]model.AggregateValue{}
	rightAggs := map[string]model.AggregateValue{}
	for _, agg := range left.Aggregates {
		leftAggs[agg.Name] = agg
	}
	for _, agg := range right.Aggregates {
		rightAggs[agg.Name] = agg
	}
	if len(leftAggs) != len(rightAggs) {
		report.AggregatesConsistent = false
		report.Mismatches = append(report.Mismatches, Mismatch{
			Kind: "aggregate_set", Left: fmt.Sprintf("%d", len(leftAggs)), Right: fmt.Sprintf("%d", len(rightAggs)),
			Detail: "aggregate name count differs",
		})
	}
	for name, l := range leftAggs {
		r, ok := rightAggs[name]
		if !ok {
			report.AggregatesConsistent = false
			report.Mismatches = append(report.Mismatches, Mismatch{Kind: "aggregate", Name: name, Left: l.ValueText, Right: "<missing>"})
			continue
		}
		if l.Type != r.Type || l.Function != r.Function || l.Count != r.Count || l.NullsSkipped != r.NullsSkipped || l.RoundingErrorUnits != r.RoundingErrorUnits {
			report.AggregatesConsistent = false
			report.Mismatches = append(report.Mismatches, Mismatch{
				Kind: "aggregate", Name: name,
				Left: fmt.Sprintf("%+v", l), Right: fmt.Sprintf("%+v", r),
				Detail: "aggregate metadata differs",
			})
			continue
		}
		equal := false
		switch l.Type {
		case model.ColumnFixed:
			equal = fixed.EqualRelative(l.ValueUnits, r.ValueUnits, tolerancePPM)
		default:
			equal = l.ValueUnits == r.ValueUnits
		}
		if !equal {
			report.AggregatesConsistent = false
			report.Mismatches = append(report.Mismatches, Mismatch{
				Kind: "aggregate", Name: name, Left: l.ValueText, Right: r.ValueText,
				Detail: "aggregate value differs outside fixed relative tolerance",
			})
		}
	}
	return report
}

func flattenRows(batches []model.BatchTrace) []model.ProjectedRow {
	var rows []model.ProjectedRow
	for _, batch := range batches {
		rows = append(rows, batch.Rows...)
	}
	return rows
}

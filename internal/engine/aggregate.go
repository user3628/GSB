package engine

import (
	"sort"
	"strconv"
	"strings"

	"columnar-executor/internal/fixed"
	"columnar-executor/internal/model"
)

type accumulator struct {
	spec         model.AggregateSpec
	function     string
	column       *model.Column
	typeName     string
	count        int64
	nullsSkipped int64
	sum          int64
	hasValue     bool
	min          int64
	max          int64
	distinct     map[string]struct{}
}

func newAccumulators(snap model.Snapshot, specs []model.AggregateSpec) ([]*accumulator, error) {
	out := make([]*accumulator, 0, len(specs))
	for _, spec := range specs {
		function := strings.ToLower(strings.TrimSpace(spec.Func))
		acc := &accumulator{spec: spec, function: function}
		if function == "count" && spec.Column == "" {
			acc.typeName = "count"
			out = append(out, acc)
			continue
		}
		column, ok := snap.Column(spec.Column)
		if !ok {
			return nil, invalidChain("aggregate column is missing").With("column", spec.Column)
		}
		acc.column = &column
		switch function {
		case "count", "count_non_null":
			acc.typeName = "count"
		case "sum", "avg", "min", "max":
			if column.Type != model.ColumnInt && column.Type != model.ColumnFixed {
				return nil, invalidChain("numeric aggregate requires numeric column").With("column", spec.Column)
			}
			acc.typeName = column.Type
		case "count_distinct":
			if column.Type != model.ColumnDict {
				return nil, invalidChain("count_distinct requires dictionary column").With("column", spec.Column)
			}
			acc.typeName = model.ColumnDict
			acc.distinct = map[string]struct{}{}
		default:
			return nil, invalidChain("unsupported aggregate function").With("function", spec.Func)
		}
		out = append(out, acc)
	}
	return out, nil
}

func (a *accumulator) add(snap model.Snapshot, row int, batchNo int) error {
	if a.function == "count" && a.column == nil {
		a.count++
		return nil
	}
	column := mustColumn(snap, a.spec.Column)
	if column.NullAt(row) {
		a.nullsSkipped++
		return nil
	}
	switch a.function {
	case "count", "count_non_null":
		a.count++
	case "count_distinct":
		a.count++
		idx := column.DictIndices[row]
		if idx >= 0 && int(idx) < len(column.Dictionary) {
			a.distinct[column.Dictionary[idx]] = struct{}{}
		}
	case "sum", "avg":
		var value int64
		if column.Type == model.ColumnFixed {
			value = column.FixedValues[row]
		} else {
			value = column.IntValues[row]
		}
		next, err := fixed.Add(a.sum, value)
		if err != nil {
			return model.NewBizError(model.CodeNumericOverflow, "aggregate sum overflow").
				With("column", a.spec.Column).With("batch", batchNo).With("function", a.function)
		}
		a.sum = next
		a.count++
	case "min":
		value := numericValue(column, row)
		if !a.hasValue || value < a.min {
			a.min = value
		}
		a.hasValue = true
		a.count++
	case "max":
		value := numericValue(column, row)
		if !a.hasValue || value > a.max {
			a.max = value
		}
		a.hasValue = true
		a.count++
	default:
		return invalidChain("unsupported aggregate function").With("function", a.function)
	}
	return nil
}

func numericValue(column model.Column, row int) int64 {
	if column.Type == model.ColumnFixed {
		return column.FixedValues[row]
	}
	return column.IntValues[row]
}

func (a *accumulator) value() (model.AggregateValue, error) {
	value := model.AggregateValue{
		Name:         a.spec.Name,
		Function:     a.function,
		Column:       a.spec.Column,
		Type:         a.typeName,
		Count:        a.count,
		NullsSkipped: a.nullsSkipped,
	}
	if a.typeName == model.ColumnFixed {
		value.ScaleUnits = model.FixedScale
	}
	switch a.function {
	case "count", "count_non_null":
		value.ValueUnits = a.count
		value.ValueText = strconv.FormatInt(a.count, 10)
	case "count_distinct":
		value.Count = int64(len(a.distinct))
		value.ValueUnits = value.Count
		names := make([]string, 0, len(a.distinct))
		for name := range a.distinct {
			names = append(names, name)
		}
		sort.Strings(names)
		value.ValueText = strings.Join(names, ",")
	case "sum":
		value.ValueUnits = a.sum
		if a.typeName == model.ColumnFixed {
			value.ValueText = model.FormatFixed(a.sum)
		} else {
			value.ValueText = strconv.FormatInt(a.sum, 10)
		}
	case "avg":
		if a.count == 0 {
			value.ValueUnits = 0
			value.Rounding = "no_rows"
			if a.typeName == model.ColumnFixed {
				value.ValueText = model.FormatFixed(0)
			} else {
				value.ValueText = "0"
			}
			return value, nil
		}
		rounded, roundingError, err := fixed.Average(a.sum, a.count)
		if err != nil {
			return model.AggregateValue{}, model.NewBizError(model.CodeNumericOverflow, "average overflow").
				With("column", a.spec.Column).With("function", "avg")
		}
		value.ValueUnits = rounded
		value.Rounding = "half_away_from_zero"
		value.RoundingErrorUnits = roundingError
		if a.typeName == model.ColumnFixed {
			value.ValueText = model.FormatFixed(rounded)
		} else {
			value.ValueText = strconv.FormatInt(rounded, 10)
		}
	case "min", "max":
		if !a.hasValue {
			value.ValueText = ""
			value.Rounding = "no_rows"
			return value, nil
		}
		value.ValueUnits = a.min
		if a.function == "max" {
			value.ValueUnits = a.max
		}
		if a.typeName == model.ColumnFixed {
			value.ValueText = model.FormatFixed(value.ValueUnits)
		} else {
			value.ValueText = strconv.FormatInt(value.ValueUnits, 10)
		}
	}
	return value, nil
}

func aggregateRows(snap model.Snapshot, rowIDs []int, batchNo int, batchAccs, finalAccs []*accumulator) ([]model.AggregateValue, error) {
	for _, row := range rowIDs {
		for _, acc := range batchAccs {
			if err := acc.add(snap, row, batchNo); err != nil {
				return nil, err
			}
		}
		for _, acc := range finalAccs {
			if err := acc.add(snap, row, batchNo); err != nil {
				return nil, err
			}
		}
	}
	out := make([]model.AggregateValue, 0, len(batchAccs))
	for _, acc := range batchAccs {
		value, err := acc.value()
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

func finalAggregateValues(accs []*accumulator) ([]model.AggregateValue, error) {
	out := make([]model.AggregateValue, 0, len(accs))
	for _, acc := range accs {
		value, err := acc.value()
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

func aggregateSpecs(chain model.Chain) []model.AggregateSpec {
	for _, op := range chain.Operators {
		if operatorKind(op) == "aggregate" && op.Aggregate != nil {
			return op.Aggregate.Specs
		}
	}
	return nil
}

func projectionColumns(chain model.Chain) []string {
	var scanColumns []string
	var projectColumns []string
	for _, op := range chain.Operators {
		switch operatorKind(op) {
		case "scan":
			if op.Scan != nil && len(op.Scan.Columns) > 0 {
				scanColumns = append([]string(nil), op.Scan.Columns...)
			}
		case "project":
			if op.Project != nil {
				projectColumns = append([]string(nil), op.Project.Columns...)
			}
		}
	}
	if len(projectColumns) > 0 {
		return projectColumns
	}
	if len(scanColumns) > 0 {
		return scanColumns
	}
	return nil
}

func projectRows(snap model.Snapshot, rowIDs []int, columns []string) []model.ProjectedRow {
	if len(columns) == 0 {
		return nil
	}
	cols := make([]model.Column, len(columns))
	for i, name := range columns {
		cols[i] = mustColumn(snap, name)
	}
	rows := make([]model.ProjectedRow, 0, len(rowIDs))
	for _, row := range rowIDs {
		projected := model.ProjectedRow{RowID: int64(row), Values: make([]model.ProjectedValue, 0, len(cols))}
		for _, column := range cols {
			value := model.ProjectedValue{Column: column.Name, Type: column.Type, Null: column.NullAt(row)}
			if !value.Null {
				switch column.Type {
				case model.ColumnInt:
					v := column.IntValues[row]
					value.Int = &v
				case model.ColumnFixed:
					v := column.FixedValues[row]
					value.Units = &v
					value.Text = model.FormatFixed(v)
				case model.ColumnDict:
					v := column.DictIndices[row]
					value.DictIndex = &v
					if v >= 0 && int(v) < len(column.Dictionary) {
						value.Text = column.Dictionary[v]
					}
				}
			}
			projected.Values = append(projected.Values, value)
		}
		rows = append(rows, projected)
	}
	return rows
}

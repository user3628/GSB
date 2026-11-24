package engine

import (
	"strings"

	"columnar-executor/internal/fixed"
	"columnar-executor/internal/model"
)

func evalPredicate(snap model.Snapshot, column model.Column, row int, predicate model.Predicate) (bool, error) {
	op := strings.ToLower(strings.TrimSpace(predicate.Op))
	isNull := column.NullAt(row)
	if op == "is_null" {
		return isNull, nil
	}
	if op == "is_not_null" {
		return !isNull, nil
	}
	if isNull {
		return false, nil
	}

	switch column.Type {
	case model.ColumnInt:
		value := column.IntValues[row]
		return compareInt(value, *predicate.ValueInt, op)
	case model.ColumnFixed:
		value := column.FixedValues[row]
		if strings.HasPrefix(op, "mul_") {
			product, err := fixed.Mul(value, predicate.FactorUnits)
			if err != nil {
				return false, fixedPredicateError(err, column.Name, row, predicate.FactorUnits)
			}
			return compareInt(product, *predicate.ValueFixed, strings.TrimPrefix(op, "mul_"))
		}
		if strings.HasPrefix(op, "div_") {
			quotient, err := fixed.DivInt(value, predicate.DivisorUnits)
			if err != nil {
				return false, fixedPredicateError(err, column.Name, row, predicate.DivisorUnits)
			}
			return compareInt(quotient, *predicate.ValueFixed, strings.TrimPrefix(op, "div_"))
		}
		return compareInt(value, *predicate.ValueFixed, op)
	case model.ColumnDict:
		if predicate.ValueString == nil {
			return false, model.NewBizError(model.CodeInvalidRequest, "dictionary predicate value is missing").With("column", column.Name)
		}
		target := int32(-1)
		for i, value := range column.Dictionary {
			if value == *predicate.ValueString {
				target = int32(i)
				break
			}
		}
		if target < 0 {
			return false, nil
		}
		equal := column.DictIndices[row] == target
		if op == "dict_eq" {
			return equal, nil
		}
		if op == "dict_ne" {
			return !equal, nil
		}
	}
	return false, model.NewBizError(model.CodeInvalidRequest, "unsupported predicate type/op combination").
		With("column", column.Name).With("op", op)
}

func fixedPredicateError(err error, column string, row int, operand int64) error {
	if err == fixed.ErrDivideByZero {
		return model.NewBizError(model.CodeDivideByZero, "fixed point predicate divided by zero").
			With("column", column).With("row", row).With("divisor_units", operand)
	}
	return model.NewBizError(model.CodeNumericOverflow, "fixed point predicate arithmetic overflow").
		With("column", column).With("row", row).With("operand_units", operand)
}

func compareInt(left, right int64, op string) (bool, error) {
	switch op {
	case "eq":
		return left == right, nil
	case "ne":
		return left != right, nil
	case "gt":
		return left > right, nil
	case "ge":
		return left >= right, nil
	case "lt":
		return left < right, nil
	case "le":
		return left <= right, nil
	default:
		return false, model.NewBizError(model.CodeInvalidRequest, "unsupported comparison operation").With("op", op)
	}
}

// applyFilter creates a fresh predicate bitmap, intersects it with the fresh
// batch input bitmap, and leaves the original selection untouched.
func applyFilter(snap model.Snapshot, input model.Bitmap, predicate model.Predicate, batchNo int) (model.Bitmap, model.FilterStage, error) {
	predicateBitmap := model.NewNone(snap.RowCount)
	for _, row := range input.RowIDs() {
		matched, err := evalPredicate(snap, mustColumn(snap, predicate.Column), row, predicate)
		if err != nil {
			if biz, ok := model.AsBizError(err); ok {
				biz.With("batch", batchNo)
				return model.Bitmap{}, model.FilterStage{}, biz
			}
			return model.Bitmap{}, model.FilterStage{}, err
		}
		if matched {
			_ = predicateBitmap.Set(row)
		}
	}
	output, err := predicateBitmap.And(input)
	if err != nil {
		if biz, ok := model.AsBizError(err); ok {
			biz.With("batch", batchNo)
			return model.Bitmap{}, model.FilterStage{}, biz
		}
		return model.Bitmap{}, model.FilterStage{}, err
	}
	return output, model.FilterStage{
		Column:      predicate.Column,
		Op:          strings.ToLower(strings.TrimSpace(predicate.Op)),
		InputRows:   input.Count(),
		MatchedRows: predicateBitmap.Count(),
		OutputRows:  output.Count(),
	}, nil
}

func mustColumn(snap model.Snapshot, name string) model.Column {
	column, _ := snap.Column(name)
	return column
}

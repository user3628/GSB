package engine

import (
	"fmt"
	"strings"

	"columnar-executor/internal/model"
)

func invalidChain(message string) *model.BizError {
	return model.NewBizError(model.CodeInvalidRequest, message)
}

var validFilterOps = map[string]struct{}{
	"eq": {}, "ne": {}, "gt": {}, "ge": {}, "lt": {}, "le": {},
	"is_null": {}, "is_not_null": {}, "dict_eq": {}, "dict_ne": {},
	"mul_eq": {}, "mul_ne": {}, "mul_gt": {}, "mul_ge": {}, "mul_lt": {}, "mul_le": {},
	"div_eq": {}, "div_ne": {}, "div_gt": {}, "div_ge": {}, "div_lt": {}, "div_le": {},
}

var validAggregateFuncs = map[string]struct{}{
	"count": {}, "count_non_null": {}, "sum": {}, "avg": {}, "min": {}, "max": {}, "count_distinct": {},
}

func ValidateChain(chain model.Chain, snap model.Snapshot) error {
	if strings.TrimSpace(chain.ID) == "" {
		return invalidChain("chain id is required")
	}
	if chain.Version <= 0 {
		return invalidChain("chain version must be positive").With("version", chain.Version)
	}
	if chain.BaseSnapshotVersion != snap.Version {
		return model.NewBizError(model.CodeStaleVersion, "base snapshot version is not the requested snapshot").
			With("base_snapshot_version", chain.BaseSnapshotVersion).With("snapshot_version", snap.Version)
	}
	if len(chain.Operators) == 0 {
		return invalidChain("operator chain must not be empty")
	}
	if strings.ToLower(chain.Operators[0].Kind) != "scan" {
		return invalidChain("operator chain must start with scan")
	}

	aggregateCount := 0
	for index, op := range chain.Operators {
		kind := strings.ToLower(strings.TrimSpace(op.Kind))
		switch kind {
		case "scan":
			if index != 0 {
				return invalidChain("scan must be the first operator").With("operator_index", index)
			}
			if op.Scan == nil {
				return invalidChain("scan operator payload is required").With("operator_index", index)
			}
			if err := validateColumns(snap, op.Scan.Columns, false); err != nil {
				return err.With("operator_index", index)
			}
		case "filter":
			if op.Filter == nil {
				return invalidChain("filter operator payload is required").With("operator_index", index)
			}
			if err := validatePredicate(snap, op.Filter.Predicate); err != nil {
				return err.With("operator_index", index)
			}
		case "project":
			if op.Project == nil || len(op.Project.Columns) == 0 {
				return invalidChain("project operator requires at least one column").With("operator_index", index)
			}
			if err := validateColumns(snap, op.Project.Columns, true); err != nil {
				return err.With("operator_index", index)
			}
		case "aggregate":
			aggregateCount++
			if index != len(chain.Operators)-1 {
				return invalidChain("aggregate must be the last operator").With("operator_index", index)
			}
			if op.Aggregate == nil || len(op.Aggregate.Specs) == 0 {
				return invalidChain("aggregate operator requires at least one spec").With("operator_index", index)
			}
			seen := map[string]struct{}{}
			for _, spec := range op.Aggregate.Specs {
				if strings.TrimSpace(spec.Name) == "" {
					return invalidChain("aggregate spec name is required").With("operator_index", index)
				}
				if _, exists := seen[spec.Name]; exists {
					return invalidChain("duplicate aggregate spec name").With("name", spec.Name)
				}
				seen[spec.Name] = struct{}{}
				function := strings.ToLower(strings.TrimSpace(spec.Func))
				if _, ok := validAggregateFuncs[function]; !ok {
					return invalidChain("unsupported aggregate function").With("function", spec.Func).With("name", spec.Name)
				}
				if function == "count" && spec.Column == "" {
					continue
				}
				col, ok := snap.Column(spec.Column)
				if !ok {
					return invalidChain("aggregate column is not present in snapshot").With("column", spec.Column).With("name", spec.Name)
				}
				if function == "count_distinct" {
					if col.Type != model.ColumnDict {
						return invalidChain("count_distinct requires a dictionary column").With("column", spec.Column)
					}
					continue
				}
				if col.Type != model.ColumnInt && col.Type != model.ColumnFixed {
					return invalidChain("numeric aggregate requires an integer or fixed column").With("column", spec.Column).With("type", col.Type)
				}
			}
		default:
			return invalidChain("unsupported operator kind").With("kind", op.Kind).With("operator_index", index)
		}
	}
	if aggregateCount != 1 {
		return invalidChain("operator chain must contain exactly one aggregate operator").With("aggregate_count", aggregateCount)
	}
	return nil
}

func validateColumns(snap model.Snapshot, columns []string, require bool) *model.BizError {
	if len(columns) == 0 && require {
		return invalidChain("at least one column is required")
	}
	seen := map[string]struct{}{}
	for _, name := range columns {
		if _, ok := snap.Column(name); !ok {
			return invalidChain("column is not present in snapshot").With("column", name)
		}
		if _, duplicate := seen[name]; duplicate {
			return invalidChain("duplicate column").With("column", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func validatePredicate(snap model.Snapshot, predicate model.Predicate) *model.BizError {
	col, ok := snap.Column(predicate.Column)
	if !ok {
		return invalidChain("filter column is not present in snapshot").With("column", predicate.Column)
	}
	op := strings.ToLower(strings.TrimSpace(predicate.Op))
	if _, ok := validFilterOps[op]; !ok {
		return invalidChain("unsupported filter operation").With("op", predicate.Op)
	}
	switch op {
	case "is_null", "is_not_null":
		return nil
	case "dict_eq", "dict_ne":
		if col.Type != model.ColumnDict || predicate.ValueString == nil {
			return invalidChain("dictionary comparison requires a dictionary column and value_string").With("column", predicate.Column)
		}
		found := false
		for _, value := range col.Dictionary {
			if value == *predicate.ValueString {
				found = true
				break
			}
		}
		if !found {
			return invalidChain("dictionary comparison value is outside dictionary").
				With("column", predicate.Column).With("value", *predicate.ValueString)
		}
		return nil
	}

	if strings.HasPrefix(op, "mul_") || strings.HasPrefix(op, "div_") {
		if col.Type != model.ColumnFixed {
			return invalidChain("fixed arithmetic predicate requires a fixed column").With("column", predicate.Column)
		}
		if predicate.ValueFixed == nil {
			return invalidChain("fixed arithmetic predicate requires value_fixed").With("column", predicate.Column)
		}
		if strings.HasPrefix(op, "div_") && predicate.DivisorUnits == 0 {
			// Kept valid deliberately: the execution path must return
			// E_DIVIDE_BY_ZERO rather than hiding the fault at validation time.
			return nil
		}
		return nil
	}

	switch col.Type {
	case model.ColumnInt:
		if predicate.ValueInt == nil {
			return invalidChain("integer comparison requires value_int").With("column", predicate.Column)
		}
	case model.ColumnFixed:
		if predicate.ValueFixed == nil {
			return invalidChain("fixed comparison requires value_fixed").With("column", predicate.Column)
		}
	case model.ColumnDict:
		return invalidChain("dictionary column requires dict_eq or dict_ne").With("column", predicate.Column)
	}
	return nil
}

func validateOperatorChainForExecution(chain model.Chain, snap model.Snapshot) error {
	return ValidateChain(chain, snap)
}

func operatorKind(op model.Operator) string {
	return strings.ToLower(strings.TrimSpace(op.Kind))
}

func describeAggregates(specs []model.AggregateSpec) string {
	names := make([]string, len(specs))
	for i := range specs {
		names[i] = fmt.Sprintf("%s:%s(%s)", specs[i].Name, strings.ToLower(specs[i].Func), specs[i].Column)
	}
	return strings.Join(names, ",")
}

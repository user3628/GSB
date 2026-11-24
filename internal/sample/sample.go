package sample

import "columnar-executor/internal/model"

func Chain(chainID string, version, snapshotVersion int64) model.Chain {
	qtyMin := int64(20)
	return model.Chain{
		ID:                  chainID,
		Version:             version,
		BaseSnapshotVersion: snapshotVersion,
		Operators: []model.Operator{
			{Kind: "scan", Scan: &model.ScanOperator{Columns: []string{"id", "quantity", "price", "category"}}},
			{Kind: "filter", Filter: &model.FilterOperator{Predicate: model.Predicate{
				Column: "quantity", Op: "ge", ValueInt: &qtyMin,
			}}},
			{Kind: "project", Project: &model.ProjectOperator{Columns: []string{"id", "quantity", "price", "category"}}},
			{Kind: "aggregate", Aggregate: &model.AggregateOperator{Specs: []model.AggregateSpec{
				{Name: "row_count", Func: "count"},
				{Name: "quantity_count", Func: "count_non_null", Column: "quantity"},
				{Name: "quantity_sum", Func: "sum", Column: "quantity"},
				{Name: "quantity_avg", Func: "avg", Column: "quantity"},
				{Name: "price_sum", Func: "sum", Column: "price"},
				{Name: "price_avg", Func: "avg", Column: "price"},
				{Name: "category_distinct", Func: "count_distinct", Column: "category"},
			}}},
		},
	}
}

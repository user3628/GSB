package fixtures

import (
	"time"

	"example.com/logicplan/editor/internal/model"
)

var GeneratedAt = time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)

func col(qualifier, name string) model.ColumnRef {
	return model.ColumnRef{Qualifier: qualifier, Name: name}
}

func stats(id string, rows int64, distinct map[string]int64) *model.StatsBinding {
	return &model.StatsBinding{OperatorID: id, RowCount: rows, Distinct: distinct}
}

// Current returns the canonical six-operator plan used by both current wire
// formats. Its snapshot sequence is supplied by the caller so that separate
// deterministic fixtures can be imported monotonically.
func Current(version int, seq uint64) *model.Snapshot {
	nodes := []*model.Node{
		{
			ID: "sort_amount", Type: model.TypeSort, Children: []string{"project_output"},
			Columns: []model.ColumnRef{col("orders", "amount")},
			Stats:   stats("sort_amount", 2500, map[string]int64{"orders.amount": 2300}),
		},
		{
			ID: "project_output", Type: model.TypeProject, Children: []string{"filter_high_value"},
			Columns: []model.ColumnRef{col("orders", "id"), col("customers", "region"), col("orders", "amount")},
			Stats:   stats("project_output", 2500, map[string]int64{"orders.id": 2500, "customers.region": 8, "orders.amount": 2300}),
		},
		{
			ID: "filter_high_value", Type: model.TypeFilter, Children: []string{"join_orders_customers"},
			Columns: []model.ColumnRef{col("orders", "amount")},
			Predicate: &model.Predicate{
				Expr: "orders.amount > 100", Columns: []model.ColumnRef{col("orders", "amount")},
			},
			Stats: stats("filter_high_value", 2500, map[string]int64{"orders.amount": 2300}),
		},
		{
			ID: "join_orders_customers", Type: model.TypeJoin,
			Children: []string{"scan_orders", "scan_customers"}, Columns: []model.ColumnRef{},
			Predicate: &model.Predicate{
				Expr:    "orders.customer_id = customers.id",
				Columns: []model.ColumnRef{col("orders", "customer_id"), col("customers", "id")},
			},
			Stats: stats("join_orders_customers", 10000, map[string]int64{"orders.customer_id": 500, "customers.id": 500}),
		},
		{
			ID: "scan_orders", Type: model.TypeScan,
			Columns: []model.ColumnRef{col("orders", "id"), col("orders", "customer_id"), col("orders", "amount")},
			Stats:   stats("scan_orders", 10000, map[string]int64{"orders.id": 10000, "orders.customer_id": 500, "orders.amount": 8000}),
		},
		{
			ID: "scan_customers", Type: model.TypeScan,
			Columns: []model.ColumnRef{col("customers", "id"), col("customers", "region")},
			Stats:   stats("scan_customers", 500, map[string]int64{"customers.id": 500, "customers.region": 8}),
		},
	}
	return &model.Snapshot{FormatVersion: version, SnapshotSeq: seq, GeneratedAt: GeneratedAt, Nodes: nodes}
}

// Legacy returns a valid v1 tree. Both columns and stats are present in memory
// but the v1 encoders omit them, exercising the declared degradation path.
func Legacy(version int, seq uint64) *model.Snapshot {
	nodes := []*model.Node{
		{
			ID: "sort_legacy", Type: model.TypeSort, Children: []string{"project_legacy"},
			Columns: []model.ColumnRef{col("orders", "amount")},
			Stats:   stats("sort_legacy", 900, map[string]int64{"orders.amount": 900}),
		},
		{
			ID: "project_legacy", Type: model.TypeProject, Children: []string{"filter_legacy"},
			Columns: []model.ColumnRef{col("orders", "id"), col("orders", "amount")},
			Stats:   stats("project_legacy", 900, map[string]int64{"orders.id": 900, "orders.amount": 900}),
		},
		{
			ID: "filter_legacy", Type: model.TypeFilter, Children: []string{"scan_legacy"},
			Columns:   []model.ColumnRef{col("orders", "amount")},
			Predicate: &model.Predicate{Expr: "orders.amount > 10"},
			Stats:     stats("filter_legacy", 900, map[string]int64{"orders.amount": 900}),
		},
		{
			ID: "scan_legacy", Type: model.TypeScan,
			Columns: []model.ColumnRef{col("orders", "id"), col("orders", "amount")},
			Stats:   stats("scan_legacy", 1000, map[string]int64{"orders.id": 1000, "orders.amount": 950}),
		},
	}
	return &model.Snapshot{FormatVersion: version, SnapshotSeq: seq, GeneratedAt: GeneratedAt, Nodes: nodes}
}

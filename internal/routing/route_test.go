package routing_test

import (
	"reflect"
	"testing"

	"example.com/gsb-router/internal/model"
	"example.com/gsb-router/internal/routing"
	"example.com/gsb-router/internal/sample"
)

func ptr[T any](value T) *T { return &value }

func findTable(snapshot model.Snapshot, id string) *model.LogicalTable {
	for i := range snapshot.LogicalTables {
		if snapshot.LogicalTables[i].ID == id {
			return &snapshot.LogicalTables[i]
		}
	}
	return nil
}

func TestEqualAndInRouting(t *testing.T) {
	snapshot := sample.SnapshotV1()
	orders := findTable(snapshot, "orders")
	result, err := routing.Compute(orders, routing.Query{TableID: "orders", Kind: routing.KindEqual, Value: ptr("5")})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.PhysicalTables, []string{"db0.orders_01"}) {
		t.Fatalf("unexpected equal route: %#v", result.PhysicalTables)
	}
	in, err := routing.Compute(orders, routing.Query{TableID: "orders", Kind: routing.KindIn, Values: []string{"0", "1", "5"}})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"db0.orders_00", "db0.orders_01"}
	if !reflect.DeepEqual(in.PhysicalTables, want) {
		t.Fatalf("unexpected in route: %#v", in.PhysicalTables)
	}
}

func TestModRangeCrossesResidueBoundaryExactly(t *testing.T) {
	snapshot := sample.SnapshotV1()
	orders := findTable(snapshot, "orders")
	result, err := routing.Compute(orders, routing.Query{
		TableID: "orders", Kind: routing.KindRange,
		Lower: ptr("3"), Upper: ptr("5"), IncludeLower: ptr(true), IncludeUpper: ptr(true),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"db0.orders_00", "db0.orders_01", "db1.orders_03"}
	if !reflect.DeepEqual(result.PhysicalTables, want) {
		t.Fatalf("unexpected residues: %#v", result.PhysicalTables)
	}
}

func TestRangeBoundarySemanticsAndEmptyRange(t *testing.T) {
	snapshot := sample.SnapshotV1()
	ledger := findTable(snapshot, "ledger")
	lowerOnly, err := routing.Compute(ledger, routing.Query{
		TableID: "ledger", Kind: routing.KindRange,
		Lower: ptr("999"), Upper: ptr("1000"), IncludeLower: ptr(true), IncludeUpper: ptr(false),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(lowerOnly.PhysicalTables, []string{"db2.ledger_00"}) {
		t.Fatalf("unexpected lower-boundary route: %#v", lowerOnly.PhysicalTables)
	}
	upperOnly, err := routing.Compute(ledger, routing.Query{
		TableID: "ledger", Kind: routing.KindRange,
		Lower: ptr("999"), Upper: ptr("1000"), IncludeLower: ptr(false), IncludeUpper: ptr(true),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(upperOnly.PhysicalTables, []string{"db2.ledger_01"}) {
		t.Fatalf("unexpected upper-boundary route: %#v", upperOnly.PhysicalTables)
	}
	empty, err := routing.Compute(ledger, routing.Query{
		TableID: "ledger", Kind: routing.KindRange,
		Lower: ptr("10"), Upper: ptr("9"), IncludeLower: ptr(true), IncludeUpper: ptr(true),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !empty.Empty || len(empty.PhysicalTables) != 0 {
		t.Fatalf("expected empty range, got %#v", empty)
	}
}

func TestHashRoutingDeterministicAndConservativeRange(t *testing.T) {
	snapshot := sample.SnapshotV1()
	events := findTable(snapshot, "events")
	query := routing.Query{TableID: "events", Kind: routing.KindEqual, Value: ptr("tenant-42")}
	first, err := routing.Compute(events, query)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		next, err := routing.Compute(events, query)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(first, next) {
			t.Fatalf("hash routing changed: %#v != %#v", first, next)
		}
	}
	all, err := routing.Compute(events, routing.Query{
		TableID: "events", Kind: routing.KindRange,
		Lower: ptr("0"), Upper: ptr("10"), IncludeLower: ptr(true), IncludeUpper: ptr(true),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !all.Conservative || len(all.PhysicalTables) != 4 {
		t.Fatalf("hash range must be explicit conservative all-shard: %#v", all)
	}
}

func TestMergedLogicalTablesSharePhysicalTable(t *testing.T) {
	snapshot := sample.SnapshotV1()
	seen := map[string]string{}
	for _, id := range []string{"audit_eu", "audit_us"} {
		table := findTable(snapshot, id)
		result, err := routing.Compute(table, routing.Query{TableID: id, Kind: routing.KindEqual, Value: ptr("8")})
		if err != nil {
			t.Fatal(err)
		}
		if len(result.PhysicalTables) != 1 {
			t.Fatalf("%s: %#v", id, result.PhysicalTables)
		}
		seen[id] = result.PhysicalTables[0]
	}
	if seen["audit_eu"] != "db0.audit_shared_00" || seen["audit_us"] != "db0.audit_shared_00" {
		t.Fatalf("merged mappings not preserved: %#v", seen)
	}
}

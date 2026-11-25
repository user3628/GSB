package sample

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"example.com/gsb-router/internal/model"
)

const (
	Seed                    = uint64(20261125)
	InitialGeneratedAt      = "2026-01-01T00:00:00Z"
	IncrementGeneratedAt    = "2026-01-02T00:00:00Z"
	CrashIncrementGenerated = "2026-01-03T00:00:00Z"
)

func physical(id, db, name string) model.PhysicalTable {
	return model.PhysicalTable{ID: id, LogicalDB: db, Name: name}
}

func closedRange(id string, lower, upper int64, physical string, weight int64) model.ShardRange {
	return model.ShardRange{
		ID:             id,
		Lower:          lower,
		Upper:          upper,
		LowerInclusive: true,
		UpperInclusive: true,
		PhysicalTable:  physical,
		Weight:         weight,
	}
}

func modTable(id, logicalDB, key, primaryKey string, modulus int64, physicals ...string) model.LogicalTable {
	table := model.LogicalTable{
		ID:           id,
		LogicalDB:    logicalDB,
		ShardKey:     key,
		ShardKeyType: model.ShardKeyInt,
		Algorithm:    model.Algorithm{Type: model.AlgorithmMod, Modulus: modulus},
		PrimaryKey:   primaryKey,
	}
	for slot := int64(0); slot < modulus; slot++ {
		table.Ranges = append(table.Ranges, closedRange(fmt.Sprintf("slot-%02d", slot), slot, slot, physicals[slot], 1))
	}
	return table
}

// SnapshotV1 is the deterministic full baseline. It intentionally includes a
// hash-sharded table, a range-sharded table, and two logical tables that map to
// one physical table to exercise merge semantics.
func SnapshotV1() model.Snapshot {
	physicalTables := []model.PhysicalTable{
		physical("db0.orders_00", "db0", "orders_00"),
		physical("db0.orders_01", "db0", "orders_01"),
		physical("db1.orders_02", "db1", "orders_02"),
		physical("db1.orders_03", "db1", "orders_03"),
		physical("db0.order_items_00", "db0", "order_items_00"),
		physical("db0.order_items_01", "db0", "order_items_01"),
		physical("db1.order_items_02", "db1", "order_items_02"),
		physical("db1.order_items_03", "db1", "order_items_03"),
		physical("db0.events_00", "db0", "events_00"),
		physical("db0.events_01", "db0", "events_01"),
		physical("db1.events_02", "db1", "events_02"),
		physical("db1.events_03", "db1", "events_03"),
		physical("db2.ledger_00", "db2", "ledger_00"),
		physical("db2.ledger_01", "db2", "ledger_01"),
		physical("db2.ledger_02", "db2", "ledger_02"),
		physical("db0.audit_shared_00", "db0", "audit_shared_00"),
	}
	orders := modTable("orders", "db0", "user_id", "order_id", 4,
		"db0.orders_00", "db0.orders_01", "db1.orders_02", "db1.orders_03")
	orders.Algorithm = model.Algorithm{Type: model.AlgorithmMod, Modulus: 4, Seed: Seed}
	orderItems := modTable("order_items", "db0", "user_id", "item_id", 4,
		"db0.order_items_00", "db0.order_items_01", "db1.order_items_02", "db1.order_items_03")
	events := model.LogicalTable{
		ID:           "events",
		LogicalDB:    "db0",
		ShardKey:     "tenant",
		ShardKeyType: model.ShardKeyString,
		Algorithm:    model.Algorithm{Type: model.AlgorithmHash, Modulus: 4, Seed: Seed},
		PrimaryKey:   "event_id",
		Ranges: []model.ShardRange{
			closedRange("hash-0", 0, 0, "db0.events_00", 1),
			closedRange("hash-1", 1, 1, "db0.events_01", 1),
			closedRange("hash-2", 2, 2, "db1.events_02", 1),
			closedRange("hash-3", 3, 3, "db1.events_03", 1),
		},
	}
	ledger := model.LogicalTable{
		ID:           "ledger",
		LogicalDB:    "db2",
		ShardKey:     "account_id",
		ShardKeyType: model.ShardKeyInt,
		Algorithm:    model.Algorithm{Type: model.AlgorithmRange},
		PrimaryKey:   "entry_id",
		Ranges: []model.ShardRange{
			closedRange("ledger-low", 0, 999, "db2.ledger_00", 1),
			closedRange("ledger-mid", 1000, 1999, "db2.ledger_01", 2),
			closedRange("ledger-high", 2000, 2999, "db2.ledger_02", 3),
		},
	}
	auditEU := model.LogicalTable{
		ID:           "audit_eu",
		LogicalDB:    "db0",
		ShardKey:     "tenant_id",
		ShardKeyType: model.ShardKeyInt,
		Algorithm:    model.Algorithm{Type: model.AlgorithmMod, Modulus: 4, Seed: Seed},
		PrimaryKey:   "audit_id",
		Ranges:       []model.ShardRange{closedRange("shared-eu", 0, 3, "db0.audit_shared_00", 4)},
	}
	auditUS := model.LogicalTable{
		ID:           "audit_us",
		LogicalDB:    "db0",
		ShardKey:     "tenant_id",
		ShardKeyType: model.ShardKeyInt,
		Algorithm:    model.Algorithm{Type: model.AlgorithmMod, Modulus: 4, Seed: Seed},
		PrimaryKey:   "audit_id",
		Ranges:       []model.ShardRange{closedRange("shared-us", 0, 3, "db0.audit_shared_00", 4)},
	}
	snapshot := model.Snapshot{
		Version:        1,
		GeneratedAt:    InitialGeneratedAt,
		Cluster:        "local-demo-cluster",
		ShardKey:       "user_id",
		ShardKeyType:   model.ShardKeyInt,
		Algorithm:      model.Algorithm{Type: model.AlgorithmMod, Modulus: 4, Seed: Seed},
		PhysicalTables: physicalTables,
		LogicalTables:  []model.LogicalTable{orders, orderItems, events, ledger, auditEU, auditUS},
		Aggregates: []model.AggregateView{
			{ID: "order_overview", DependsOn: []string{"orders", "order_items"}, Kind: "sales_rollup"},
			{ID: "audit_rollup", DependsOn: []string{"audit_eu", "audit_us", "events"}, Kind: "compliance_rollup"},
		},
		Checksum: "auto",
	}
	sealed, err := model.ValidateSnapshot(snapshot, model.ValidationOptions{SealChecksum: true})
	if err != nil {
		panic(fmt.Sprintf("sealed sample is invalid: %v", err))
	}
	return sealed
}

func IncrementV2() model.Increment {
	increment := model.Increment{
		CommitID:    "inc-orders-repartition-v2",
		BaseVersion: 1,
		GeneratedAt: IncrementGeneratedAt,
		Changes: []model.TableChange{
			{
				Op:      model.ChangeReplaceRanges,
				TableID: "orders",
				Ranges: []model.ShardRange{
					closedRange("slot-00-01", 0, 1, "db0.orders_00", 2),
					closedRange("slot-02", 2, 2, "db1.orders_02", 1),
					closedRange("slot-03", 3, 3, "db1.orders_03", 1),
				},
			},
		},
	}
	sum, err := model.IncrementChecksum(increment)
	if err != nil {
		panic(err)
	}
	increment.Checksum = sum
	return increment
}

func IncrementLedgerCrash() model.Increment {
	increment := model.Increment{
		CommitID:    "inc-ledger-crash-v3",
		BaseVersion: 2,
		GeneratedAt: CrashIncrementGenerated,
		Inject:      "rebuild_interrupt",
		Changes: []model.TableChange{
			{
				Op:      model.ChangeReplaceRanges,
				TableID: "ledger",
				Ranges: []model.ShardRange{
					closedRange("ledger-low-v3", 0, 999, "db2.ledger_00", 1),
					closedRange("ledger-mid-v3", 1000, 1999, "db2.ledger_01", 1),
					closedRange("ledger-high-v3", 2000, 2999, "db2.ledger_02", 4),
				},
			},
		},
	}
	sum, err := model.IncrementChecksum(increment)
	if err != nil {
		panic(err)
	}
	increment.Checksum = sum
	return increment
}

func InvalidMappingSample() model.Snapshot {
	snapshot := SnapshotV1()
	snapshot.Version = 2
	snapshot.GeneratedAt = IncrementGeneratedAt
	findLogical(&snapshot, "orders").Ranges[0].PhysicalTable = "db9.missing_orders"
	snapshot.Checksum = "auto"
	sum, _ := model.SnapshotChecksum(snapshot)
	snapshot.Checksum = sum
	return snapshot
}

func InvalidOverlapSample() model.Snapshot {
	snapshot := SnapshotV1()
	snapshot.Version = 2
	snapshot.GeneratedAt = IncrementGeneratedAt
	orders := findLogical(&snapshot, "orders")
	orders.Ranges[1].Lower = orders.Ranges[0].Upper
	orders.Ranges[1].LowerInclusive = true
	orders.Ranges[0].UpperInclusive = true
	snapshot.Checksum = "auto"
	sum, _ := model.SnapshotChecksum(snapshot)
	snapshot.Checksum = sum
	return snapshot
}

func findLogical(snapshot *model.Snapshot, id string) *model.LogicalTable {
	for i := range snapshot.LogicalTables {
		if snapshot.LogicalTables[i].ID == id {
			return &snapshot.LogicalTables[i]
		}
	}
	panic("sample table not found: " + id)
}

func StaleBaseIncrement() model.Increment {
	increment := IncrementV2()
	increment.CommitID = "inc-stale-base-demo"
	increment.BaseVersion = 0
	sum, _ := model.IncrementChecksum(increment)
	increment.Checksum = sum
	return increment
}

type Files struct {
	SnapshotV1     model.Snapshot  `json:"snapshot_v1"`
	IncrementV2    model.Increment `json:"increment_v2"`
	LedgerCrash    model.Increment `json:"ledger_crash_increment"`
	InvalidMapping model.Snapshot  `json:"invalid_mapping_snapshot"`
	InvalidOverlap model.Snapshot  `json:"invalid_overlap_snapshot"`
	StaleBase      model.Increment `json:"stale_base_increment"`
}

func All() Files {
	return Files{
		SnapshotV1:     SnapshotV1(),
		IncrementV2:    IncrementV2(),
		LedgerCrash:    IncrementLedgerCrash(),
		InvalidMapping: InvalidMappingSample(),
		InvalidOverlap: InvalidOverlapSample(),
		StaleBase:      StaleBaseIncrement(),
	}
}

func WriteFiles(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	files := All()
	outputs := map[string]any{
		"snapshot-v1.json":              files.SnapshotV1,
		"increment-v2.json":             files.IncrementV2,
		"increment-crash-v3.json":       files.LedgerCrash,
		"snapshot-invalid-mapping.json": files.InvalidMapping,
		"snapshot-invalid-overlap.json": files.InvalidOverlap,
		"increment-stale-base.json":     files.StaleBase,
	}
	for name, value := range outputs {
		raw, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return err
		}
		raw = append(raw, '\n')
		if err := os.WriteFile(filepath.Join(dir, name), raw, 0o644); err != nil {
			return err
		}
	}
	return nil
}

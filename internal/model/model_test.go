package model_test

import (
	"testing"

	"example.com/gsb-router/internal/errs"
	"example.com/gsb-router/internal/model"
	"example.com/gsb-router/internal/sample"
)

func TestSnapshotV1ValidatesAndIsDeterministic(t *testing.T) {
	snapshot := sample.SnapshotV1()
	validated, err := model.ValidateSnapshot(snapshot, model.ValidationOptions{})
	if err != nil {
		t.Fatalf("validate sample: %v", err)
	}
	again, err := model.SnapshotChecksum(validated)
	if err != nil {
		t.Fatal(err)
	}
	if again != validated.Checksum {
		t.Fatalf("checksum changed: %s != %s", again, validated.Checksum)
	}
	if len(validated.LogicalTables) != 6 {
		t.Fatalf("expected six logical tables, got %d", len(validated.LogicalTables))
	}
}

func TestChecksumMismatchRejected(t *testing.T) {
	snapshot := sample.SnapshotV1()
	snapshot.Checksum = "deadbeef"
	_, err := model.ValidateSnapshot(snapshot, model.ValidationOptions{})
	assertCode(t, err, errs.CodeSnapshotInvalid, errs.CodeSnapshotInvalid)
}

func TestMissingPhysicalTableUsesSpecificCodeAndCategory(t *testing.T) {
	snapshot := sample.SnapshotV1()
	snapshot.Checksum = "auto"
	findTable(&snapshot, "orders").Ranges[0].PhysicalTable = "db9.missing"
	_, err := model.ValidateSnapshot(snapshot, model.ValidationOptions{SealChecksum: true})
	assertCode(t, err, errs.CodePhysicalTableMiss, errs.CodeSnapshotInvalid)
}

func TestOverlappingRangesRejected(t *testing.T) {
	snapshot := sample.SnapshotV1()
	snapshot.Checksum = "auto"
	orders := findTable(&snapshot, "orders")
	orders.Ranges[1].Lower = orders.Ranges[0].Upper
	orders.Ranges[1].LowerInclusive = true
	orders.Ranges[0].UpperInclusive = true
	_, err := model.ValidateSnapshot(snapshot, model.ValidationOptions{SealChecksum: true})
	assertCode(t, err, errs.CodeRangeOverlap, errs.CodeSnapshotInvalid)
}

func findTable(snapshot *model.Snapshot, id string) *model.LogicalTable {
	for i := range snapshot.LogicalTables {
		if snapshot.LogicalTables[i].ID == id {
			return &snapshot.LogicalTables[i]
		}
	}
	panic("table not found: " + id)
}

func TestDuplicateLogicalTableRejected(t *testing.T) {
	snapshot := sample.SnapshotV1()
	snapshot.Checksum = "auto"
	snapshot.LogicalTables = append(snapshot.LogicalTables, snapshot.LogicalTables[0])
	_, err := model.ValidateSnapshot(snapshot, model.ValidationOptions{SealChecksum: true})
	assertCode(t, err, errs.CodeSnapshotInvalid, errs.CodeSnapshotInvalid)
}

func assertCode(t *testing.T, err error, code, category string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil", code)
	}
	biz := errs.As(err)
	if biz == nil {
		t.Fatalf("expected business error, got %T: %v", err, err)
	}
	if biz.Code != code {
		t.Fatalf("expected code %s, got %s (%v)", code, biz.Code, err)
	}
	if category != "" && biz.Category != category {
		t.Fatalf("expected category %s, got %s", category, biz.Category)
	}
}

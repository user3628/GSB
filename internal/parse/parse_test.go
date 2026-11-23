package parse

import (
	"strings"
	"testing"

	"baselineworkbench/internal/codes"
	"baselineworkbench/internal/policy"
)

func TestSnapshotPreservesValidEntriesAndReportsUnknownUnit(t *testing.T) {
	data := []byte(`{
  "items": [
    {"id":"valid.item","unit":"second","value":"900","weight":"1.0"},
    {"id":"bad.unit","unit":"fortnight","value":"1","weight":"1.0"},
    {"id":"valid.score","unit":"score","value":"3.141592","weight":"1.25"}
  ]
}`)
	result := Snapshot(data)
	if len(result.Items) != 2 {
		t.Fatalf("expected two valid items, got %d", len(result.Items))
	}
	if len(result.Issues) != 1 || result.Issues[0].Code != codes.UnitUnknown {
		t.Fatalf("expected E_UNIT_UNKNOWN, got %+v", result.Issues)
	}
}

func TestSnapshotDuplicateAndInvalidID(t *testing.T) {
	data := []byte(`{
  "items": [
    {"id":"valid.item","unit":"second","value":"900"},
    {"id":"valid.item","unit":"second","value":"600"},
    {"id":"9bad","unit":"second","value":"1"}
  ]
}`)
	result := Snapshot(data)
	if len(result.Items) != 1 {
		t.Fatalf("expected one accepted item, got %d", len(result.Items))
	}
	foundMalformed := false
	for _, issue := range result.Issues {
		if issue.Code == codes.PolicyMalformed {
			foundMalformed = true
		}
	}
	if !foundMalformed {
		t.Fatalf("expected malformed issue, got %+v", result.Issues)
	}
}

func TestRepairsLengthPrefixAndTruncation(t *testing.T) {
	first := RepairBlock(policy.RawRecord{ID: "first.item", Unit: "minute", Value: "15", Weight: "1.0"})
	second := RepairBlock(policy.RawRecord{ID: "second.item", Unit: "second", Value: "600", Weight: "1.0"})
	data := append(append([]byte{}, first...), second...)
	result := Repairs(data)
	if len(result.Items) != 2 || len(result.Issues) != 0 {
		t.Fatalf("valid blocks parsed incorrectly: items=%d issues=%+v", len(result.Items), result.Issues)
	}
	truncated := append(append([]byte{}, first...), []byte("999:{")...)
	result = Repairs(truncated)
	if len(result.Items) != 1 {
		t.Fatalf("expected prior item preserved, got %d", len(result.Items))
	}
	if len(result.Issues) != 1 || result.Issues[0].Code != codes.PolicyMalformed {
		t.Fatalf("expected E_POLICY_MALFORMED, got %+v", result.Issues)
	}
}

func TestRepairsUnknownUnitKeepsPriorItems(t *testing.T) {
	first := RepairBlock(policy.RawRecord{ID: "first.item", Unit: "second", Value: "600"})
	unknown := RepairBlock(policy.RawRecord{ID: "second.item", Unit: "fortnight", Value: "1"})
	last := RepairBlock(policy.RawRecord{ID: "third.item", Unit: "score", Value: "1.5"})
	result := Repairs([]byte(strings.Join([]string{string(first), string(unknown), string(last)}, "")))
	if len(result.Items) != 2 {
		t.Fatalf("expected two valid items, got %d", len(result.Items))
	}
	if len(result.Issues) != 1 || result.Issues[0].Code != codes.UnitUnknown {
		t.Fatalf("expected unknown unit issue, got %+v", result.Issues)
	}
}

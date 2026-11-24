package policy

import (
	"testing"

	"example.com/replicabudget/internal/apperr"
)

func TestLegacyParseRejectsEntireBatchOnDuplicate(t *testing.T) {
	data := []byte(`[
	  {"service_id":"svc-a","fixed_replicas":2,"vram_bytes_per_replica":1,"compute_milli_per_replica":1,"elastic_enabled":true},
	  {"service_id":"svc-a","fixed_replicas":3,"vram_bytes_per_replica":1,"compute_milli_per_replica":1,"elastic_enabled":true}
	]`)
	_, err := ParseLegacyJSON(data)
	assertBizError(t, err, apperr.SpecInvalid, 2)
}

func TestLegacyParseDoesNotSilentlyIgnoreDeprecatedFields(t *testing.T) {
	legacy, err := ParseLegacyJSON(readSample(t, "legacy.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(legacy) != 3 {
		t.Fatalf("len=%d", len(legacy))
	}
	if _, ok := legacy[0].DeprecatedFields["legacy_note"]; !ok {
		t.Fatalf("deprecated fields were not retained")
	}
}

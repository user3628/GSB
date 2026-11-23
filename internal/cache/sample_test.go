package cache

import (
	"context"
	"encoding/json"
	"testing"
)

func TestInvalidSampleBatchPersistsNothing(t *testing.T) {
	validData := []byte(`{
  "entries": [
    {"stage_id":"a","input_fingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","toolchain_version":"v1","artifact_content":"one","artifact_digest":"sha256:7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed","artifact_size":3,"retain_until":"2030-01-01T00:00:00Z"},
    {"stage_id":"b","input_fingerprint":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","toolchain_version":"v1","artifact_content":"two","artifact_digest":"sha256:NOT-A-DIGEST","artifact_size":3,"retain_until":"2030-01-01T00:00:00Z"}
  ]
}`)
	inputs, err := ParseSampleBatch(validData)
	if !IsCode(err, CodeEntryInvalid) {
		t.Fatalf("expected E_ENTRY_INVALID, got inputs=%v err=%v", inputs, err)
	}
	if inputs != nil {
		t.Fatalf("invalid batch returned partial inputs: %+v", inputs)
	}

	store := openTestStore(t, 1024, nil)
	if _, err := store.PutBatch(context.Background(), []EntryInput{{StageID: "bad"}}); !IsCode(err, CodeEntryInvalid) {
		t.Fatalf("expected E_ENTRY_INVALID from PutBatch, got %v", err)
	}
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM entries`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("invalid batch persisted %d entries", count)
	}
}

func TestNegativeArtifactSizeRejected(t *testing.T) {
	var batch sampleBatch
	if err := json.Unmarshal([]byte(`{"entries":[{"stage_id":"a","input_fingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","toolchain_version":"v1","artifact_content":"one","artifact_digest":"sha256:7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed","artifact_size":-1,"retain_until":"2030-01-01T00:00:00Z"}]}`), &batch); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(batch)
	_, err := ParseSampleBatch(data)
	if !IsCode(err, CodeEntryInvalid) {
		t.Fatalf("expected E_ENTRY_INVALID, got %v", err)
	}
}

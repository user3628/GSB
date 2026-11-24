package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"example.com/replicabudget/internal/apperr"
	"example.com/replicabudget/internal/policy"
	"example.com/replicabudget/internal/store"
)

func sample(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "samples", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func newAPI(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	var quotas []policy.QuotaGroup
	if err := json.Unmarshal(sample(t, "quotas.json"), &quotas); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SetQuotaGroups(context.Background(), 0, quotas); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((&Server{Store: st}).Handler())
	t.Cleanup(func() {
		server.Close()
		_ = st.Close()
	})
	return server, st
}

func TestHTTPImportAndUnifiedError(t *testing.T) {
	server, _ := newAPI(t)
	resp, err := http.Post(server.URL+"/v1/import?base_version=1", "application/jsonl", bytes.NewReader(sample(t, "new.jsonl")))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	resp, err = http.Post(server.URL+"/v1/import?base_version=0", "application/jsonl", bytes.NewReader(sample(t, "new.jsonl")))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var envelope errorEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error == nil || envelope.Error.Code != apperr.StaleBase {
		t.Fatalf("unexpected error body: %+v", envelope)
	}
	if envelope.Error.Details["current_version"].(float64) != 2 {
		t.Fatalf("current version detail: %+v", envelope.Error.Details)
	}
}

func TestHTTPCorruptBinaryAndAmbiguousMigration(t *testing.T) {
	server, _ := newAPI(t)
	corrupt := append([]byte(nil), sample(t, "new.bin")...)
	corrupt[len(corrupt)-1] ^= 0xff
	resp, err := http.Post(server.URL+"/v1/import?base_version=1", "application/octet-stream", bytes.NewReader(corrupt))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	var corruptEnvelope errorEnvelope
	if err := json.Unmarshal(body, &corruptEnvelope); err != nil {
		t.Fatal(err)
	}
	if corruptEnvelope.Error.Code != apperr.RecordCorrupt || corruptEnvelope.Error.RecordIndex != 3 {
		t.Fatalf("unexpected corrupt error: %+v", corruptEnvelope.Error)
	}
	resp, err = http.Post(server.URL+"/v1/migrate?base_version=1", "application/json", bytes.NewReader(sample(t, "legacy_ambiguous.json")))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var migration errorEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&migration); err != nil {
		t.Fatal(err)
	}
	if migration.Error.Code != apperr.MigrationAmbiguous || migration.Error.Details["numeric_mismatches"] == nil {
		t.Fatalf("unexpected migration error: %+v", migration.Error)
	}
}

package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"replicationlag/internal/compat"
	"replicationlag/internal/domain"
	"replicationlag/internal/engine"
	"replicationlag/internal/store"
)

const httpSource = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

func TestObservationLifecycleAndErrorCodes(t *testing.T) {
	server, engineInstance := newTestServer(t)
	observation := map[string]any{
		"replica_id": "replica-x", "sampled_at_ms": 1000,
		"gtid_segments": []string{httpSource + ":1-5"}, "position_name": httpSource + ":1-5",
		"lag_ms": 120, "reported_version": "8.0.36",
	}
	response := doJSON(t, server, http.MethodPost, "/v1/observations", map[string]any{
		"batch_id": "edit-1", "base_version": 0, "observation": observation,
	}, nil)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.StatusCode, readAll(t, response))
	}
	if response.Header.Get("X-Engine-Version") != "1" {
		t.Fatalf("version header = %q", response.Header.Get("X-Engine-Version"))
	}
	replay := doJSON(t, server, http.MethodPost, "/v1/observations", map[string]any{
		"batch_id": "edit-1", "base_version": 0, "observation": observation,
	}, nil)
	if replay.StatusCode != http.StatusOK || replay.Header.Get("X-Idempotent-Replay") != "true" {
		t.Fatalf("replay status=%d replay=%q", replay.StatusCode, replay.Header.Get("X-Idempotent-Replay"))
	}

	sequenceResponse := doJSON(t, server, http.MethodGet, "/v1/replicas/replica-x/sequence", nil, nil)
	if sequenceResponse.StatusCode != http.StatusOK {
		t.Fatalf("sequence status=%d body=%s", sequenceResponse.StatusCode, readAll(t, sequenceResponse))
	}
	var sequenceEnvelope map[string]any
	if err := json.NewDecoder(sequenceResponse.Body).Decode(&sequenceEnvelope); err != nil {
		t.Fatal(err)
	}
	sequence := sequenceEnvelope["sequence"].(map[string]any)
	if sequence["current_state"] != "HEALTHY" || len(sequence["steps"].([]any)) != 1 {
		t.Fatalf("sequence = %#v", sequence)
	}

	stale := doJSON(t, server, http.MethodPost, "/v1/observations", map[string]any{
		"batch_id": "stale", "base_version": 0,
		"observation": map[string]any{
			"replica_id": "replica-y", "sampled_at_ms": 1000, "gtid_segments": []string{httpSource + ":1"},
			"position_name": httpSource + ":1", "lag_ms": 1, "reported_version": "8.0.0",
		},
	}, nil)
	staleBody := decodeError(t, stale)
	if stale.StatusCode != http.StatusConflict || staleBody["code"] != "E_STALE_BASE" {
		t.Fatalf("stale status=%d body=%s", stale.StatusCode, readAll(t, stale))
	}
	if _, ok := staleBody["context"].(map[string]any)["current_version"]; !ok {
		t.Fatalf("stale context = %#v", staleBody)
	}
	version, _ := engineInstance.Store().Version(context.Background())
	if version != 1 {
		t.Fatalf("stale changed version to %d", version)
	}

	invalid := doJSON(t, server, http.MethodPost, "/v1/state/validate", map[string]any{
		"from": "HEALTHY", "to": "RECOVERING",
	}, nil)
	invalidBody := decodeError(t, invalid)
	if invalid.StatusCode != http.StatusUnprocessableEntity || invalidBody["code"] != "E_INVALID_TRANSITION" {
		t.Fatalf("invalid transition status=%d body=%#v", invalid.StatusCode, invalidBody)
	}
}

func TestRawImportIdempotencyAndExport(t *testing.T) {
	server, _ := newTestServer(t)
	body := []byte(`{"format":"replication-observation-jsonl","format_version":1}
{"replica_id":"replica-a","sampled_at_ms":1000,"gtid_segments":["` + httpSource + `:1-5"],"position_name":"` + httpSource + `:1-5","lag_ms":10,"reported_version":"8.0.36","last_write_ms":1}
`)
	url := "/v1/import?batch_id=raw-1&format=jsonl"
	first := doRaw(t, server, http.MethodPost, url, "application/x-ndjson", body)
	if first.StatusCode != http.StatusOK {
		t.Fatalf("import status=%d body=%s", first.StatusCode, readAll(t, first))
	}
	if first.Header.Get("X-Engine-Version") != "1" {
		t.Fatalf("import version header = %q", first.Header.Get("X-Engine-Version"))
	}
	second := doRaw(t, server, http.MethodPost, url, "application/x-ndjson", body)
	if second.StatusCode != http.StatusOK || second.Header.Get("X-Idempotent-Replay") != "true" {
		t.Fatalf("replay status=%d headers=%v", second.StatusCode, second.Header)
	}

	export := doJSON(t, server, http.MethodGet, "/v1/export?format=jsonl", nil, nil)
	if export.StatusCode != http.StatusOK {
		t.Fatalf("export status=%d body=%s", export.StatusCode, readAll(t, export))
	}
	data := readAll(t, export)
	if !bytes.Contains(data, []byte("replication-observation-jsonl")) || !bytes.Contains(data, []byte(httpSource+":1-5")) {
		t.Fatalf("unexpected export: %s", data)
	}
}

func TestInvalidObservationJSONUsesUnifiedError(t *testing.T) {
	server, _ := newTestServer(t)
	response := doRaw(t, server, http.MethodPost, "/v1/observations", "application/json", []byte(`{"observation":{"replica_id":"r1","lag_ms":"not-a-number"}}`))
	body := decodeError(t, response)
	if response.StatusCode != http.StatusBadRequest || body["code"] != "E_OBSERVATION_INVALID" {
		t.Fatalf("status=%d body=%#v", response.StatusCode, body)
	}
	context := body["context"].(map[string]any)
	if context["field"] != "lag_ms" || context["replica_id"] != "r1" {
		t.Fatalf("context = %#v", context)
	}

	missingLag := doRaw(t, server, http.MethodPost, "/v1/observations", "application/json", []byte(`{"observation":{"replica_id":"r2","sampled_at_ms":1000,"gtid_segments":["`+httpSource+`:1"],"position_name":"`+httpSource+`:1","reported_version":"8.0.0"}}`))
	missingBody := decodeError(t, missingLag)
	if missingLag.StatusCode != http.StatusBadRequest || missingBody["code"] != "E_OBSERVATION_INVALID" {
		t.Fatalf("missing lag status=%d body=%#v", missingLag.StatusCode, missingBody)
	}
	if missingBody["context"].(map[string]any)["field"] != "lag_ms" {
		t.Fatalf("missing lag context = %#v", missingBody)
	}
}

func newTestServer(t *testing.T) (*httptest.Server, *engine.Engine) {
	t.Helper()
	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "http.db"), store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	engineInstance, err := engine.New(database, compat.Default(), domain.DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(New(engineInstance).Handler())
	t.Cleanup(func() {
		server.Close()
		_ = database.Close()
	})
	return server, engineInstance
}

func doJSON(t *testing.T, server *httptest.Server, method, path string, payload any, headers map[string]string) *http.Response {
	t.Helper()
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		body = bytes.NewReader(data)
	}
	request, err := http.NewRequest(method, server.URL+path, body)
	if err != nil {
		t.Fatal(err)
	}
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return response
}

func doRaw(t *testing.T, server *httptest.Server, method, path, contentType string, body []byte) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, server.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", contentType)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func decodeError(t *testing.T, response *http.Response) map[string]any {
	t.Helper()
	defer response.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if _, ok := body["code"]; !ok {
		t.Fatalf("business error has no code: %#v", body)
	}
	if _, ok := body["context"]; !ok {
		t.Fatalf("business error has no context: %#v", body)
	}
	return body
}

func readAll(t *testing.T, response *http.Response) []byte {
	t.Helper()
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

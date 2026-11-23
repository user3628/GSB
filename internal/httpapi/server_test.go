package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"recoveryengine/internal/engine"
	"recoveryengine/internal/httpapi"
	"recoveryengine/internal/model"
	"recoveryengine/internal/spec"
	"recoveryengine/samples"
)

type apiEnvelope struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data"`
	Error *struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Context map[string]any `json:"context"`
	} `json:"error"`
	PartialResult json.RawMessage `json:"partial_result"`
}

func testServer(t *testing.T) (*httptest.Server, *engine.Engine) {
	t.Helper()
	app, err := engine.New(context.Background(), filepath.Join(t.TempDir(), "httpapi.db"), true)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(httpapi.New(app, true))
	t.Cleanup(func() {
		server.Close()
		_ = app.Close()
	})
	return server, app
}

func callJSON(t *testing.T, client *http.Client, method, endpoint string, body any, headers map[string]string) (int, apiEnvelope, http.Header) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(payload)
	}
	request, err := http.NewRequest(method, endpoint, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var envelope apiEnvelope
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode response (%d): %v", response.StatusCode, err)
	}
	return response.StatusCode, envelope, response.Header
}

func requireData(t *testing.T, envelope apiEnvelope, target any) {
	t.Helper()
	if !envelope.OK || envelope.Error != nil {
		t.Fatalf("expected success envelope, got %#v", envelope)
	}
	if err := json.Unmarshal(envelope.Data, target); err != nil {
		t.Fatal(err)
	}
}

func requireError(t *testing.T, status, wantStatus int, envelope apiEnvelope, code string) {
	t.Helper()
	if status != wantStatus || envelope.OK || envelope.Error == nil || envelope.Error.Code != code {
		t.Fatalf("expected HTTP %d %s, got HTTP %d %#v", wantStatus, code, status, envelope)
	}
	if envelope.Error.Message == "" {
		t.Fatal("business error must include a message")
	}
}

func TestHTTPContractValidationConflictAndSnapshotReplacement(t *testing.T) {
	server, _ := testServer(t)
	client := server.Client()

	status, envelope, headers := callJSON(t, client, http.MethodPost, server.URL+"/api/v1/samples/load", map[string]any{
		"name": "full-snapshot.json", "idempotency_key": "http-bootstrap",
	}, nil)
	if status != http.StatusOK {
		t.Fatalf("bootstrap failed: HTTP %d %#v", status, envelope)
	}
	if got := headers.Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Fatalf("unexpected content type: %s", got)
	}
	var loaded engine.CommitResult
	requireData(t, envelope, &loaded)
	if loaded.Version != 1 {
		t.Fatalf("bootstrap version = %d", loaded.Version)
	}

	status, envelope, _ = callJSON(t, client, http.MethodPost, server.URL+"/api/v1/samples/load", map[string]any{
		"name": "full-snapshot.json", "idempotency_key": "http-bootstrap",
	}, nil)
	requireData(t, envelope, &loaded)
	if status != http.StatusOK || !loaded.Replayed {
		t.Fatalf("expected idempotent replay, status=%d result=%#v", status, loaded)
	}

	status, envelope, _ = callJSON(t, client, http.MethodPost, server.URL+"/api/v1/samples/validate", map[string]any{
		"name": "invalid-negative-resource.json",
	}, nil)
	requireError(t, status, http.StatusUnprocessableEntity, envelope, model.CodeSpecInvalid)

	status, envelope, _ = callJSON(t, client, http.MethodPost, server.URL+"/api/v1/samples/validate", map[string]any{
		"name": "invalid-weight.json",
	}, nil)
	requireError(t, status, http.StatusUnprocessableEntity, envelope, model.CodeSpecInvalid)

	status, envelope, _ = callJSON(t, client, http.MethodPost, server.URL+"/api/v1/samples/validate", map[string]any{
		"name": "boundary-weight-snapshot.json",
	}, nil)
	if status != http.StatusOK {
		t.Fatalf("boundary snapshot sample was rejected: HTTP %d %#v", status, envelope)
	}

	status, envelope, _ = callJSON(t, client, http.MethodPost, server.URL+"/api/v1/samples/validate", map[string]any{
		"name": "increments/resource-adjust.json",
	}, nil)
	if status != http.StatusOK {
		t.Fatalf("valid increment sample was rejected: HTTP %d %#v", status, envelope)
	}

	status, envelope, _ = callJSON(t, client, http.MethodPost, server.URL+"/api/v1/samples/validate", map[string]any{
		"name": "increments/cycle-add.json",
	}, nil)
	requireError(t, status, http.StatusUnprocessableEntity, envelope, model.CodeDependencyCycle)
	if len(envelope.PartialResult) == 0 || bytes.Equal(envelope.PartialResult, []byte("null")) {
		t.Fatal("cycle validation must include partial graph ordering")
	}

	firstIncrement := map[string]any{
		"idempotency_key": "http-conflict-key", "base_version": 1,
		"generated_at": "2026-01-15T12:00:00Z",
		"operations": []map[string]any{{
			"type": "patch_resource", "component_id": "api",
			"resources": map[string]any{"cpu_millicores": 2800, "memory_bytes": 3221225472},
		}},
	}
	status, envelope, _ = callJSON(t, client, http.MethodPost, server.URL+"/api/v1/increments", firstIncrement, nil)
	if status != http.StatusOK {
		t.Fatalf("increment failed: HTTP %d %#v", status, envelope)
	}
	secondIncrement := map[string]any{
		"idempotency_key": "http-conflict-key", "base_version": 1,
		"generated_at": "2026-01-15T12:01:00Z",
		"operations": []map[string]any{{
			"type": "patch_resource", "component_id": "cache",
			"resources": map[string]any{"cpu_millicores": 1700, "memory_bytes": 4294967296},
		}},
	}
	status, envelope, _ = callJSON(t, client, http.MethodPost, server.URL+"/api/v1/increments", secondIncrement, nil)
	requireError(t, status, http.StatusConflict, envelope, model.CodeConflictingRequest)

	var state engine.StateView
	status, envelope, _ = callJSON(t, client, http.MethodGet, server.URL+"/api/v1/state", nil, nil)
	requireData(t, envelope, &state)
	if status != http.StatusOK || state.Version != 2 {
		t.Fatalf("conflicting request changed state: HTTP %d version=%d", status, state.Version)
	}

	rawSnapshot := readSampleSnapshot(t)
	rawSnapshot.Version = 5
	rawSnapshot.GeneratedAt = "2026-02-01T08:00:00Z"
	rawSnapshot.ChecksumSHA256 = ""
	rawSnapshot.Components = removeComponent(rawSnapshot.Components, "notifier")
	rawSnapshot.Edges = removeEdgesTo(rawSnapshot.Edges, "notifier")
	delete(rawSnapshot.Observations, "notifier")
	normalized, business := spec.NormalizeSnapshot(rawSnapshot, false)
	if business != nil {
		t.Fatal(business)
	}
	status, envelope, _ = callJSON(t, client, http.MethodPost, server.URL+"/api/v1/snapshots", model.FullSnapshotRequest{
		IdempotencyKey: "http-replacement", Snapshot: normalized.Snapshot,
	}, nil)
	if status != http.StatusOK {
		t.Fatalf("snapshot replacement failed: HTTP %d %#v", status, envelope)
	}
	var replaced engine.CommitResult
	requireData(t, envelope, &replaced)
	if replaced.Version != 5 || replaced.BaseVersion != 2 {
		t.Fatalf("unexpected replacement result: %#v", replaced)
	}
	status, envelope, _ = callJSON(t, client, http.MethodGet, server.URL+"/api/v1/state", nil, nil)
	requireData(t, envelope, &state)
	if status != http.StatusOK || state.Version != 5 || len(state.Components) != 8 || len(state.Edges) != 7 {
		t.Fatalf("snapshot was not replaced atomically: HTTP %d version=%d components=%d edges=%d",
			status, state.Version, len(state.Components), len(state.Edges))
	}

	request, err := http.NewRequest(http.MethodPost, server.URL+"/api/v1/snapshots", bytes.NewBufferString("{"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	requireError(t, response.StatusCode, http.StatusUnprocessableEntity, envelope, model.CodeInvalidRequest)
}

func readSampleSnapshot(t *testing.T) model.Snapshot {
	t.Helper()
	data, err := samples.Read("full-snapshot.json")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, business := spec.DecodeSnapshot(data)
	if business != nil {
		t.Fatal(business)
	}
	return snapshot.Clone()
}

func removeComponent(components []model.Component, id string) []model.Component {
	out := make([]model.Component, 0, len(components))
	for _, component := range components {
		if component.ID != id {
			out = append(out, component)
		}
	}
	return out
}

func removeEdgesTo(edges []model.Edge, id string) []model.Edge {
	out := make([]model.Edge, 0, len(edges))
	for _, edge := range edges {
		if edge.From != id && edge.To != id {
			out = append(out, edge)
		}
	}
	return out
}

func TestHTTPRecoveryReportIsAvailableAfterCleanStart(t *testing.T) {
	// Startup recovery must always leave an audit report, even when there was no
	// interrupted work. This makes the evidence view deterministic after restart.
	server, _ := testServer(t)
	status, envelope, _ := callJSON(t, server.Client(), http.MethodGet, server.URL+"/api/v1/recovery-reports", nil, nil)
	var reports struct {
		Items []struct {
			ScannedIntents int    `json:"scanned_intents"`
			StartedAt      string `json:"started_at"`
		} `json:"items"`
	}
	requireData(t, envelope, &reports)
	if status != http.StatusOK || len(reports.Items) != 1 || reports.Items[0].ScannedIntents != 0 {
		t.Fatalf("unexpected startup recovery report: HTTP %d %#v", status, reports)
	}
	if _, err := time.Parse(time.RFC3339Nano, reports.Items[0].StartedAt); err != nil {
		t.Fatalf("recovery report timestamp is invalid: %v", err)
	}
}

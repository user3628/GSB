package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"testing/fstest"

	"local/rollout-guard/internal/domain"
	"local/rollout-guard/internal/engine"
	"local/rollout-guard/internal/store"
	"local/rollout-guard/samples"
)

type errorEnvelope struct {
	OK    bool `json:"ok"`
	Error struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Context map[string]any `json:"context"`
	} `json:"error"`
}

func newAPIServer(t *testing.T) (*httptest.Server, *engine.Engine) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	eng := engine.New(st, engine.Options{Now: func() int64 { return 1_800_000_000_000 }})
	if _, err := eng.RecoverOnStartup(context.Background()); err != nil {
		t.Fatal(err)
	}
	assets := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("<html></html>")}}
	server := httptest.NewServer(New(eng, assets, "test"))
	t.Cleanup(server.Close)
	return server, eng
}

func postJSON(t *testing.T, url string, value any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(url, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func requireErrorCode(t *testing.T, response *http.Response, status int, code string) {
	t.Helper()
	defer response.Body.Close()
	if response.StatusCode != status {
		t.Fatalf("status = %d, want %d", response.StatusCode, status)
	}
	var envelope errorEnvelope
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.OK || envelope.Error.Code != code || envelope.Error.Message == "" {
		t.Fatalf("unexpected error envelope: %+v", envelope)
	}
}

func TestUnifiedBusinessErrorEnvelopes(t *testing.T) {
	server, _ := newAPIServer(t)
	policy, _ := samples.Policy()
	response := postJSON(t, server.URL+"/api/policies", policy)
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("policy status = %d", response.StatusCode)
	}
	requireErrorCode(t, postJSON(t, server.URL+"/api/policies", policy), http.StatusConflict, "E_STALE_SNAPSHOT")

	batch := domain.Batch{
		ID: "api-batch", Service: "api-test", ChangeType: domain.ChangeConfig, BatchSize: 1,
		Baseline: domain.Baseline{TimestampMS: 1700000000000, ErrorRatePPM: 1000, P90LatencyMS: 100, SuccessRatePPM: 999000},
	}
	response = postJSON(t, server.URL+"/api/batches", batch)
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("batch status = %d", response.StatusCode)
	}
	invalidObservation := map[string]any{"observations": []map[string]any{{
		"seq": 1, "timestamp_ms": 1700000001000, "metric_type": "error_rate", "sample_count": -1, "success_count": 0,
	}}}
	requireErrorCode(t, postJSON(t, server.URL+"/api/batches/api-batch/observations", invalidObservation), http.StatusUnprocessableEntity, "E_OBSERVATION_INVALID")

	transition := map[string]string{"to": domain.StatusRolledBack, "reason": "illegal"}
	requireErrorCode(t, postJSON(t, server.URL+"/api/batches/api-batch/transition", transition), http.StatusConflict, "E_INVALID_TRANSITION")
}

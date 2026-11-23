package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"localinfer/workbench/internal/api"
	"localinfer/workbench/internal/domain"
	"localinfer/workbench/internal/service"
	"localinfer/workbench/internal/store"
)

func apiCatalog(t *testing.T) *domain.ModelCatalog {
	t.Helper()
	catalog, err := domain.ParseModelCatalog([]byte(`{"version":"test","models":[
		{"model_id":"small","vram_per_sample":"256Mi","cost_per_sample":"333us","max_batch_samples":512},
		{"model_id":"large","vram_per_sample":"2Gi","cost_per_sample":"3ms","max_batch_samples":128}
	]}`))
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

type apiHarness struct {
	server *httptest.Server
	store  *store.Store
}

func newHarness(t *testing.T) *apiHarness {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	catalog := apiCatalog(t)
	if err := st.InstallCatalog(catalog); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Recover(); err != nil {
		t.Fatal(err)
	}
	svc := service.New(st, catalog, false)
	server := httptest.NewServer(api.NewServer(svc, "", log.New(io.Discard, "", 0)))
	t.Cleanup(func() { server.Close(); _ = st.Close() })
	return &apiHarness{server: server, store: st}
}

func (h *apiHarness) do(t *testing.T, method, path string, body []byte) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, h.server.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]any{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &out); err != nil {
			t.Fatalf("decode %s %s response %q: %v", method, path, data, err)
		}
	}
	return resp.StatusCode, out
}

const apiTraceJSON = `{"trace_id":"api-trace","version":"v1","requests":[
	{"request_id":"r1","arrival_ms":0,"model_id":"small","samples":10,"max_batch_latency":"100ms","priority":10},
	{"request_id":"r2","arrival_ms":1,"model_id":"large","samples":2,"max_batch_latency":"100ms","priority":10},
	{"request_id":"r3","arrival_ms":5,"model_id":"small","samples":1,"max_batch_latency":"100ms","priority":5}
]}`

func TestHTTPContractsAndBusinessCodes(t *testing.T) {
	h := newHarness(t)
	status, body := h.do(t, http.MethodPost, "/api/v1/traces", []byte(apiTraceJSON))
	if status != http.StatusCreated || body["created"] != true {
		t.Fatalf("submit trace: %d %+v", status, body)
	}
	status, body = h.do(t, http.MethodPost, "/api/v1/traces", []byte(apiTraceJSON))
	if status != http.StatusOK || body["created"] != false {
		t.Fatalf("idempotent trace: %d %+v", status, body)
	}

	opts := `{"mode":"full","node_vram_bytes":"128Gi","allow_mixed_models":true,"latency_decimals":2}`
	status, body = h.do(t, http.MethodPost, "/api/v1/traces/api-trace/organize?version=v1", []byte(opts))
	if status != http.StatusOK || body["status"] != "complete" {
		t.Fatalf("organize full: %d %+v", status, body)
	}
	batches, _ := body["batches"].([]any)
	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %+v", batches)
	}
	first, _ := batches[0].(map[string]any)
	second, _ := batches[1].(map[string]any)
	firstID, _ := first["batch_id"].(string)
	secondID, _ := second["batch_id"].(string)
	if firstID == "" || secondID == "" {
		t.Fatalf("missing batch ids: %+v", batches)
	}
	status, body = h.do(t, http.MethodGet, "/api/v1/batches/"+secondID+"/quantiles?p=99", nil)
	if status != http.StatusUnprocessableEntity || body["code"] != domain.CodeInsufficientSample {
		t.Fatalf("expected insufficient samples, got %d %+v", status, body)
	}
	status, body = h.do(t, http.MethodGet, "/api/v1/batches/"+firstID, nil)
	if status != http.StatusOK {
		t.Fatalf("batch lookup: %d %+v", status, body)
	}
	budget, _ := body["vram_budget"].(map[string]any)
	if budget["remaining_after_batch_bytes"] == nil {
		t.Fatalf("missing budget detail: %+v", body)
	}

	status, body = h.do(t, http.MethodPost, "/api/v1/traces/api-trace/organize?version=v1", []byte(`{"mode":"full","node_vram_bytes":"128Gi","allow_mixed_models":true,"latency_decimals":2,"faults":{"precision_offset_us":2000}}`))
	if status != http.StatusUnprocessableEntity || body["code"] != domain.CodePrecisionBreach {
		t.Fatalf("expected precision breach, got %d %+v", status, body)
	}
	context, _ := body["context"].(map[string]any)
	run, _ := context["run"].(map[string]any)
	if run["status"] != "precision_failed" {
		t.Fatalf("breach facts were not persisted: %+v", body)
	}

	status, body = h.do(t, http.MethodPost, "/api/v1/traces/api-trace/organize?version=v1", []byte(`{"mode":"full","node_vram_bytes":"1Mi"}`))
	if status != http.StatusConflict || body["code"] != domain.CodeInsufficientCap {
		t.Fatalf("expected capacity error, got %d %+v", status, body)
	}
	status, body = h.do(t, http.MethodPost, "/api/v1/traces/api-trace/organize?version=v1", []byte(`{"mode":"full","node_vram_bytes":"128Gi","faults":{"duplicate_request_id":"r1"}}`))
	if status != http.StatusConflict || body["code"] != domain.CodeConflicting {
		t.Fatalf("expected duplicate conflict, got %d %+v", status, body)
	}
	status, body = h.do(t, http.MethodPost, "/api/v1/traces/api-trace/organize?version=v1", []byte(`{"mode":"full","node_vram_bytes":"128Gi","faults":{"vram_shortfall_bytes":"127Gi"}}`))
	if status != http.StatusConflict || body["code"] != domain.CodeInsufficientCap {
		t.Fatalf("expected injected shortfall capacity error, got %d %+v", status, body)
	}

	status, body = h.do(t, http.MethodPost, "/api/v1/traces", []byte(`{"trace_id":"bad","version":"v1","requests":[
		{"request_id":"a","arrival_ms":2,"model_id":"small","samples":1,"max_batch_latency":"1ms","priority":1},
		{"request_id":"b","arrival_ms":1,"model_id":"small","samples":1,"max_batch_latency":"1ms","priority":1}
	]}`))
	if status != http.StatusBadRequest || body["code"] != domain.CodeTraceInvalid {
		t.Fatalf("expected trace invalid, got %d %+v", status, body)
	}
	status, body = h.do(t, http.MethodPost, "/api/v1/models", []byte(`{"models":[{"model_id":"x","vram_per_sample":"-1Mi","cost_per_sample":"1us","max_batch_samples":1}]}`))
	if status != http.StatusBadRequest || body["code"] != domain.CodeSpecInvalid {
		t.Fatalf("expected spec invalid, got %d %+v", status, body)
	}

	status, body = h.do(t, http.MethodGet, "/api/v1/recovery/report", nil)
	if status != http.StatusOK || body["completion_claim"] != "NO_INTERRUPTED_OPERATION" {
		t.Fatalf("recovery report: %d %+v", status, body)
	}
	status, body = h.do(t, http.MethodGet, "/api/v1/intents?limit=10", nil)
	if status != http.StatusOK || len(body["intents"].([]any)) == 0 {
		t.Fatalf("intent log: %d %+v", status, body)
	}
}

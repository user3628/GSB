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

	"columnar-executor/internal/config"
	"columnar-executor/internal/httpapi"
	"columnar-executor/internal/model"
	"columnar-executor/internal/sample"
	"columnar-executor/internal/service"
	"columnar-executor/internal/snapshot"
	"columnar-executor/internal/store"
)

type harness struct {
	server *httptest.Server
	svc    *service.Service
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "http.db"))
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Options{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(httpapi.New(svc))
	t.Cleanup(func() {
		server.Close()
		_ = st.Close()
	})
	return &harness{server: server, svc: svc}
}

func (h *harness) request(t *testing.T, method, path string, body any, out any) int {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, h.server.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("decode response %s: %v", string(raw), err)
		}
	}
	return resp.StatusCode
}

func TestHTTPEndToEndAndFaultJSON(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	var snap model.Snapshot
	status := h.request(t, http.MethodPost, "/api/v1/snapshots/generate", model.SnapshotGenerateRequest{
		Version: 1, Rows: config.DefaultRows, Seed: config.DefaultSeed,
	}, &snap)
	if status != http.StatusCreated || snap.Version != 1 || snap.SHA256Digest == "" {
		t.Fatalf("generate snapshot status=%d snapshot=%+v", status, snap)
	}
	var chain model.Chain
	status = h.request(t, http.MethodPost, "/api/v1/chains", sample.Chain("http-sales", 1, 1), &chain)
	if status != http.StatusCreated {
		t.Fatalf("submit chain status=%d", status)
	}

	var small model.ExecutionView
	status = h.request(t, http.MethodPost, "/api/v1/chains/http-sales/execute", model.ExecuteRequest{
		ExecutionID: "http-small", ChainVersion: 1, BitmapForm: model.FormAll, BatchSize: 8, LargeVectorThreshold: 128,
	}, &small)
	if status != http.StatusOK || !small.Published {
		t.Fatalf("small execution status=%d view=%+v", status, small)
	}
	var large model.ExecutionView
	status = h.request(t, http.MethodPost, "/api/v1/chains/http-sales/execute", model.ExecuteRequest{
		ExecutionID: "http-large", ChainVersion: 1, BitmapForm: model.FormAll, BatchSize: 256, LargeVectorThreshold: 128,
	}, &large)
	if status != http.StatusOK || !large.Published {
		t.Fatalf("large execution status=%d view=%+v", status, large)
	}

	var comparison struct {
		RowsConsistent       bool `json:"rows_consistent"`
		AggregatesConsistent bool `json:"aggregates_consistent"`
	}
	status = h.request(t, http.MethodGet, "/api/v1/executions/http-small/compare?right=http-large&tolerance_ppm=1", nil, &comparison)
	if status != http.StatusOK || !comparison.RowsConsistent || !comparison.AggregatesConsistent {
		t.Fatalf("comparison status=%d report=%+v", status, comparison)
	}

	var failure model.ErrorResponse
	status = h.request(t, http.MethodPost, "/api/v1/chains/http-sales/execute", model.ExecuteRequest{
		ExecutionID: "http-fault", ChainVersion: 1, BitmapForm: model.FormAll, BatchSize: 8, LargeVectorThreshold: 128,
		Injections: []model.FaultSpec{{Kind: model.FaultFixedOverflow, Batch: 2}},
	}, &failure)
	if status != http.StatusUnprocessableEntity || failure.Error.Code != model.CodeNumericOverflow {
		t.Fatalf("fault status=%d response=%+v", status, failure)
	}
	if failure.Error.Context["column"] != "price" || failure.Error.Context["batch"] != float64(2) {
		t.Fatalf("fault context=%+v", failure.Error.Context)
	}

	var failedView model.ExecutionView
	status = h.request(t, http.MethodGet, "/api/v1/executions/http-fault", nil, &failedView)
	if status != http.StatusOK || failedView.Published || len(failedView.Batches) != 0 || len(failedView.Aggregates) != 0 {
		t.Fatalf("failed execution leaked partial state: status=%d view=%+v", status, failedView)
	}

	var batches struct {
		Published bool               `json:"published"`
		Batches   []model.BatchTrace `json:"batches"`
	}
	status = h.request(t, http.MethodGet, "/api/v1/chains/http-sales/versions/1/batches?execution_id=http-fault", nil, &batches)
	if status != http.StatusOK || batches.Published || len(batches.Batches) != 0 {
		t.Fatalf("batch query leaked provisional rows: status=%d response=%+v", status, batches)
	}

	var reports struct {
		Reports []model.RecoveryReport `json:"reports"`
	}
	status = h.request(t, http.MethodGet, "/api/v1/recovery/reports", nil, &reports)
	if status != http.StatusOK || len(reports.Reports) != 0 {
		t.Fatalf("unexpected recovery reports: status=%d reports=%+v", status, reports)
	}

	if _, err := h.svc.CurrentSnapshot(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPDirectSnapshotLoadKeepsOldVersion(t *testing.T) {
	h := newHarness(t)
	if status := h.request(t, http.MethodPost, "/api/v1/snapshots/generate", model.SnapshotGenerateRequest{
		Version: 1, Rows: 33, Seed: 7,
	}, nil); status != http.StatusCreated {
		t.Fatalf("generate status=%d", status)
	}
	second, err := snapshot.Generate(2, 33, 8)
	if err != nil {
		t.Fatal(err)
	}
	var loaded model.Snapshot
	if status := h.request(t, http.MethodPost, "/api/v1/snapshots/load", second, &loaded); status != http.StatusCreated {
		t.Fatalf("direct load status=%d", status)
	}
	if loaded.Version != 2 {
		t.Fatalf("loaded version=%d", loaded.Version)
	}

	bad, err := snapshot.Generate(3, 33, 9)
	if err != nil {
		t.Fatal(err)
	}
	bad.Columns[4].DictIndices[0] = 500
	bad.Columns[4].Nulls[0] &= ^byte(1)
	var failure model.ErrorResponse
	if status := h.request(t, http.MethodPost, "/api/v1/snapshots/load", bad, &failure); status != http.StatusUnprocessableEntity {
		t.Fatalf("bad load status=%d", status)
	}
	if failure.Error.Code != model.CodeSnapshotInvalid {
		t.Fatalf("bad load code=%+v", failure.Error)
	}
	var current model.Snapshot
	if status := h.request(t, http.MethodGet, "/api/v1/snapshots/current", nil, &current); status != http.StatusOK {
		t.Fatalf("current status=%d", status)
	}
	if current.Version != 2 {
		t.Fatalf("old snapshot was replaced: %+v", current)
	}
}

func TestHTTPInvalidJSONAndNotFoundAreUnified(t *testing.T) {
	h := newHarness(t)
	req, _ := http.NewRequest(http.MethodPost, h.server.URL+"/api/v1/chains", bytes.NewBufferString("{"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body model.ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest || body.Error.Code != model.CodeInvalidRequest {
		t.Fatalf("unexpected invalid request response: %d %+v", resp.StatusCode, body)
	}

	var notFound model.ErrorResponse
	status := h.request(t, http.MethodGet, "/api/v1/snapshots/99", nil, &notFound)
	if status != http.StatusNotFound || notFound.Error.Code != model.CodeNotFound {
		t.Fatalf("unexpected not found response: %d %+v", status, notFound)
	}

	var routeNotFound model.ErrorResponse
	status = h.request(t, http.MethodGet, "/api/v1/does-not-exist", nil, &routeNotFound)
	if status != http.StatusNotFound || routeNotFound.Error.Code != model.CodeNotFound {
		t.Fatalf("unexpected route not found response: %d %+v", status, routeNotFound)
	}
}

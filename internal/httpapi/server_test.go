package httpapi_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"layoutread/internal/httpapi"
	"layoutread/internal/model"
	"layoutread/internal/service"
	"layoutread/internal/store"
)

func newTestServer(t *testing.T) (*httptest.Server, *service.Service) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "http.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := service.New(st)
	server := httptest.NewServer(httpapi.New(svc).Handler())
	t.Cleanup(server.Close)
	return server, svc
}

func requestJSON(t *testing.T, method, url string, body []byte) (int, []byte) {
	t.Helper()
	request, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, data
}

func TestHTTPImportAnalyzeQueryAndErrors(t *testing.T) {
	server, _ := newTestServer(t)

	status, demoBody := requestJSON(t, http.MethodGet, server.URL+"/api/demo", nil)
	if status != http.StatusOK {
		t.Fatalf("demo bundle status %d: %s", status, demoBody)
	}
	var bundle model.Bundle
	if err := json.Unmarshal(demoBody, &bundle); err != nil {
		t.Fatal(err)
	}
	if len(bundle.Documents) < 4 {
		t.Fatalf("demo has fewer than four documents: %d", len(bundle.Documents))
	}

	status, body := requestJSON(t, http.MethodPost, server.URL+"/api/import", demoBody)
	if status != http.StatusOK {
		t.Fatalf("import status %d: %s", status, body)
	}
	status, body = requestJSON(t, http.MethodPost, server.URL+"/api/analyze", nil)
	if status != http.StatusOK {
		t.Fatalf("analyze status %d: %s", status, body)
	}
	var report service.RunReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatal(err)
	}
	if report.Processed != 4 || report.ResultFingerprint == "" {
		t.Fatalf("unexpected run report: %+v", report)
	}

	status, body = requestJSON(t, http.MethodGet, server.URL+"/api/status", nil)
	if status != http.StatusOK {
		t.Fatalf("status %d: %s", status, body)
	}
	var statusReport service.StatusReport
	if err := json.Unmarshal(body, &statusReport); err != nil {
		t.Fatal(err)
	}
	if len(statusReport.Documents) != 4 || statusReport.SnapshotDigest == "" {
		t.Fatalf("unexpected status: %+v", statusReport)
	}
	for _, doc := range statusReport.Documents {
		if len(doc.Stages) != 4 {
			t.Fatalf("document %s has %d stage checkpoints", doc.DocID, len(doc.Stages))
		}
	}

	status, body = requestJSON(t, http.MethodGet, server.URL+"/api/fingerprint", nil)
	if status != http.StatusOK || !strings.Contains(string(body), "strategy_fingerprint") {
		t.Fatalf("fingerprint status %d: %s", status, body)
	}

	queryURL := server.URL + "/api/query?doc_id=doc-double-en&scope=blocks&budget_ms=100"
	status, first := requestJSON(t, http.MethodGet, queryURL, nil)
	if status != http.StatusOK {
		t.Fatalf("query status %d: %s", status, first)
	}
	var queryResponse service.QueryResponse
	if err := json.Unmarshal(first, &queryResponse); err != nil {
		t.Fatal(err)
	}
	if queryResponse.CacheHit || len(queryResponse.Data) == 0 {
		t.Fatalf("unexpected query response: %+v", queryResponse)
	}
	status, second := requestJSON(t, http.MethodGet, queryURL, nil)
	if status != http.StatusOK {
		t.Fatalf("second query status %d: %s", status, second)
	}
	var cached service.QueryResponse
	if err := json.Unmarshal(second, &cached); err != nil {
		t.Fatal(err)
	}
	if !cached.CacheHit || string(cached.Data) != string(queryResponse.Data) {
		t.Fatal("HTTP cache hit did not return byte-identical query data")
	}

	status, body = requestJSON(t, http.MethodGet, server.URL+"/api/query?doc_id=doc-double-en&scope=blocks&budget_ms=0&query_id=budget-check", nil)
	if status != http.StatusRequestTimeout {
		t.Fatalf("expected 408, got %d: %s", status, body)
	}
	var latencyErr model.Error
	if err := json.Unmarshal(body, &latencyErr); err != nil {
		t.Fatal(err)
	}
	if latencyErr.Code != model.CodeLatencyBudget || latencyErr.QueryID != "budget-check" {
		t.Fatalf("unexpected latency error: %+v", latencyErr)
	}

	status, body = requestJSON(t, http.MethodPost, server.URL+"/api/stop", nil)
	if status != http.StatusOK || !strings.Contains(string(body), "stop_requested") {
		t.Fatalf("stop status %d: %s", status, body)
	}

	status, body = requestJSON(t, http.MethodPost, server.URL+"/api/import", []byte("{bad json"))
	if status != http.StatusBadRequest {
		t.Fatalf("expected malformed import 400, got %d: %s", status, body)
	}
}

func TestHTTPDemoRunEndpoint(t *testing.T) {
	server, _ := newTestServer(t)
	status, body := requestJSON(t, http.MethodPost, server.URL+"/api/demo/run", nil)
	if status != http.StatusOK {
		t.Fatalf("demo run status %d: %s", status, body)
	}
	var result struct {
		Run    service.RunReport    `json:"run"`
		Export service.ExportReport `json:"export"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	if result.Run.Processed != 4 || result.Export.ResultFingerprint == "" {
		t.Fatalf("unexpected demo run result: %+v", result)
	}
}

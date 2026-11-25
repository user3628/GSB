package httpapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"dialogalign/internal/domain"
	"dialogalign/internal/engine"
	"dialogalign/internal/httpapi"
)

func TestHTTPGenerateRunAndExport(t *testing.T) {
	root := t.TempDir()
	server := httptest.NewServer(httpapi.New(root).Handler())
	defer server.Close()

	call := func(method, path string, body []byte) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, server.URL+path, bytes.NewReader(body))
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
		return resp
	}

	resp := call(http.MethodPost, "/v1/generate", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("generate status %d", resp.StatusCode)
	}
	resp.Body.Close()
	resp = call(http.MethodPost, "/v1/run", []byte(`{}`))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("run status %d", resp.StatusCode)
	}
	var report domain.RunReport
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if report.CompletedTurns != 16 {
		t.Fatalf("completed=%d", report.CompletedTurns)
	}

	resp = call(http.MethodGet, "/v1/sessions", nil)
	var summary []engine.SessionSummary
	if err := json.NewDecoder(resp.Body).Decode(&summary); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(summary) != 4 || !summary[0].Complete {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	resp = call(http.MethodGet, "/v1/export", nil)
	var bundle domain.ExportBundle
	if err := json.NewDecoder(resp.Body).Decode(&bundle); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(bundle.Sessions) != 4 || len(bundle.Alignments) != 16 {
		t.Fatalf("unexpected export: sessions=%d alignments=%d", len(bundle.Sessions), len(bundle.Alignments))
	}

	resp = call(http.MethodPost, "/v1/active-map", []byte(`{"version":"missing"}`))
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("missing map status = %d", resp.StatusCode)
	}
	var be domain.BusinessError
	if err := json.NewDecoder(resp.Body).Decode(&be); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if be.Code != domain.CodeMapVersionConflict || be.Stage != "upgrade" {
		t.Fatalf("unexpected stable error: %+v", be)
	}

	resp = call(http.MethodPost, "/v1/import", []byte("not json\n"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("import status = %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestHTTPHealth(t *testing.T) {
	server := httptest.NewServer(httpapi.New(t.TempDir()).Handler())
	defer server.Close()
	resp, err := http.Get(server.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

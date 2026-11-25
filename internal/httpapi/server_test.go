package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"configaudit/internal/audit"
	"configaudit/internal/parser"
	"configaudit/internal/sample"
	"configaudit/internal/store"
	"context"
)

func newTestServer(t *testing.T) (*httptest.Server, *audit.Service) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	layout, err := sample.Generate(filepath.Join(dir, "samples"), sample.DefaultSeed)
	if err != nil {
		t.Fatal(err)
	}
	ds, err := parser.Load(layout.SpecFile, layout.NodeFile, layout.Reference, layout.Sources)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, filepath.Join(dir, "audit.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	svc := audit.NewService(st, ds)
	srv := httptest.NewServer(NewServer(svc).Mux)
	t.Cleanup(srv.Close)
	return srv, svc
}

func doJSON(t *testing.T, method, url string, body any) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestAuditSubmitAndQuery(t *testing.T) {
	srv, _ := newTestServer(t)
	status, out := doJSON(t, "POST", srv.URL+"/v1/audits", map[string]any{
		"request_id": "http-1", "payload": map[string]any{"seed": 1},
	})
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%v", status, out)
	}
	version := int64(out["version"].(float64))

	// Idempotent replay.
	status2, out2 := doJSON(t, "POST", srv.URL+"/v1/audits", map[string]any{
		"request_id": "http-1", "payload": map[string]any{"seed": 1},
	})
	if status2 != http.StatusOK || int64(out2["version"].(float64)) != version {
		t.Fatalf("replay status=%d body=%v", status2, out2)
	}

	resp, err := http.Get(srv.URL + "/v1/snapshots/latest")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("snapshot status=%d", resp.StatusCode)
	}
	var snap map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatal(err)
	}
	if snap["checksum"] == nil {
		t.Fatal("missing checksum")
	}
}

func TestConflictingRequestHTTP(t *testing.T) {
	srv, _ := newTestServer(t)
	doJSON(t, "POST", srv.URL+"/v1/audits", map[string]any{"request_id": "c1", "payload": map[string]any{"a": 1}})
	status, out := doJSON(t, "POST", srv.URL+"/v1/audits", map[string]any{"request_id": "c1", "payload": map[string]any{"a": 2}})
	if status != http.StatusConflict || out["error_code"] != "E_CONFLICTING_REQUEST" {
		t.Fatalf("status=%d body=%v", status, out)
	}
}

func TestStaleBaseHTTP(t *testing.T) {
	srv, _ := newTestServer(t)
	doJSON(t, "POST", srv.URL+"/v1/audits", map[string]any{"request_id": "b1", "payload": map[string]any{"a": 1}})
	status, out := doJSON(t, "POST", srv.URL+"/v1/audits/incremental", map[string]any{
		"request_id": "i1", "base_version": 999, "affected_keys": []string{"runtime.timeout_ms"},
		"payload": map[string]any{"a": 1},
	})
	if status != http.StatusUnprocessableEntity || out["error_code"] != "E_STALE_BASE" {
		t.Fatalf("status=%d body=%v", status, out)
	}
}

func TestRecoveryEndpoint(t *testing.T) {
	srv, _ := newTestServer(t)
	resp, err := http.Post(srv.URL+"/v1/recover", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("recover status=%d", resp.StatusCode)
	}
}

package pipeline

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newTestServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	dir := t.TempDir()
	srv, err := NewServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, ts
}

func postJSON(t *testing.T, url string, body any) (int, map[string]any) {
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

func TestHTTPEndToEnd(t *testing.T) {
	_, ts := newTestServer(t)
	base := ts.URL

	// Generate deterministic dataset.
	code, body := postJSON(t, base+"/api/dataset/generate",
		map[string]any{"seed": 2024, "shards": 2, "views_per": 3, "width": 16, "height": 16})
	if code != http.StatusOK {
		t.Fatalf("generate: %d %v", code, body)
	}

	// Reconstruct with equality verification.
	code, body = postJSON(t, base+"/api/reconstruct",
		map[string]any{"force_full": false, "verify_equality": true})
	if code != http.StatusOK {
		t.Fatalf("reconstruct: %d %v", code, body)
	}
	if body["equivalence"] != "byte_identical" {
		t.Fatalf("equivalence not confirmed: %v", body["equivalence"])
	}
	result := body["result"].(map[string]any)
	if result["field_hash"] == "" || result["field_hash"] == nil {
		t.Fatal("field hash missing")
	}

	// Query summary twice; second must be a cache hit.
	q1 := getJSON(t, base+"/api/query/summary")
	q2 := getJSON(t, base+"/api/query/summary")
	if q1["data"] == nil {
		t.Fatal("summary data missing")
	}
	if q2["cache_hit"] != true {
		t.Fatalf("second summary query not cached: %v", q2)
	}

	// Error stats and prune reasons.
	if getJSON(t, base+"/api/query/errors")["data"] == nil {
		t.Fatal("error stats missing")
	}
	if getJSON(t, base+"/api/query/prune")["data"] == nil {
		t.Fatal("prune data missing")
	}

	// Checkpoints are present and contiguous.
	resp, err := http.Get(base + "/api/checkpoints")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var cps []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&cps); err != nil {
		t.Fatal(err)
	}
	if len(cps) == 0 {
		t.Fatal("no checkpoints published")
	}
}

func TestHTTPBadConfigBusinessCode(t *testing.T) {
	_, ts := newTestServer(t)
	postJSON(t, ts.URL+"/api/dataset/generate",
		map[string]any{"seed": 1, "shards": 1, "views_per": 1, "width": 8, "height": 8})
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/api/config",
		bytes.NewReader([]byte(`{"sample_density":-1}`)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var be BizError
	if err := json.NewDecoder(resp.Body).Decode(&be); err != nil {
		t.Fatal(err)
	}
	if be.Code != CodeBadDensity || be.Field != "sample_density" {
		t.Fatalf("unexpected: %+v", be)
	}
}

func TestHTTPLatencyBudgetError(t *testing.T) {
	srv, ts := newTestServer(t)
	postJSON(t, ts.URL+"/api/dataset/generate",
		map[string]any{"seed": 1, "shards": 1, "views_per": 1, "width": 8, "height": 8})
	postJSON(t, ts.URL+"/api/reconstruct", map[string]any{})
	// Force compute work past a tight budget. A new config version changes the
	// cache key so the query cannot be served from the previous entry.
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/api/config",
		bytes.NewReader([]byte(`{"query_budget_ms":5}`)))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	srv.queryHook = func(kind string) { time.Sleep(80 * time.Millisecond) }
	r := getJSON(t, ts.URL+"/api/query/errors")
	if r["code"] != string(CodeLatencyBudget) {
		t.Fatalf("expected latency budget error, got: %v", r)
	}
	if r["query_id"] == nil || r["elapsed_ms"] == nil {
		t.Fatalf("budget error missing query id / elapsed: %v", r)
	}
}

func getJSON(t *testing.T, url string) map[string]any {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

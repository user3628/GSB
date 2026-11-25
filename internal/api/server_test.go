package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPDeterministicWorkflow(t *testing.T) {
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	post(t, ts.URL+"/api/datasets/generate", map[string]any{})
	body := map[string]any{
		"id":          "http-small",
		"dataset_key": "small",
		"config": map[string]any{
			"k_folds":        2,
			"lambdas":        []float64{0, 0.01},
			"l1_ratios":      []float64{0, 1},
			"max_iterations": 25,
		},
	}
	post(t, ts.URL+"/api/sessions", body)
	runResp := post(t, ts.URL+"/api/sessions/http-small/run", map[string]any{})
	if runResp["state"] != "completed" {
		t.Fatalf("run response = %v", runResp)
	}
	resp, err := http.Get(ts.URL + "/api/sessions/http-small/query?kind=selection")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("selection status = %d", resp.StatusCode)
	}
	resp2, err := http.Get(ts.URL + "/api/sessions/http-small/query?kind=state&budget_ms=0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	var budgetErr map[string]any
	_ = json.NewDecoder(resp2.Body).Decode(&budgetErr)
	if resp2.StatusCode != http.StatusRequestTimeout || budgetErr["code"] != "E_LATENCY_BUDGET" {
		t.Fatalf("budget response status=%d body=%v", resp2.StatusCode, budgetErr)
	}
	exportResp, err := http.Get(ts.URL + "/api/sessions/http-small/export?kind=coefficients")
	if err != nil {
		t.Fatal(err)
	}
	defer exportResp.Body.Close()
	if exportResp.StatusCode != http.StatusOK {
		t.Fatalf("export status = %d", exportResp.StatusCode)
	}
}

func post(t *testing.T, url string, body any) map[string]any {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode >= 400 {
		t.Fatalf("POST %s status %d body=%v", url, resp.StatusCode, out)
	}
	return out
}

package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"deployverify/internal/api"
	"deployverify/internal/demo"
	"deployverify/internal/model"
	"deployverify/internal/store"
)

type envelope struct {
	Code  string          `json:"code"`
	Error *model.APIError `json:"error"`
}

func postJSON(t *testing.T, base, path string, body any) (int, map[string]any) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(base+path, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var decoded map[string]any
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return response.StatusCode, decoded
}

func getJSON(t *testing.T, base, path string) (int, map[string]any) {
	t.Helper()
	response, err := http.Get(base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var decoded map[string]any
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return response.StatusCode, decoded
}

func apiCode(body map[string]any) string {
	if err, ok := body["error"].(map[string]any); ok {
		if code, ok := err["code"].(string); ok {
			return code
		}
	}
	if code, ok := body["code"].(string); ok {
		return code
	}
	return ""
}

func TestHTTPEndToEndAcceptance(t *testing.T) {
	st, err := store.Open(context.Background(), store.DSN(filepath.Join(t.TempDir(), "e2e.db")))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	server := api.NewServer(st, "", t.TempDir())
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	server.ProbeBaseURL = ts.URL

	invalid := demo.InvalidSnapshot()
	status, body := postJSON(t, ts.URL, "/api/snapshots", invalid)
	if status != http.StatusUnprocessableEntity || apiCode(body) != model.CodeSpecInvalid {
		t.Fatalf("invalid status=%d body=%v", status, body)
	}
	version, err := st.CurrentVersion(context.Background())
	if err != nil || version != 0 {
		t.Fatalf("invalid snapshot changed state version=%d err=%v", version, err)
	}

	status, body = postJSON(t, ts.URL, "/api/snapshots", demo.SnapshotV1())
	if status != http.StatusCreated || apiCode(body) != "OK" {
		t.Fatalf("snapshot status=%d body code=%s", status, apiCode(body))
	}

	var wg sync.WaitGroup
	codes := make(chan string, 2)
	for _, inc := range []*model.Increment{demo.ConcurrentIncrement("a"), demo.ConcurrentIncrement("b")} {
		inc := inc
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, response := postJSON(t, ts.URL, "/api/increments", inc)
			if status == http.StatusCreated {
				codes <- "OK"
			} else {
				codes <- apiCode(response)
			}
		}()
	}
	wg.Wait()
	close(codes)
	seen := map[string]int{}
	for code := range codes {
		seen[code]++
	}
	if seen["OK"] != 1 || seen[model.CodeStaleBase] != 1 {
		t.Fatalf("concurrent increment codes=%v", seen)
	}

	status, body = postJSON(t, ts.URL, "/api/increments", demo.CascadeIncrement())
	if status != http.StatusCreated {
		t.Fatalf("cascade increment status=%d code=%s body=%v", status, apiCode(body), body)
	}
	status, body = postJSON(t, ts.URL, "/api/increments", demo.CycleIncrement())
	if status != http.StatusCreated {
		t.Fatalf("cycle increment status=%d code=%s", status, apiCode(body))
	}
	status, body = getJSON(t, ts.URL, "/api/diagnostics/dependency-cycle?environment=staging")
	if status != http.StatusUnprocessableEntity || apiCode(body) != model.CodeDependencyCycle {
		t.Fatalf("cycle diagnostic status=%d code=%s body=%v", status, apiCode(body), body)
	}
	errDetails, _ := body["error"].(map[string]any)["details"].(map[string]any)
	if blocked, _ := errDetails["blocked"].([]any); len(blocked) < 2 {
		t.Fatalf("cycle details missing blocked list: %v", body)
	}

	status, body = getJSON(t, ts.URL, "/api/diffs?baseline=prod&toleranceUnits=0")
	if status != http.StatusConflict || apiCode(body) != model.CodeEnvDrift {
		t.Fatalf("drift status=%d code=%s", status, apiCode(body))
	}

	status, body = postJSON(t, ts.URL, "/api/probe-rounds", map[string]any{"allServices": false})
	if status != http.StatusOK {
		t.Fatalf("empty round status=%d body=%v", status, body)
	}
	report, _ := body["report"].(map[string]any)
	if report["empty"] != true || report["conclusion"] == "" {
		t.Fatalf("empty report=%v", report)
	}

	status, body = postJSON(t, ts.URL, "/api/probe-rounds", map[string]any{
		"environment": "dev", "services": []string{"slowprobe"}, "globalConcurrency": 1, "envConcurrency": 1,
		"queueCapacity": 1, "budgetMs": 1000,
	})
	if status != http.StatusOK {
		t.Fatalf("timeout round status=%d body=%v", status, body)
	}
	report, _ = body["report"].(map[string]any)
	if timeoutCount(report) != 1 {
		t.Fatalf("expected one timeout report=%v", report)
	}

	status, body = postJSON(t, ts.URL, "/api/probe-rounds", map[string]any{
		"environment": "prod", "globalConcurrency": 1, "envConcurrency": 1, "queueCapacity": 1,
		"budgetMs": 2000, "probePath": "/probe/slow?delay=2s", "probeTimeoutMs": 300,
	})
	if status != http.StatusTooManyRequests || apiCode(body) != model.CodeProbeQueueFull {
		t.Fatalf("queue full status=%d code=%s body=%v", status, apiCode(body), body)
	}

	status, body = postJSON(t, ts.URL, "/api/probe-rounds", map[string]any{
		"environment": "prod", "services": []string{"identity"}, "globalConcurrency": 1, "envConcurrency": 1,
		"queueCapacity": 1, "budgetMs": 1000,
	})
	if status != http.StatusOK {
		t.Fatalf("success probe status=%d body=%v", status, body)
	}
	_, plansBody := getJSON(t, ts.URL, "/api/plans?environment=prod")
	plans, _ := plansBody["plans"].([]any)
	found := false
	for _, item := range plans {
		plan, _ := item.(map[string]any)
		if plan["service"] == "identity" && plan["status"] == "success" {
			found = true
		}
	}
	if !found {
		t.Fatalf("identity plan not atomically successful: %v", plansBody)
	}

	_, intentsBody := getJSON(t, ts.URL, "/api/intents?status=pending")
	intents, _ := intentsBody["intents"].([]any)
	if len(intents) != 0 {
		t.Fatalf("pending intents after completed requests: %v", intentsBody)
	}

	// Ensure typed APIError remains compatible with errors.As for wrapped store failures.
	var apiErr *model.APIError
	if !errors.As(fmt.Errorf("wrap: %w", model.NewError(model.CodeStaleBase, "x", nil)), &apiErr) {
		t.Fatal("errors.As APIError failed")
	}
}

func timeoutCount(report map[string]any) float64 {
	value, _ := report["timeoutCount"].(float64)
	return value
}

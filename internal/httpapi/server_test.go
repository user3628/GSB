package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gatewayfreshness.local/workbench/internal/config"
	"gatewayfreshness.local/workbench/internal/service"
	"gatewayfreshness.local/workbench/internal/store"
	sampleassets "gatewayfreshness.local/workbench/samples"
)

func TestHTTPAdjudicationAndDiagnostics(t *testing.T) {
	server, closeFn := newTestHTTPServer(t)
	defer closeFn()

	accepted := postSample(t, server.URL, "01-sample-accept-001.json")
	if accepted.Outcome != "accepted" || accepted.Code != "OK" || accepted.WindowVersion != 1 {
		t.Fatalf("accepted decision=%#v", accepted)
	}
	if accepted.Items[0].SignatureStage.WindowVersion != accepted.WindowVersion || accepted.Items[0].NonceStage.ToleranceConfigVersion != accepted.ToleranceConfigVersion {
		t.Fatal("response did not expose per-stage version identifiers")
	}

	var envelope struct {
		OK    bool `json:"ok"`
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	invalid := postRaw(t, server.URL+"/api/v1/adjudications", mustSample(t, "11-sample-invalid-nonce.json"))
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid status=%d", invalid.Code)
	}
	if err := json.Unmarshal(invalid.Body, &envelope); err != nil || envelope.Error.Code != "E_REQUEST_INVALID" {
		t.Fatalf("invalid error body=%s", invalid.Body)
	}
	overflow := postRaw(t, server.URL+"/api/v1/adjudications", mustSample(t, "invalid-weight-overflow.json"))
	if overflow.Code != http.StatusBadRequest || !bytes.Contains(overflow.Body, []byte("E_REQUEST_INVALID")) {
		t.Fatalf("overflow status=%d body=%s", overflow.Code, overflow.Body)
	}

	conflict := postSampleRaw(t, server.URL, "conflict-same-id-001.json")
	if conflict.Code != http.StatusConflict || !bytes.Contains(conflict.Body, []byte("E_CONFLICTING_REQUEST")) {
		t.Fatalf("conflict status=%d body=%s", conflict.Code, conflict.Body)
	}
	idempotent := postSample(t, server.URL, "01-sample-accept-001.json")
	if !idempotent.IdempotentReplay || idempotent.WindowVersion != 1 {
		t.Fatalf("idempotent replay=%#v", idempotent)
	}

	modeDecision := postSample(t, server.URL, "10-sample-mode-full-010.json")
	resp, err := http.Get(server.URL + "/api/v1/diagnostics/mode-differences?batch_id=" + modeDecision.BatchID + "&budget_ms=100")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`"fast_result":"not_prechecked"`)) || !bytes.Contains(body, []byte(`"full_result_available":true`)) {
		t.Fatalf("mode response=%d %s", resp.StatusCode, body)
	}

	resp, err = http.Get(server.URL + "/api/v1/diagnostics/snapshot?budget_ms=100")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`"complete":true`)) || !bytes.Contains(body, []byte(`"snapshot_window_version":2`)) {
		t.Fatalf("snapshot response=%d %s", resp.StatusCode, body)
	}
	resp, err = http.Get(server.URL + "/api/v1/diagnostics/snapshot?budget_ms=0")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`"partial":true`)) || !bytes.Contains(body, []byte(`"snapshot_window_version":2`)) || !bytes.Contains(body, []byte(`"completed_sections":[]`)) {
		t.Fatalf("zero-budget snapshot=%d %s", resp.StatusCode, body)
	}

	resp, err = http.Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "Freshness Workbench") && !strings.Contains(string(body), "请求新鲜度裁决工作台") {
		t.Fatalf("workbench static page unavailable: %d", resp.StatusCode)
	}
}

func TestInvalidBatchCreatesNoVersionOrDecision(t *testing.T) {
	server, closeFn := newTestHTTPServer(t)
	defer closeFn()
	res := postRaw(t, server.URL+"/api/v1/adjudications", mustSample(t, "11-sample-invalid-nonce.json"))
	if res.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", res.Code)
	}
	resp, err := http.Get(server.URL + "/api/v1/adjudications/11-sample-invalid-nonce")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("invalid batch became visible: %d", resp.StatusCode)
	}
	resp, err = http.Get(server.URL + "/api/v1/diagnostics/snapshot?budget_ms=50")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !bytes.Contains(body, []byte(`"snapshot_window_version":0`)) {
		t.Fatalf("invalid batch consumed a version: %s", body)
	}
}

func TestInconsistencyHTTPContract(t *testing.T) {
	server, closeFn := newTestHTTPServer(t)
	defer closeFn()
	base := postSample(t, server.URL, "06-sample-reclaim-base-006.json")
	if base.WindowVersion != 1 {
		t.Fatalf("base version=%d", base.WindowVersion)
	}
	postSample(t, server.URL, "07-sample-reclaim-trigger-007.json")
	postSample(t, server.URL, "08-sample-reoccupy-008.json")
	requestBody, _ := json.Marshal(map[string]any{
		"batch_id": base.BatchID, "budget_ms": 100,
		"views": []map[string]any{
			{"client_id": "reader-old", "picked_window_version": 1},
			{"client_id": "reader-new", "picked_window_version": 3},
		},
	})
	res := postRaw(t, server.URL+"/api/v1/diagnostics/inconsistencies", requestBody)
	if res.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", res.Code, res.Body)
	}
	if !bytes.Contains(res.Body, []byte(`"client_id":"reader-new"`)) ||
		!bytes.Contains(res.Body, []byte(`"picked_window_version":3`)) ||
		!bytes.Contains(res.Body, []byte(`"client_viewed_value":"E_NONCE_REPLAYED"`)) {
		t.Fatalf("inconsistency detail body=%s", res.Body)
	}
}

type testResult struct {
	Code int
	Body []byte
}

func newTestHTTPServer(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	dir := t.TempDir()
	configJSON := `{
  "http_addr": "127.0.0.1:8787",
  "db_path": "` + filepath.ToSlash(filepath.Join(dir, "workbench.db")) + `",
  "signing_key": "local-deterministic-demo-key-2026-not-for-production",
  "signing_key_id": "test-key",
  "max_tolerance_ms": 5000,
  "max_window_ms": 60000,
  "max_requests_per_batch": 200,
  "fast_precheck_limit": 2,
  "default_query_budget_ms": 40,
  "max_diagnostic_budget_ms": 5000,
  "allow_reference_override": true,
  "max_request_body_bytes": 4194304
}`
	configPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(context.Background(), store.Options{
		Path: cfg.DBPath, ConfigVersion: cfg.Version(), ConfigSnapshotJSON: cfg.SnapshotJSON(),
		SigningKey: cfg.SigningKeyBytes(), SigningKeyID: cfg.SigningKeyID, FastPrecheckLimit: cfg.FastPrecheckLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	svc := service.New(st, cfg)
	if _, err := svc.Recovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	apiServer := New(svc, cfg.MaxRequestBodyBytes)
	server := httptest.NewServer(apiServer.Handler())
	return server, func() {
		server.Close()
		_ = st.Close()
	}
}

func postSample(t *testing.T, baseURL, name string) *decisionEnvelope {
	t.Helper()
	res := postSampleRaw(t, baseURL, name)
	if res.Code != http.StatusOK {
		t.Fatalf("sample %s status=%d body=%s", name, res.Code, res.Body)
	}
	var envelope struct {
		OK   bool              `json:"ok"`
		Data *decisionEnvelope `json:"data"`
	}
	if err := json.Unmarshal(res.Body, &envelope); err != nil || !envelope.OK || envelope.Data == nil {
		t.Fatalf("decode sample response: %v %s", err, res.Body)
	}
	return envelope.Data
}

func postSampleRaw(t *testing.T, baseURL, name string) testResult {
	t.Helper()
	return postRaw(t, baseURL+"/api/v1/adjudications", mustSample(t, name))
}

func mustSample(t *testing.T, name string) []byte {
	t.Helper()
	data, err := sampleassets.FS.ReadFile("batches/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func postRaw(t *testing.T, url string, body []byte) testResult {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return testResult{Code: resp.StatusCode, Body: responseBody}
}

type decisionEnvelope struct {
	BatchID                string `json:"batch_id"`
	WindowVersion          int64  `json:"window_version"`
	ToleranceConfigVersion string `json:"tolerance_config_version"`
	Outcome                string `json:"outcome"`
	Code                   string `json:"code"`
	IdempotentReplay       bool   `json:"idempotent_replay"`
	Items                  []struct {
		SignatureStage struct {
			WindowVersion int64 `json:"window_version"`
		} `json:"signature_stage"`
		NonceStage struct {
			ToleranceConfigVersion string `json:"tolerance_config_version"`
		} `json:"nonce_stage"`
	} `json:"items"`
}

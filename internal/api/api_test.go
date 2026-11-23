package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"gatewayworkbench/internal/engine"
	"gatewayworkbench/internal/model"
	"gatewayworkbench/internal/scheduler"
	"gatewayworkbench/internal/store"
)

type apiFixture struct {
	server   *httptest.Server
	upstream *httptest.Server
	store    *store.Store
	sample   *model.Sample
}

func newAPIFixture(t *testing.T) *apiFixture {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("UPSTREAM-OK"))
	}))
	port := upstream.Listener.Addr().(*net.TCPAddr).Port
	cfg := model.RuntimeConfig{
		GlobalConcurrency: 2, PerInstanceLimit: 1, QueueCapacity: 2,
		RoundBudgetMillis: 2000, ProbeConcurrency: 4,
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	eng := engine.New(st, cfg)
	sch := scheduler.New(st, cfg)
	server := httptest.NewServer(New(eng, sch, st).Routes())
	sample := &model.Sample{
		Instances: []model.Instance{{
			ID: "i1", Port: port, Weight: 600, Zone: "z-a",
			Probe: model.ProbeSpec{Mode: "http", Path: "/healthz", ExpectedStatus: 200,
				BodyMarker: "UPSTREAM-OK", TimeoutMillis: 500, SuccessThreshold: 1, FailureThreshold: 1},
		}, {
			ID: "i2", Port: 1, Weight: 400, Zone: "z-b",
			Probe: model.ProbeSpec{Mode: "tcp", TimeoutMillis: 200,
				SuccessThreshold: 1, FailureThreshold: 1},
		}},
		Trace: []model.RequestRecord{
			{TimestampMillis: 1, ClientID: "c1", RequestWeight: 1, RequestBytes: 8},
			{TimestampMillis: 2, ClientID: "c2", RequestWeight: 1, RequestBytes: 8},
			{TimestampMillis: 3, ClientID: "c3", RequestWeight: 1, RequestBytes: 8},
		},
		Affinity: model.ZoneAffinity{Ratios: map[string]int{"z-a": 500, "z-b": 500}},
	}
	t.Cleanup(func() {
		server.Close()
		upstream.Close()
		_ = st.Close()
	})
	return &apiFixture{server: server, upstream: upstream, store: st, sample: sample}
}

func requestJSON(t *testing.T, method, url string, body any) (map[string]any, int) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("response is not JSON: %s", string(raw))
		}
	}
	return out, resp.StatusCode
}

func postJSON(t *testing.T, url string, body any) (map[string]any, int) {
	return requestJSON(t, http.MethodPost, url, body)
}

func TestAPIInvalidSampleEnvelope(t *testing.T) {
	f := newAPIFixture(t)
	bad := map[string]any{
		"instances": []map[string]any{{
			"id": "x", "port": 0, "weight": 100, "zone": "z-a",
			"probe": map[string]any{"mode": "tcp", "timeout_millis": 100,
				"success_threshold": 1, "failure_threshold": 1},
		}},
		"trace": []map[string]any{
			{"timestamp_millis": 1, "client_id": "c1", "request_weight": 1, "request_bytes": 8},
		},
		"affinity": map[string]any{"ratios": map[string]int{"z-a": 1000}},
	}
	body, status := postJSON(t, f.server.URL+"/v1/samples/load", bad)
	if status != http.StatusBadRequest || body["code"] != model.CodeInstanceInvalid {
		t.Fatalf("expected E_INSTANCE_INVALID/400, got %d %v", status, body)
	}
	if _, ok := body["timestamp"]; !ok {
		t.Fatalf("error envelope must carry timestamp, got %v", body)
	}
}

func TestAPIFullLifecycleAndQueueFull(t *testing.T) {
	f := newAPIFixture(t)
	base := f.server.URL

	if body, status := postJSON(t, base+"/v1/samples/load", f.sample); status != http.StatusOK || body["loaded"] != true {
		t.Fatalf("sample load failed: %d %v", status, body)
	}
	if body, status := postJSON(t, base+"/v1/probe-rounds", map[string]string{"round_id": "p1"}); status != http.StatusOK {
		t.Fatalf("probe round failed: %d %v", status, body)
	}
	body, status := requestJSON(t, http.MethodGet, base+"/v1/instances", nil)
	if status != http.StatusOK {
		t.Fatalf("list instances failed: %d %v", status, body)
	}
	items := body["instances"].([]any)
	phases := map[string]string{}
	for _, item := range items {
		row := item.(map[string]any)
		phases[row["id"].(string)] = row["phase"].(string)
	}
	if phases["i1"] != "HEALTHY" || phases["i2"] != "UNHEALTHY" {
		t.Fatalf("unexpected phases after real probes: %v", phases)
	}

	// 队列容量 2、请求 3：第三笔应返回 E_DISPATCH_QUEUE_FULL(429)。
	dispatch, status := postJSON(t, base+"/v1/dispatch-rounds", map[string]string{"round_id": "d1"})
	if status != http.StatusTooManyRequests {
		t.Fatalf("expected 429 queue full, got %d", status)
	}
	if dispatch["queue_full_error"] == nil {
		t.Fatalf("expected queue full detail, got %v", dispatch)
	}
	decision, status := requestJSON(t, http.MethodGet, base+"/v1/requests/0", nil)
	if status != http.StatusOK || decision["status"] != "DISPATCHED" {
		t.Fatalf("request decision missing, got %d %v", status, decision)
	}

	// DRAINING 上再次摘除必须返回 E_INVALID_TRANSITION(409)。
	if _, status := postJSON(t, base+"/v1/instances/i1/drain", map[string]string{"reason": "first"}); status != http.StatusOK {
		t.Fatalf("first drain should succeed")
	}
	errBody, status := postJSON(t, base+"/v1/instances/i1/drain", map[string]string{"reason": "again"})
	if status != http.StatusConflict || errBody["code"] != model.CodeInvalidTransition {
		t.Fatalf("expected E_INVALID_TRANSITION/409, got %d %v", status, errBody)
	}
}

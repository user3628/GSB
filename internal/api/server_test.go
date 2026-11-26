package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"logagg/internal/engine"
	"logagg/internal/sample"
	"logagg/internal/store"
)

func TestHTTPLoopbackAdvanceQueryAndRecovery(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	eng := engine.New(st, engine.DefaultConfig())
	defer eng.Close()
	if _, err := eng.CommitGeneratedShard(context.Background(), sample.ShardID, 1); err != nil {
		t.Fatal(err)
	}

	server := NewServer(eng, log.New(io.Discard, "", 0))
	ts := httptest.NewServer(server.Mux)
	defer ts.Close()

	getJSON(t, ts.URL+"/healthz", http.StatusOK)
	events := getJSON(t, ts.URL+"/v1/events?count=5", http.StatusOK)
	if events["events"] == nil {
		t.Fatalf("missing events: %+v", events)
	}

	body := map[string]any{
		"batch_id": "http-batch",
		"items": []map[string]any{
			{"shard_id": sample.ShardID, "version": 1, "block_no": 0},
			{"shard_id": sample.ShardID, "version": 1, "block_no": 1},
		},
		"group_column":  sample.GroupColumn,
		"metric_column": sample.MetricColumn,
		"input_scale":   sample.MetricScale,
		"precision":     map[string]any{"input_scale": sample.MetricScale, "result_scale": 2, "rounding_mode": "half_up"},
		"byte_budget":   1,
	}
	resp := postJSON(t, ts.URL+"/v1/sessions/http-session/advance", body, http.StatusAccepted)
	if resp["status"] != "deferred" {
		t.Fatalf("expected deferred response: %+v", resp)
	}

	body["byte_budget"] = 1 << 40
	continued := postJSON(t, ts.URL+"/v1/sessions/http-session/continue", body, http.StatusOK)
	if continued["status"] != "completed" {
		t.Fatalf("expected completed continuation: %+v", continued)
	}

	query := getJSON(t, ts.URL+"/v1/sessions/http-session/query?result_scale=2", http.StatusOK)
	result, ok := query["result"].(map[string]any)
	if !ok || len(result["groups"].([]any)) == 0 {
		t.Fatalf("missing query groups: %+v", query)
	}

	recovery := postJSON(t, ts.URL+"/v1/recovery", map[string]any{}, http.StatusOK)
	if _, ok := recovery["needs_recompute"].([]any); !ok {
		t.Fatalf("bad recovery response: %+v", recovery)
	}
}

func getJSON(t *testing.T, url string, wantStatus int) map[string]any {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return decodeStatusJSON(t, resp, wantStatus)
}

func postJSON(t *testing.T, url string, body any, wantStatus int) map[string]any {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return decodeStatusJSON(t, resp, wantStatus)
}

func decodeStatusJSON(t *testing.T, resp *http.Response, wantStatus int) map[string]any {
	t.Helper()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != wantStatus {
		t.Fatalf("status=%d want %d body=%s", resp.StatusCode, wantStatus, string(data))
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("decode %s: %v", string(data), err)
	}
	return out
}

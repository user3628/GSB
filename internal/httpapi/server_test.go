package httpapi_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"ropechunk/internal/engine"
	"ropechunk/internal/httpapi"
	"ropechunk/internal/model"
	"ropechunk/internal/store"
)

func TestHTTPControlPlane(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "http.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	eng := engine.New(st)
	server := httptest.NewServer(httpapi.New(eng).Handler())
	defer server.Close()

	bundle := httpBundle()
	postJSON(t, server.URL+"/api/import", bundle, http.StatusOK)
	postJSON(t, server.URL+"/api/compute", map[string]any{"stage": "rotation"}, http.StatusOK)

	status := getJSON[engine.StatusReport](t, server.URL+"/api/status")
	if len(status.Shards) != 1 || status.Shards[0].Stages[0].CompleteRows != 2 {
		t.Fatalf("unexpected status: %+v", status)
	}
	blocks := getJSON[map[string]any](t, server.URL+"/api/blocks?shard_id=http-shard&stage=rotation")
	if values, ok := blocks["blocks"].([]any); !ok || len(values) != 2 {
		t.Fatalf("unexpected blocks: %+v", blocks)
	}
	budget := getJSON[[]model.BudgetEvent](t, server.URL+"/api/budget")
	if len(budget) == 0 {
		t.Fatal("budget events not published")
	}
	fingerprints := getJSON[map[string]any](t, server.URL+"/api/fingerprints")
	if fingerprints["config_digest"] == "" {
		t.Fatalf("missing fingerprint response: %+v", fingerprints)
	}
	postJSON(t, server.URL+"/api/stop", map[string]any{}, http.StatusAccepted)
	health := getJSON[map[string]any](t, server.URL+"/api/health")
	if health["stopped"] != true {
		t.Fatalf("stop flag not visible: %+v", health)
	}
	req, err := http.NewRequest(http.MethodPost, server.URL+"/api/resume", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("resume status=%d body=%s", resp.StatusCode, body)
	}
	health = getJSON[map[string]any](t, server.URL+"/api/health")
	if health["stopped"] != false {
		t.Fatalf("resume did not clear stop flag: %+v", health)
	}
}

func postJSON(t *testing.T, url string, value any, wantStatus int) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("POST %s status=%d want=%d body=%s", url, resp.StatusCode, wantStatus, data)
	}
	return data
}

func getJSON[T any](t *testing.T, url string) T {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s status=%d body=%s", url, resp.StatusCode, body)
	}
	var out T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func httpBundle() model.Bundle {
	cfg := model.Config{SnapshotVersion: 1, BaseFrequency: 10_000, ScalingMode: model.ScaleNone, ScaleFactor: 1,
		AngleBlockSize: 2, CausalMask: true, RowBlockSize: 2, KeyBlockSize: 2, MaxConcurrentBlock: 2,
		WorkerBlockQuota: 2, RoundTimeBudgetMS: 5000, PeakMemoryBytes: 16 << 20}
	shape := model.Shape{1, 1, 4, 4}
	data := make([]int64, shape.Len())
	kdata := make([]int64, shape.Len())
	for i := range data {
		data[i] = int64(i*1000 - 7000)
		kdata[i] = int64(i*1300 - 9000)
	}
	shard := model.Shard{ID: "http-shard", Shape: shape,
		Q: model.QuantTensor{Shape: shape, Scale: 10_000, Data: data},
		K: model.QuantTensor{Shape: shape, Scale: 10_000, Data: kdata}}
	return model.Bundle{FormatVersion: 1, Config: cfg, Shards: []model.Shard{shard},
		Declarations: []model.ShapeDeclaration{{ShardID: "http-shard", Shape: shape}}}
}

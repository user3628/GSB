package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"hostacl-workbench/internal/engine"
	"hostacl-workbench/internal/model"
)

func TestHTTPContracts(t *testing.T) {
	root := filepath.Join(t.TempDir(), "root")
	eng, err := engine.New(engine.Config{Root: root, DBPath: filepath.Join(t.TempDir(), "db.sqlite"), MaxConcurrency: 2, QueueSize: 4, VerifyBudget: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	server := httptest.NewServer(New(eng))
	defer server.Close()
	sample := model.DeterministicSample()

	reply := post(t, server.URL+"/v1/snapshots", sample.SnapshotV1)
	if reply.StatusCode != http.StatusOK {
		t.Fatalf("snapshot status=%d body=%s", reply.StatusCode, readBody(reply))
	}
	reply = post(t, server.URL+"/v1/reads", map[string]any{"subject_id": "u-api", "path": "data", "observed_version": 1})
	if reply.StatusCode != http.StatusOK {
		t.Fatalf("read status=%d body=%s", reply.StatusCode, readBody(reply))
	}
	reply = post(t, server.URL+"/v1/reads", map[string]any{"subject_id": "u-api", "path": "data", "observed_version": 2})
	if reply.StatusCode != http.StatusConflict {
		t.Fatalf("stale read status=%d body=%s", reply.StatusCode, readBody(reply))
	}
	var failure struct {
		Code    string         `json:"code"`
		Context map[string]any `json:"context"`
	}
	if err := json.NewDecoder(reply.Body).Decode(&failure); err != nil {
		t.Fatal(err)
	}
	if failure.Code != "E_STALE_READ" || failure.Context["view_version"] != float64(1) || failure.Context["observed_version"] != float64(2) {
		t.Fatalf("unexpected stale read body: %+v", failure)
	}
	bad := model.ClonePolicy(sample.SnapshotV1)
	bad.Paths[0].Path = "C:/escape"
	bad.Digest = model.SnapshotDigest(bad)
	reply = post(t, server.URL+"/v1/snapshots", bad)
	if reply.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid status=%d body=%s", reply.StatusCode, readBody(reply))
	}
	if err := json.NewDecoder(reply.Body).Decode(&failure); err != nil {
		t.Fatal(err)
	}
	if failure.Code != "E_SPEC_INVALID" {
		t.Fatalf("invalid code=%s", failure.Code)
	}
	reply = post(t, server.URL+"/v1/verify", map[string]any{"max_concurrency": 1, "queue_size": 1, "subject_ids": []string{"u-api", "svc-indexer"}, "paths": []string{"data", "data/private.json"}})
	if reply.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("queue status=%d body=%s", reply.StatusCode, readBody(reply))
	}
}

func post(t *testing.T, url string, value any) *http.Response {
	t.Helper()
	raw, _ := json.Marshal(value)
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func readBody(response *http.Response) string {
	defer response.Body.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(response.Body)
	return buf.String()
}

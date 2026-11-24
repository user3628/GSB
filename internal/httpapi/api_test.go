package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"checkpointengine/internal/engine"
	"checkpointengine/internal/model"
	"checkpointengine/internal/store"
)

func TestHTTPWorkflow(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "http.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	fixed := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	eng, err := engine.New(st, engine.Options{DataDir: filepath.Join(dir, "data"), Now: func() time.Time { return fixed }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(New(eng, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	defer server.Close()

	policy := readSample(t, "policy_v1.json")
	resp, body := postJSON(t, server.URL+"/api/v1/snapshots", policy)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("snapshot status = %d body=%s", resp.StatusCode, body)
	}
	r1 := readSample(t, "round1_increment.json")
	resp, body = postJSON(t, server.URL+"/api/v1/checkpoints", r1)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("round1 status = %d body=%s", resp.StatusCode, body)
	}
	var submission model.SubmissionResponse
	if err := json.Unmarshal(body, &submission); err != nil {
		t.Fatal(err)
	}
	if submission.Revision != 2 || len(submission.Writes) != 2 || len(submission.Deferred) != 2 {
		t.Fatalf("unexpected round1 HTTP response: %+v", submission)
	}

	status, body := getJSON(t, server.URL+"/api/v1/pools")
	if status != http.StatusOK {
		t.Fatalf("pools status=%d body=%s", status, body)
	}
	var pools struct {
		Pools []model.PoolUsage `json:"pools"`
	}
	if err := json.Unmarshal(body, &pools); err != nil {
		t.Fatal(err)
	}
	if poolBytes(pools.Pools, model.PoolVRAM) != 1792 {
		t.Fatalf("unexpected pools: %s", body)
	}

	status, body = getJSON(t, server.URL+"/api/v1/rounds?round=1")
	if status != http.StatusOK || !bytes.Contains(body, []byte(`"status":"INCOMPLETE"`)) {
		t.Fatalf("rounds status=%d body=%s", status, body)
	}
	status, body = getJSON(t, server.URL+"/api/v1/recovery-report")
	if status != http.StatusOK || !bytes.Contains(body, []byte(`"facts_basis":"committed_sqlite_transactions_and_verified_blob_manifests"`)) {
		t.Fatalf("recovery report status=%d body=%s", status, body)
	}

	resp, body = postJSON(t, server.URL+"/api/v1/checkpoints", readSample(t, "round2_increment.json"))
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("round2 status = %d body=%s", resp.StatusCode, body)
	}
	resp, body = postJSON(t, server.URL+"/api/v1/checkpoints", readSample(t, "round3_drift_increment.json"))
	if resp.StatusCode != http.StatusUnprocessableEntity || !bytes.Contains(body, []byte(model.CodePrecisionDrift)) {
		t.Fatalf("drift status=%d body=%s", resp.StatusCode, body)
	}
	status, body = getJSON(t, server.URL+"/api/v1/shards?round=3")
	if status != http.StatusOK || !bytes.Contains(body, []byte(`"shards":null`)) {
		t.Fatalf("drift leaked shards: status=%d body=%s", status, body)
	}
	resp, body = postJSON(t, server.URL+"/api/v1/checkpoints", readSample(t, "round4_complete_increment.json"))
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("round4 status=%d body=%s", resp.StatusCode, body)
	}
	status, body = getJSON(t, server.URL+"/api/v1/rounds?round=4")
	if status != http.StatusOK || !bytes.Contains(body, []byte(`"status":"PASS"`)) {
		t.Fatalf("round4 did not reach PASS: status=%d body=%s", status, body)
	}

	resp, body = postJSON(t, server.URL+"/api/v1/checkpoints", readSample(t, "round1_increment.json"))
	if resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`"idempotent_replay":true`)) {
		t.Fatalf("idempotent HTTP status=%d body=%s", resp.StatusCode, body)
	}
	conflict := map[string]any{}
	_ = json.Unmarshal([]byte(readSample(t, "round1_increment.json")), &conflict)
	conflict["submission_id"] = "sub-round-2"
	conflictBody, err := json.Marshal(conflict)
	if err != nil {
		t.Fatal(err)
	}
	resp, body = postJSON(t, server.URL+"/api/v1/checkpoints", conflictBody)
	if resp.StatusCode != http.StatusConflict || !bytes.Contains(body, []byte(model.CodeConflictingSubmit)) {
		t.Fatalf("conflict status=%d body=%s", resp.StatusCode, body)
	}

	resp, body = postJSON(t, server.URL+"/api/v1/snapshots", readSample(t, "policy_v1.json"))
	if resp.StatusCode != http.StatusConflict || !bytes.Contains(body, []byte(model.CodeStaleSnapshot)) {
		t.Fatalf("stale snapshot status=%d body=%s", resp.StatusCode, body)
	}

	verifyBody := []byte(`{"round":1,"submission_id":"verify-http"}`)
	resp, body = postJSON(t, server.URL+"/api/v1/precision/verify", verifyBody)
	if resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`"passed":true`)) {
		t.Fatalf("verify status=%d body=%s", resp.StatusCode, body)
	}
	resp, body = postJSON(t, server.URL+"/api/v1/schedule", []byte(`{"submission_id":"schedule-http"}`))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("schedule status=%d body=%s", resp.StatusCode, body)
	}
	status, body = getJSON(t, server.URL+"/does-not-exist")
	if status != http.StatusNotFound || !bytes.Contains(body, []byte(model.CodeNotFound)) {
		t.Fatalf("not found status=%d body=%s", status, body)
	}
}

func readSample(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "samples", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func postJSON(t *testing.T, url string, body []byte) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, data
}

func getJSON(t *testing.T, url string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, data
}

func poolBytes(pools []model.PoolUsage, pool string) int64 {
	for _, p := range pools {
		if p.Pool == pool {
			return p.UsedBytes
		}
	}
	return -1
}

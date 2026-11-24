package httpapi_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"ampcheckpoint/internal/domain"
	"ampcheckpoint/internal/engine"
	"ampcheckpoint/internal/httpapi"
	"ampcheckpoint/internal/sample"
)

type apiRig struct {
	server   *httptest.Server
	engine   *engine.Engine
	manifest sample.Manifest
}

func newAPIRig(t *testing.T) *apiRig {
	t.Helper()
	dir := t.TempDir()
	gen, err := sample.New(sample.DefaultSeed, filepath.Join(dir, "sample"))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := gen.Ensure()
	if err != nil {
		t.Fatal(err)
	}
	eng, err := engine.Open(filepath.Join(dir, "checkpoints.db"), gen)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(httpapi.New(eng))
	t.Cleanup(func() {
		server.Close()
		_ = eng.Close()
	})
	return &apiRig{server: server, engine: eng, manifest: manifest}
}

func (r *apiRig) request(t *testing.T, method, path string, body any) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, r.server.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, raw
}

func decode[T any](t *testing.T, raw []byte) T {
	t.Helper()
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return out
}

func errorCode(t *testing.T, raw []byte) string {
	t.Helper()
	payload := decode[struct {
		Error domain.BusinessError `json:"error"`
	}](t, raw)
	if payload.Error.Code == "" || payload.Error.Message == "" {
		t.Fatalf("invalid error envelope: %s", raw)
	}
	return payload.Error.Code
}

func TestHTTPEndToEndAndUniformErrors(t *testing.T) {
	r := newAPIRig(t)
	status, raw := r.request(t, http.MethodGet, "/healthz", nil)
	if status != http.StatusOK || decode[map[string]string](t, raw)["status"] != "ok" {
		t.Fatalf("health status=%d body=%s", status, raw)
	}
	status, raw = r.request(t, http.MethodGet, "/v1/sample", nil)
	if status != http.StatusOK || decode[sample.Manifest](t, raw).Seed != sample.DefaultSeed {
		t.Fatalf("sample status=%d body=%s", status, raw)
	}

	status, raw = r.request(t, http.MethodPost, "/v1/policies", r.manifest.Policy)
	if status != http.StatusCreated {
		t.Fatalf("submit policy status=%d body=%s", status, raw)
	}
	if decode[struct {
		PolicyVersion int64 `json:"policy_version"`
	}](t, raw).PolicyVersion != 1 {
		t.Fatalf("unexpected policy response: %s", raw)
	}
	status, raw = r.request(t, http.MethodPost, "/v1/checkpoints", domain.CheckpointRequest{Round: 1})
	if status != http.StatusCreated {
		t.Fatalf("checkpoint status=%d body=%s", status, raw)
	}
	cp := decode[domain.CheckpointResult](t, raw)
	if cp.Version != 1 || len(cp.Pools) != 2 {
		t.Fatalf("checkpoint response=%+v", cp)
	}

	for _, path := range []string{
		"/v1/versions/1",
		"/v1/versions/1/meta",
		"/v1/versions/1/shards",
		"/v1/versions/1/pools",
		"/v1/versions/1/precision",
		"/v1/versions/1/recovery",
		"/v1/versions/1/derived",
		"/v1/versions/1/writes",
		"/v1/versions/latest/shards",
		"/v1/checkpoints/1/pools",
	} {
		status, raw = r.request(t, http.MethodGet, path, nil)
		if status != http.StatusOK {
			t.Fatalf("GET %s status=%d body=%s", path, status, raw)
		}
	}

	status, raw = r.request(t, http.MethodPost, "/v1/policies", r.manifest.Policy)
	if status != http.StatusConflict || errorCode(t, raw) != domain.ErrStaleSnapshot {
		t.Fatalf("stale policy status=%d body=%s", status, raw)
	}
	status, raw = r.request(t, http.MethodPost, "/v1/deltas", domain.DeltaFragment{BaseVersion: 0})
	if status != http.StatusConflict || errorCode(t, raw) != domain.ErrStaleBase {
		t.Fatalf("stale base status=%d body=%s", status, raw)
	}
	bad := domain.PartitionBatch{Partitions: []domain.PartitionInput{{CopyID: "x", ShardID: "s", Round: 1, Precision: "BF16", SizeBytes: 1}}}
	status, raw = r.request(t, http.MethodPost, "/v1/partitions/validate", bad)
	if status != http.StatusUnprocessableEntity || errorCode(t, raw) != domain.ErrPartitionInvalid {
		t.Fatalf("bad partition status=%d body=%s", status, raw)
	}
	status, raw = r.request(t, http.MethodGet, "/v1/versions/999/shards", nil)
	if status != http.StatusNotFound || errorCode(t, raw) != domain.ErrNotFound {
		t.Fatalf("not found status=%d body=%s", status, raw)
	}
	status, raw = r.request(t, http.MethodGet, "/v1/no-such-route", nil)
	if status != http.StatusNotFound || errorCode(t, raw) != domain.ErrNotFound {
		t.Fatalf("unknown route status=%d body=%s", status, raw)
	}
	status, raw = r.request(t, http.MethodGet, "/v1/checkpoints", nil)
	if status != http.StatusMethodNotAllowed || errorCode(t, raw) != domain.ErrInvalidRequest {
		t.Fatalf("method mismatch status=%d body=%s", status, raw)
	}

	req, err := http.NewRequest(http.MethodPost, r.server.URL+"/v1/checkpoints", bytes.NewBufferString("{"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest || errorCode(t, raw) != domain.ErrInvalidRequest {
		t.Fatalf("malformed JSON status=%d body=%s", resp.StatusCode, raw)
	}
}

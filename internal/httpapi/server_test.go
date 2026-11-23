package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"ciartifacts/internal/cache"
)

func TestHTTPErrorEnvelopeAndHit(t *testing.T) {
	inputs, err := cache.LoadSampleFile(filepath.Join("..", "..", "samples", "entries.json"))
	if err != nil {
		t.Fatal(err)
	}
	input, err := cache.SampleByName(inputs, "build")
	if err != nil {
		t.Fatal(err)
	}
	store, err := cache.Open(context.Background(), cache.Config{
		Dir:           t.TempDir(),
		CapacityBytes: 1024,
		Clock:         cache.NewManualClock(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server := httptest.NewServer(New(store))
	defer server.Close()

	badBody := map[string]any{
		"stage_id":          input.StageID,
		"input_fingerprint": input.InputFingerprint,
		"toolchain_version": input.ToolchainVersion,
		"artifact_content":  string(input.ArtifactContent),
		"artifact_digest":   "sha256:bad",
		"artifact_size":     input.ArtifactSize,
		"retain_until":      input.RetainUntil.Format(time.RFC3339),
	}
	status, body := request(t, server, http.MethodPost, "/v1/entries", badBody)
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", status, body)
	}
	var apiErr map[string]any
	if err := json.Unmarshal(body, &apiErr); err != nil {
		t.Fatal(err)
	}
	if apiErr["code"] != cache.CodeEntryInvalid || apiErr["context"] == nil {
		t.Fatalf("non-uniform HTTP error: %s", body)
	}

	validBody := map[string]any{
		"stage_id":          input.StageID,
		"input_fingerprint": input.InputFingerprint,
		"toolchain_version": input.ToolchainVersion,
		"artifact_content":  string(input.ArtifactContent),
		"artifact_digest":   input.ArtifactDigest,
		"artifact_size":     input.ArtifactSize,
		"retain_until":      input.RetainUntil.Format(time.RFC3339),
	}
	status, body = request(t, server, http.MethodPost, "/v1/entries", validBody)
	if status != http.StatusCreated {
		t.Fatalf("write failed: %d %s", status, body)
	}
	values := url.Values{}
	values.Set("stage_id", input.StageID)
	values.Set("input_fingerprint", input.InputFingerprint)
	values.Set("toolchain_version", input.ToolchainVersion)
	status, body = request(t, server, http.MethodGet, "/v1/hit?"+values.Encode(), nil)
	if status != http.StatusOK {
		t.Fatalf("hit failed: %d %s", status, body)
	}
	var hit map[string]any
	_ = json.Unmarshal(body, &hit)
	if hit["hit"] != true || hit["hits"].(float64) != 1 {
		t.Fatalf("unexpected hit body: %s", body)
	}

	values.Set("stage_id", "missing")
	status, body = request(t, server, http.MethodGet, "/v1/hit?"+values.Encode(), nil)
	if status != http.StatusNotFound {
		t.Fatalf("expected miss 404, got %d: %s", status, body)
	}
	var miss map[string]any
	_ = json.Unmarshal(body, &miss)
	contextValues, _ := miss["context"].(map[string]any)
	if miss["code"] != cache.CodeCacheMiss || contextValues["reason"] != "KEY_NOT_FOUND" {
		t.Fatalf("unexpected miss response: %s", body)
	}

	status, body = request(t, server, http.MethodPost, "/v1/hit", map[string]any{})
	if status != http.StatusMethodNotAllowed || !bytes.Contains(body, []byte(cache.CodeMethodNotAllowed)) {
		t.Fatalf("method mismatch was not a uniform JSON error: %d %s", status, body)
	}
	status, body = request(t, server, http.MethodGet, "/v1/does-not-exist", nil)
	if status != http.StatusNotFound || !bytes.Contains(body, []byte(cache.CodeNotFound)) {
		t.Fatalf("unknown route was not a uniform JSON error: %d %s", status, body)
	}
}

func request(t *testing.T, server *httptest.Server, method, path string, value any) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if value != nil {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, server.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, body
}

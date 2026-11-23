package httpapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"blocksnap/internal/store"
)

func httpTestServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "api.db"), store.WithClock(func() time.Time {
		return time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(New(st, true))
	t.Cleanup(func() { ts.Close(); st.Close() })
	return ts, st
}

func doJSON(t *testing.T, method, url string, body any) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, url, reader)
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
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func TestHTTPWriteSnapshotAnalysisAndErrors(t *testing.T) {
	ts, _ := httpTestServer(t)
	status, body := doJSON(t, http.MethodPost, ts.URL+"/v1/volumes", map[string]any{
		"request_id": "c", "volume_id": "v", "block_count": 4, "block_size": 8,
		"writeback_overhead_bytes_per_block": 2,
	})
	if status != 200 {
		t.Fatalf("create status=%d body=%s", status, body)
	}
	content := []byte("abcdefgh")
	sum := sha256.Sum256(content)
	writeBody := map[string]any{
		"request_id": "w", "expected_version": 0,
		"content_base64": base64.StdEncoding.EncodeToString(content), "sha256": hex.EncodeToString(sum[:]),
	}
	status, body = doJSON(t, http.MethodPut, ts.URL+"/v1/volumes/v/blocks/0", writeBody)
	if status != 200 {
		t.Fatalf("write status=%d body=%s", status, body)
	}
	// Exact replay must produce identical bytes.
	status, replay := doJSON(t, http.MethodPut, ts.URL+"/v1/volumes/v/blocks/0", writeBody)
	if status != 200 || !bytes.Equal(body, replay) {
		t.Fatalf("replay status=%d body=%s replay=%s", status, body, replay)
	}
	bad := map[string]any{
		"request_id": "bad", "expected_version": 0,
		"content_base64": base64.StdEncoding.EncodeToString(content), "sha256": "00",
	}
	status, body = doJSON(t, http.MethodPut, ts.URL+"/v1/volumes/v/blocks/1", bad)
	if status != 422 || !bytes.Contains(body, []byte("E_BLOCK_CHECKSUM")) {
		t.Fatalf("checksum status=%d body=%s", status, body)
	}
	status, body = doJSON(t, http.MethodPost, ts.URL+"/v1/volumes/v/snapshots", map[string]any{
		"request_id": "s", "snapshot_id": "snap", "kind": "full", "weight": 2,
	})
	if status != 200 {
		t.Fatalf("snapshot status=%d body=%s", status, body)
	}
	status, body = doJSON(t, http.MethodGet, ts.URL+"/v1/volumes/v/snapshots/snap/analysis?margin_floor=0.100000&cost_budget_bytes=1000&relative_tolerance=0.000001", nil)
	if status != 200 || !bytes.Contains(body, []byte("\"decision\"")) {
		t.Fatalf("analysis status=%d body=%s", status, body)
	}
	status, body = doJSON(t, http.MethodGet, ts.URL+"/v1/no-such-route", nil)
	if status != 404 || !bytes.Contains(body, []byte("E_NOT_FOUND")) {
		t.Fatalf("not found status=%d body=%s", status, body)
	}
	status, body = doJSON(t, http.MethodGet, ts.URL+"/v1/volumes", nil)
	if status != 405 || !bytes.Contains(body, []byte("E_METHOD_NOT_ALLOWED")) {
		t.Fatalf("method status=%d body=%s", status, body)
	}
	status, body = doJSON(t, http.MethodGet, ts.URL+"/v1/recovery/reports/latest", nil)
	if status != 200 || !bytes.Contains(body, []byte("\"status\":\"complete\"")) {
		t.Fatalf("recovery status=%d body=%s", status, body)
	}
}

func TestHTTPSimulateCrashDisabledReturnsJSON(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ts := httptest.NewServer(New(st, false))
	defer ts.Close()
	status, body := doJSON(t, http.MethodPost, ts.URL+"/v1/debug/crash", map[string]any{"volume_id": "v", "operation": "merge"})
	if status != 404 || !bytes.Contains(body, []byte("E_NOT_FOUND")) {
		t.Fatalf("status=%d body=%s", status, body)
	}
}

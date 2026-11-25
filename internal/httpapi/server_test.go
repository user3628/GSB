package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"configtree/internal/engine"
	"configtree/internal/seed"
	"configtree/internal/spec"
)

func newTestServer(t *testing.T) (http.Handler, *engine.Engine) {
	t.Helper()
	eng, err := engine.Open(filepath.Join(t.TempDir(), "http.db"))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	if _, err := eng.Commit(context.Background(), &engine.CommitRequest{
		CommitID: "s1", Kind: "snapshot", Snapshot: seed.V1(),
	}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	return New(eng).Handler(), eng
}

func envelope(t *testing.T, cid string, payload any) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"commit_id": cid, "payload": payload})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func do(t *testing.T, h http.Handler, method, target string, body []byte) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("invalid JSON response: %v", err)
		}
	}
	return rec.Code, out
}

func TestHTTPSnapshotAndStaleCodes(t *testing.T) {
	h, _ := newTestServer(t)

	code, body := do(t, h, http.MethodPost, "/v1/commits/snapshots",
		envelope(t, "stale", seed.StaleSnapshot()))
	if code != http.StatusConflict || body["code"] != spec.CodeStaleSnapshot {
		t.Fatalf("stale snapshot: code=%d body=%v", code, body)
	}

	code, body = do(t, h, http.MethodPost, "/v1/commits/snapshots",
		envelope(t, "bad", seed.InvalidV1()))
	if code != http.StatusUnprocessableEntity || body["code"] != spec.CodeSpecInvalid {
		t.Fatalf("invalid snapshot: code=%d body=%v", code, body)
	}
	if _, ok := body["context"]; !ok {
		t.Fatal("business error must carry context")
	}
}

func TestHTTPDeltaStaleBaseAndRead(t *testing.T) {
	h, _ := newTestServer(t)

	code, _ := do(t, h, http.MethodPost, "/v1/commits/deltas",
		envelope(t, "d2", seed.DeltaV2()))
	if code != http.StatusOK {
		t.Fatalf("delta v2 failed: %d", code)
	}
	code, body := do(t, h, http.MethodPost, "/v1/commits/deltas",
		envelope(t, "d2c", seed.DeltaV2Concurrent()))
	if code != http.StatusConflict || body["code"] != spec.CodeStaleBase {
		t.Fatalf("concurrent delta: code=%d body=%v", code, body)
	}

	code, body = do(t, h, http.MethodGet,
		"/v1/components/gateway/view?observed_version=2", nil)
	if code != http.StatusOK {
		t.Fatalf("read failed: %d body=%v", code, body)
	}
	if body["used_version"].(float64) != 2 || body["converged"] != true {
		t.Fatalf("unexpected read body: %v", body)
	}

	code, body = do(t, h, http.MethodGet,
		"/v1/components/ghost/view?observed_version=1", nil)
	if code != http.StatusNotFound || body["code"] != spec.CodeNotFound {
		t.Fatalf("missing component: code=%d body=%v", code, body)
	}
}

func TestHTTPIdempotencyConflict(t *testing.T) {
	h, _ := newTestServer(t)
	body1 := envelope(t, "same-id", seed.DeltaV2())
	if code, _ := do(t, h, http.MethodPost, "/v1/commits/deltas", body1); code != 200 {
		t.Fatal("first delta failed")
	}
	if code, body := do(t, h, http.MethodPost, "/v1/commits/deltas", body1); code != 200 ||
		body["reused"] != true {
		t.Fatalf("idempotent retry failed: %d %v", code, body)
	}
	code, body := do(t, h, http.MethodPost, "/v1/commits/deltas",
		envelope(t, "same-id", seed.DeltaV2Concurrent()))
	if code != http.StatusConflict || body["code"] != spec.CodeConflictingRequest {
		t.Fatalf("conflict detection failed: %d %v", code, body)
	}
}

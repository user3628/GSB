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
	"sync"
	"testing"
	"time"

	"blockpool"
)

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func newTestAPI(t *testing.T) (*httptest.Server, *testClock) {
	t.Helper()
	clock := &testClock{now: time.UnixMilli(1_800_000_000_000)}
	pool, err := blockpool.Open(filepath.Join(t.TempDir(), "api.sqlite"), blockpool.Options{Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	server := httptest.NewServer(New(pool).Handler())
	t.Cleanup(server.Close)
	return server, clock
}

func post(t *testing.T, url string, body any) (int, map[string]any) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var decoded map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("decode response %s: %v", raw, err)
		}
	}
	return resp.StatusCode, decoded
}

func get(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var decoded map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("decode response %s: %v", raw, err)
		}
	}
	return resp.StatusCode, decoded
}

func TestHTTPContractLeaseExpiryAndErrors(t *testing.T) {
	server, clock := newTestAPI(t)
	base := server.URL
	code, _ := post(t, base+"/v1/namespaces", map[string]any{"name": "ns"})
	if code != http.StatusCreated {
		t.Fatalf("namespace status = %d", code)
	}
	code, _ = post(t, base+"/v1/nodes", map[string]any{"namespace": "ns", "path": "file/a"})
	if code != http.StatusCreated {
		t.Fatalf("node status = %d", code)
	}
	content := []byte("HTTP content block")
	sum := sha256.Sum256(content)
	digest := hex.EncodeToString(sum[:])
	code, ingested := post(t, base+"/v1/blocks", map[string]any{
		"request_id": "ingest-http", "digest": digest,
		"data_base64": base64.StdEncoding.EncodeToString(content), "retention_ms": 0,
	})
	if code != http.StatusCreated || ingested["digest"] != digest {
		t.Fatalf("ingest = %d %+v", code, ingested)
	}

	code, bad := post(t, base+"/v1/blocks", map[string]any{"digest": "bad", "data_base64": ""})
	if code != http.StatusBadRequest || bad["code"] != string(blockpool.CodeSpecInvalid) {
		t.Fatalf("bad digest = %d %+v", code, bad)
	}
	code, corrupt := post(t, base+"/v1/blocks", map[string]any{
		"digest": digest, "data_base64": base64.StdEncoding.EncodeToString([]byte("other")),
	})
	if code != http.StatusConflict || corrupt["code"] != string(blockpool.CodeContentCorrupt) {
		t.Fatalf("corrupt = %d %+v", code, corrupt)
	}
	code, conflict := post(t, base+"/v1/blocks", map[string]any{
		"request_id": "ingest-http", "digest": digest,
		"data_base64": base64.StdEncoding.EncodeToString(content), "retention_ms": 999,
	})
	if code != http.StatusConflict || conflict["code"] != string(blockpool.CodeConflictingRequest) {
		t.Fatalf("idempotency conflict = %d %+v", code, conflict)
	}

	mountBody := map[string]any{"request_id": "mount-http", "namespace": "ns", "path": "file/a",
		"digest": digest, "lease_ms": 100}
	code, mounted := post(t, base+"/v1/mount", mountBody)
	if code != http.StatusOK || mounted["version"] != float64(1) {
		t.Fatalf("mount = %d %+v", code, mounted)
	}
	code, repeated := post(t, base+"/v1/mount", mountBody)
	if code != http.StatusOK || repeated["version"] != float64(1) {
		t.Fatalf("idempotent mount = %d %+v", code, repeated)
	}
	contentResp, contentErr := http.Get(base + "/v1/blocks/" + digest + "/content")
	if contentErr != nil {
		t.Fatal(contentErr)
	}
	contentBytes, _ := io.ReadAll(contentResp.Body)
	contentResp.Body.Close()
	if contentResp.StatusCode != http.StatusOK || !bytes.Equal(contentBytes, content) {
		t.Fatalf("content read = %d %q", contentResp.StatusCode, contentBytes)
	}
	code, stale := post(t, base+"/v1/renew", map[string]any{
		"request_id": "renew-stale-http", "namespace": "ns", "path": "file/a", "digest": digest,
		"version": 99, "lease_ms": 100,
	})
	if code != http.StatusConflict || stale["code"] != string(blockpool.CodeLeaseStale) {
		t.Fatalf("stale renew = %d %+v", code, stale)
	}
	contextMap, _ := stale["context"].(map[string]any)
	if contextMap["current_version"] != float64(1) {
		t.Fatalf("stale context = %+v", stale)
	}
	code, unsafe := post(t, base+"/v1/reclaim", map[string]any{"request_id": "reclaim-unsafe-http", "digest": digest})
	if code != http.StatusConflict || unsafe["code"] != string(blockpool.CodeReclaimUnsafe) {
		t.Fatalf("unsafe reclaim = %d %+v", code, unsafe)
	}

	clock.advance(101 * time.Millisecond)
	code, details := get(t, base+"/v1/blocks/"+digest)
	if code != http.StatusOK || details["ref_count"] != float64(0) || details["active_lease_count"] != float64(0) {
		t.Fatalf("expired block = %d %+v", code, details)
	}
	code, leases := get(t, base+"/v1/blocks/"+digest+"/leases?history=1")
	if code != http.StatusOK {
		t.Fatalf("lease details = %d %+v", code, leases)
	}
	code, reclaimed := post(t, base+"/v1/reclaim", map[string]any{"request_id": "reclaim-http", "digest": digest})
	if code != http.StatusOK || reclaimed["released_bytes"] != float64(len(content)) {
		t.Fatalf("reclaim = %d %+v", code, reclaimed)
	}
	code, details = get(t, base+"/v1/blocks/"+digest)
	if code != http.StatusOK || details["state"] != blockpool.BlockReclaimed || details["ref_count"] != float64(0) {
		t.Fatalf("reclaimed block = %d %+v", code, details)
	}
	code, report := get(t, base+"/v1/recovery")
	if _, ok := report["id"]; code != http.StatusOK || !ok {
		t.Fatalf("recovery = %d %+v", code, report)
	}
}

func TestHTTPNotFoundAndLoopbackGuard(t *testing.T) {
	server, _ := newTestAPI(t)
	code, body := get(t, server.URL+"/v1/blocks/not-a-digest")
	if code != http.StatusBadRequest || body["code"] != string(blockpool.CodeSpecInvalid) {
		t.Fatalf("invalid block route = %d %+v", code, body)
	}
	code, body = get(t, server.URL+"/v1/unknown")
	if code != http.StatusNotFound || body["code"] != string(blockpool.CodeNotFound) {
		t.Fatalf("unknown route = %d %+v", code, body)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ListenAndServe(ctx, "0.0.0.0:0", http.NotFoundHandler()); err == nil {
		t.Fatal("non-loopback listen was accepted")
	}
}

package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"leasearbiter/internal/arbiter"
	"leasearbiter/internal/clock"
	"leasearbiter/internal/store"
)

func startTestServer(t *testing.T) (string, *arbiter.Engine) {
	t.Helper()
	clk := clock.NewManual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	st, err := store.Open(filepath.Join(t.TempDir(), "http.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	eng := arbiter.New(arbiter.Config{Store: st, Clock: clk, Sleeper: clk})
	t.Cleanup(eng.Close)
	if _, err := eng.Seed(context.Background(), []arbiter.SeedRow{{RowID: "r1", Content: "seed"}}); err != nil {
		t.Fatal(err)
	}
	ln, err := ListenLoopback("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: NewServer(arbiter.NewIdempotentService(eng)).Handler()}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return "http://" + ln.Addr().String(), eng
}

func postJSON(t *testing.T, base, path string, body any) map[string]any {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := http.Post(base+path, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestLoopbackGuardAndErrorEnvelope(t *testing.T) {
	if _, err := ListenLoopback("0.0.0.0:0"); err == nil {
		t.Fatal("non-loopback bind must be rejected")
	}
	base, _ := startTestServer(t)

	ahead := postJSON(t, base, "/rows", map[string]any{"row_id": "r1", "snapshot_ver": 999})
	errObj, _ := ahead["error"].(map[string]any)
	if errObj["code"] != "E_SNAPSHOT_AHEAD" {
		t.Fatalf("unexpected envelope: %+v", ahead)
	}

	grant := postJSON(t, base, "/leases/acquire", map[string]any{
		"row_id": "r1", "holder": "alice", "ttl_ms": 1000, "request_id": "r1",
	})
	data, _ := grant["data"].(map[string]any)
	if grant["ok"] != true || data["lease_version"].(float64) != 1 {
		t.Fatalf("unexpected grant: %+v", grant)
	}

	held := postJSON(t, base, "/leases/acquire", map[string]any{
		"row_id": "r1", "holder": "bob", "ttl_ms": 1000,
	})
	if held["error"].(map[string]any)["code"] != "E_WRITE_LEASE_HELD" {
		t.Fatalf("expected held error: %+v", held)
	}
}

func TestHTTPEndToEndCommitAndRetiredRead(t *testing.T) {
	base, _ := startTestServer(t)
	grant := postJSON(t, base, "/leases/acquire", map[string]any{"row_id": "r1", "holder": "alice", "ttl_ms": 5000})
	lv := int64(grant["data"].(map[string]any)["lease_version"].(float64))
	commit := postJSON(t, base, "/edits/commit", map[string]any{
		"row_id": "r1", "holder": "alice", "lease_version": lv,
		"base_row_version": 1, "content": "edited", "request_id": "c1",
	})
	if commit["ok"] != true {
		t.Fatalf("commit failed: %+v", commit)
	}
	// Duplicate request id returns the stored result, not a second version.
	dup := postJSON(t, base, "/edits/commit", map[string]any{
		"row_id": "r1", "holder": "alice", "lease_version": lv,
		"base_row_version": 1, "content": "edited", "request_id": "c1",
	})
	if dup["data"].(map[string]any)["row_version"].(float64) != 2 {
		t.Fatalf("idempotent reuse failed: %+v", dup)
	}
	// Recycle the seed version with no active snapshot.
	res := postJSON(t, base, "/recycle", map[string]any{
		"row_id": "r1", "min_retain_snapshots": 99, "min_retain_versions": 1,
	})
	if res["ok"] != true {
		t.Fatalf("recycle failed: %+v", res)
	}
	retired := postJSON(t, base, "/rows", map[string]any{"row_id": "r1", "snapshot_ver": 1})
	if retired["error"].(map[string]any)["code"] != "E_VERSION_RETIRED" {
		t.Fatalf("expected retired: %+v", retired)
	}
	// Latest snapshot still reads the edited content (seed=1, edit=2).
	view := postJSON(t, base, "/rows", map[string]any{"row_id": "r1", "snapshot_ver": 2})
	if view["data"].(map[string]any)["content"] != "edited" {
		t.Fatalf("latest read wrong: %+v", view)
	}
}

package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"example.com/gsb/dnscache/internal/dnsmsg"
	"example.com/gsb/dnscache/internal/resolver"
	"example.com/gsb/dnscache/internal/scheduler"
	"example.com/gsb/dnscache/internal/store"
)

func adminSnapshot() store.ZoneSnapshot {
	s := store.ZoneSnapshot{
		Zone: "example.test.", Version: 1, GeneratedAtMS: 1767225600000,
		RRsets: []store.RRset{
			{Owner: "example.test.", Type: dnsmsg.TypeSOA, TTL: 30, Records: []dnsmsg.RData{{SOA: &dnsmsg.SOA{
				MName: "ns1.example.test.", RName: "hostmaster.example.test.", Serial: 1, Refresh: 3600, Retry: 600, Expire: 86400, Minimum: 5,
			}}}},
			{Owner: "www.example.test.", Type: dnsmsg.TypeA, TTL: 60, Records: []dnsmsg.RData{{Address: "192.0.2.10"}}},
		},
	}
	s.Checksum = store.ComputeSnapshotChecksum(s)
	return s
}

func adminDelta(version int64, address string) store.ZoneDelta {
	d := store.ZoneDelta{
		DeltaID: "delta-admin", Zone: "example.test.", BaseVersion: 1, Version: version, GeneratedAtMS: 1767225601000,
		Changes: []store.RRsetChange{{Owner: "www.example.test.", Type: dnsmsg.TypeA, TTL: 60, Records: []dnsmsg.RData{{Address: address}}}},
	}
	d.Checksum = store.ComputeDeltaChecksum(d)
	return d
}

func TestManagementAPICommitIdempotencyAndStale(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "admin.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	sched := scheduler.New(scheduler.Config{GlobalLimit: 2, PerServerLimit: 1, QueueLimit: 4})
	a := New(st, sched, resolver.NewMetrics(), nil)
	ts := httptest.NewServer(a.Handler())
	defer ts.Close()

	post := func(path, requestID string, body any) (int, map[string]any) {
		b, _ := json.Marshal(body)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+path, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		if requestID != "" {
			req.Header.Set("X-Request-ID", requestID)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, out
	}

	status, out := post("/api/zones/snapshot", "snap-1", adminSnapshot())
	if status != http.StatusOK || out["ok"] != true {
		t.Fatalf("snapshot status=%d body=%+v", status, out)
	}
	delta := adminDelta(2, "192.0.2.11")
	status, out = post("/api/zones/delta", "delta-1", delta)
	if status != http.StatusOK {
		t.Fatalf("delta status=%d body=%+v", status, out)
	}
	status, out = post("/api/zones/delta", "delta-1", delta)
	if status != http.StatusOK || out["result"].(map[string]any)["idempotent"] != true {
		t.Fatalf("idempotent status=%d body=%+v", status, out)
	}
	stale := adminDelta(3, "192.0.2.12")
	stale.BaseVersion = 1
	stale.Checksum = store.ComputeDeltaChecksum(stale)
	status, out = post("/api/zones/delta", "stale-1", stale)
	if status != http.StatusConflict || out["code"] != "E_STALE_BASE" {
		t.Fatalf("stale status=%d body=%+v", status, out)
	}
	conflicting := stale
	conflicting.Checksum = store.ComputeDeltaChecksum(conflicting)
	status, out = post("/api/zones/delta", "delta-1", conflicting)
	if status != http.StatusConflict || out["code"] != "E_CONFLICTING_REQUEST" {
		t.Fatalf("conflict status=%d body=%+v", status, out)
	}

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/metrics", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics err=%v status=%v", err, resp)
	}
	resp.Body.Close()
	zone, err := st.ZoneState(context.Background(), "example.test.")
	if err != nil || zone.Version != 2 {
		t.Fatalf("zone=%+v err=%v", zone, err)
	}
}

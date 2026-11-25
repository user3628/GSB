package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"qosshaper/internal/qos"
)

func TestHTTPBusinessCodes(t *testing.T) {
	store, err := qos.OpenStore(qos.StoreOptions{
		DBPath: filepath.Join(t.TempDir(), "qos.db"), DataDir: filepath.Join(t.TempDir(), "data"), SampleSeed: qos.DefaultSampleSeed,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	srv := httptest.NewServer(New(store, nil).Handler())
	defer srv.Close()

	q := func(id, parent string) qos.Queue {
		out := qos.Queue{ID: id, CIRBPS: 1000, PIRBPS: 2000, CBSBytes: 10, PBSBytes: 20, Priority: 1,
			Match: qos.MatchRule{SrcPortMin: 0, SrcPortMax: 65535, DSCPValues: []int{0}}}
		if parent != "" {
			out.ParentID = &parent
		}
		return out
	}
	post := func(path string, body any) (int, qos.BusinessError) {
		b, _ := json.Marshal(body)
		resp, err := http.Post(srv.URL+path, "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var be qos.BusinessError
		if resp.StatusCode >= 400 {
			if err := json.NewDecoder(resp.Body).Decode(&be); err != nil {
				t.Fatal(err)
			}
		}
		return resp.StatusCode, be
	}
	get := func(path string) (int, qos.BusinessError) {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var be qos.BusinessError
		if resp.StatusCode >= 400 {
			if err := json.NewDecoder(resp.Body).Decode(&be); err != nil {
				t.Fatal(err)
			}
		}
		return resp.StatusCode, be
	}

	status, be := post("/v1/policy/snapshot", qos.SnapshotRequest{
		RequestID: "bad-duplicate", Version: 1, Queues: []qos.Queue{q("a", ""), q("a", "")},
	})
	if status != 422 || be.Code != qos.CodePolicyInvalid {
		t.Fatalf("duplicate: %d %+v", status, be)
	}
	status, be = post("/v1/policy/snapshot", qos.SnapshotRequest{
		RequestID: "bad-cycle", Version: 1, Queues: []qos.Queue{q("a", "b"), q("b", "a")},
	})
	if status != 422 || be.Code != qos.CodeDependencyCycle {
		t.Fatalf("cycle: %d %+v", status, be)
	}
	status, be = post("/v1/policy/snapshot", qos.SnapshotRequest{
		RequestID: "snap-1", Version: 1, Queues: []qos.Queue{q("root", ""), q("child", "root")},
	})
	if status != 200 || be.Code != "" {
		t.Fatalf("snapshot: %d %+v", status, be)
	}
	status, be = get("/v1/policy/queues")
	if status != 422 || be.Code != qos.CodePolicyInvalid {
		t.Fatalf("missing observe version: %d %+v", status, be)
	}
	status, be = get("/v1/policy/queues/child?observe_version=1")
	if status != 200 {
		t.Fatalf("view: %d %+v", status, be)
	}
	cir := int64(500)
	status, be = post("/v1/policy/increment", qos.IncrementRequest{
		RequestID: "inc-1", BaseVersion: 1, QueuePatches: []qos.QueuePatch{{QueueID: "root", CIRBPS: &cir}},
	})
	if status != 200 {
		t.Fatalf("increment: %d %+v", status, be)
	}
	status, be = post("/v1/policy/increment", qos.IncrementRequest{RequestID: "inc-stale", BaseVersion: 1})
	if status != 409 || be.Code != qos.CodeStaleBase {
		t.Fatalf("stale base: %d %+v", status, be)
	}
	status, be = post("/v1/policy/snapshot", qos.SnapshotRequest{
		RequestID: "snap-stale", Version: 2, Queues: []qos.Queue{q("root", "")},
	})
	if status != 409 || be.Code != qos.CodeStaleSnapshot {
		t.Fatalf("stale snapshot: %d %+v", status, be)
	}
	status, be = get("/v1/policy/queues/child?observe_version=1")
	if status != 409 || be.Code != qos.CodeStaleRead {
		t.Fatalf("stale read: %d %+v", status, be)
	}
	resp, err := http.Post(srv.URL+"/v1/policy/snapshot", "application/json", bytes.NewBufferString("{"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var malformed qos.BusinessError
	if err := json.NewDecoder(resp.Body).Decode(&malformed); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 422 || malformed.Code != qos.CodePolicyInvalid {
		t.Fatalf("malformed: %d %+v", resp.StatusCode, malformed)
	}
}

package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"gpusched/internal/domain"
	"gpusched/internal/service"
	"gpusched/internal/store"
)

func newServer(t *testing.T) (*httptest.Server, *service.App) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	app := service.New(st)
	mux := http.NewServeMux()
	New(app).Routes(mux)
	return httptest.NewServer(mux), app
}

func TestLoadSpecRejectsInvalidBatch(t *testing.T) {
	srv, _ := newServer(t)
	defer srv.Close()

	good := domain.LoadSpec{
		Groups: []domain.GroupSpec{{ID: "g1", MemoryLimit: "10Gi", ComputeLimit: "500m"}},
		Nodes:  []domain.NodeSpec{{ID: "n1", GroupID: "g1", MemoryTotal: "10Gi", ComputeTotal: "500m", Health: "HEALTHY"}},
		Requests: []domain.RequestSpec{{ID: "r1", ModelID: "m", GroupID: "g1", MemoryNeed: "2Gi",
			ComputeNeed: "100m", SamplesPerBatch: 8, LatencyLimitMs: 100, Priority: 0}},
		Snapshots: []domain.PolicySnapshotSpec{{Version: 1, Params: domain.PolicyParams{
			GlobalMaxConcurrent: 1, WaitQueueCapacity: 1, RoundBudgetMs: 100,
			LatencyBaseMs: 1, LatencyPerSampleMs: 1}}},
	}
	body, _ := json.Marshal(good)
	resp, err := http.Post(srv.URL+"/v1/spec/load", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("valid load status=%d", resp.StatusCode)
	}
	resp.Body.Close()

	// Unknown group -> E_SPEC_INVALID and nothing persisted.
	bad := good
	bad.Requests[0].ID = "r2"
	bad.Requests[0].GroupID = "ghost"
	bbody, _ := json.Marshal(bad)
	resp2, err := http.Post(srv.URL+"/v1/spec/load", "application/json", bytes.NewReader(bbody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid load status=%d want 400", resp2.StatusCode)
	}
	var env map[string]any
	_ = json.NewDecoder(resp2.Body).Decode(&env)
	if env["code"] != "E_SPEC_INVALID" {
		t.Fatalf("code=%v want E_SPEC_INVALID", env["code"])
	}
}

func TestAdmitAndQuotaAPI(t *testing.T) {
	srv, _ := newServer(t)
	defer srv.Close()

	spec := domain.LoadSpec{
		Groups: []domain.GroupSpec{{ID: "g1", MemoryLimit: "10Gi", ComputeLimit: "500m"}},
		Nodes:  []domain.NodeSpec{{ID: "n1", GroupID: "g1", MemoryTotal: "10Gi", ComputeTotal: "500m", Health: "HEALTHY"}},
		Requests: []domain.RequestSpec{
			{ID: "r1", ModelID: "m", GroupID: "g1", MemoryNeed: "9Gi", ComputeNeed: "100m",
				SamplesPerBatch: 8, LatencyLimitMs: 100, Priority: 0},
			{ID: "r2", ModelID: "m", GroupID: "g1", MemoryNeed: "9Gi", ComputeNeed: "100m",
				SamplesPerBatch: 8, LatencyLimitMs: 100, Priority: 0},
		},
		Snapshots: []domain.PolicySnapshotSpec{{Version: 1, Params: domain.PolicyParams{
			GlobalMaxConcurrent: 1, WaitQueueCapacity: 1, RoundBudgetMs: 100,
			LatencyBaseMs: 1, LatencyPerSampleMs: 1, MaxSamplesPerModel: map[string]int64{"m": 64}}}},
	}
	b, _ := json.Marshal(spec)
	resp, _ := http.Post(srv.URL+"/v1/spec/load", "application/json", bytes.NewReader(b))
	resp.Body.Close()

	// First admission succeeds.
	r1, _ := http.Post(srv.URL+"/v1/requests/r1/admit", "application/json", nil)
	if r1.StatusCode != http.StatusOK {
		t.Fatalf("admit r1 status=%d", r1.StatusCode)
	}
	r1.Body.Close()

	// Second exceeds node free memory -> E_QUOTA_EXCEEDED (409).
	r2, err := http.Post(srv.URL+"/v1/requests/r2/admit", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Body.Close()
	if r2.StatusCode != http.StatusConflict {
		t.Fatalf("admit r2 status=%d want 409", r2.StatusCode)
	}
	var env map[string]any
	_ = json.NewDecoder(r2.Body).Decode(&env)
	if env["code"] != "E_QUOTA_EXCEEDED" {
		t.Fatalf("code=%v", env["code"])
	}

	// Illegal transition returns 400 E_INVALID_TRANSITION.
	tb, _ := json.Marshal(map[string]string{"to": "RUNNING"})
	tr, err := http.Post(srv.URL+"/v1/requests/r1/transition", "application/json", bytes.NewReader(tb))
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Body.Close()
	if tr.StatusCode != http.StatusBadRequest {
		t.Fatalf("transition status=%d want 400", tr.StatusCode)
	}
}

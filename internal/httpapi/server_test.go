package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"example.com/gsb-router/internal/errs"
	"example.com/gsb-router/internal/model"
	"example.com/gsb-router/internal/routing"
	"example.com/gsb-router/internal/sample"
	"example.com/gsb-router/internal/store"
)

type envelope struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data"`
	Error *errs.Error     `json:"error"`
}

func testServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	routerStore, err := store.Open(filepath.Join(t.TempDir(), "router.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = routerStore.Close() })
	api := New(routerStore, nil, false)
	server := httptest.NewServer(api.Handler())
	t.Cleanup(server.Close)
	return server, routerStore
}

func request(t *testing.T, server *httptest.Server, method, path string, body any) (int, envelope) {
	t.Helper()
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, server.URL+path, reader)
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
	var env envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode %s %s response: %v", method, path, err)
	}
	return resp.StatusCode, env
}

func commitSnapshot(t *testing.T, server *httptest.Server, snapshot model.Snapshot, inject string) {
	t.Helper()
	status, env := request(t, server, http.MethodPost, "/api/snapshots", store.SnapshotCommitRequest{Snapshot: snapshot, SealChecksum: true, Inject: inject})
	if status != http.StatusOK || !env.OK {
		t.Fatalf("commit snapshot status=%d error=%#v", status, env.Error)
	}
}

func TestUIHealthAndSnapshotFlow(t *testing.T) {
	server, _ := testServer(t)
	resp, err := http.Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") == "" {
		t.Fatalf("UI not served: %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	status, env := request(t, server, http.MethodGet, "/api/health", nil)
	if status != http.StatusOK || !env.OK {
		t.Fatalf("health: %d %#v", status, env.Error)
	}
	commitSnapshot(t, server, sample.SnapshotV1(), "")
	query := routing.Query{TableID: "orders", Observed: 1, Kind: routing.KindEqual, Value: ptr("1")}
	status, env = request(t, server, http.MethodPost, "/api/query", query)
	if status != http.StatusOK || !env.OK {
		t.Fatalf("query: %d %#v", status, env.Error)
	}
	var result store.RouteResponse
	if err := json.Unmarshal(env.Data, &result); err != nil {
		t.Fatal(err)
	}
	if result.CacheVersion != 1 || len(result.PhysicalTables) != 1 || result.PhysicalTables[0] != "db0.orders_01" {
		t.Fatalf("unexpected query response: %#v", result)
	}
}

func TestFiveFaultCodesAreDistinctAndStateSafe(t *testing.T) {
	server, routerStore := testServer(t)
	commitSnapshot(t, server, sample.SnapshotV1(), "")
	codes := map[string]bool{}

	missing := sample.SnapshotV1()
	missing.Version = 2
	missing.Checksum = "auto"
	status, env := request(t, server, http.MethodPost, "/api/snapshots", store.SnapshotCommitRequest{Snapshot: missing, SealChecksum: true, Inject: store.InjectMappingMissing})
	if status != http.StatusBadRequest || env.Error == nil || env.Error.Code != errs.CodePhysicalTableMiss {
		t.Fatalf("mapping fault: %d %#v", status, env.Error)
	}
	codes[env.Error.Code] = true

	overlap := sample.SnapshotV1()
	overlap.Version = 2
	overlap.Checksum = "auto"
	status, env = request(t, server, http.MethodPost, "/api/snapshots", store.SnapshotCommitRequest{Snapshot: overlap, SealChecksum: true, Inject: store.InjectRangeOverlap})
	if status != http.StatusBadRequest || env.Error == nil || env.Error.Code != errs.CodeRangeOverlap {
		t.Fatalf("overlap fault: %d %#v", status, env.Error)
	}
	codes[env.Error.Code] = true

	stale := sample.IncrementV2()
	stale.CommitID = "http-base-regression"
	stale.Inject = store.InjectBaseRegression
	stale.Checksum = ""
	status, env = request(t, server, http.MethodPost, "/api/increments", stale)
	if status != http.StatusConflict || env.Error == nil || env.Error.Code != errs.CodeStaleBase {
		t.Fatalf("base regression fault: %d %#v", status, env.Error)
	}
	codes[env.Error.Code] = true

	query := map[string]any{"table_id": "orders", "observed_version": 1, "kind": "eq", "value": "1", "inject": store.InjectObservedAhead}
	status, env = request(t, server, http.MethodPost, "/api/query", query)
	if status != http.StatusConflict || env.Error == nil || env.Error.Code != errs.CodeObservedAhead {
		t.Fatalf("observed ahead fault: %d %#v", status, env.Error)
	}
	codes[env.Error.Code] = true

	interrupt := sample.IncrementV2()
	interrupt.CommitID = "http-interrupt"
	interrupt.Inject = store.InjectRebuildInterrupt
	interrupt.Checksum = ""
	status, env = request(t, server, http.MethodPost, "/api/increments", interrupt)
	if status != http.StatusConflict || env.Error == nil || env.Error.Code != errs.CodeRebuildInterrupted {
		t.Fatalf("interrupt fault: %d %#v", status, env.Error)
	}
	codes[env.Error.Code] = true

	if len(codes) != 5 {
		t.Fatalf("expected five distinct fault codes, got %#v", codes)
	}
	if routerStore.CurrentVersion() != 1 {
		t.Fatalf("fault injection changed committed version to %d", routerStore.CurrentVersion())
	}
	status, env = request(t, server, http.MethodPost, "/api/query", routing.Query{TableID: "orders", Observed: 1, Kind: routing.KindEqual, Value: ptr("1")})
	if status != http.StatusOK || !env.OK {
		t.Fatalf("old snapshot unusable after faults: %d %#v", status, env.Error)
	}
}

func TestHTTPStaleReadContext(t *testing.T) {
	server, _ := testServer(t)
	commitSnapshot(t, server, sample.SnapshotV1(), "")
	status, env := request(t, server, http.MethodPost, "/api/increments", sample.IncrementV2())
	if status != http.StatusOK || !env.OK {
		t.Fatalf("increment failed: %d %#v", status, env.Error)
	}
	status, env = request(t, server, http.MethodPost, "/api/query", routing.Query{TableID: "orders", Observed: 1, Kind: routing.KindEqual, Value: ptr("1")})
	if status != http.StatusConflict || env.Error == nil || env.Error.Code != errs.CodeStaleRead {
		t.Fatalf("stale read fault: %d %#v", status, env.Error)
	}
	if env.Error.Context["cache_version"] != float64(2) || env.Error.Context["observed_version"] != float64(1) {
		t.Fatalf("stale context missing versions: %#v", env.Error.Context)
	}
}

func TestMalformedSnapshotJSONUsesSnapshotInvalidCode(t *testing.T) {
	server, _ := testServer(t)
	resp, err := http.Post(server.URL+"/api/snapshots", "application/json", strings.NewReader("{"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var env envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest || env.Error == nil || env.Error.Code != errs.CodeSnapshotInvalid || env.Error.Category != errs.CodeSnapshotInvalid {
		t.Fatalf("unexpected malformed snapshot response: %d %#v", resp.StatusCode, env.Error)
	}
}

func TestDirectSnapshotJSONAccepted(t *testing.T) {
	server, _ := testServer(t)
	raw, err := json.Marshal(sample.SnapshotV1())
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(server.URL+"/api/snapshots", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var env envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || !env.OK {
		t.Fatalf("direct snapshot failed: %d %#v", resp.StatusCode, env.Error)
	}
	var result store.CommitResult
	if err := json.Unmarshal(env.Data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Version != 1 {
		t.Fatalf("unexpected direct snapshot result: %#v", result)
	}
}

func TestIncrementFaultWithOriginalChecksumKeepsFaultCode(t *testing.T) {
	server, _ := testServer(t)
	commitSnapshot(t, server, sample.SnapshotV1(), "")
	inc := sample.IncrementV2()
	inc.Inject = store.InjectMappingMissing
	status, env := request(t, server, http.MethodPost, "/api/increments", inc)
	if status != http.StatusBadRequest || env.Error == nil || env.Error.Code != errs.CodePhysicalTableMiss {
		t.Fatalf("fault code was masked by checksum: %d %#v", status, env.Error)
	}
}

func ptr[T any](value T) *T { return &value }

package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"releasegate/domain"
	"releasegate/service"
	"releasegate/store"
)

func testServer(t *testing.T) (*httptest.Server, *service.Service) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	svc := service.New(st)
	api := New(svc, Options{})
	server := httptest.NewServer(api.Handler())
	t.Cleanup(server.Close)
	return server, svc
}

func requestJSON(t *testing.T, client *http.Client, method, url string, body any) (int, map[string]any) {
	t.Helper()
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, url, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var decoded map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp.StatusCode, decoded
}

func TestHTTPErrorContracts(t *testing.T) {
	server, _ := testServer(t)
	client := server.Client()
	base := server.URL

	status, body := requestJSON(t, client, "POST", base+"/v1/releases", map[string]any{})
	if status != http.StatusBadRequest || errorCode(body) != domain.CodeReleaseInvalid {
		t.Fatalf("invalid release: status=%d body=%v", status, body)
	}

	status, body = requestJSON(t, client, "GET", base+"/v1/releases/missing", nil)
	if status != http.StatusNotFound || errorCode(body) != domain.CodeNotFound {
		t.Fatalf("missing release: status=%d body=%v", status, body)
	}

	def := testDefinition()
	status, _ = requestJSON(t, client, "POST", base+"/v1/releases", service.CreateReleaseRequest{ReleaseID: def.ReleaseID, Definition: def})
	if status != http.StatusCreated {
		t.Fatalf("create release status=%d", status)
	}
	status, _ = requestJSON(t, client, "POST", base+"/v1/releases/release-http/start", nil)
	if status != http.StatusOK {
		t.Fatalf("start status=%d", status)
	}

	status, body = requestJSON(t, client, "POST", base+"/v1/releases/release-http/gate", nil)
	if errorCode(body) != domain.CodeInsufficientSamples {
		t.Fatalf("gate without data: status=%d body=%v", status, body)
	}
	status, body = requestJSON(t, client, "POST", base+"/v1/releases/release-http/transitions", map[string]any{"to_state": domain.StateCanary})
	if status != http.StatusConflict || errorCode(body) != domain.CodeInvalidTransition {
		t.Fatalf("invalid transition: status=%d body=%v", status, body)
	}
	status, body = requestJSON(t, client, "GET", base+"/v1/releases/release-http/stats?version=999", nil)
	if status != http.StatusConflict || errorCode(body) != domain.CodeVersionRetired {
		t.Fatalf("retired version: status=%d body=%v", status, body)
	}
}

func errorCode(body map[string]any) string {
	e, _ := body["error"].(map[string]any)
	code, _ := e["code"].(string)
	return code
}

func testDefinition() domain.ReleaseDefinition {
	return domain.ReleaseDefinition{
		ReleaseID: "release-http", QuantileMethod: "linear", ConfidenceLevelPPM: 9500,
		Stages: []domain.StageDefinition{{Number: 1, TargetTrafficPerMille: 100, MinObservationSamples: 0, LatencyQuantilePerMille: 950,
			Gate: domain.GateThresholds{MaxAbsSuccessDropPPM: 60000, MaxRelativeSuccessDropPPM: 100000, MaxLatencyDeltaUS: 2000, MaxCIHalfWidthPPM: 80000, RelativeTolerancePPM: 1000}}},
	}
}

func TestHTTPFixturePromoteAndGateFailure(t *testing.T) {
	server, _ := testServer(t)
	client := server.Client()
	base := server.URL

	var create service.CreateReleaseRequest
	readFixture(t, "../testdata/release.json", &create)
	status, body := requestJSON(t, client, "POST", base+"/v1/releases", create)
	if status != http.StatusCreated {
		t.Fatalf("create status=%d body=%v", status, body)
	}
	if status, body = requestJSON(t, client, "POST", base+"/v1/releases/demo-progressive-release/start", nil); status != http.StatusOK {
		t.Fatalf("start status=%d body=%v", status, body)
	}

	var stage1 domain.BatchInput
	readFixture(t, "../testdata/observations-stage-1.json", &stage1)
	status, body = requestJSON(t, client, "POST", base+"/v1/releases/demo-progressive-release/observations", stage1)
	if status != http.StatusCreated {
		t.Fatalf("stage1 ingest status=%d body=%v", status, body)
	}
	status, body = requestJSON(t, client, "POST", base+"/v1/releases/demo-progressive-release/gate", map[string]any{"stats_version": 1})
	if status != http.StatusOK {
		t.Fatalf("gate1 status=%d body=%v", status, body)
	}
	decision := body["decision"].(map[string]any)
	if decision["outcome"] != "PROMOTE" {
		t.Fatalf("gate1 decision=%v", decision)
	}

	var stage2 domain.BatchInput
	readFixture(t, "../testdata/observations-stage-2.json", &stage2)
	status, body = requestJSON(t, client, "POST", base+"/v1/releases/demo-progressive-release/observations", stage2)
	if status != http.StatusCreated {
		t.Fatalf("stage2 ingest status=%d body=%v", status, body)
	}
	status, body = requestJSON(t, client, "POST", base+"/v1/releases/demo-progressive-release/gate", map[string]any{"stats_version": 2})
	if status != http.StatusConflict || errorCode(body) != domain.CodeGateFailed {
		t.Fatalf("gate2 status=%d body=%v", status, body)
	}

	stage2.Observations[0].InstanceID = "mutated"
	status, body = requestJSON(t, client, "POST", base+"/v1/releases/demo-progressive-release/observations", stage2)
	if status != http.StatusConflict || errorCode(body) != domain.CodeConflictingBatch {
		t.Fatalf("conflict status=%d body=%v", status, body)
	}
}

func readFixture(t *testing.T, path string, dst any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, dst); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPAcceptsBareReleaseDefinition(t *testing.T) {
	server, _ := testServer(t)
	client := server.Client()
	var create service.CreateReleaseRequest
	readFixture(t, "../testdata/release.json", &create)
	status, body := requestJSON(t, client, "POST", server.URL+"/v1/releases", create.Definition)
	if status != http.StatusCreated {
		t.Fatalf("bare definition status=%d body=%v", status, body)
	}
	if body["release_id"] != create.Definition.ReleaseID {
		t.Fatalf("unexpected release response: %v", body)
	}
}

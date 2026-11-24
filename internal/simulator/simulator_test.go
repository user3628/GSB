package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"driftconverge/internal/model"
)

func TestNormalWriteUpdatesActualAndSources(t *testing.T) {
	cluster := fixtureCluster()
	server, err := New(cluster, Config{ListenAddr: "127.0.0.1:0", Mode: "normal"})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := server.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close(context.Background())
	request := WriteRequest{
		RoundID: "r", BatchID: "b", ObjectID: "ns/obj",
		DesiredFields: map[string]model.FieldValue{
			"x": {Type: model.TypeInteger, Value: "2"},
		},
		ExpectedSources: map[string]model.FieldSource{
			"x": {Expected: "gitops", Actual: "kubectl"},
		},
	}
	payload, _ := json.Marshal(request)
	resp, err := http.Post(endpoint+"/v1/write", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write status=%d", resp.StatusCode)
	}
	readResp, err := http.Get(endpoint + "/v1/read?objectId=" + url.QueryEscape("ns/obj"))
	if err != nil {
		t.Fatal(err)
	}
	defer readResp.Body.Close()
	var state ObjectState
	if err := json.NewDecoder(readResp.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	if state.Actual["x"].Value != "2" || state.Sources["x"].Actual != "gitops" || state.Revision != 1 {
		t.Fatalf("write did not update state: %+v", state)
	}
}

func TestUnresolvedModeDoesNotMutate(t *testing.T) {
	server, err := New(fixtureCluster(), Config{ListenAddr: "127.0.0.1:0", Mode: "unresolved"})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := server.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close(context.Background())
	request := WriteRequest{ObjectID: "ns/obj", DesiredFields: map[string]model.FieldValue{"x": {Type: model.TypeInteger, Value: "2"}}}
	payload, _ := json.Marshal(request)
	resp, err := http.Post(endpoint+"/v1/write", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var writeResp WriteResponse
	if err := json.NewDecoder(resp.Body).Decode(&writeResp); err != nil {
		t.Fatal(err)
	}
	if writeResp.Applied || !writeResp.OK {
		t.Fatalf("unresolved mode must acknowledge without applying: %+v", writeResp)
	}
	state, _ := server.State("ns/obj")
	if state.Actual["x"].Value != "1" {
		t.Fatalf("unresolved mode mutated state: %+v", state.Actual)
	}
}

func TestRejectsNonLoopbackAndWildcard(t *testing.T) {
	server, err := New(fixtureCluster(), Config{ListenAddr: "127.0.0.1:0", Mode: "normal"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Start(":0"); err == nil {
		t.Fatal("wildcard listen must be rejected")
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	request.RemoteAddr = "8.8.8.8:1234"
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("non-loopback client status=%d", recorder.Code)
	}
}

func fixtureCluster() *model.ClusterSpec {
	return &model.ClusterSpec{SnapshotVersion: 1, Objects: []model.Object{{
		ID: "ns/obj", Kind: "Deployment", Namespace: "ns", ResourceBudget: 1,
		Expected: map[string]model.FieldValue{"x": {Type: model.TypeInteger, Value: "2"}},
		Actual:   map[string]model.FieldValue{"x": {Type: model.TypeInteger, Value: "1"}},
		Sources:  map[string]model.FieldSource{"x": {Expected: "gitops", Actual: "manual"}},
	}}}
}

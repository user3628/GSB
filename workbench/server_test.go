package workbench_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"registrylab/registry"
	"registrylab/testfixture"
	"registrylab/workbench"
)

type apiEnvelope struct {
	OK        bool            `json:"ok"`
	Data      json.RawMessage `json:"data"`
	Error     *registry.Error `json:"error"`
	RequestID string          `json:"requestId"`
}

func TestHTTPWorkbenchContract(t *testing.T) {
	fixture, err := testfixture.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Close()
	engine, err := registry.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	server := httptest.NewServer(workbench.New(engine, fixture).Handler())
	defer server.Close()

	ui, err := http.Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	uiBody, _ := io.ReadAll(ui.Body)
	ui.Body.Close()
	if ui.StatusCode != 200 || !bytes.Contains(uiBody, []byte("REGISTRY")) {
		t.Fatalf("workbench UI missing: status=%d", ui.StatusCode)
	}

	raw := getJSON(t, server.URL+"/api/v1/sample")
	var sampleEnvelope struct {
		Snapshot registry.SnapshotRequest `json:"snapshot"`
	}
	if err := json.Unmarshal(raw.Data, &sampleEnvelope); err != nil {
		t.Fatal(err)
	}
	snap := sampleEnvelope.Snapshot
	snap.RequestID = "api-snapshot"
	env := postJSON(t, server.URL+"/api/v1/snapshots", snap)
	if !env.OK || env.RequestID != "api-snapshot" {
		t.Fatalf("snapshot response: %+v", env)
	}

	changed := snap
	changed.Instances = append([]registry.Instance(nil), snap.Instances...)
	changed.Instances[0].Weight = 999
	changed.Digest = registry.ComputeDigest(changed.Version, changed.GeneratedAt, changed.Instances)
	resp := postRaw(t, server.URL+"/api/v1/snapshots", changed)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("conflict status=%d", resp.StatusCode)
	}
	var conflict apiEnvelope
	json.NewDecoder(resp.Body).Decode(&conflict)
	resp.Body.Close()
	if conflict.Error == nil || conflict.Error.Code != registry.ErrConflictingRequest || len(conflict.Error.Context) == 0 {
		t.Fatalf("conflict envelope: %+v", conflict)
	}

	badResp := postBytes(t, server.URL+"/api/v1/snapshots", []byte("{"))
	if badResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad JSON status=%d", badResp.StatusCode)
	}
	var bad apiEnvelope
	json.NewDecoder(badResp.Body).Decode(&bad)
	badResp.Body.Close()
	if bad.Error == nil || bad.Error.Code != registry.ErrValidation {
		t.Fatalf("bad JSON envelope: %+v", bad)
	}

	overview := getJSON(t, server.URL+"/api/v1/overview")
	var overviewData struct {
		Current registry.CurrentSnapshot `json:"current"`
		Derived registry.DerivedView     `json:"derived"`
	}
	if err := json.Unmarshal(overview.Data, &overviewData); err != nil {
		t.Fatal(err)
	}
	if overviewData.Current.Version != 1 || overviewData.Current.DerivedRevision != overviewData.Derived.Revision {
		t.Fatalf("overview mismatch: %+v", overviewData)
	}

	round := postJSON(t, server.URL+"/api/v1/probe-rounds", registry.RoundOptions{
		RequestID: "api-round", GlobalConcurrency: 4, PerServiceConcurrency: 2, BudgetMS: 1200, WaitQueueCapacity: 64,
	})
	if !round.OK {
		t.Fatalf("round response: %+v", round)
	}
	var roundResult registry.RoundResult
	if err := json.Unmarshal(round.Data, &roundResult); err != nil {
		t.Fatal(err)
	}
	if roundResult.Started != 9 {
		t.Fatalf("round started=%d", roundResult.Started)
	}
	recovery := getJSON(t, server.URL+"/api/v1/recovery")
	if !recovery.OK {
		t.Fatalf("recovery response: %+v", recovery)
	}
}

func getJSON(t *testing.T, url string) apiEnvelope {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var env apiEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || !env.OK {
		t.Fatalf("GET %s: status=%d envelope=%+v", url, resp.StatusCode, env)
	}
	return env
}

func postJSON(t *testing.T, url string, value any) apiEnvelope {
	t.Helper()
	raw, _ := json.Marshal(value)
	resp := postBytes(t, url, raw)
	defer resp.Body.Close()
	var env apiEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || !env.OK {
		t.Fatalf("POST %s: status=%d envelope=%+v", url, resp.StatusCode, env)
	}
	return env
}

func postRaw(t *testing.T, url string, value any) *http.Response {
	t.Helper()
	raw, _ := json.Marshal(value)
	return postBytes(t, url, raw)
}

func postBytes(t *testing.T, url string, body []byte) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

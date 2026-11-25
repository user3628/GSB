package prefixcache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

type apiEnvelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result"`
	Error  *Error          `json:"error"`
}

func apiRequest(t *testing.T, client *http.Client, method, endpoint string, body any) (int, apiEnvelope) {
	t.Helper()
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	request, err := http.NewRequest(method, endpoint, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, endpoint, err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var envelope apiEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("decode response %s: %v", raw, err)
	}
	return response.StatusCode, envelope
}

func TestHTTPEndToEndAndGracefulShutdown(t *testing.T) {
	engine := openTestEngine(t, testDBPath(t))
	server := NewHTTPServer(engine)
	listener, err := server.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	baseURL := "http://" + listener.Addr().String()
	client := &http.Client{Timeout: 5 * time.Second}

	status, envelope := apiRequest(t, client, http.MethodGet, baseURL+"/v1/healthz", nil)
	if status != http.StatusOK || !envelope.OK {
		t.Fatalf("health status=%d envelope=%+v", status, envelope)
	}

	dataset := GenerateDataset(DefaultSeed)
	status, envelope = apiRequest(t, client, http.MethodPost, baseURL+"/v1/bootstrap", map[string]any{
		"policy":   dataset.Policy,
		"requests": dataset.Requests,
	})
	if status != http.StatusCreated || !envelope.OK {
		t.Fatalf("bootstrap status=%d error=%+v", status, envelope.Error)
	}
	var initResult CommitResult
	if err := json.Unmarshal(envelope.Result, &initResult); err != nil {
		t.Fatal(err)
	}
	if initResult.Version != 1 {
		t.Fatalf("bootstrap version=%d", initResult.Version)
	}

	status, envelope = apiRequest(t, client, http.MethodPost, baseURL+"/v1/hits", HitQuery{ObservedVersion: 1, RequestID: "req-base"})
	if status != http.StatusOK || !envelope.OK {
		t.Fatalf("hit status=%d error=%+v", status, envelope.Error)
	}
	var hitResult HitResult
	if err := json.Unmarshal(envelope.Result, &hitResult); err != nil {
		t.Fatal(err)
	}
	if hitResult.Decision != DecisionExact {
		t.Fatalf("base hit decision=%s evidence=%+v", hitResult.Decision, hitResult.Evidence)
	}

	threshold := 0.81
	status, envelope = apiRequest(t, client, http.MethodPost, baseURL+"/v1/policy/patch", map[string]any{
		"expected_version": 1,
		"patch":            PolicyPatch{MergeThreshold: &threshold},
	})
	if status != http.StatusCreated || !envelope.OK {
		t.Fatalf("patch status=%d error=%+v", status, envelope.Error)
	}

	status, envelope = apiRequest(t, client, http.MethodPost, baseURL+"/v1/hits", HitQuery{ObservedVersion: 1, RequestID: "req-base"})
	if status != http.StatusConflict || envelope.OK || envelope.Error == nil || envelope.Error.Code != CodeStaleVersion {
		t.Fatalf("stale HTTP response status=%d envelope=%+v", status, envelope)
	}
	if envelope.Error.CurrentVersion != 2 || envelope.Error.ObservedVersion != 1 {
		t.Fatalf("stale evidence current=%d observed=%d", envelope.Error.CurrentVersion, envelope.Error.ObservedVersion)
	}

	status, envelope = apiRequest(t, client, http.MethodGet, baseURL+"/v1/status", nil)
	if status != http.StatusOK || !envelope.OK {
		t.Fatalf("status endpoint: %d %+v", status, envelope.Error)
	}
	var statusResult Status
	if err := json.Unmarshal(envelope.Result, &statusResult); err != nil {
		t.Fatal(err)
	}
	if statusResult.CurrentVersion != 2 || statusResult.StageDigests.Version == "" {
		t.Fatalf("unexpected status: %+v", statusResult)
	}

	status, envelope = apiRequest(t, client, http.MethodGet, fmt.Sprintf("%s/v1/invalidations?version=2", baseURL), nil)
	if status != http.StatusOK || !envelope.OK {
		t.Fatalf("invalidations endpoint: %d %+v", status, envelope.Error)
	}
	var invalidations []Invalidation
	if err := json.Unmarshal(envelope.Result, &invalidations); err != nil {
		t.Fatal(err)
	}
	if len(invalidations) == 0 || invalidations[0].Stage != StageHit {
		t.Fatalf("unexpected invalidations: %+v", invalidations)
	}

	status, envelope = apiRequest(t, client, http.MethodPost, baseURL+"/v1/shutdown", map[string]any{})
	if status != http.StatusOK || !envelope.OK {
		t.Fatalf("shutdown status=%d envelope=%+v", status, envelope)
	}
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("serve returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down gracefully")
	}
}

func TestHTTPRejectsUnknownFields(t *testing.T) {
	engine := openTestEngine(t, testDBPath(t))
	handler := NewHTTPServer(engine).Handler()
	request := httptestRequest(http.MethodPost, "/v1/hits", []byte(`{"observed_version":1,"unknown":true}`))
	recorder := newRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

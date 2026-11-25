package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"profilehotspot/internal/apperr"
	"profilehotspot/internal/model"
	"profilehotspot/internal/service"
	"profilehotspot/internal/storage"
)

type apiHarness struct {
	t      *testing.T
	server *httptest.Server
	svc    *service.Service
}

func newAPIHarness(t *testing.T) *apiHarness {
	t.Helper()
	store, err := storage.Open(context.Background(), filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	svc := service.New(store)
	server := httptest.NewServer(New(svc).Handler())
	t.Cleanup(func() {
		server.Close()
		_ = svc.Close()
	})
	return &apiHarness{t: t, server: server, svc: svc}
}

func (h *apiHarness) request(method, path string, body any) (int, http.Header, []byte) {
	h.t.Helper()
	var reader io.Reader
	if body != nil {
		switch value := body.(type) {
		case []byte:
			reader = bytes.NewReader(value)
		case string:
			reader = bytes.NewBufferString(value)
		default:
			raw, err := json.Marshal(body)
			if err != nil {
				h.t.Fatal(err)
			}
			reader = bytes.NewReader(raw)
		}
	}
	request, err := http.NewRequest(method, h.server.URL+path, reader)
	if err != nil {
		h.t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		h.t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		h.t.Fatal(err)
	}
	return response.StatusCode, response.Header, payload
}

func decodeBody(t *testing.T, payload []byte, destination any) {
	t.Helper()
	if err := json.Unmarshal(payload, destination); err != nil {
		t.Fatalf("decode %s: %v", payload, err)
	}
}

func TestHTTPErrorEnvelopeAndMethodRouting(t *testing.T) {
	harness := newAPIHarness(t)
	status, headers, body := harness.request(http.MethodPost, "/v1/samples", `{"batch_id":"bad","samples":[{"id":"x","timestamp_ms":1,"service":"svc","frames":[],"weight":0,"channel":"cpu"}]}`)
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", status, body)
	}
	if headers.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatalf("content type = %q", headers.Get("Content-Type"))
	}
	if headers.Get("X-Request-ID") == "" {
		t.Fatal("missing request id")
	}
	var envelope struct {
		Code      string         `json:"code"`
		Message   string         `json:"message"`
		Context   map[string]any `json:"context"`
		RequestID string         `json:"request_id"`
	}
	decodeBody(t, body, &envelope)
	if envelope.Code != apperr.CodeSampleInvalid || envelope.RequestID == "" || envelope.Context == nil {
		t.Fatalf("unexpected error envelope: %+v", envelope)
	}

	status, _, body = harness.request(http.MethodDelete, "/healthz", nil)
	if status != http.StatusMethodNotAllowed {
		t.Fatalf("method status = %d body=%s", status, body)
	}
	decodeBody(t, body, &envelope)
	if envelope.Code != apperr.CodeMethodNotAllowed {
		t.Fatalf("method error code = %s", envelope.Code)
	}
	status, _, body = harness.request(http.MethodGet, "/does-not-exist", nil)
	if status != http.StatusNotFound {
		t.Fatalf("not-found status = %d body=%s", status, body)
	}
	decodeBody(t, body, &envelope)
	if envelope.Code != apperr.CodeNotFound {
		t.Fatalf("not-found code = %s", envelope.Code)
	}
}

func TestHTTPEndToEndPipelineAndBusinessCodes(t *testing.T) {
	harness := newAPIHarness(t)
	var policy model.Policy
	status, _, body := harness.request(http.MethodGet, "/v1/demo/policy?version=1", nil)
	if status != http.StatusOK {
		t.Fatalf("demo policy status = %d body=%s", status, body)
	}
	decodeBody(t, body, &policy)
	status, _, body = harness.request(http.MethodPost, "/v1/policies", policy)
	if status != http.StatusCreated {
		t.Fatalf("commit policy status = %d body=%s", status, body)
	}

	status, _, body = harness.request(http.MethodGet, "/v1/demo/chunk?seed=42&batch_id=http-batch&count=24&duplicate_every=9", nil)
	if status != http.StatusOK {
		t.Fatalf("demo chunk status = %d body=%s", status, body)
	}
	var chunk model.SampleChunk
	decodeBody(t, body, &chunk)
	status, _, body = harness.request(http.MethodPost, "/v1/samples", chunk)
	if status != http.StatusCreated {
		t.Fatalf("ingest status = %d body=%s", status, body)
	}
	status, _, body = harness.request(http.MethodPost, "/v1/samples", chunk)
	if status != http.StatusOK {
		t.Fatalf("idempotent ingest status = %d body=%s", status, body)
	}
	status, _, body = harness.request(http.MethodGet, "/v1/versions/1/snapshot?limit=5", nil)
	if status != http.StatusOK {
		t.Fatalf("snapshot status = %d body=%s", status, body)
	}
	var snapshot map[string]any
	decodeBody(t, body, &snapshot)
	if snapshot["commit_seq"].(float64) < 1 || snapshot["hotspots"] == nil || snapshot["reservoirs"] == nil {
		t.Fatalf("incomplete snapshot: %s", body)
	}
	for _, path := range []string{"/v1/versions/1/hotspots", "/v1/versions/1/reservoirs", "/v1/versions/1/differences", "/v1/versions/1/modes", "/v1/recovery/report", "/v1/current"} {
		status, _, body = harness.request(http.MethodGet, path, nil)
		if status != http.StatusOK {
			t.Fatalf("GET %s status=%d body=%s", path, status, body)
		}
	}

	capacity := 5
	increment := model.PolicyIncrement{BaseVersion: 1, Services: map[string]model.ServicePatch{"checkout": {ReservoirCapacity: &capacity}}}
	status, _, body = harness.request(http.MethodPost, "/v1/policies/increments", increment)
	if status != http.StatusCreated {
		t.Fatalf("increment status = %d body=%s", status, body)
	}
	status, _, body = harness.request(http.MethodPost, "/v1/policies/increments", increment)
	if status != http.StatusConflict {
		t.Fatalf("stale base status = %d body=%s", status, body)
	}
	var envelope struct {
		Code string `json:"code"`
	}
	decodeBody(t, body, &envelope)
	if envelope.Code != apperr.CodeStaleBase {
		t.Fatalf("stale base code = %s", envelope.Code)
	}

	conflict := chunk
	conflict.BatchID = "http-conflict"
	currentVersion := uint64(2)
	conflict.PolicyVersion = &currentVersion
	conflict.Samples = append([]model.SampleInput(nil), chunk.Samples...)
	conflict.Samples[0].Weight++
	status, _, body = harness.request(http.MethodPost, "/v1/samples", conflict)
	if status != http.StatusConflict {
		t.Fatalf("sample conflict status = %d body=%s", status, body)
	}
	decodeBody(t, body, &envelope)
	if envelope.Code != apperr.CodeSampleConflict {
		t.Fatalf("sample conflict code = %s", envelope.Code)
	}

	staleBatch := chunk
	staleBatch.BatchID = "http-stale-base"
	status, _, body = harness.request(http.MethodPost, "/v1/samples", staleBatch)
	if status != http.StatusConflict {
		t.Fatalf("stale base ingest status = %d body=%s", status, body)
	}
	decodeBody(t, body, &envelope)
	if envelope.Code != apperr.CodeStaleBase {
		t.Fatalf("stale base ingest code = %s", envelope.Code)
	}

	conflictingBatch := chunk
	conflictingBatch.PolicyVersion = &currentVersion
	conflictingBatch.Samples = append([]model.SampleInput(nil), chunk.Samples...)
	conflictingBatch.Samples[0].Weight++
	status, _, body = harness.request(http.MethodPost, "/v1/samples", conflictingBatch)
	if status != http.StatusConflict {
		t.Fatalf("conflicting batch status = %d body=%s", status, body)
	}
	decodeBody(t, body, &envelope)
	if envelope.Code != apperr.CodeConflictingRequest {
		t.Fatalf("conflicting batch code = %s", envelope.Code)
	}

	fullStale := policy
	fullStale.Version = 2
	status, _, body = harness.request(http.MethodPost, "/v1/policies", fullStale)
	if status != http.StatusConflict {
		t.Fatalf("stale snapshot status = %d body=%s", status, body)
	}
	decodeBody(t, body, &envelope)
	if envelope.Code != apperr.CodeStaleSnapshot {
		t.Fatalf("stale snapshot code = %s", envelope.Code)
	}
}

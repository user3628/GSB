package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"local/configmiddleware/internal/core"
	"local/configmiddleware/internal/engine"
	"local/configmiddleware/internal/pipeline"
	"local/configmiddleware/internal/store"
)

func TestUnknownRouteAndMethodUseBusinessJSON(t *testing.T) {
	handler := New(nil).Handler()
	tests := []struct {
		method string
		path   string
		status int
		code   string
	}{
		{http.MethodGet, "/api/v1/not-a-route", http.StatusNotFound, "E_ROUTE_NOT_FOUND"},
		{http.MethodGet, "/api/v1/snapshots", http.StatusMethodNotAllowed, "E_METHOD_NOT_ALLOWED"},
	}
	for _, test := range tests {
		req := httptest.NewRequest(test.method, test.path, nil)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)
		if recorder.Code != test.status {
			t.Fatalf("%s %s status = %d, want %d", test.method, test.path, recorder.Code, test.status)
		}
		var envelope struct {
			Error core.BizError `json:"error"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("response is not JSON: %v %s", err, recorder.Body.String())
		}
		if envelope.Error.Code != test.code {
			t.Fatalf("error code = %s, want %s", envelope.Error.Code, test.code)
		}
	}
}

func TestTrailingJSONIsRejected(t *testing.T) {
	handler := New(nil).Handler()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/snapshots", strings.NewReader(`{"version":1} {"version":2}`))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	var envelope struct {
		Error core.BizError `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("response is not JSON: %v %s", err, recorder.Body.String())
	}
	if envelope.Error.Code != "E_BAD_REQUEST" {
		t.Fatalf("error code = %s, want E_BAD_REQUEST", envelope.Error.Code)
	}
}

func TestBusinessNotFoundIsNotRewrittenAsRouteError(t *testing.T) {
	doc, err := pipeline.Load(filepath.Join("..", "..", "samples", "samples.json"))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	handler := New(engine.New(st, doc, engine.Config{})).Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/snapshots/99", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", recorder.Code)
	}
	var envelope struct {
		Error core.BizError `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("response is not JSON: %v %s", err, recorder.Body.String())
	}
	if envelope.Error.Code != "E_SNAPSHOT_NOT_FOUND" {
		t.Fatalf("error code = %s, want E_SNAPSHOT_NOT_FOUND", envelope.Error.Code)
	}
}

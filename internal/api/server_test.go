package api_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"example.com/logicplan/editor/internal/api"
	"example.com/logicplan/editor/internal/bizerr"
	"example.com/logicplan/editor/internal/engine"
	"example.com/logicplan/editor/internal/fault"
)

func TestHTTPImportPatchQueryAndErrors(t *testing.T) {
	editor, err := engine.Open(context.Background(), filepath.Join(t.TempDir(), "editor.db"), fault.New())
	if err != nil {
		t.Fatal(err)
	}
	defer editor.Close()
	handler := api.New(editor).Handler()

	response := request(t, handler, http.MethodGet, "/v1/snapshot", nil, nil)
	if response.Code != http.StatusNotFound {
		t.Fatalf("snapshot before import status = %d", response.Code)
	}
	assertErrorCode(t, response, bizerr.CodeSnapshotNotFound)
	response = request(t, handler, http.MethodPut, "/v1/snapshot", nil, nil)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("wrong method status = %d", response.Code)
	}
	assertErrorCode(t, response, bizerr.CodeBadRequest)
	response = request(t, handler, http.MethodGet, "/no-such-route", nil, nil)
	assertErrorCode(t, response, bizerr.CodeBadRequest)

	headers := map[string]string{"Content-Type": "text/plain", "X-Request-ID": "http-import-200"}
	response = request(t, handler, http.MethodPost, "/v1/snapshots/import", sample(t, "plan_current.jsonl"), headers)
	if response.Code != http.StatusOK {
		t.Fatalf("import status = %d body=%s", response.Code, response.Body.String())
	}
	var imported map[string]any
	decodeBody(t, response, &imported)
	if imported["version"].(float64) != 200 || imported["decision"] != "accept" {
		t.Fatalf("import response = %#v", imported)
	}

	response = request(t, handler, http.MethodPost, "/v1/snapshots/import", sample(t, "plan_current.jsonl"), headers)
	if response.Code != http.StatusOK {
		t.Fatalf("replay status = %d", response.Code)
	}
	decodeBody(t, response, &imported)
	if replayed, _ := imported["replayed"].(bool); !replayed {
		t.Fatalf("expected replay response: %#v", imported)
	}

	response = request(t, handler, http.MethodPost, "/v1/snapshots/import", sample(t, "plan_v3_required.jsonl"), map[string]string{"Content-Type": "text/plain"})
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("future import status = %d body=%s", response.Code, response.Body.String())
	}
	assertErrorCode(t, response, bizerr.CodeFormatUnsupported)

	response = request(t, handler, http.MethodPost, "/v1/patches", sample(t, "patch_001_set_predicate.json"), map[string]string{"Content-Type": "application/json"})
	if response.Code != http.StatusOK {
		t.Fatalf("patch status = %d body=%s", response.Code, response.Body.String())
	}
	var patched map[string]any
	decodeBody(t, response, &patched)
	if patched["version"].(float64) != 201 {
		t.Fatalf("patch response = %#v", patched)
	}

	response = request(t, handler, http.MethodPost, "/v1/patches", sample(t, "patch_stale_base.json"), map[string]string{"Content-Type": "application/json"})
	if response.Code != http.StatusConflict {
		t.Fatalf("stale patch status = %d body=%s", response.Code, response.Body.String())
	}
	assertErrorCode(t, response, bizerr.CodeStaleBase)

	response = request(t, handler, http.MethodGet, "/v1/tree", nil, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("tree status = %d", response.Code)
	}
	if !bytes.Contains(response.Body.Bytes(), []byte("orders.amount > 500")) {
		t.Fatalf("tree does not contain edited predicate: %s", response.Body.String())
	}
	response = request(t, handler, http.MethodGet, "/v1/indexes/predicates", nil, nil)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte("filter_high_value")) {
		t.Fatalf("predicate index response = %d %s", response.Code, response.Body.String())
	}
	response = request(t, handler, http.MethodGet, "/v1/degradations", nil, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("degradations status = %d", response.Code)
	}
	response = request(t, handler, http.MethodGet, "/v1/recovery", nil, nil)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte("\"consistent\":true")) {
		t.Fatalf("recovery response = %d %s", response.Code, response.Body.String())
	}

	arm := []byte(`{"stage":"patch_after_intent","mode":"crash"}`)
	response = request(t, handler, http.MethodPost, "/v1/admin/crash-point", arm, map[string]string{"Content-Type": "application/json"})
	if response.Code != http.StatusOK {
		t.Fatalf("arm fault status = %d body=%s", response.Code, response.Body.String())
	}
	response = request(t, handler, http.MethodGet, "/v1/admin/crash-point", nil, nil)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte("patch_after_intent")) {
		t.Fatalf("list faults = %d %s", response.Code, response.Body.String())
	}
	response = request(t, handler, http.MethodDelete, "/v1/admin/crash-point?stage=patch_after_intent", nil, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("disarm fault status = %d", response.Code)
	}
}

func TestHTTPBase64EnvelopeImport(t *testing.T) {
	editor, err := engine.Open(context.Background(), filepath.Join(t.TempDir(), "editor.db"), fault.New())
	if err != nil {
		t.Fatal(err)
	}
	defer editor.Close()
	handler := api.New(editor).Handler()
	envelope, _ := json.Marshal(map[string]any{
		"request_id": "envelope-binary", "encoding": "base64",
		"data_base64": base64.StdEncoding.EncodeToString(sample(t, "plan_current.bin")),
	})
	response := request(t, handler, http.MethodPost, "/v1/snapshots/import", envelope, map[string]string{"Content-Type": "application/json; charset=utf-8"})
	if response.Code != http.StatusOK {
		t.Fatalf("envelope import = %d %s", response.Code, response.Body.String())
	}
	var body map[string]any
	decodeBody(t, response, &body)
	if body["format"] != "binary" || body["version"].(float64) != 201 {
		t.Fatalf("envelope import response = %#v", body)
	}
}

func request(t *testing.T, handler http.Handler, method, path string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder
}

func assertErrorCode(t *testing.T, response *httptest.ResponseRecorder, code string) {
	t.Helper()
	var body struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Context map[string]any `json:"context"`
	}
	decodeBody(t, response, &body)
	if body.Code != code || body.Message == "" || body.Context == nil {
		t.Fatalf("error body = %#v, want code %s with context", body, code)
	}
}

func decodeBody(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.Unmarshal(response.Body.Bytes(), target); err != nil {
		t.Fatalf("decode %s: %v", response.Body.String(), err)
	}
}

func sample(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "samples", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

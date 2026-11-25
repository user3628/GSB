package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"elastic-scale/internal/app"
	"elastic-scale/internal/httpapi"
	"elastic-scale/internal/model"
	"elastic-scale/internal/seed"
)

func TestHTTPSnapshotReadAndStaleError(t *testing.T) {
	a, err := app.New(context.Background(), filepath.Join(t.TempDir(), "http.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	server := httptest.NewServer(httpapi.New(a).Handler())
	defer server.Close()
	bundle := seed.Generate()

	putJSON(t, server.URL+"/v1/catalog", app.CatalogRequest{CommitID: "cat", Services: bundle.Catalog.Services}, http.StatusOK)
	putJSON(t, server.URL+"/v1/snapshots", model.SnapshotRequest{CommitID: "snap", Snapshot: bundle.Snapshot}, http.StatusOK)

	resp, err := http.Get(server.URL + "/v1/metrics/inventory-svc?observed_version=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status=%d", resp.StatusCode)
	}

	resp, err = http.Get(server.URL + "/v1/metrics/inventory-svc?observed_version=999")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("stale status=%d want 409", resp.StatusCode)
	}
	var body struct {
		Error model.BusinessError `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != model.CodeStaleRead {
		t.Fatalf("code=%s want E_STALE_READ", body.Error.Code)
	}
}

func putJSON(t *testing.T, url string, body any, wantStatus int) {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Fatalf("POST %s status=%d want %d", url, resp.StatusCode, wantStatus)
	}
}

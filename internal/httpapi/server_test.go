package httpapi_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"maskrevision/internal/generator"
	"maskrevision/internal/httpapi"
	"maskrevision/internal/workspace"
)

func TestWorkbenchHTTPRoutes(t *testing.T) {
	root := filepath.Join(t.TempDir(), "ws")
	if _, err := workspace.Prepare(root, true); err != nil {
		t.Fatal(err)
	}
	if _, err := generator.Generate(filepath.Join(root, "data"), generator.DefaultSeed); err != nil {
		t.Fatal(err)
	}
	ws, err := workspace.Open(root, workspace.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	server := httptest.NewServer(httpapi.New(ws).Handler())
	defer server.Close()

	if body := getBody(t, server.URL+"/", http.StatusOK); !strings.Contains(body, "Panoptic Revision Workbench") {
		t.Fatal("dashboard HTML missing")
	}
	var shards struct {
		Shards []workspace.ShardSummary `json:"shards"`
	}
	if err := json.Unmarshal([]byte(getBody(t, server.URL+"/api/shards", http.StatusOK)), &shards); err != nil {
		t.Fatal(err)
	}
	if len(shards.Shards) != 4 {
		t.Fatalf("expected 4 shards, got %d", len(shards.Shards))
	}
	resp, err := http.Get(server.URL + "/api/shards/shard-00/masks/4.png")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "image/png" {
		t.Fatalf("mask endpoint: status=%d content-type=%s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	png, _ := io.ReadAll(resp.Body)
	if len(png) < 100 || string(png[1:4]) != "PNG" {
		t.Fatal("mask endpoint did not return PNG")
	}
	body := getBody(t, server.URL+"/api/shards/shard-00/snapshots/2", http.StatusUnprocessableEntity)
	if !strings.Contains(body, "E_REVISION_UNDONE") {
		t.Fatalf("expected undone business code, got %s", body)
	}
	diff := getBody(t, server.URL+"/api/shards/shard-00/diff?from=1&to=4", http.StatusOK)
	if !strings.Contains(diff, "difference_pixel_count") || !strings.Contains(diff, "first_cause") {
		t.Fatalf("diff payload missing attribution: %s", diff)
	}
}

func getBody(t *testing.T, url string, wantStatus int) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("GET %s: status %d want %d body=%s", url, resp.StatusCode, wantStatus, data)
	}
	return string(data)
}

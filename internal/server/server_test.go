package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"qaudit/internal/engine"
	"qaudit/internal/generator"
	"qaudit/internal/server"
)

func TestHTTPEndToEnd(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	ds, err := generator.Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := engine.New(dir, filepath.Join(dir, "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.SaveDataset(ds); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(server.New(eng).Handler())
	defer srv.Close()

	get := func(path string) map[string]any {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	post := func(path string, body any) (int, map[string]any) {
		b, _ := json.Marshal(body)
		resp, err := http.Post(srv.URL+path, "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	if h := get("/healthz"); h["status"] != "ok" {
		t.Fatal("healthz failed")
	}
	if _, err := http.Post(srv.URL+"/api/run", "application/json", nil); err != nil {
		t.Fatal(err)
	}
	runResp := get("/api/status")
	layers, ok := runResp["layers"].([]any)
	if !ok || len(layers) != 4 {
		t.Fatalf("status should list 4 layers, got %v", runResp["layers"])
	}
	fp := get("/api/fingerprints")
	if fp["version_fingerprint"] == nil || fp["version_fingerprint"] == "" {
		t.Fatal("version fingerprint missing")
	}
	auditView := get("/api/audit")
	if auditView["audit"] == nil {
		t.Fatal("audit view missing")
	}
	versions := get("/api/versions")
	if versions["versions"] == nil {
		t.Fatal("versions missing")
	}
	exp := get("/api/export")
	if exp["digest"] == nil {
		t.Fatal("export digest missing")
	}
	// Graceful stop marker endpoint.
	if code, _ := post("/api/stop", map[string]any{}); code != http.StatusOK {
		t.Fatalf("stop endpoint status %d", code)
	}
}

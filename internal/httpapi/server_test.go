package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"

	"searchlab/internal/apperr"
	"searchlab/internal/config"
	"searchlab/internal/engine"
	"searchlab/internal/model"
)

func TestHTTPImportBuildQueryCacheAndReports(t *testing.T) {
	dir := t.TempDir()
	cfg := config.MustDefault()
	cfg.Query.CacheCapacity = 4
	cfgPath := filepath.Join(dir, "config.yaml")
	cfgPayload, _ := yaml.Marshal(cfg)
	if err := os.WriteFile(cfgPath, cfgPayload, 0o644); err != nil {
		t.Fatal(err)
	}
	shardsDir := filepath.Join(dir, "shards")
	if err := os.MkdirAll(shardsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	docs := []model.Document{
		{DocID: 1, Title: "Go 语言", Body: "本地搜索 index 查询", Lang: "zh-en", Source: "http"},
		{DocID: 2, Title: "SQLite 索引", Body: "compression search postings", Lang: "zh-en", Source: "http"},
		{DocID: 3, Title: "空正文", Body: "", Lang: "zh", Source: "http"},
	}
	var lines bytes.Buffer
	for _, doc := range docs {
		payload, _ := json.Marshal(doc)
		lines.Write(payload)
		lines.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(shardsDir, "shard.jsonl"), lines.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	app, err := New(Options{DBPath: filepath.Join(dir, "state.db"), LockPath: filepath.Join(dir, "state.lock"), Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	server := httptest.NewServer(app.Handler())
	defer server.Close()

	status, _, body := requestJSON(t, http.MethodPost, server.URL+"/v1/import", map[string]string{"config_path": cfgPath, "shards_dir": shardsDir})
	if status != http.StatusOK {
		t.Fatalf("import status=%d body=%s", status, body)
	}
	var imported map[string]any
	if err := json.Unmarshal(body, &imported); err != nil || imported["documents"].(float64) != 3 {
		t.Fatalf("import response=%s err=%v", body, err)
	}
	status, _, body = requestJSON(t, http.MethodPost, server.URL+"/v1/build", map[string]bool{"force": true})
	if status != http.StatusOK {
		t.Fatalf("build status=%d body=%s", status, body)
	}
	var report engine.BuildReport
	if err := json.Unmarshal(body, &report); err != nil || len(report.Stages) != 4 {
		t.Fatalf("build report=%s err=%v", body, err)
	}

	query := engine.QueryRequest{QueryID: "http-q", Terms: []string{"Go", "语言"}, Mode: "AND"}
	status, headers, firstBody := requestJSON(t, http.MethodPost, server.URL+"/v1/query", query)
	if status != http.StatusOK {
		t.Fatalf("query status=%d body=%s", status, firstBody)
	}
	if headers.Get("X-Cache") != "miss" {
		t.Fatalf("first X-Cache=%q", headers.Get("X-Cache"))
	}
	status, headers, secondBody := requestJSON(t, http.MethodPost, server.URL+"/v1/query", query)
	if status != http.StatusOK {
		t.Fatalf("cached query status=%d body=%s", status, secondBody)
	}
	if headers.Get("X-Cache") != "hit" {
		t.Fatalf("second X-Cache=%q", headers.Get("X-Cache"))
	}
	if !bytes.Equal(firstBody, secondBody) {
		t.Fatal("HTTP cache hit differs byte-for-byte")
	}
	var result engine.QueryResult
	if err := json.Unmarshal(firstBody, &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "ok" || len(result.Hits) == 0 || result.Diagnostics.CacheKey == "" {
		t.Fatalf("query result=%+v", result)
	}
	status, _, reportBody := requestGET(t, server.URL+"/v1/diagnostics?query_id=http-q")
	if status != http.StatusOK || !bytes.Equal(reportBody, firstBody) {
		t.Fatalf("query report mismatch status=%d", status)
	}
	status, _, cacheBody := requestGET(t, server.URL+"/v1/cache")
	if status != http.StatusOK {
		t.Fatalf("cache status=%d body=%s", status, cacheBody)
	}
	var cache struct {
		Cache engine.CacheStats `json:"cache"`
	}
	if err := json.Unmarshal(cacheBody, &cache); err != nil || cache.Cache.Hits != 1 {
		t.Fatalf("cache response=%s err=%v", cacheBody, err)
	}
	status, _, statusBody := requestGET(t, server.URL+"/v1/status")
	if status != http.StatusOK || !bytes.Contains(statusBody, []byte("index_version")) {
		t.Fatalf("status response=%s", statusBody)
	}
	status, _, versionBody := requestGET(t, server.URL+"/v1/version")
	if status != http.StatusOK || !bytes.Contains(versionBody, []byte("postings_artifact")) {
		t.Fatalf("version response=%s", versionBody)
	}
	status, _, _ = requestJSON(t, http.MethodPost, server.URL+"/v1/shutdown", map[string]bool{})
	if status != http.StatusAccepted {
		t.Fatalf("shutdown status=%d", status)
	}
}

func TestHTTPRejectsNonLoopbackAndMissingRuntime(t *testing.T) {
	_, err := New(Options{DBPath: filepath.Join(t.TempDir(), "db"), LockPath: filepath.Join(t.TempDir(), "lock"), Addr: "0.0.0.0:8080"})
	if !apperr.IsCode(err, apperr.CodeInput) {
		t.Fatalf("non-loopback error = %v", err)
	}
	dir := t.TempDir()
	app, err := New(Options{DBPath: filepath.Join(dir, "db"), LockPath: filepath.Join(dir, "lock"), Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	server := httptest.NewServer(app.Handler())
	defer server.Close()
	status, _, body := requestJSON(t, http.MethodPost, server.URL+"/v1/query", engine.QueryRequest{QueryID: "q", Terms: []string{"go"}, Mode: "OR"})
	if status != http.StatusConflict || !bytes.Contains(body, []byte(apperr.CodePrereqMissing)) {
		t.Fatalf("missing runtime status=%d body=%s", status, body)
	}
}

func requestJSON(t *testing.T, method, url string, value any) (int, http.Header, []byte) {
	t.Helper()
	payload, _ := json.Marshal(value)
	req, err := http.NewRequest(method, url, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, body
}

func requestGET(t *testing.T, url string) (int, http.Header, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, body
}

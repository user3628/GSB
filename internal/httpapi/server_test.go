package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"alarmengine/internal/engine"
	"alarmengine/internal/errcode"
	"alarmengine/internal/sample"
	"alarmengine/internal/storage"
	"path/filepath"
)

func newServer(t *testing.T, port int) (*Server, *engine.Engine) {
	t.Helper()
	dir := t.TempDir()
	st, err := storage.Open(filepath.Join(dir, "e.db"))
	if err != nil {
		t.Fatal(err)
	}
	eng, err := engine.New(st, engine.Config{Now: func() int64 { return 1 }})
	if err != nil {
		t.Fatal(err)
	}
	data := sample.Generate()
	if err := eng.BootstrapServices(data.Services); err != nil {
		t.Fatal(err)
	}
	if err := eng.BootstrapPolicy(data.Policy); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(eng, port)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })
	return srv, eng
}

func TestHealthAndIngestOverLoopback(t *testing.T) {
	srv, eng := newServer(t, 18099)
	resp, err := http.Get("http://" + srv.Addr() + "/health")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("health status %d", resp.StatusCode)
	}
	resp.Body.Close()

	data := sample.Generate()
	body, _ := json.Marshal(engine.IngestRequest{
		RequestID: "req-http", BaseVersion: 1, Observations: data.Observations,
	})
	post, err := http.Post("http://"+srv.Addr()+"/ingest", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer post.Body.Close()
	if post.StatusCode != 200 {
		t.Fatalf("ingest status %d", post.StatusCode)
	}
	var env struct {
		OK   bool `json:"ok"`
		Data engine.IngestResult
	}
	if err := json.NewDecoder(post.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	if !env.OK || env.Data.Alerts == 0 || eng.Version() != env.Data.CommitVersion {
		t.Fatalf("unexpected ingest response: %+v", env)
	}
}

func TestStaleBaseReturnsBusinessCode(t *testing.T) {
	srv, eng := newServer(t, 18100)
	data := sample.Generate()
	_, _ = eng.IngestObservations(engine.IngestRequest{
		RequestID: "req-first", BaseVersion: 1, Observations: data.Observations[:4],
	})
	body, _ := json.Marshal(engine.IngestRequest{
		RequestID: "req-stale", BaseVersion: 1, Observations: data.Observations[:2],
	})
	resp, err := http.Post("http://"+srv.Addr()+"/ingest", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var env struct {
		OK   bool   `json:"ok"`
		Code string `json:"code"`
	}
	json.NewDecoder(resp.Body).Decode(&env)
	if env.Code != errcode.EStaleBase {
		t.Fatalf("want %s, got %s", errcode.EStaleBase, env.Code)
	}
}

func TestBindFailureExplicit(t *testing.T) {
	srv1, _ := newServer(t, 18101)
	// Second server on the same high port must fail loudly, not skip.
	dir := t.TempDir()
	st, _ := storage.Open(filepath.Join(dir, "x.db"))
	eng, _ := engine.New(st, engine.Config{})
	port := 18101
	srv2 := NewServer(eng, port)
	err := srv2.Start()
	if errcode.CodeOf(err) != errcode.EBindFailed {
		t.Fatalf("want E_BIND_FAILED on occupied port %s, got %v", srv1.Addr(), err)
	}
}

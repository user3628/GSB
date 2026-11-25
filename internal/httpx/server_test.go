package httpx

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"ticketpipeline/internal/engine"
	"ticketpipeline/internal/gen"
)

func TestLoopbackAPI(t *testing.T) {
	base := t.TempDir()
	dataDir, outDir := filepath.Join(base, "data"), filepath.Join(base, "out")
	for _, sh := range gen.GenerateShards() {
		if err := gen.WriteShard(dataDir, sh); err != nil {
			t.Fatal(err)
		}
	}
	if err := gen.WritePolicy(dataDir, gen.GeneratePolicy()); err != nil {
		t.Fatal(err)
	}
	eng, err := engine.New(dataDir, outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	srv := NewServer(eng, dataDir, outDir)
	addr, err := srv.Start(0)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	client := &http.Client{Timeout: 3 * time.Second}
	get := func(path string) []byte {
		resp, err := client.Get("http://" + addr + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status %d body=%s", path, resp.StatusCode, body)
		}
		return body
	}
	if len(get("/healthz")) == 0 {
		t.Fatal("empty healthz body")
	}
	// Trigger a run then query status and report over the loopback API.
	resp, err := client.Post("http://"+addr+"/run", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("run status %d", resp.StatusCode)
	}
	if len(get("/status")) == 0 {
		t.Fatal("empty status body")
	}
	if len(get("/report")) == 0 {
		t.Fatal("empty report body")
	}
}

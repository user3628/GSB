package appcli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPEndpoints(t *testing.T) {
	svc := newTestService(t)
	srv := NewHTTPServer(svc, "127.0.0.1:0")
	ts := httptest.NewServer(srv.srv.Handler)
	defer ts.Close()

	post := func(path string, body any) (int, map[string]any) {
		var buf bytes.Buffer
		_ = json.NewEncoder(&buf).Encode(body)
		resp, err := http.Post(ts.URL+path, "application/json", &buf)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	get := func(path string) (int, map[string]any) {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	if code, _ := post("/generate", struct{}{}); code != http.StatusOK {
		t.Fatalf("generate status = %d", code)
	}
	if code, _ := post("/pipeline/run", struct{}{}); code != http.StatusOK {
		t.Fatalf("run status = %d", code)
	}
	if code, body := post("/versions/commit", struct{}{}); code != http.StatusCreated {
		t.Fatalf("commit status = %d body=%v", code, body)
	}
	if code, _ := get("/errors/heads"); code != http.StatusOK {
		t.Fatalf("head errors status = %d", code)
	}
	if code, _ := get("/errors/coupling"); code != http.StatusOK {
		t.Fatalf("coupling status = %d", code)
	}
	if code, _ := get("/quantization"); code != http.StatusOK {
		t.Fatalf("quantization status = %d", code)
	}
	if code, _ := get("/versions"); code != http.StatusOK {
		t.Fatalf("history status = %d", code)
	}
	if code, _ := post("/rollback/1", struct{}{}); code != http.StatusCreated {
		t.Fatalf("rollback status = %d", code)
	}
	if code, body := get("/versions/99"); code != http.StatusConflict {
		t.Fatalf("missing version status = %d body=%v", code, body)
	} else {
		errObj := body["error"].(map[string]any)
		if errObj["code"] != "E_STALE_VERSION" {
			t.Fatalf("expected stale code, got %v", errObj["code"])
		}
	}
	if code, _ := post("/stop", struct{}{}); code != http.StatusOK {
		t.Fatalf("stop status = %d", code)
	}
	if code, body := post("/pipeline/run", struct{}{}); code != http.StatusConflict {
		t.Fatalf("stopped run status = %d body=%v", code, body)
	}
	if code, _ := post("/resume", struct{}{}); code != http.StatusOK {
		t.Fatalf("resume status = %d", code)
	}
	if code, _ := get("/health"); code != http.StatusOK {
		t.Fatalf("health status = %d", code)
	}
}

package httpapi_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"motionlab/internal/engine"
	"motionlab/internal/httpapi"
	"motionlab/internal/model"
	"motionlab/internal/motion"
	"motionlab/internal/store"
)

type envelope struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data"`
	Error *model.AppError `json:"error"`
}

func server(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	if err := motion.Initialize(dir, true); err != nil {
		t.Fatal(err)
	}
	cat, err := motion.LoadCatalog(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng := engine.New(cat, st, engine.Options{})
	ts := httptest.NewServer(httpapi.New(eng, dir).Handler())
	t.Cleanup(func() { ts.Close(); _ = st.Close() })
	return ts, st
}

func decode(t *testing.T, resp *http.Response) envelope {
	t.Helper()
	defer resp.Body.Close()
	var env envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	return env
}

func TestHTTPRunLifecycleAndMedia(t *testing.T) {
	ts, _ := server(t)
	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Contains(body, []byte("MOTIONLAB")) {
		t.Fatalf("control surface not served: %d", resp.StatusCode)
	}
	resp, err = http.Get(ts.URL + "/api/sequences/harbor_slide/truth/3.png")
	if err != nil || resp.StatusCode != 200 || resp.Header.Get("X-Frame-Digest") == "" {
		t.Fatalf("truth frame route failed: %v %d", err, resp.StatusCode)
	}
	resp.Body.Close()

	payload := map[string]any{
		"sequence_id": "harbor_slide", "requested_steps": 12, "concurrency": 2,
		"branch_quota": 12, "round_time_budget_ms": 80, "prune_threshold": 2.5,
		"fresh": true, "auto_start": true,
	}
	b, _ := json.Marshal(payload)
	resp, err = http.Post(ts.URL+"/api/runs", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	env := decode(t, resp)
	if !env.OK {
		t.Fatalf("create run failed: %+v", env.Error)
	}
	var created struct {
		RunID string `json:"run_id"`
	}
	_ = json.Unmarshal(env.Data, &created)
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err = http.Get(ts.URL + "/api/runs/" + created.RunID)
		if err != nil {
			t.Fatal(err)
		}
		env = decode(t, resp)
		var detail struct {
			Active bool             `json:"active"`
			Report *model.RunReport `json:"report"`
		}
		_ = json.Unmarshal(env.Data, &detail)
		if !detail.Active && detail.Report != nil && detail.Report.Status == "completed" {
			if detail.Report.DeterministicSHA == "" || detail.Report.Summary.Pruned == 0 {
				t.Fatalf("unexpected report: %+v", detail.Report.Summary)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("run did not complete")
		}
		time.Sleep(60 * time.Millisecond)
	}
	resp, err = http.Get(ts.URL + "/api/runs/" + created.RunID + "/branches/0/frames/3.png")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("predicted frame route failed: %v %d", err, resp.StatusCode)
	}
	resp.Body.Close()
}

func TestHTTPOverflowErrorShape(t *testing.T) {
	ts, _ := server(t)
	b := []byte(`{"sequence_id":"orchid_drift","requested_steps":99,"fresh":true,"auto_start":false}`)
	resp, err := http.Post(ts.URL+"/api/runs", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	env := decode(t, resp)
	if resp.StatusCode != http.StatusUnprocessableEntity || env.Error == nil || env.Error.Code != model.CodeTimelineOverflow {
		t.Fatalf("unexpected overflow response: %d %+v", resp.StatusCode, env.Error)
	}
	if env.Error.SequenceID != "orchid_drift" || env.Error.Details["overflow_steps"] != float64(85) {
		t.Fatalf("overflow error misses context: %+v", env.Error)
	}
}

func TestHTTPServerRequiresStructuredBodies(t *testing.T) {
	ts, _ := server(t)
	resp, err := http.Post(ts.URL+"/api/runs", "application/json", strings.NewReader(`{"unexpected":1}`))
	if err != nil {
		t.Fatal(err)
	}
	env := decode(t, resp)
	if resp.StatusCode != http.StatusBadRequest || env.Error == nil || env.Error.Code != model.CodeValidation {
		t.Fatalf("unexpected validation failure: %d %+v", resp.StatusCode, env.Error)
	}
}

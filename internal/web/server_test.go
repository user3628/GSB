package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"driftconverge/internal/engine"
	"driftconverge/internal/model"
	"driftconverge/internal/simulator"
	"driftconverge/internal/store"
)

func TestDashboardAndStatusAPI(t *testing.T) {
	samples, err := model.LoadSamples("../../samples/cluster-objects.json", "../../samples/policy-snapshot.json", "../../samples/drift-field-values.json")
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(t.TempDir() + "/web.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	sim, err := simulator.New(samples.Cluster, simulator.Config{ListenAddr: "127.0.0.1:0", Mode: "normal"})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := sim.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer sim.Close(context.Background())
	session := engine.NewSession(st, samples)
	if _, err := session.Analyze(context.Background()); err != nil {
		t.Fatal(err)
	}
	app := New(session, sim, endpoint, "normal")

	index := loopbackRequest(http.MethodGet, "/", nil)
	indexRecorder := httptest.NewRecorder()
	app.Handler().ServeHTTP(indexRecorder, index)
	if indexRecorder.Code != http.StatusOK || !strings.Contains(indexRecorder.Body.String(), "DriftLoom") {
		t.Fatalf("dashboard unavailable status=%d", indexRecorder.Code)
	}

	statusRecorder := httptest.NewRecorder()
	app.Handler().ServeHTTP(statusRecorder, loopbackRequest(http.MethodGet, "/api/status", nil))
	if statusRecorder.Code != http.StatusOK {
		t.Fatalf("status code=%d body=%s", statusRecorder.Code, statusRecorder.Body.String())
	}
	var status Status
	if err := json.NewDecoder(statusRecorder.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if status.SnapshotVersion != 7 || status.ObjectCount != 10 || !status.PathReconciled || len(status.Plan) == 0 {
		t.Fatalf("incomplete dashboard status: %+v", status)
	}
}

func TestNonLoopbackClientRejected(t *testing.T) {
	app := &App{}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.RemoteAddr = "203.0.113.9:1234"
	app.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("non-loopback dashboard request status=%d", recorder.Code)
	}
}

func loopbackRequest(method, target string, body *strings.Reader) *http.Request {
	var request *http.Request
	if body == nil {
		request = httptest.NewRequest(method, target, nil)
	} else {
		request = httptest.NewRequest(method, target, body)
	}
	request.RemoteAddr = "127.0.0.1:43210"
	return request
}

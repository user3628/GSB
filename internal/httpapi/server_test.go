package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"local/windowengine/internal/domain"
	"local/windowengine/internal/engine"
	"local/windowengine/internal/httpapi"
	"local/windowengine/internal/sample"
	"local/windowengine/internal/store"
)

func TestHTTPWorkflowAndErrorEnvelope(t *testing.T) {
	server, database := newHTTPServer(t)
	defer server.Close()

	sampleData, err := sample.Generate(242)
	if err != nil {
		t.Fatal(err)
	}
	response := request(t, http.MethodPost, server.URL+"/api/snapshots", sampleData)
	if response.status != http.StatusCreated {
		t.Fatalf("load status=%d body=%s", response.status, response.body)
	}
	if code := request(t, http.MethodPost, server.URL+"/api/snapshots", sampleData); code.status != http.StatusConflict {
		t.Fatalf("duplicate snapshot status=%d, want 409", code.status)
	}
	var duplicate errorResponse
	if err := json.Unmarshal([]byte(request(t, http.MethodPost, server.URL+"/api/snapshots", sampleData).body), &duplicate); err != nil {
		t.Fatal(err)
	}
	if duplicate.Error.Code != domain.CodeInvalidTransition {
		t.Fatalf("duplicate code=%s, want E_INVALID_TRANSITION", duplicate.Error.Code)
	}

	recalcBody := []byte(`{
	  "snapshot_id":"det-sample-seed-242",
	  "budgets":{"global_concurrency":4,"tenant_concurrency":2,"time_budget_ms":10000,"queue_capacity":500}
	}`)
	recalc := request(t, http.MethodPost, server.URL+"/api/recalc", recalcBody)
	if recalc.status != http.StatusOK {
		t.Fatalf("recalc status=%d body=%s", recalc.status, recalc.body)
	}
	var roundResult engine.TriggerResult
	if err := json.Unmarshal([]byte(recalc.body), &roundResult); err != nil {
		t.Fatal(err)
	}
	if roundResult.Round.Status != "COMPLETED" || roundResult.Round.ShedCount != 0 {
		t.Fatalf("unexpected round: %+v", roundResult.Round)
	}

	aggregate := request(t, http.MethodGet, server.URL+"/api/aggregates?snapshot_id=det-sample-seed-242&tenant_id=tenant-b", nil)
	if aggregate.status != http.StatusOK {
		t.Fatalf("aggregate status=%d body=%s", aggregate.status, aggregate.body)
	}
	var aggregateResult store.AggregateQueryResult
	if err := json.Unmarshal([]byte(aggregate.body), &aggregateResult); err != nil {
		t.Fatal(err)
	}
	if aggregateResult.Version <= 0 || len(aggregateResult.Windows) == 0 {
		t.Fatalf("empty aggregate result: %+v", aggregateResult)
	}
	stale := request(t, http.MethodGet, server.URL+"/api/aggregates?snapshot_id=det-sample-seed-242&tenant_id=tenant-b&version=0", nil)
	if stale.status != http.StatusConflict {
		t.Fatalf("stale status=%d body=%s", stale.status, stale.body)
	}
	var staleError errorResponse
	_ = json.Unmarshal([]byte(stale.body), &staleError)
	if staleError.Error.Code != domain.CodeStaleVersion {
		t.Fatalf("stale code=%s", staleError.Error.Code)
	}

	schedule := request(t, http.MethodGet, server.URL+"/api/schedule?round_id="+roundResult.Round.RoundID, nil)
	if schedule.status != http.StatusOK || !strings.Contains(schedule.body, "BUDGET_CUTOFF") {
		t.Fatalf("schedule status=%d body=%s", schedule.status, schedule.body)
	}
	recovery := request(t, http.MethodGet, server.URL+"/api/recovery", nil)
	if recovery.status != http.StatusOK || !strings.Contains(recovery.body, "based_on_durable_facts") {
		t.Fatalf("recovery status=%d body=%s", recovery.status, recovery.body)
	}

	invalidSnapshot := []byte(`{"snapshot_id":"bad","tenants":[{"id":"x","priority":1},{"id":"x","priority":2}],"window_specs":[{"id":"w","type":"tumbling","width_ms":1000}]}`)
	invalid := request(t, http.MethodPost, server.URL+"/api/snapshots", invalidSnapshot)
	if invalid.status != http.StatusUnprocessableEntity || !strings.Contains(invalid.body, "E_SNAPSHOT_INVALID") {
		t.Fatalf("invalid snapshot status=%d body=%s", invalid.status, invalid.body)
	}

	queueFull := request(t, http.MethodPost, server.URL+"/api/recalc", []byte(`{
	  "snapshot_id":"det-sample-seed-242","force":true,
	  "budgets":{"global_concurrency":1,"tenant_concurrency":1,"time_budget_ms":1000,"queue_capacity":0}
	}`))
	if queueFull.status != http.StatusTooManyRequests || !strings.Contains(queueFull.body, "E_RECALC_QUEUE_FULL") {
		t.Fatalf("queue full status=%d body=%s", queueFull.status, queueFull.body)
	}

	snapshots, err := database.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("invalid load changed snapshots: %+v", snapshots)
	}
	ui := request(t, http.MethodGet, server.URL+"/", nil)
	if ui.status != http.StatusOK || !strings.Contains(ui.body, "Window Commit Lab") {
		t.Fatalf("embedded UI unavailable: %d %s", ui.status, ui.body)
	}
	unknown := request(t, http.MethodGet, server.URL+"/api/does-not-exist", nil)
	if unknown.status != http.StatusNotFound || !strings.Contains(unknown.body, `"code": "E_NOT_FOUND"`) {
		t.Fatalf("unknown route did not use JSON error envelope: %d %s", unknown.status, unknown.body)
	}
}

func TestHTTPNumericOverflowRollsBack(t *testing.T) {
	server, database := newHTTPServer(t)
	defer server.Close()
	overflowSnapshot := []byte(`{
	  "snapshot_id":"overflow",
	  "tenants":[{"id":"t","priority":1}],
	  "window_specs":[{"id":"w","type":"tumbling","width_ms":1000}],
	  "events":[
	    {"event_id":"a","tenant_id":"t","group_key":"g","timestamp_ms":1,"seq":1,"measure":"9223372036854.775807"},
	    {"event_id":"b","tenant_id":"t","group_key":"g","timestamp_ms":2,"seq":2,"measure":"0.000001"}
	  ]
	}`)
	if loaded := request(t, http.MethodPost, server.URL+"/api/snapshots", overflowSnapshot); loaded.status != http.StatusCreated {
		t.Fatalf("load overflow snapshot: %d %s", loaded.status, loaded.body)
	}
	response := request(t, http.MethodPost, server.URL+"/api/recalc", []byte(`{
	  "snapshot_id":"overflow",
	  "budgets":{"global_concurrency":1,"tenant_concurrency":1,"time_budget_ms":100,"queue_capacity":10}
	}`))
	if response.status != http.StatusUnprocessableEntity || !strings.Contains(response.body, "E_NUMERIC_OVERFLOW") {
		t.Fatalf("overflow status=%d body=%s", response.status, response.body)
	}
	var errorBody errorResponse
	_ = json.Unmarshal([]byte(response.body), &errorBody)
	errorContext := errorBody.Error.Context
	if errorContext["tenant_id"] != "t" || errorContext["group_key"] != "g" || errorContext["window_id"] == "" {
		t.Fatalf("overflow context incomplete: %+v", errorContext)
	}
	state, stateErr := database.TenantStateVersioned(context.Background(), "overflow", "t", nil)
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	if state.Version != 0 || state.TotalCount != 0 {
		t.Fatalf("overflow changed tenant state: %+v", state)
	}
	pending, err := database.ListIntents(context.Background(), "PENDING")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("overflow left pending intents: %+v", pending)
	}
}

type httpResponse struct {
	status int
	body   string
}

type errorResponse struct {
	Error *domain.Error `json:"error"`
}

func newHTTPServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	database, err := store.Open(filepath.Join(t.TempDir(), "http.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	appEngine := engine.New(database, 8)
	report, err := appEngine.Recover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(httpapi.New(appEngine, database, report.ReportID))
	return server, database
}

func request(t *testing.T, method, url string, body []byte) httpResponse {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return httpResponse{status: response.StatusCode, body: string(data)}
}

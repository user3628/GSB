package grayrouter_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	grayrouter "grayrouter"
)

func TestGraphValidationSignatureAndDiff(t *testing.T) {
	bundle := grayrouter.SampleBundleFixed()
	if report := grayrouter.ValidateGraph(bundle.Baseline.Models[0].Graph); !report.Valid {
		t.Fatalf("sample graph should be valid: %+v", report.Errors)
	}
	_, err := grayrouter.ParseGraph([]byte(`{"nodes":[{"id":"","op":"bad","quant_bits":7,"outputs":[{"name":"x","shape":[0,-1],"dtype":"wat"}]}]}`))
	var apiErr *grayrouter.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != grayrouter.CodeGraphInvalid {
		t.Fatalf("expected E_GRAPH_INVALID, got %v", err)
	}
	contextJSON := string(grayrouter.CanonicalBytes(apiErr.Context))
	if !strings.Contains(contextJSON, "shape[0]") || !strings.Contains(contextJSON, "dtype") {
		t.Fatalf("error context should contain field details: %s", contextJSON)
	}

	for _, changed := range bundle.Delta.Models {
		var old grayrouter.ModelVersion
		for _, candidate := range bundle.Baseline.Models {
			if candidate.Name == changed.Name {
				old = candidate
			}
		}
		s1 := grayrouter.GraphSignature(changed.Graph)
		s2 := grayrouter.GraphSignature(changed.Graph)
		if s1 != s2 {
			t.Fatalf("signature not stable for %s", changed.Key())
		}
		diff := grayrouter.DiffModelVersions(old, changed)
		if !diff.Changed || len(diff.Categories) == 0 {
			t.Fatalf("expected diff categories for %s", changed.Key())
		}
		plan := grayrouter.PlanMigration(&old, changed, diff)
		if len(plan.Decisions) != 4 || len(plan.Rebuild) == 0 {
			t.Fatalf("bad migration plan: %+v", plan)
		}
	}
}

func TestGrayNormalizationAndStableDecision(t *testing.T) {
	norm, err := grayrouter.NormalizeWeights(map[string]float64{"base": 0.7, "canary": 0.3})
	if err != nil {
		t.Fatal(err)
	}
	if norm.TotalFixed != grayrouter.FixedScale {
		t.Fatalf("fixed total = %d", norm.TotalFixed)
	}
	req := grayrouter.GrayDecisionRequest{RequestID: "req-test", RouteKey: "r", SnapshotVersion: 9, Baseline: "a@1", Canary: "a@2", CanaryRate: 0.25}
	d1, err := grayrouter.DecideGray(req)
	if err != nil {
		t.Fatal(err)
	}
	d2, err := grayrouter.DecideGray(req)
	if err != nil {
		t.Fatal(err)
	}
	if string(grayrouter.CanonicalBytes(d1)) != string(grayrouter.CanonicalBytes(d2)) {
		t.Fatal("gray decisions differ")
	}
	if d1.Bucket >= grayrouter.FixedScale || d1.Selected == "" {
		t.Fatalf("bad decision: %+v", d1)
	}

	cases := []map[string]float64{
		{"only": 1},
		{"zero": 0, "only": 3},
		{"a": 0.1, "b": 0.2, "c": 0.3, "d": 0.4},
		{"a": 0.0001, "b": 0.0002, "c": 0.9997},
	}
	for _, weights := range cases {
		normalized, err := grayrouter.NormalizeWeights(weights)
		if err != nil {
			t.Fatalf("normalize %v: %v", weights, err)
		}
		if normalized.TotalFixed != grayrouter.FixedScale {
			t.Fatalf("fixed total for %v = %d", weights, normalized.TotalFixed)
		}
	}
}

func TestCommitInferPinIdempotencyAndQueries(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	host, err := grayrouter.Open(ctx, grayrouter.Config{DBPath: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	bundle := grayrouter.SampleBundleFixed()

	invalid := grayrouter.Snapshot{Version: 99, Models: []grayrouter.ModelVersion{bundle.Invalid}}
	if _, err := host.SubmitSnapshot(ctx, grayrouter.CommitSnapshotInput{RequestID: "invalid", Snapshot: invalid}); err == nil {
		t.Fatal("expected invalid snapshot error")
	}

	baseline, err := host.SubmitSnapshot(ctx, grayrouter.CommitSnapshotInput{RequestID: "baseline", Snapshot: bundle.Baseline})
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := host.BindInference(ctx, grayrouter.InferRequest{RequestID: "pin-1", ModelKey: "demo-op@1.0.0"})
	if err != nil {
		t.Fatal(err)
	}
	delta, err := host.SubmitDelta(ctx, grayrouter.CommitDeltaInput{RequestID: "delta", Delta: bundle.Delta})
	if err != nil {
		t.Fatal(err)
	}
	if baseline.Version != 1 || delta.Version != 2 || len(delta.Plans) != 5 {
		t.Fatalf("bad versions/plans: %+v %+v", baseline, delta)
	}
	completed, err := host.CompleteBound(ctx, pinned.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.ModelKey != "demo-op@1.0.0" || completed.Result == nil {
		t.Fatalf("pin did not complete old version: %+v", completed)
	}

	r1, err := host.Infer(ctx, grayrouter.InferRequest{RequestID: "i1", ModelKey: "demo-quant@1.1.0"})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := host.Infer(ctx, grayrouter.InferRequest{RequestID: "i2", ModelKey: "demo-quant@1.1.0"})
	if err != nil {
		t.Fatal(err)
	}
	if string(grayrouter.CanonicalBytes(r1.Result)) != string(grayrouter.CanonicalBytes(r2.Result)) {
		t.Fatal("inference results differ")
	}

	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := host.Infer(ctx, grayrouter.InferRequest{RequestID: "same-request", ModelKey: "demo-stable@1.0.0"}); err != nil {
				errs <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	state, err := host.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.HeadVersion != 2 || state.Counts["inflight_bindings"] < 3 {
		t.Fatalf("bad state: %+v", state.Counts)
	}
	report, err := host.CommitReport(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Rebuild) == 0 || len(report.Migrate) == 0 {
		t.Fatal("expected migration actions")
	}

	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := grayrouter.Open(ctx, grayrouter.Config{DBPath: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovery := reopened.LatestRecoveryReport()
	if recovery == nil || !recovery.Consistent || recovery.LastCompleteSnapshot != 2 {
		t.Fatalf("bad recovery after clean restart: %+v", recovery)
	}
}

func TestHTTPEnvelopeAndEndpoints(t *testing.T) {
	ctx := context.Background()
	host, err := grayrouter.Open(ctx, grayrouter.Config{DBPath: filepath.Join(t.TempDir(), "http.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.SubmitSnapshot(ctx, grayrouter.CommitSnapshotInput{RequestID: "baseline", Snapshot: grayrouter.SampleBundleFixed().Baseline}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(grayrouter.NewHTTPServer(host, t.TempDir()).Handler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/api/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status %d", resp.StatusCode)
	}

	inferBody := `{"request_id":"http-infer","model_key":"demo-stable@1.0.0"}`
	resp, err = http.Post(server.URL+"/api/infer", "application/json", strings.NewReader(inferBody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var envelope struct {
		OK   bool `json:"ok"`
		Data struct {
			Result json.RawMessage `json:"result"`
		} `json:"data"`
		Error *grayrouter.APIError `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if !envelope.OK || len(envelope.Data.Result) == 0 {
		t.Fatalf("bad infer envelope: %+v", envelope.Error)
	}

	resp, err = http.Post(server.URL+"/api/graphs/validate", "application/json", strings.NewReader(`{"graph":{"nodes":[]}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", resp.StatusCode)
	}
	var invalidEnvelope struct {
		OK    bool                `json:"ok"`
		Error grayrouter.APIError `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&invalidEnvelope); err != nil {
		t.Fatal(err)
	}
	if invalidEnvelope.Error.Code != grayrouter.CodeGraphInvalid {
		t.Fatalf("bad error code: %+v", invalidEnvelope.Error)
	}
}

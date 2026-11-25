package querysvc_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"ocrlab/internal/apperr"
	"ocrlab/internal/pipeline"
	"ocrlab/internal/querysvc"
)

func TestCacheHitAndForcedRecomputeHaveIdenticalBytes(t *testing.T) {
	ctx := context.Background()
	p, err := pipeline.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Run(ctx, pipeline.RunOptions{Groups: []string{"small"}}); err != nil {
		t.Fatal(err)
	}
	service := querysvc.New(p)
	first, err := service.Execute(ctx, querysvc.Request{Kind: querysvc.KindBoxes, ImageID: "small-001"})
	if err != nil {
		t.Fatal(err)
	}
	if first.CacheHit {
		t.Fatal("first query unexpectedly hit an empty cache")
	}
	assertKeyParts(t, first)
	warm, err := service.Execute(ctx, querysvc.Request{Kind: querysvc.KindBoxes, ImageID: "small-001"})
	if err != nil {
		t.Fatal(err)
	}
	if !warm.CacheHit {
		t.Fatal("second query did not hit the cache")
	}
	if string(first.Result) != string(warm.Result) || first.ResultSHA256 != warm.ResultSHA256 || first.CacheKey != warm.CacheKey {
		t.Fatal("cache hit and initial result bytes are not identical")
	}
	forced, err := service.Execute(ctx, querysvc.Request{Kind: querysvc.KindBoxes, ImageID: "small-001", ForceRecompute: true})
	if err != nil {
		t.Fatal(err)
	}
	if forced.CacheHit {
		t.Fatal("forced recompute reported a cache hit")
	}
	if string(forced.Result) != string(first.Result) || forced.ResultSHA256 != first.ResultSHA256 {
		t.Fatal("recomputed result differs byte-for-byte from the cache hit")
	}

	recognition, err := service.Execute(ctx, querysvc.Request{Kind: querysvc.KindRecognition, ImageID: "small-001"})
	if err != nil {
		t.Fatal(err)
	}
	image, err := service.Execute(ctx, querysvc.Request{Kind: querysvc.KindImage, ImageID: "small-001"})
	if err != nil {
		t.Fatal(err)
	}
	var combined struct {
		Detection   json.RawMessage `json:"detection"`
		Recognition json.RawMessage `json:"recognition"`
	}
	if err := json.Unmarshal(image.Result, &combined); err != nil {
		t.Fatal(err)
	}
	if string(combined.Detection) != string(first.Result) {
		t.Fatal("detection boxes differ between boxes and image query paths")
	}
	if string(combined.Recognition) != string(recognition.Result) {
		t.Fatal("recognition details differ between recognition and image query paths")
	}
}

func TestLatencyBudgetReturnsStructuredError(t *testing.T) {
	ctx := context.Background()
	p, err := pipeline.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Run(ctx, pipeline.RunOptions{Groups: []string{"small"}}); err != nil {
		t.Fatal(err)
	}
	service := querysvc.New(p)
	budget := 0
	response, err := service.Execute(ctx, querysvc.Request{Kind: querysvc.KindDiagnostics, Group: "small", BudgetMS: &budget, ForceRecompute: true})
	appErr := apperr.From(err)
	if appErr.Code != apperr.CodeLatencyBudget {
		t.Fatalf("expected E_LATENCY_BUDGET, got %+v", appErr)
	}
	if appErr.QueryID == "" || appErr.ElapsedMS < 0 || response.BudgetMS != 0 {
		t.Fatalf("latency error did not include query id/elapsed/budget: %+v %+v", appErr, response)
	}
}

func TestDiagnosticMetricsAreRecomputable(t *testing.T) {
	ctx := context.Background()
	p, err := pipeline.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Run(ctx, pipeline.RunOptions{Groups: []string{"small"}}); err != nil {
		t.Fatal(err)
	}
	service := querysvc.New(p)
	first, err := service.Execute(ctx, querysvc.Request{Kind: querysvc.KindDiagnostics, Group: "small"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Execute(ctx, querysvc.Request{Kind: querysvc.KindDiagnostics, Group: "small", ForceRecompute: true})
	if err != nil {
		t.Fatal(err)
	}
	if first.ResultSHA256 != second.ResultSHA256 || string(first.Result) != string(second.Result) {
		t.Fatal("diagnostic metrics differ between cached and recomputed query paths")
	}
	var report map[string]any
	if err := json.Unmarshal(first.Result, &report); err != nil {
		t.Fatal(err)
	}
	groups, ok := report["groups"].([]any)
	if !ok || len(groups) != 1 {
		t.Fatalf("expected one group in scoped diagnostics: %s", first.Result)
	}
	group := groups[0].(map[string]any)
	if !reflect.DeepEqual(group["detection_recall"].(map[string]any)["value"], float64(1)) {
		t.Fatalf("unexpected recall in diagnostics: %v", group["detection_recall"])
	}
}

func assertKeyParts(t *testing.T, response querysvc.Response) {
	t.Helper()
	if len(response.KeyParts.ContentSHA256) != 64 || response.KeyParts.ConfigVersion == "" || response.KeyParts.StageVersion == "" || response.KeyParts.ArgumentDigest == "" {
		t.Fatalf("cache key does not include all required version dimensions: %+v", response.KeyParts)
	}
}

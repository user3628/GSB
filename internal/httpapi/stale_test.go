package httpapi

import (
	"context"
	"net/http"
	"testing"

	"configtree/internal/engine"
	"configtree/internal/seed"
	"configtree/internal/spec"
)

func TestHTTPStaleReadDuringUnfinishedRecompute(t *testing.T) {
	eng, err := engine.Open(t.TempDir() + "/stale.db")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if _, err := eng.Commit(context.Background(), &engine.CommitRequest{
		CommitID: "s1", Kind: "snapshot", Snapshot: seed.V1(),
	}); err != nil {
		t.Fatal(err)
	}
	eng.SetTestSkipExit(true)
	eng.SetCrashHook(func(phase string) bool { return phase == "after_facts_before_derived" })
	if _, err := eng.Commit(context.Background(), &engine.CommitRequest{
		CommitID: "d2", Kind: "delta", Delta: seed.DeltaV2(),
	}); err == nil {
		t.Fatal("expected simulated hard kill")
	}

	h := New(eng).Handler()
	code, body := do(t, h, http.MethodGet,
		"/v1/components/worker/view?observed_version=2", nil)
	if code != http.StatusConflict {
		t.Fatalf("status=%d body=%v", code, body)
	}
	if body["code"] != spec.CodeStaleRead {
		t.Fatalf("code=%v", body["code"])
	}
	detail, ok := body["context"].(map[string]any)["detail"].(map[string]any)
	if !ok {
		t.Fatalf("missing stale detail: %v", body["context"])
	}
	if detail["view_version"].(float64) != 1 ||
		detail["observed_version"].(float64) != 2 ||
		len(detail["pending_keys"].([]any)) == 0 {
		t.Fatalf("stale detail incomplete: %v", detail)
	}
}

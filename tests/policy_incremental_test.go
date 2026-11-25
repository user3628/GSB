package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"voiceline/internal/httpapi"
	"voiceline/internal/model"
	"voiceline/internal/pipeline"
	"voiceline/internal/policy"
)

func intPtr(v int) *int           { return &v }
func int64Ptr(v int64) *int64     { return &v }
func floatPtr(v float64) *float64 { return &v }

func TestPolicyStoreStaleBaseAndSnapshotClassification(t *testing.T) {
	store := policy.NewStore(policy.Strategy{LineageID: "local", Version: 1})
	_, _, err := store.ApplyPatch(policy.Patch{BaseVersion: 2, F0Shift: floatPtr(1.2)})
	var business *model.BusinessError
	if !errors.As(err, &business) || business.Code != model.CodeStaleBase || business.CurrentVersion != 1 {
		t.Fatalf("expected E_STALE_BASE current=1, got %v", err)
	}

	next, inv, err := store.ApplyPatch(policy.Patch{BaseVersion: 1, F0Shift: floatPtr(1.2)})
	if err != nil {
		t.Fatal(err)
	}
	if next.Version != 2 || inv.EarliestStage != policy.StageMapping || inv.EarliestBlock != 0 {
		t.Fatalf("bad patch invalidation: version=%d inv=%+v", next.Version, inv)
	}

	rollback := policy.ClassifySnapshot(store.Current(), policy.Strategy{LineageID: "local", Version: 1})
	if rollback != policy.DecisionRollback {
		t.Fatalf("classification = %s, want rollback", rollback)
	}
	replacement, err := store.ApplySnapshot(policy.Strategy{LineageID: "other-generation", Version: 9}, false)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Invalidation.Decision != policy.DecisionFullReplacement || !replacement.Invalidation.RebuildAllBlocks {
		t.Fatalf("full replacement did not invalidate all: %+v", replacement.Invalidation)
	}
}

func TestHTTPStaleBase(t *testing.T) {
	server := httptest.NewServer(httpapi.NewServer(policy.NewStore(policy.Strategy{Version: 1, LineageID: "local"})).Handler())
	defer server.Close()

	body := []byte(`{"base_version":1,"envelope_warp":1.13}`)
	resp, err := http.Post(server.URL+"/v1/policy/patches", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first patch status = %d", resp.StatusCode)
	}

	resp, err = http.Post(server.URL+"/v1/policy/patches", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("stale patch status = %d, want 409", resp.StatusCode)
	}
	var decoded struct {
		Error model.BusinessError `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Error.Code != model.CodeStaleBase || decoded.Error.CurrentVersion != 2 {
		t.Fatalf("bad stale error: %+v", decoded.Error)
	}
}

func TestIncrementalRebuildReuse(t *testing.T) {
	input := generatedInput(t)
	ctx := context.Background()
	initial := policy.Strategy{
		LineageID:        "inc",
		Version:          1,
		MaxGroups:        intPtr(2),
		FramesPerGroup:   intPtr(8),
		RoundBudgetNanos: int64Ptr(int64(time.Hour)),
	}
	engine := pipeline.NewIncrementalEngine(input, initial, pipeline.ConvertOptions{PolicyVersion: 1})
	first, firstStats, err := engine.Rebuild(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstStats.RebuiltBlocks) != 12 {
		t.Fatalf("initial rebuilt blocks = %v, want 0..11", firstStats.RebuiltBlocks)
	}

	_, inv, err := engine.ApplyPatch(policy.Patch{BaseVersion: 1, RoundBudgetNanos: int64Ptr(30 * time.Minute.Nanoseconds())})
	if err != nil {
		t.Fatal(err)
	}
	second, stats, err := engine.Rebuild(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if inv.EarliestStage != policy.StageDispatch || len(stats.RebuiltBlocks) != 0 || len(stats.ReusedBlocks) != 12 {
		t.Fatalf("budget change should only touch dispatch and reuse PCM: inv=%+v stats=%+v", inv, stats)
	}
	if string(second.WAVBytes) != string(first.WAVBytes) {
		t.Fatal("budget-only change altered waveform")
	}

	if _, _, err := engine.ApplyPatch(policy.Patch{BaseVersion: 1, F0Shift: floatPtr(1.18)}); err == nil {
		t.Fatal("expected stale base version error")
	}
	_, _, err = engine.ApplyPatch(policy.Patch{BaseVersion: 2, F0Shift: floatPtr(1.18)})
	if err != nil {
		t.Fatal(err)
	}
	third, stats, err := engine.Rebuild(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.EarliestStage != policy.StageMapping || len(stats.ReusedBlocks) != 0 || len(stats.RebuiltBlocks) != 12 {
		t.Fatalf("global f0 change should rebuild all blocks: stats=%+v", stats)
	}
	if string(third.WAVBytes) == string(first.WAVBytes) {
		t.Fatal("f0 change did not affect waveform")
	}
}

func TestBlockFingerprintChainFindsEarliestLocalBlock(t *testing.T) {
	base := make([]policy.BlockFingerprint, 5)
	for i := range base {
		base[i] = policy.BlockFingerprint{
			AnalysisDigest: "a", MappingDigest: "m", DispatchDigest: "d", RenderDigest: "r",
		}
	}
	old := policy.ChainBlockFingerprints(base)
	changed := make([]policy.BlockFingerprint, len(base))
	copy(changed, base)
	changed[3].MappingDigest = "m-changed"
	newBlocks := policy.ChainBlockFingerprints(changed)
	stage, block := policy.EarliestMismatchedBlock(old, newBlocks)
	if stage != policy.StageMapping || block != 3 {
		t.Fatalf("earliest mismatch = %s/%d, want mapping/3", stage, block)
	}
}

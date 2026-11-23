package store

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"vpnbench/internal/model"
)

func openTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	return database, path
}

func policySnapshot(content string, identities ...string) SnapshotInput {
	return SnapshotInput{Kind: "policy", Content: []byte(content), IdentityKeys: identities}
}

func TestSnapshotVersionsRetainHistoryAndClassifyReplacement(t *testing.T) {
	database, _ := openTestStore(t)
	ctx := context.Background()
	v1, err := database.SavePolicySnapshot(ctx, policySnapshot(`{"v":1}`, "a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	if v1.Version != 1 || v1.ChangeType != "INITIAL" {
		t.Fatalf("v1 = %+v", v1)
	}
	v2, err := database.SavePolicySnapshot(ctx, policySnapshot(`{"v":1}`, "a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	if v2.Version != 2 || v2.ChangeType != "SAME_CONTENT" || v2.WholeReplacement {
		t.Fatalf("v2 = %+v", v2)
	}
	v3, err := database.SavePolicySnapshot(ctx, policySnapshot(`{"v":2}`, "c", "d"))
	if err != nil {
		t.Fatal(err)
	}
	if v3.Version != 3 || v3.ChangeType != "WHOLE_REPLACEMENT" || !v3.WholeReplacement {
		t.Fatalf("v3 = %+v", v3)
	}
	if strings.Join(v3.BeforeSummary.IdentityKeys, ",") != "a,b" || strings.Join(v3.AfterSummary.IdentityKeys, ",") != "c,d" {
		t.Fatalf("replacement summaries = %+v -> %+v", v3.BeforeSummary, v3.AfterSummary)
	}
	chain, err := database.Chain(ctx, "policy")
	if err != nil {
		t.Fatal(err)
	}
	if len(chain.Snapshots) != 3 || chain.LatestVersion != 3 {
		t.Fatalf("old snapshots not retained: %+v", chain)
	}
	if _, err := database.LatestContiguous(ctx, "policy"); err != nil {
		t.Fatalf("contiguous chain error: %v", err)
	}
}

func TestSnapshotGapFallsBackToLatestContiguousContent(t *testing.T) {
	database, _ := openTestStore(t)
	ctx := context.Background()
	v1, err := database.SavePolicySnapshot(ctx, policySnapshot(`{"v":1}`, "a"))
	if err != nil {
		t.Fatal(err)
	}
	gapVersion, err := database.InjectSnapshotGap(ctx, "policy", []byte(`{"injected":true}`))
	if err != nil {
		t.Fatal(err)
	}
	chain, err := database.LatestContiguous(ctx, "policy")
	if model.CodeOf(err) != model.CodeSnapshotVersion {
		t.Fatalf("code = %s, err=%v", model.CodeOf(err), err)
	}
	if chain.Fallback == nil || chain.Fallback.Version != v1.Version || chain.Fallback.ContentHash != v1.ContentHash {
		t.Fatalf("fallback = %+v, want v1 %+v", chain.Fallback, v1)
	}
	if len(chain.MissingVersions) != 1 || chain.MissingVersions[0] != v1.Version+1 || chain.LatestVersion != gapVersion {
		t.Fatalf("chain = %+v", chain)
	}
}

func TestSettlementSnapshotIsIdempotentAndCorrectionsQueryable(t *testing.T) {
	database, _ := openTestStore(t)
	ctx := context.Background()
	correction := model.Correction{
		SessionID:  "s1",
		StreamID:   "flow",
		RequestSeq: 1,
		Code:       model.CorrectionOrder,
		Reason:     "test",
		Before:     model.Decision{Code: model.CodeOK},
		After:      model.Decision{Code: model.CodeOK},
	}
	input := SettlementInput{
		SessionID:     "s1",
		WindowID:      "w1",
		Status:        "FINALIZED",
		PolicyVersion: 1,
		OutcomeJSON:   []byte(`{"outcome":1}`),
		Results:       []WindowResult{{StreamID: "flow", RequestSeq: 1, Code: model.CodeOK, ResultJSON: []byte(`{}`)}},
		Corrections:   []model.Correction{correction},
		Ledger:        []model.LedgerEntry{{Sequence: 0, SessionID: "s1", StreamID: "flow", RequestSeq: 1, TunnelID: "t"}},
	}
	first, found, err := database.SaveSettlementSnapshot(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if found || first.Version != 1 {
		t.Fatalf("first settlement = %+v found=%t", first, found)
	}
	second, found, err := database.SaveSettlementSnapshot(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if !found || second.Version != 1 {
		t.Fatalf("idempotent settlement = %+v found=%t", second, found)
	}
	chain, err := database.Chain(ctx, "settlement")
	if err != nil {
		t.Fatal(err)
	}
	if len(chain.Snapshots) != 1 {
		t.Fatalf("settlement chain = %+v", chain)
	}
	corrections, err := database.QueryCorrections(ctx, "s1", "flow", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(corrections) != 1 || corrections[0].Code != model.CorrectionOrder {
		t.Fatalf("corrections = %+v", corrections)
	}
	conflicting := input
	conflicting.OutcomeJSON = []byte(`{"outcome":2}`)
	if _, _, err := database.SaveSettlementSnapshot(ctx, conflicting); model.CodeOf(err) != model.CodeResultMismatch {
		t.Fatalf("conflicting code = %s, err=%v", model.CodeOf(err), err)
	}
}

func TestForcedTerminationRollsBackPolicySnapshot(t *testing.T) {
	database, path := openTestStore(t)
	ctx := context.Background()
	seed, err := database.SavePolicySnapshot(ctx, policySnapshot(`{"seed":true}`, "seed"))
	if err != nil {
		t.Fatal(err)
	}
	database.Close()
	exit := runStoreCrashHelper(t, "policy", path, "policy_after_write")
	if exit != 86 {
		t.Fatalf("policy helper exit = %d", exit)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	chain, err := reopened.Chain(ctx, "policy")
	if err != nil {
		t.Fatal(err)
	}
	if len(chain.Snapshots) != 1 || chain.Snapshots[0].Version != seed.Version {
		t.Fatalf("uncommitted policy snapshot survived: %+v", chain)
	}
	next, err := reopened.SavePolicySnapshot(ctx, policySnapshot(`{"after":true}`, "after"))
	if err != nil {
		t.Fatal(err)
	}
	if next.Version != 2 {
		t.Fatalf("next version = %d", next.Version)
	}
	reopened.Close()
}

func TestForcedTerminationRollsBackSettlementWindow(t *testing.T) {
	database, path := openTestStore(t)
	ctx := context.Background()
	database.Close()
	exit := runStoreCrashHelper(t, "settlement", path, "settlement_after_write")
	if exit != 86 {
		t.Fatalf("settlement helper exit = %d", exit)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	settlementChain, err := reopened.Chain(ctx, "settlement")
	if err != nil {
		t.Fatal(err)
	}
	if len(settlementChain.Snapshots) != 0 {
		t.Fatalf("uncommitted settlement snapshot survived: %+v", settlementChain)
	}
	windows, err := reopened.Windows(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(windows) != 0 {
		t.Fatalf("uncommitted window survived: %+v", windows)
	}
	snapshot, found, err := reopened.SaveSettlementSnapshot(ctx, SettlementInput{
		SessionID:     "s1",
		WindowID:      "w-retry",
		Status:        "FINALIZED",
		PolicyVersion: 1,
		OutcomeJSON:   []byte(`{"retry":true}`),
		Results:       []WindowResult{{StreamID: "flow", RequestSeq: 1, Code: model.CodeOK, ResultJSON: []byte(`{}`)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if found || snapshot.Version != 1 {
		t.Fatalf("retry = %+v found=%t", snapshot, found)
	}
	reopened.Close()
}

func TestStoreCrashHelper(t *testing.T) {
	mode := os.Getenv("VPNBENCH_STORE_HELPER")
	if mode == "" {
		t.Skip("helper process only")
	}
	path := os.Getenv("VPNBENCH_HELPER_DB")
	database, err := Open(path)
	if err != nil {
		t.Fatalf("open helper db: %v", err)
	}
	switch mode {
	case "policy":
		_, _ = database.SavePolicySnapshot(context.Background(), policySnapshot(`{"child":true}`, "child"))
	case "settlement":
		_, _, _ = database.SaveSettlementSnapshot(context.Background(), SettlementInput{
			SessionID:     "child",
			WindowID:      "child-window",
			Status:        "FINALIZED",
			PolicyVersion: 1,
			OutcomeJSON:   []byte(`{"child":true}`),
			Results:       []WindowResult{{StreamID: "flow", RequestSeq: 1, Code: model.CodeOK, ResultJSON: []byte(`{}`)}},
		})
	default:
		t.Fatalf("unknown helper mode %s", mode)
	}
	t.Fatalf("crash point did not terminate the helper process")
}

func runStoreCrashHelper(t *testing.T, mode, dbPath, crashPoint string) int {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	command := exec.Command(executable, "-test.run=^TestStoreCrashHelper$")
	env := make([]string, 0, len(os.Environ())+3)
	for _, value := range os.Environ() {
		if strings.HasPrefix(value, "VPNBENCH_STORE_HELPER=") ||
			strings.HasPrefix(value, "VPNBENCH_HELPER_DB=") ||
			strings.HasPrefix(value, "VPNBENCH_CRASH_POINT=") {
			continue
		}
		env = append(env, value)
	}
	env = append(env,
		"VPNBENCH_STORE_HELPER="+mode,
		"VPNBENCH_HELPER_DB="+dbPath,
		"VPNBENCH_CRASH_POINT="+crashPoint,
	)
	command.Env = env
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("helper exited normally; output=%s", output)
	}
	exitError, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("helper failed: %v output=%s", err, output)
	}
	return exitError.ExitCode()
}

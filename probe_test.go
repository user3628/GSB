package ledger_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	. "ledgerworkbench"
	"ledgerworkbench/probetarget"
)

func newSingleSlotWorkbench(t *testing.T) *Workbench {
	t.Helper()
	wb, err := NewWorkbench(Config{
		DBPath: filepath.Join(t.TempDir(), "probe.db"), ProbeConcurrency: 1, ProbeTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wb.Close() })
	return wb
}

func TestRealProbeBehaviorsAndTimeoutSlotRelease(t *testing.T) {
	wb := newSingleSlotWorkbench(t)
	target := probetarget.NewTestServer(t)
	ctx := context.Background()

	ok := wb.PerformProbe(ctx, ProbeRequest{
		BatchID: "probe-ok", RouteKey: "route-probe", BaseURL: target.URL,
		ExpectedOwner: "owner", TimeoutMS: 1000,
	})
	if !ok.OK || ok.Code != ProbeOutcomeOK || !ok.SlotReleased {
		t.Fatalf("ok probe failed: %+v", ok)
	}

	mismatch := wb.PerformProbe(ctx, ProbeRequest{
		BatchID: "probe-mismatch", RouteKey: "route-probe", BaseURL: target.URL,
		ExpectedOwner: "owner", TimeoutMS: 1000,
		Injection: &ProbeInjection{Mode: ProbeModeMismatch},
	})
	if mismatch.Code != CodeProbeMismatch {
		t.Fatalf("expected mismatch, got %+v", mismatch)
	}

	late := wb.PerformProbe(ctx, ProbeRequest{
		BatchID: "probe-late", RouteKey: "route-probe", BaseURL: target.URL,
		ExpectedOwner: "owner", TimeoutMS: 1000, LateThresholdMS: 30,
		Injection: &ProbeInjection{Mode: ProbeModeLate, DelayMS: 100},
	})
	if late.Code != CodeProbeLate {
		t.Fatalf("expected late response code, got %+v", late)
	}

	refusedEndpoint := probetarget.RefusedEndpoint(t)
	refused := wb.PerformProbe(ctx, ProbeRequest{
		BatchID: "probe-refused", RouteKey: "route-probe", Endpoint: refusedEndpoint, TimeoutMS: 500,
	})
	if refused.Code != CodeProbeRefused {
		t.Fatalf("expected refused, got %+v", refused)
	}

	timeoutBatch := "probe-timeout-slot"
	timeoutResult := wb.PerformProbe(ctx, ProbeRequest{
		BatchID: timeoutBatch, RouteKey: "route-probe", BaseURL: target.URL,
		ExpectedOwner: "owner", TimeoutMS: 40,
		Injection: &ProbeInjection{Mode: ProbeModeTimeout},
	})
	if timeoutResult.Code != CodeProbeTimeout || !timeoutResult.TimedOut ||
		!timeoutResult.ConnectionClosed || !timeoutResult.SlotReleased {
		t.Fatalf("timeout result wrong: %+v", timeoutResult)
	}
	if !wb.WaitProbeConnectionsClosed(timeoutBatch, "route-probe", 2*time.Second) {
		t.Fatal("timed out probe underlying connection was not observed closed")
	}

	after := wb.PerformProbe(ctx, ProbeRequest{
		BatchID: "probe-after-timeout", RouteKey: "route-probe", BaseURL: target.URL,
		ExpectedOwner: "owner", TimeoutMS: 1000,
	})
	if !after.OK || !after.SlotReleased {
		t.Fatalf("concurrency slot was not released after timeout: %+v", after)
	}
}

func TestProbeRoundIsIdempotentAndPersistsBatch(t *testing.T) {
	wb := newTestWorkbench(t)
	target := probetarget.NewTestServer(t)
	request := ProbeRoundRequest{
		CommitID: "probe-round-1", BatchID: "probe-round-1", RouteKeys: []string{"route-probe"},
		BaseURL: target.URL, TimeoutMS: 1000, ExpectedOwners: map[string]string{"route-probe": "owner"},
	}
	first, err := wb.SubmitProbeRound(context.Background(), request)
	if err != nil || first.OKCount != 1 {
		t.Fatalf("first round: result=%+v err=%v", first, err)
	}
	second, err := wb.SubmitProbeRound(context.Background(), request)
	if err != nil || !second.IdempotentReplay {
		t.Fatalf("idempotent round replay=%+v err=%v", second, err)
	}
	var status string
	if err := wb.DB().QueryRowContext(context.Background(),
		`SELECT status FROM probe_batches WHERE batch_id=?`, "probe-round-1").Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("batch status=%s", status)
	}
}

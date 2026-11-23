package store

import (
	"context"
	"path/filepath"
	"testing"

	"gatewayworkbench/internal/model"
)

func testSample() *model.Sample {
	return &model.Sample{
		Instances: []model.Instance{{
			ID: "i1", Port: 19001, Weight: 600, Zone: "z-a",
			Probe: model.ProbeSpec{Mode: "tcp", TimeoutMillis: 100,
				SuccessThreshold: 1, FailureThreshold: 1},
		}, {
			ID: "i2", Port: 19002, Weight: 400, Zone: "z-b",
			Probe: model.ProbeSpec{Mode: "tcp", TimeoutMillis: 100,
				SuccessThreshold: 1, FailureThreshold: 1},
		}},
		Trace: []model.RequestRecord{
			{TimestampMillis: 1, ClientID: "c1", RequestWeight: 1, RequestBytes: 10},
			{TimestampMillis: 2, ClientID: "c2", RequestWeight: 1, RequestBytes: 20},
		},
		Affinity: model.ZoneAffinity{Ratios: map[string]int{"z-a": 500, "z-b": 500}},
	}
}

func TestRecoverRollsBackOpenDispatchIntent(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "recovery.db")
	st, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.ReplaceSample(ctx, testSample()); err != nil {
		t.Fatal(err)
	}

	// 模拟一个已提交的完整快照边界 v1。
	tx, intentID, err := st.BeginIntent(ctx, "probe-1", "probe", nil)
	if err != nil {
		t.Fatal(err)
	}
	phases := map[string]model.Phase{"i1": model.PhaseHealthy, "i2": model.PhaseHealthy}
	if err := st.SetInstanceState(ctx, tx, InstanceStateRow{
		InstanceID: "i1", Phase: model.PhaseHealthy}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetInstanceState(ctx, tx, InstanceStateRow{
		InstanceID: "i2", Phase: model.PhaseHealthy}); err != nil {
		t.Fatal(err)
	}
	version, err := st.SaveSnapshot(ctx, tx, &model.SnapshotVersion{
		Reason: "committed boundary", InstanceState: phases,
		ZoneWeights: map[string]map[string]int{
			"z-a": {"i1": 1000}, "z-b": {"i2": 1000},
		},
		ZoneRatios: map[string]int{"z-a": 500, "z-b": 500},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CommitIntent(ctx, tx, intentID); err != nil {
		t.Fatal(err)
	}

	// 模拟调度计划第一阶段：OPEN 意图 + 轮次 + 派发 + 请求标记已提交，
	// 但第二阶段 CommitOpenIntent 尚未发生（等价于进程在此处被 SIGKILL）。
	planTx, planIntent, err := st.BeginIntent(ctx, "dispatch-kill", "dispatch", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRound(ctx, planTx, "dispatch-kill", "dispatch", "{}"); err != nil {
		t.Fatal(err)
	}
	dispatchID, err := st.InsertDispatch(ctx, planTx, "dispatch-kill", 0, "i1")
	if err != nil {
		t.Fatal(err)
	}
	decision := &model.Decision{RequestSeq: 0, ClientID: "c1", SelectedInstance: "i1"}
	if err := st.MarkRequestDispatched(ctx, planTx, 0, "dispatch-kill", decision); err != nil {
		t.Fatal(err)
	}
	if err := st.FinishRound(ctx, planTx, "dispatch-kill", "PLANNED"); err != nil {
		t.Fatal(err)
	}
	if err := planTx.Commit(); err != nil {
		t.Fatal(err)
	}
	if status, err := st.IntentStatus(ctx, planIntent); err != nil || status != "OPEN" {
		t.Fatalf("planned intent must remain OPEN, status=%s err=%v", status, err)
	}
	_ = dispatchID
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：恢复必须只依据落盘事实裁决。
	reopened, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	report, err := reopened.RecoverOnStartup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.OpenIntents) != 1 || report.OpenIntents[0].Kind != "dispatch" {
		t.Fatalf("expected one open dispatch intent, got %+v", report.OpenIntents)
	}
	if len(report.AbortedRounds) != 1 || report.AbortedRounds[0] != "dispatch-kill" {
		t.Fatalf("expected dispatch-kill aborted, got %+v", report.AbortedRounds)
	}
	if len(report.RolledBackRequests) != 1 || report.RolledBackRequests[0] != 0 {
		t.Fatalf("expected request seq 0 rollback, got %+v", report.RolledBackRequests)
	}
	if report.SnapshotKept != version {
		t.Fatalf("snapshot must remain at last committed boundary v%d, got v%d",
			version, report.SnapshotKept)
	}
	requestView, err := reopened.RequestDecision(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if requestView["status"] != "PENDING" || requestView["round_id"] != "" {
		t.Fatalf("request must be restored to PENDING without round, got %+v", requestView)
	}
	snapshot, err := reopened.LatestSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Version != version {
		t.Fatalf("weight view must stay at last committed version, got %d", snapshot.Version)
	}

	// 再次恢复应幂等：没有新的 OPEN 意图。
	second, err := reopened.RecoverOnStartup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.OpenIntents) != 0 || len(second.AbortedRounds) != 0 {
		t.Fatalf("second recovery must be idempotent, got %+v", second)
	}
}

func TestInvalidSampleRejectedBeforeReplace(t *testing.T) {
	ctx := context.Background()
	st, err := Open(filepath.Join(t.TempDir(), "invalid.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	good := testSample()
	if err := st.ReplaceSample(ctx, good); err != nil {
		t.Fatal(err)
	}
	bad := testSample()
	bad.Instances[0].Port = 0
	if err := bad.Validate(); err == nil {
		t.Fatal("expected E_INSTANCE_INVALID")
	}
	// 引擎层会先 Validate；这里直接确认旧实例仍可读取。
	instances, err := st.ListInstances(ctx)
	if err != nil || len(instances) != 2 {
		t.Fatalf("old sample must remain effective, got %d instances err=%v", len(instances), err)
	}
}

package convergence

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenRecoversPreparedWriteAndSnapshotIntent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recovery.db")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	service, _, err := OpenService(ctx, ServiceOptions{DBPath: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	commitSample(t, service)
	state := service.View().BatchStates["B001-S1"]
	if state == nil {
		t.Fatal("missing B001-S1 state")
	}
	if _, err := service.Store().CreateIntent(ctx, "batch_apply", batchScope(1, "B001-S1"), nil); err != nil {
		t.Fatal(err)
	}
	if err := service.Store().PrepareBatchRun(ctx, 1, state, state.Batch.ObjectIDs); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Store().CreateIntent(ctx, "snapshot_switch", "99", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Store().CreateIntent(ctx, "plan_build", "99", nil); err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, report, err := OpenService(ctx, ServiceOptions{DBPath: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if report == nil {
		t.Fatal("missing recovery report")
	}
	if len(report.RolledBackWriteBatches) != 1 || report.RolledBackWriteBatches[0].BatchID != "B001-S1" {
		t.Fatalf("rollback report wrong: %+v", report.RolledBackWriteBatches)
	}
	if report.RolledBackWriteBatches[0].NewStatus != "pending" {
		t.Fatalf("batch not rewound to pending: %+v", report.RolledBackWriteBatches[0])
	}
	if len(report.AbortedSnapshotSwitches) != 1 {
		t.Fatalf("snapshot switch not aborted: %+v", report.AbortedSnapshotSwitches)
	}
	if len(report.AbortedPlanBuilds) != 1 {
		t.Fatalf("plan build not aborted: %+v", report.AbortedPlanBuilds)
	}
	if report.ExitReportPromise {
		t.Fatal("recovery must not promise an exit report")
	}
	recoveredState := reopened.View().BatchStates["B001-S1"]
	if recoveredState.Status != "pending" {
		t.Fatalf("recovered in-memory status=%s, want pending", recoveredState.Status)
	}
}

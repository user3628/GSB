package ledger_test

import (
	"context"
	"path/filepath"
	"testing"

	. "ledgerworkbench"
)

func TestProbeAndRebuildIntentsAreRecoveredFromDurableRows(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "probe-recovery.db")
	wb, err := NewWorkbench(Config{DBPath: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := wb.NowMS()

	_, err = wb.DB().ExecContext(ctx, `
		INSERT INTO intents(intent_id,commit_id,kind,attempt,payload_json,status,created_at_ms,updated_at_ms)
		VALUES('intent:probe-crash','probe-crash','probe',1,'{"batch_id":"probe-crash-batch"}','probing',?,?)`, now, now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = wb.DB().ExecContext(ctx, `
		INSERT INTO probe_batches(batch_id,commit_id,view_version,snapshot_version,endpoint_base,config_json,status,total_routes,created_at_ms,updated_at_ms)
		VALUES('probe-crash-batch','probe-crash',7,1,'http://127.0.0.1:0','{}','started',1,?,?)`, now, now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = wb.DB().ExecContext(ctx, `
		INSERT INTO probe_results(batch_id,route_key,endpoint,observed_owner,outcome_code,http_status,elapsed_ms,
			timed_out,conn_closed,slot_released,detail_json,is_current,persisted_at_ms)
		VALUES('probe-crash-batch','route-probe','http://127.0.0.1:9/probe','','E_PROBE_TIMEOUT',0,41,1,1,1,'{}',0,?)`, now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = wb.DB().ExecContext(ctx, `
		INSERT INTO intents(intent_id,commit_id,kind,attempt,payload_json,status,created_at_ms,updated_at_ms)
		VALUES('intent:rebuild-crash','rebuild-crash','rebuild',1,'{}','started',?,?)`, now, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := wb.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := NewWorkbench(Config{DBPath: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	report := recovered.RecoveryReport()
	if len(report.InterruptedProbeBatches) != 1 || report.InterruptedProbeBatches[0] != "probe-crash-batch" {
		t.Fatalf("interrupted batches=%+v", report.InterruptedProbeBatches)
	}
	if report.PersistedProbeResultFacts != 1 {
		t.Fatalf("persisted probe facts=%d", report.PersistedProbeResultFacts)
	}
	kinds := map[string]bool{}
	for _, intent := range report.UnfinishedIntents {
		kinds[intent.Kind] = true
	}
	if !kinds["probe"] || !kinds["rebuild"] {
		t.Fatalf("missing recovered intent kinds: %+v", report.UnfinishedIntents)
	}
}

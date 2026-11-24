package store

import (
	"context"
	"encoding/json"
	"testing"

	"driftconverge/internal/apperr"
	"driftconverge/internal/batch"
	"driftconverge/internal/detect"
	"driftconverge/internal/model"
	"driftconverge/internal/plan"
)

func TestSnapshotStaleDoesNotChangeCommittedState(t *testing.T) {
	st, samples, drift, batches, cost, batchVersionID := setupStore(t)
	defer st.Close()
	if batchVersionID == 0 {
		t.Fatal("batch version was not persisted")
	}
	before := stateCounts(t, st)
	err := st.CommitAnalysis(context.Background(), samples.Cluster, samples.Policy, drift)
	if apperr.CodeOf(err) != apperr.StaleSnapshot {
		t.Fatalf("want E_STALE_SNAPSHOT, got %v", err)
	}
	after := stateCounts(t, st)
	if before != after {
		t.Fatalf("stale rejection changed state: before=%+v after=%+v", before, after)
	}
	if len(batches.Batches) == 0 || cost.TotalEstimatedCost == "" {
		t.Fatal("batch/plan fixture was not complete")
	}
}

func TestRecoveryRollsBackPendingWriteAndAbortsStagingBatch(t *testing.T) {
	st, samples, _, batches, cost, batchVersionID := setupStore(t)
	defer st.Close()
	ctx := context.Background()
	round, created, err := st.CreateRound(ctx, "recover-round", samples.Cluster.SnapshotVersion, batchVersionID)
	if err != nil || !created || round == nil {
		t.Fatalf("create round: created=%v round=%+v err=%v", created, round, err)
	}
	intentID, err := st.BeginWriteAttempt(ctx, "recover-round", "batch-0001", "prod/service/web", 1, map[string]any{"test": true})
	if err != nil {
		t.Fatal(err)
	}
	now := nowText()
	if _, err := st.DB().Exec(`INSERT INTO write_intents(intent_id,kind,round_id,state,payload,created_at,updated_at) VALUES(?,?,?,?,?,?,?)`,
		"staged-batch-intent", "BATCH_COMMIT", "recover-round", "pending", `{}`, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().Exec(`INSERT INTO batch_versions(snapshot_version,round_id,intent_id,state,signature,payload,error_code,created_at) VALUES(?,?,?,?,?,?,?,?)`,
		samples.Cluster.SnapshotVersion, "recover-round", "staged-batch-intent", "staging", "staging-signature", `{}`, "", now); err != nil {
		t.Fatal(err)
	}
	recovery, err := st.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.ScannedIntents < 2 || recovery.PendingWrites < 1 || recovery.AbortedBatchVersions < 1 || recovery.AbortedRounds < 1 {
		t.Fatalf("incomplete recovery report: %+v", recovery)
	}
	if recovery.LastCompleteBatchVersion != batchVersionID {
		t.Fatalf("last complete batch=%d want %d", recovery.LastCompleteBatchVersion, batchVersionID)
	}
	var writeState, intentState, batchState, roundState string
	if err := st.DB().QueryRow(`SELECT state FROM write_records WHERE intent_id=?`, intentID).Scan(&writeState); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRow(`SELECT state FROM write_intents WHERE intent_id=?`, intentID).Scan(&intentState); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRow(`SELECT state FROM batch_versions WHERE intent_id='staged-batch-intent'`).Scan(&batchState); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRow(`SELECT state FROM rounds WHERE round_id='recover-round'`).Scan(&roundState); err != nil {
		t.Fatal(err)
	}
	if writeState != "pending_writeback" || intentState != "rolled_back" || batchState != "aborted" || roundState != "aborted" {
		t.Fatalf("unexpected recovered states write=%s intent=%s batch=%s round=%s", writeState, intentState, batchState, roundState)
	}
	if len(batches.Batches) == 0 || cost.TotalEstimatedCost == "" {
		t.Fatal("setup incomplete")
	}
}

func TestSnapshotIntentRecoveryUsesDurableFacts(t *testing.T) {
	st, samples, _, _, _, _ := setupStore(t)
	defer st.Close()
	ctx := context.Background()
	now := nowText()
	payload, _ := json.Marshal(map[string]any{"snapshotVersion": samples.Policy.Version})
	if _, err := st.DB().Exec(`INSERT INTO write_intents(intent_id,kind,state,payload,created_at,updated_at) VALUES(?,?,?,?,?,?)`,
		"snapshot-present", "SNAPSHOT_COMMIT", "pending", string(payload), now, now); err != nil {
		t.Fatal(err)
	}
	missing, _ := json.Marshal(map[string]any{"snapshotVersion": 99})
	if _, err := st.DB().Exec(`INSERT INTO write_intents(intent_id,kind,state,payload,created_at,updated_at) VALUES(?,?,?,?,?,?)`,
		"snapshot-missing", "SNAPSHOT_COMMIT", "pending", string(missing), now, now); err != nil {
		t.Fatal(err)
	}
	report, err := st.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.CommittedIntents < 1 || report.RolledBackIntents < 1 {
		t.Fatalf("unexpected recovery counts: %+v", report)
	}
	var present, absent string
	_ = st.DB().QueryRow(`SELECT state FROM write_intents WHERE intent_id='snapshot-present'`).Scan(&present)
	_ = st.DB().QueryRow(`SELECT state FROM write_intents WHERE intent_id='snapshot-missing'`).Scan(&absent)
	if present != "committed" || absent != "rolled_back" {
		t.Fatalf("snapshot recovery states present=%s absent=%s", present, absent)
	}
}

func TestConcurrentReadersWithSingleWriter(t *testing.T) {
	st, samples, _, _, _, _ := setupStore(t)
	defer st.Close()
	const readers = 12
	errs := make(chan error, readers)
	for i := 0; i < readers; i++ {
		go func() {
			var version int64
			var count int
			if err := st.DB().QueryRow(`SELECT MAX(version) FROM policy_snapshots`).Scan(&version); err != nil {
				errs <- err
				return
			}
			if err := st.DB().QueryRow(`SELECT COUNT(*) FROM drift_results WHERE snapshot_version=?`, samples.Policy.Version).Scan(&count); err != nil {
				errs <- err
				return
			}
			if version != samples.Policy.Version || count != len(samples.Cluster.Objects) {
				errs <- apperr.New(apperr.Internal, "inconsistent reader snapshot version=%d count=%d", version, count)
				return
			}
			errs <- nil
		}()
	}
	for i := 0; i < readers; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func setupStore(t *testing.T) (*Store, *model.Samples, *detect.Report, *batch.Report, *plan.Report, int64) {
	t.Helper()
	st, err := Open(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	samples, err := model.LoadSamples("../../samples/cluster-objects.json", "../../samples/policy-snapshot.json", "../../samples/drift-field-values.json")
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	drift, err := detect.Analyze(samples.Cluster, samples.Policy, samples.Values)
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	batches, err := batch.Organize(samples.Cluster, drift.Results, samples.Policy)
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	cost, err := plan.Build(samples.Cluster, drift, batches, samples.Policy)
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	if err := st.CommitAnalysis(context.Background(), samples.Cluster, samples.Policy, drift); err != nil {
		st.Close()
		t.Fatal(err)
	}
	batchVersionID, err := st.CommitBatchPlan(context.Background(), samples.Cluster.SnapshotVersion, "", batches, cost)
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	return st, samples, drift, batches, cost, batchVersionID
}

type counts struct {
	policies int
	objects  int
	drift    int
	batches  int
}

func stateCounts(t *testing.T, st *Store) counts {
	t.Helper()
	var c counts
	for query, target := range map[string]*int{
		`SELECT COUNT(*) FROM policy_snapshots`: &c.policies,
		`SELECT COUNT(*) FROM cluster_objects`:  &c.objects,
		`SELECT COUNT(*) FROM drift_results`:    &c.drift,
		`SELECT COUNT(*) FROM batch_versions`:   &c.batches,
	} {
		if err := st.DB().QueryRow(query).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

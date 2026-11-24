package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"example.com/replicabudget/internal/apperr"
	"example.com/replicabudget/internal/policy"
)

func sample(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "samples", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func loadQuotaSample(t *testing.T) []policy.QuotaGroup {
	t.Helper()
	var groups []policy.QuotaGroup
	if err := json.Unmarshal(sample(t, "quotas.json"), &groups); err != nil {
		t.Fatal(err)
	}
	return groups
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(context.Background(), filepath.Join(t.TempDir(), "policies.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func seedQuotasAndJSONL(t *testing.T, st *Store) StateView {
	t.Helper()
	if _, err := st.SetQuotaGroups(context.Background(), 0, loadQuotaSample(t)); err != nil {
		t.Fatal(err)
	}
	result, err := st.Import(context.Background(), ImportRequest{BaseVersion: 1, Data: sample(t, "new.jsonl"), FormatHint: policy.FormatJSONL})
	if err != nil {
		t.Fatal(err)
	}
	if result.SnapshotVersion != 2 {
		t.Fatalf("snapshot version=%d", result.SnapshotVersion)
	}
	return result.State
}

func TestSQLiteDualFormatAndDerivedContracts(t *testing.T) {
	st := openTestStore(t)
	state := seedQuotasAndJSONL(t, st)
	if state.Version != 2 || state.Index.IndexMicros != 4_833_333 {
		t.Fatalf("unexpected initial state: version=%d index=%d", state.Version, state.Index.IndexMicros)
	}
	if len(state.Policies) != 3 || len(state.References) != 12 {
		t.Fatalf("policies=%d refs=%d", len(state.Policies), len(state.References))
	}
	for _, service := range state.Policies {
		if len(service.Placements) != service.DesiredReplicas {
			t.Fatalf("placement count mismatch for %s: %d != %d", service.Strategy.ServiceID, len(service.Placements), service.DesiredReplicas)
		}
	}
	binaryResult, err := st.Import(context.Background(), ImportRequest{BaseVersion: 2, Data: sample(t, "new.bin"), FormatHint: policy.FormatBinary})
	if err != nil {
		t.Fatal(err)
	}
	if binaryResult.SnapshotVersion != 3 || binaryResult.State.Index.IndexMicros != state.Index.IndexMicros {
		t.Fatalf("binary import changed semantics: %+v", binaryResult.State.Index)
	}
	for i := range state.Policies {
		if state.Policies[i].Strategy != binaryResult.State.Policies[i].Strategy {
			t.Fatalf("policy mismatch after binary import: %+v != %+v", state.Policies[i].Strategy, binaryResult.State.Policies[i].Strategy)
		}
	}
}

func TestMigrationAcceptAndAmbiguousAreSnapshotBound(t *testing.T) {
	st := openTestStore(t)
	if _, err := st.SetQuotaGroups(context.Background(), 0, loadQuotaSample(t)); err != nil {
		t.Fatal(err)
	}
	result, err := st.Import(context.Background(), ImportRequest{BaseVersion: 1, Data: sample(t, "legacy.json"), FormatHint: policy.FormatLegacyJSON})
	if err != nil {
		t.Fatal(err)
	}
	if result.Migration == nil || result.Migration.Decision != "accept" || result.State.Version != 2 {
		t.Fatalf("migration result: %+v", result)
	}
	if result.State.Index.IndexMicros != 4_333_333 {
		t.Fatalf("default weight index=%d", result.State.Index.IndexMicros)
	}
	_, err = st.Import(context.Background(), ImportRequest{BaseVersion: 2, Data: sample(t, "legacy_ambiguous.json"), FormatHint: policy.FormatLegacyJSON})
	if err == nil || apperr.As(err).Code != apperr.MigrationAmbiguous {
		t.Fatalf("expected ambiguous, got %v", err)
	}
	state, err := st.State(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.Version != 2 || len(state.Policies) != 3 {
		t.Fatalf("ambiguous migration changed state: %+v", state)
	}
	var accepted, aborted int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM migration_batches WHERE status='accepted'`).Scan(&accepted); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM migration_batches WHERE status='aborted'`).Scan(&aborted); err != nil {
		t.Fatal(err)
	}
	if accepted != 1 || aborted != 1 {
		t.Fatalf("batch statuses accepted=%d aborted=%d", accepted, aborted)
	}
	var auditVersion int
	var migrationJSON string
	if err := st.db.QueryRow(`SELECT version, migration_json FROM import_audits WHERE version=2`).Scan(&auditVersion, &migrationJSON); err != nil {
		t.Fatal(err)
	}
	var auditPlan policy.MigrationPlan
	if err := json.Unmarshal([]byte(migrationJSON), &auditPlan); err != nil {
		t.Fatal(err)
	}
	if auditVersion != 2 || auditPlan.Decision != "accept" || !auditPlan.WeightedIndexEquivalence.WithinTolerance {
		t.Fatalf("audit not bound to snapshot: version=%d plan=%+v", auditVersion, auditPlan.WeightedIndexEquivalence)
	}
}

func TestIncrementalCascadeAndQuotaReferenceCascade(t *testing.T) {
	st := openTestStore(t)
	initial := seedQuotasAndJSONL(t, st)
	threshold := int64(4_900_000)
	result, err := st.ApplyIncremental(context.Background(), IncrementalRequest{
		BaseVersion: 2, ThresholdMicros: &threshold,
		Patches: []IncrementalPatch{{ServiceID: "svc-b", WeightPermille: intPtr(3000)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.SnapshotVersion != 3 || result.State.Index.IndexMicros == initial.Index.IndexMicros {
		t.Fatalf("weight change did not rebuild index: %+v", result.State.Index)
	}
	if !result.State.Threshold.Exceeded {
		t.Fatalf("expected threshold exceeded: %+v", result.State.Threshold)
	}
	lastEvent := result.State.Rebuilds[len(result.State.Rebuilds)-1]
	if lastEvent.EventType != "incremental_cascade" || len(lastEvent.AffectedServices) != 1 || lastEvent.AffectedServices[0] != "svc-b" {
		t.Fatalf("unexpected cascade event: %+v", lastEvent)
	}
	quotaResult, err := st.UpdateQuotaGroup(context.Background(), 3, policy.QuotaGroup{ID: "q-a", VRAMBytes: 68_719_476_736, ComputeMilli: 1600})
	if err != nil {
		t.Fatal(err)
	}
	if quotaResult.SnapshotVersion != 4 {
		t.Fatalf("version=%d", quotaResult.SnapshotVersion)
	}
	foundA := false
	for _, id := range quotaResult.Affected {
		if id == "svc-a" {
			foundA = true
		}
	}
	if !foundA {
		t.Fatalf("quota reference cascade did not include svc-a: %+v", quotaResult.Affected)
	}
	lastEvent = quotaResult.State.Rebuilds[len(quotaResult.State.Rebuilds)-1]
	if lastEvent.EventType != "quota_reference_cascade" {
		t.Fatalf("unexpected quota event: %+v", lastEvent)
	}
	if len(quotaResult.State.References) != 12 {
		t.Fatalf("references were not rebuilt: %d", len(quotaResult.State.References))
	}
}

func intPtr(v int) *int { return &v }

func TestConcurrentIncrementalSameBaseHasOneWinner(t *testing.T) {
	st := openTestStore(t)
	seedQuotasAndJSONL(t, st)
	type outcome struct {
		result MutationResult
		err    error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	var wg sync.WaitGroup
	for _, weight := range []int{1100, 1200} {
		wg.Add(1)
		go func(weight int) {
			defer wg.Done()
			<-start
			result, err := st.ApplyIncremental(context.Background(), IncrementalRequest{
				BaseVersion: 2, Patches: []IncrementalPatch{{ServiceID: "svc-a", WeightPermille: &weight}},
			})
			results <- outcome{result: result, err: err}
		}(weight)
	}
	close(start)
	wg.Wait()
	close(results)
	success, stale := 0, 0
	for item := range results {
		if item.err == nil {
			success++
			continue
		}
		if apperr.As(item.err).Code == apperr.StaleBase {
			stale++
			continue
		}
		t.Fatalf("unexpected error: %v", item.err)
	}
	if success != 1 || stale != 1 {
		t.Fatalf("success=%d stale=%d", success, stale)
	}
	state, err := st.State(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.Version != 3 {
		t.Fatalf("current version=%d", state.Version)
	}
}

func TestSpecInvalidDoesNotAdvanceSnapshot(t *testing.T) {
	st := openTestStore(t)
	seedQuotasAndJSONL(t, st)
	duplicate := strings.Join([]string{
		`{"format":"strategy-jsonl","format_version":1,"record_type":"strategy","service_id":"dup","min_replicas":1,"max_replicas":2,"target_concurrency":64,"latency_budget_ms":100,"quota_group_ref":"q-a","weight_permille":1000}`,
		`{"format":"strategy-jsonl","format_version":1,"record_type":"strategy","service_id":"dup","min_replicas":1,"max_replicas":2,"target_concurrency":64,"latency_budget_ms":100,"quota_group_ref":"q-a","weight_permille":1000}`,
	}, "\n")
	_, err := st.Import(context.Background(), ImportRequest{BaseVersion: 2, Data: []byte(duplicate)})
	if err == nil || apperr.As(err).Code != apperr.SpecInvalid {
		t.Fatalf("expected E_SPEC_INVALID, got %v", err)
	}
	state, _ := st.State(context.Background())
	if state.Version != 2 || len(state.Policies) != 3 {
		t.Fatalf("invalid batch changed committed state: %+v", state)
	}
	var count int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM snapshots WHERE version>2`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("partial snapshot rows persisted: %d", count)
	}
}

func TestMidRebuildFailureRollsBackAndAbortsIntent(t *testing.T) {
	st := openTestStore(t)
	seedQuotasAndJSONL(t, st)
	st.SetFailpoint(func(stage string) error {
		if stage == "mid_rebuild" {
			return errors.New("injected rebuild failure")
		}
		return nil
	})
	_, err := st.ApplyIncremental(context.Background(), IncrementalRequest{
		BaseVersion: 2, Patches: []IncrementalPatch{{ServiceID: "svc-b", WeightPermille: intPtr(3000)}},
	})
	if err == nil {
		t.Fatal("expected injected failure")
	}
	state, _ := st.State(context.Background())
	if state.Version != 2 {
		t.Fatalf("version advanced after rollback: %d", state.Version)
	}
	var partial int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM derived_services WHERE version=3`).Scan(&partial); err != nil {
		t.Fatal(err)
	}
	if partial != 0 {
		t.Fatalf("partial derived rows survived: %d", partial)
	}
	var status string
	if err := st.db.QueryRow(`SELECT status FROM intents ORDER BY created_at DESC LIMIT 1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "aborted" {
		t.Fatalf("intent status=%s", status)
	}
}

func TestRecoveryAbortsPendingIntentAndRestoresCompleteVersion(t *testing.T) {
	st := openTestStore(t)
	seedQuotasAndJSONL(t, st)
	ts := now()
	if _, err := st.db.Exec(`INSERT INTO intents(intent_id,kind,base_version,target_version,status,details_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)`,
		"int_pending", "incremental", 2, 3, "pending", `{"patches":1}`, ts, ts); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO migration_batches(batch_id,intent_id,base_version,target_version,status,decision_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)`,
		"mig_pending", "int_pending", 2, 3, "pending", "{}", ts, ts); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`UPDATE state SET current_version=99 WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	report, err := st.Recover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.CurrentVersion != 2 || report.LastCompleteVersion != 2 {
		t.Fatalf("unexpected recovery versions: %+v", report)
	}
	if report.Action != "rolled_back_to_last_complete_snapshot" && report.Action != "aborted_unfinished_intents_and_migration_batches" {
		t.Fatalf("action=%s", report.Action)
	}
	if len(report.RecoveredIntents) != 1 || len(report.AbortedMigrationBatches) != 1 {
		t.Fatalf("recovery report incomplete: %+v", report)
	}
	var intentStatus, batchStatus, errorCode string
	if err := st.db.QueryRow(`SELECT status,error_code FROM intents WHERE intent_id='int_pending'`).Scan(&intentStatus, &errorCode); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT status FROM migration_batches WHERE batch_id='mig_pending'`).Scan(&batchStatus); err != nil {
		t.Fatal(err)
	}
	if intentStatus != "recovered_aborted" || batchStatus != "aborted" || errorCode != "E_PROCESS_TERMINATED" {
		t.Fatalf("intent=%s/%s batch=%s", intentStatus, errorCode, batchStatus)
	}
	latest, err := st.RecoveryReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if latest.ID != report.ID || latest.CurrentVersion != 2 {
		t.Fatalf("persisted report mismatch: %+v", latest)
	}
}

func TestStaleBaseKeepsCommittedState(t *testing.T) {
	st := openTestStore(t)
	seedQuotasAndJSONL(t, st)
	_, err := st.ApplyIncremental(context.Background(), IncrementalRequest{
		BaseVersion: 1, Patches: []IncrementalPatch{{ServiceID: "svc-a", WeightPermille: intPtr(1100)}},
	})
	if err == nil || apperr.As(err).Code != apperr.StaleBase {
		t.Fatalf("expected E_STALE_BASE, got %v", err)
	}
	state, _ := st.State(context.Background())
	if state.Version != 2 {
		t.Fatalf("stale write changed state: %d", state.Version)
	}
}

func TestSnapshotAndDerivedRowsAreReadTogether(t *testing.T) {
	st := openTestStore(t)
	seedQuotasAndJSONL(t, st)
	var strategies, derived, refs, indexes int
	for query, dest := range map[string]*int{
		`SELECT COUNT(*) FROM strategies WHERE version=2`:       &strategies,
		`SELECT COUNT(*) FROM derived_services WHERE version=2`: &derived,
		`SELECT COUNT(*) FROM references_v1 WHERE version=2`:    &refs,
		`SELECT COUNT(*) FROM derived_index WHERE version=2`:    &indexes,
	} {
		if err := st.db.QueryRow(query).Scan(dest); err != nil {
			t.Fatal(err)
		}
	}
	if strategies != 3 || derived != 3 || refs != 12 || indexes != 1 {
		t.Fatalf("snapshot rows strategies=%d derived=%d refs=%d index=%d", strategies, derived, refs, indexes)
	}
}

func TestFormatVersionRejectedWithoutStateChange(t *testing.T) {
	st := openTestStore(t)
	seedQuotasAndJSONL(t, st)
	_, err := st.Import(context.Background(), ImportRequest{BaseVersion: 2, Data: sample(t, "new_v2_unknown.jsonl")})
	if err == nil || apperr.As(err).Code != apperr.FormatUnsupported {
		t.Fatalf("expected E_FORMAT_UNSUPPORTED, got %v", err)
	}
	state, _ := st.State(context.Background())
	if state.Version != 2 {
		t.Fatalf("version changed: %d", state.Version)
	}
}

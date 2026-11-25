package store_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"

	"auditdrift/internal/model"
	"auditdrift/internal/sample"
	"auditdrift/internal/store"
)

func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "audit.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func initializeSample(t *testing.T, st *store.Store, quota int) *sample.SampleSet {
	t.Helper()
	set := sample.FixedSet()
	_, err := st.InitializeSession(store.InitializeInput{
		SessionID: "s1", Hosts: set.Hosts, Baselines: set.Baselines, Observations: set.Observations,
		PropagationMaxHops: 3, RepairQuota: quota,
	})
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func TestInvalidBaselineBatchIsNotStored(t *testing.T) {
	st := openTestStore(t)
	invalid := []model.Baseline{{ID: "b1", Category: model.CategoryAccountPolicy, SeverityWeight: 0}}
	_, err := st.InitializeSession(store.InitializeInput{SessionID: "bad", Hosts: []string{"h"}, Baselines: invalid})
	if model.CodeOf(err) != model.ErrBaselineInvalid {
		t.Fatalf("err=%v code=%s", err, model.CodeOf(err))
	}
	var baselineCount, sessionCount, intentCount int
	if err := st.ReaderDB().QueryRow(`SELECT COUNT(*) FROM baselines`).Scan(&baselineCount); err != nil {
		t.Fatal(err)
	}
	if err := st.ReaderDB().QueryRow(`SELECT COUNT(*) FROM sessions`).Scan(&sessionCount); err != nil {
		t.Fatal(err)
	}
	if err := st.ReaderDB().QueryRow(`SELECT COUNT(*) FROM intents`).Scan(&intentCount); err != nil {
		t.Fatal(err)
	}
	if baselineCount != 0 || sessionCount != 0 || intentCount != 0 {
		t.Fatalf("invalid batch wrote facts: baselines=%d sessions=%d intents=%d", baselineCount, sessionCount, intentCount)
	}
}

func TestStaleBaseAndIncrementalCascade(t *testing.T) {
	st := openTestStore(t)
	initializeSample(t, st, 100)

	set := sample.FixedSet()
	changed := findBaseline(set.Baselines, "ACCT_LOCKOUT_THRESHOLD")
	changed.SeverityWeight = 10
	_, err := st.CommitIncrement(store.Increment{SessionID: "s1", BaseVersion: 99, BaselineChanges: []store.BaselineChange{{Op: "upsert", Baseline: &changed}}})
	if model.CodeOf(err) != model.ErrStaleBase {
		t.Fatalf("stale err=%v code=%s", err, model.CodeOf(err))
	}
	session, _ := st.GetSession("s1")
	if session.CurrentVersion != 1 {
		t.Fatalf("stale increment changed version to %d", session.CurrentVersion)
	}

	result, err := st.CommitIncrement(store.Increment{SessionID: "s1", BaseVersion: 1, Reason: "weight cascade", BaselineChanges: []store.BaselineChange{{Op: "upsert", Baseline: &changed}}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Version != 2 {
		t.Fatalf("version=%d", result.Version)
	}
	conclusions, err := st.ListConclusions("s1", "host-b")
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[string]model.Conclusion)
	for _, c := range conclusions {
		byID[c.BaselineID] = c
		if c.ViewVersion != 2 {
			t.Fatalf("%s view=%d want2", c.BaselineID, c.ViewVersion)
		}
	}
	if byID["ACCT_LOCKOUT_THRESHOLD"].DriftLevel != 4 {
		t.Fatalf("changed baseline level=%d want4", byID["ACCT_LOCKOUT_THRESHOLD"].DriftLevel)
	}
	if byID["AUDIT_LOGON_LEVEL"].DriftLevel < 4 || len(byID["AUDIT_LOGON_LEVEL"].Propagation) == 0 {
		t.Fatalf("direct dependency not amplified: %+v", byID["AUDIT_LOGON_LEVEL"])
	}
	if byID["AUDIT_OBJECT_ACCESS"].DriftLevel < 3 {
		t.Fatalf("second hop not propagated: %+v", byID["AUDIT_OBJECT_ACCESS"])
	}
	if byID["REG_AUDIT_ACL"].Propagation != nil && len(byID["REG_AUDIT_ACL"].Propagation) > 0 && byID["REG_AUDIT_ACL"].Propagation[0].Hop > 3 {
		t.Fatalf("propagation exceeded max hops: %+v", byID["REG_AUDIT_ACL"].Propagation)
	}
	summary, err := st.GetHostSummary("s1", "host-b")
	if err != nil {
		t.Fatal(err)
	}
	if summary.ViewVersion != 2 {
		t.Fatalf("summary version=%d", summary.ViewVersion)
	}
}

func TestStaleReadAdjudication(t *testing.T) {
	st := openTestStore(t)
	initializeSample(t, st, 10)
	read, err := st.ReadBaselineConclusion("s1", "host-a", "ACCT_PWD_MINLEN", 1)
	if err != nil {
		t.Fatal(err)
	}
	if read.ViewVersion != 1 || read.VersionRelation != "at_or_before_observed" {
		t.Fatalf("read=%+v", read)
	}
	intentID, err := st.CreatePropagationIntent("s1", 2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.ReadBaselineConclusion("s1", "host-a", "ACCT_PWD_MINLEN", 2)
	if model.CodeOf(err) != model.ErrStaleRead {
		t.Fatalf("expected stale read, got %v", err)
	}
	coded, ok := model.AsCoded(err)
	if !ok {
		t.Fatal("error is not coded")
	}
	details, ok := coded.Details.(map[string]any)
	if !ok || details["view_version"] != int64(1) || details["observed_version"] != int64(2) {
		t.Fatalf("stale details=%#v", coded.Details)
	}
	if details["pending_intent"].(*store.Intent).IntentID != intentID {
		t.Fatalf("pending intent mismatch: %+v", details["pending_intent"])
	}
}

func TestConcurrentReadersSeeCommittedVersion(t *testing.T) {
	st := openTestStore(t)
	initializeSample(t, st, 10)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := st.WithReadOnlyTx(context.Background(), func(tx *sql.Tx) error {
				var version int
				return tx.QueryRow(`SELECT current_version FROM sessions WHERE session_id='s1'`).Scan(&version)
			}); err != nil {
				errs <- err
			}
			_, err := st.ListHostSummaries("s1")
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestRecoveryMarksInterruptedIntentAndPreservesCompleteFacts(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recovery.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	initializeSample(t, st, 25)
	window, err := st.BeginCrashWindow("s1")
	if err != nil {
		t.Fatal(err)
	}
	intentID := window.IntentID
	// Closing the crashed process's pool rolls back its uncommitted partial
	// write; the durably committed PENDING intent remains in SQLite.
	if err := window.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	recoveryStore, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveryStore.Close()
	report, err := recoveryStore.Recover("s1")
	if err != nil {
		t.Fatal(err)
	}
	if !report.Consistent || report.FinalPendingIntents != 0 {
		t.Fatalf("recovery report inconsistent: %+v violations=%v", report, report.ConsistencyViolations)
	}
	if report.PendingIntentCount != 1 || len(report.AbortedIntentIDs) != 1 {
		t.Fatalf("pending count=%d aborted=%v", report.PendingIntentCount, report.AbortedIntentIDs)
	}
	if report.AbortedIntentIDs[0] != intentID {
		t.Fatalf("recovered intent=%s want %s", report.AbortedIntentIDs[0], intentID)
	}
	if report.LastCompleteVersion != 1 || report.SessionStatus != "RECOVERED" {
		t.Fatalf("recovery version/status=%d/%s", report.LastCompleteVersion, report.SessionStatus)
	}
	summaries, err := recoveryStore.ListHostSummaries("s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 3 {
		t.Fatalf("summaries=%d", len(summaries))
	}
	for _, summary := range summaries {
		if summary.Total != 8 {
			t.Fatalf("host %s total=%d", summary.Host, summary.Total)
		}
	}
}

func findBaseline(items []model.Baseline, id string) model.Baseline {
	for _, item := range items {
		if item.ID == id {
			return item
		}
	}
	return model.Baseline{}
}

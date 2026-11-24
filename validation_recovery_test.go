package ledger_test

import (
	"context"
	"path/filepath"
	"testing"

	. "ledgerworkbench"
	"ledgerworkbench/sample"
)

func TestInvalidBatchReturnsBusinessErrorWithoutEntries(t *testing.T) {
	wb := newTestWorkbench(t)
	ctx := context.Background()
	batch := sample.FixedEntryBatch(sample.FixedBaseTimeMS(), nil)
	batch.CommitID = "commit-invalid-test"
	batch.Entries[0].RequestWeight = 0
	batch.Entries[1].HitVersion = "missing-version"
	batch.Entries[2].TimestampMS = -1
	batch.Entries[3].Weights = map[string]int{"stable": 701, "canary": 300}
	batch.Entries[3].CandidateWeights = batch.Entries[3].Weights

	_, err := wb.SubmitEntryBatch(ctx, batch)
	bizErr, ok := AsError(err)
	if !ok || bizErr.Code != CodeLedgerInvalid {
		t.Fatalf("expected E_LEDGER_INVALID, got %v", err)
	}
	issues, ok := bizErr.Context["issues"].([]ValidationIssue)
	if !ok || len(issues) < 4 {
		t.Fatalf("expected validation issues, got %#v", bizErr.Context)
	}
	version, _ := wb.Store().ViewVersion(ctx)
	if version != 0 {
		t.Fatalf("invalid batch changed view version to %d", version)
	}
	var count int
	if err := wb.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM ledger_entries`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("invalid batch wrote %d rows", count)
	}
}

func TestInjectionParameterBusinessCodes(t *testing.T) {
	wb := newTestWorkbench(t)
	ctx := context.Background()
	base := sample.FixedBaseTimeMS()
	injections := sample.FixedInjectionParameters(base)

	_, err := wb.SubmitEntryBatch(ctx, injections.LedgerInvalidSum)
	if code, _ := AsError(err); code == nil || code.Code != CodeLedgerInvalid {
		t.Fatalf("invalid sum code: %v", err)
	}
	_, err = wb.SubmitMigration(ctx, injections.MigrationRatioInvalid)
	if code, _ := AsError(err); code == nil || code.Code != CodeMigrationRatioInvalid {
		t.Fatalf("ratio code: %v", err)
	}
	if _, err := wb.SubmitSnapshot(ctx, sample.FixedSnapshot(base)); err != nil {
		t.Fatal(err)
	}
	_, err = wb.SubmitSnapshot(ctx, injections.VersionGapSnapshot)
	if code, _ := AsError(err); code == nil || code.Code != CodeVersionGap {
		t.Fatalf("version gap code: %v", err)
	}
}

func TestIntentRecoveryDiscardsEntriesAndResetsMigration(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recovery.db")
	wb, err := NewWorkbench(Config{DBPath: dbPath})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	prime, bizErr := wb.PrimeCrash(ctx, CrashRequest{
		CommitID: "unit-crash", StagedEntryCount: 4, NoKill: true,
	})
	if bizErr != nil {
		t.Fatal(bizErr)
	}
	if len(prime.IntentIDs) != 2 {
		t.Fatalf("intent ids: %v", prime.IntentIDs)
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
	if report.DiscardedStagedEntries != 4 {
		t.Fatalf("discarded=%d report=%+v", report.DiscardedStagedEntries, report)
	}
	if len(report.UnfinishedIntents) != 2 {
		t.Fatalf("unfinished intents=%d", len(report.UnfinishedIntents))
	}
	foundMigration := false
	for _, id := range report.RecoveredMigrationIDs {
		if id == "mig-crash-unit-crash" {
			foundMigration = true
		}
	}
	if !foundMigration {
		t.Fatalf("migration not reported: %+v", report.RecoveredMigrationIDs)
	}

	var staged, discarded, committed int
	if err := recovered.DB().QueryRowContext(ctx, `SELECT
		SUM(CASE WHEN state='staged' THEN 1 ELSE 0 END),
		SUM(CASE WHEN state='discarded' THEN 1 ELSE 0 END),
		SUM(CASE WHEN state='committed' THEN 1 ELSE 0 END)
		FROM ledger_entries`).Scan(&staged, &discarded, &committed); err != nil {
		t.Fatal(err)
	}
	if staged != 0 || discarded != 4 || committed != 0 {
		t.Fatalf("entry states staged=%d discarded=%d committed=%d", staged, discarded, committed)
	}
	var status string
	if err := recovered.DB().QueryRowContext(ctx, `SELECT status FROM migrations WHERE migration_id=?`,
		"mig-crash-unit-crash").Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Fatalf("migration status=%s", status)
	}
}

func TestRebuildFailureRollsBackWholeCommit(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rollback.db")
	wb, err := NewWorkbench(Config{
		DBPath: dbPath,
		BeforeCommitHook: func(kind, commitID string, viewVersion int64) error {
			if kind == "entries" {
				return assertError("forced rebuild failure")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer wb.Close()
	batch := sample.FixedEntryBatch(sample.FixedBaseTimeMS(), nil)
	batch.Entries = batch.Entries[:2]
	batch.CommitID = "commit-rebuild-fail"
	_, submitErr := wb.SubmitEntryBatch(context.Background(), batch)
	if code, _ := AsError(submitErr); code == nil || code.Code != CodeRebuildFailed {
		t.Fatalf("expected E_REBUILD_FAILED, got %v", submitErr)
	}
	version, _ := wb.Store().ViewVersion(context.Background())
	if version != 0 {
		t.Fatalf("failed rebuild advanced version to %d", version)
	}
	var count int
	if err := wb.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM ledger_entries`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("transaction rollback left %d entries", count)
	}
}

type assertError string

func (e assertError) Error() string { return string(e) }

package ledger_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	. "ledgerworkbench"
	"ledgerworkbench/sample"
)

func newTestWorkbench(t *testing.T) *Workbench {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "ledger.db")
	wb, err := NewWorkbench(Config{
		DBPath: dbPath, Now: func() time.Time { return time.UnixMilli(sample.FixedBaseTimeMS() + 999_000) },
		ProbeConcurrency: 2, ProbeTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewWorkbench: %v", err)
	}
	t.Cleanup(func() { _ = wb.Close() })
	return wb
}

func seedCoreWorkbench(t *testing.T, wb *Workbench) {
	t.Helper()
	ctx := context.Background()
	base := sample.FixedBaseTimeMS()
	snapshot := sample.FixedSnapshot(base)
	if _, bizErr := wb.SubmitSnapshot(ctx, snapshot); bizErr != nil {
		t.Fatalf("SubmitSnapshot: %v", bizErr)
	}
	migrations := sample.FixedMigrations(base)
	if _, bizErr := wb.SubmitMigration(ctx, migrations.Valid); bizErr != nil {
		t.Fatalf("SubmitMigration valid: %v", bizErr)
	}
	batch := sample.FixedEntryBatch(base, nil)
	if _, bizErr := wb.SubmitEntryBatch(ctx, batch); bizErr != nil {
		t.Fatalf("SubmitEntryBatch: %v", bizErr)
	}
}

func TestWeightConservationAndMigrationTransitionAreExact(t *testing.T) {
	wb := newTestWorkbench(t)
	seedCoreWorkbench(t, wb)
	ctx := context.Background()
	base := sample.FixedBaseTimeMS()
	migrations := sample.FixedMigrations(base)

	balanced, bizErr := wb.VerifyConservation(ctx, "route-balanced", 3, base+30_000, "0.01")
	if bizErr != nil {
		t.Fatalf("balanced conservation: %v context=%v", bizErr, bizErr.Context)
	}
	if balanced.TotalRequestWeight != 10_000 || balanced.EntryCount != 100 {
		t.Fatalf("unexpected totals: weight=%d entries=%d", balanced.TotalRequestWeight, balanced.EntryCount)
	}
	if balanced.Rounding.Mode != RoundingMode || balanced.Rounding.DecimalPlaces != 6 {
		t.Fatalf("rounding metadata wrong: %+v", balanced.Rounding)
	}
	expected := map[string][2]string{
		"stable": {"0.700000", "0.700000"},
		"canary": {"0.300000", "0.300000"},
	}
	for _, version := range balanced.Versions {
		want := expected[version.Version]
		if version.ActualRatio != want[0] || version.ExpectedRatio != want[1] || version.AbsDeviation != "0.000000" {
			t.Fatalf("version %s ratios not exact: actual=%s expected=%s deviation=%s",
				version.Version, version.ActualRatio, version.ExpectedRatio, version.AbsDeviation)
		}
	}

	during, bizErr := wb.GetConservation(ctx, "route-migrate", 3, migrations.DuringMS, "0.01")
	if bizErr != nil {
		t.Fatalf("during migration view: %v", bizErr)
	}
	if during.MigrationPhase != "transitioning" || during.MigrationID != "mig-route-migrate-v1-v2" {
		t.Fatalf("transition state wrong: phase=%s id=%s", during.MigrationPhase, during.MigrationID)
	}
	if during.EffectiveWeights["v1"] != 500 || during.EffectiveWeights["v2"] != 500 {
		t.Fatalf("transition weights wrong: %+v", during.EffectiveWeights)
	}
	if !during.Balanced {
		t.Fatalf("transition sample should be balanced: %+v", during.Versions)
	}

	ledger, bizErr := wb.GetLedger(ctx, "route-migrate", 3)
	if bizErr != nil {
		t.Fatalf("ledger: %v", bizErr)
	}
	counts := map[string]int64{}
	weights := map[string]int64{}
	for _, version := range ledger.Versions {
		counts[version.Version] = version.EntryCount
		weights[version.Version] = version.TotalRequestWeight
	}
	if counts["v1"] != 50 || counts["v2"] != 50 || weights["v1"] != 5000 || weights["v2"] != 5000 {
		t.Fatalf("historical attribution rewritten? counts=%v weights=%v", counts, weights)
	}

	after, bizErr := wb.GetConservation(ctx, "route-migrate", 3, migrations.AfterMS, "0.01")
	if bizErr != nil {
		t.Fatalf("after migration view: %v", bizErr)
	}
	if after.MigrationPhase != "completed" || after.EffectiveWeights["v1"] != 0 || after.EffectiveWeights["v2"] != 1000 {
		t.Fatalf("completed weights wrong: phase=%s weights=%+v", after.MigrationPhase, after.EffectiveWeights)
	}
	if after.Balanced {
		t.Fatal("all-time transition history should expose post-cutover imbalance rather than rewrite history")
	}
	_, imbalanceErr := wb.VerifyConservation(ctx, "route-migrate", 3, migrations.AfterMS, "0.01")
	if code, ok := AsError(imbalanceErr); !ok || code.Code != CodeWeightImbalance {
		t.Fatalf("expected E_WEIGHT_IMBALANCE, got %v", imbalanceErr)
	}
}

func TestConservationHashIsBitwiseReproducible(t *testing.T) {
	base := sample.FixedBaseTimeMS()
	var hashes []string
	for i := 0; i < 2; i++ {
		wb := newTestWorkbench(t)
		seedCoreWorkbench(t, wb)
		view, bizErr := wb.GetConservation(context.Background(), "route-balanced", 3, base+30_000, "0.01")
		if bizErr != nil {
			t.Fatalf("conservation: %v", bizErr)
		}
		hashes = append(hashes, view.ViewHash)
	}
	if hashes[0] == "" || hashes[0] != hashes[1] {
		t.Fatalf("view hashes differ or empty: %q vs %q", hashes[0], hashes[1])
	}
	t.Logf("route-balanced view_hash=%s", hashes[0])
}

func TestStaleReadAndStaleBase(t *testing.T) {
	wb := newTestWorkbench(t)
	seedCoreWorkbench(t, wb)
	ctx := context.Background()

	_, err := wb.GetConservation(ctx, "route-balanced", 42, sample.FixedBaseTimeMS(), "0.01")
	bizErr, ok := AsError(err)
	if !ok || bizErr.Code != CodeStaleRead {
		t.Fatalf("expected E_STALE_READ, got %v", err)
	}
	if bizErr.Context["view_version"] != int64(3) || bizErr.Context["observed_version"] != int64(42) {
		t.Fatalf("stale read context wrong: %+v", bizErr.Context)
	}

	fragment := sample.FixedMigrations(sample.FixedBaseTimeMS()).StaleBase
	_, submitErr := wb.SubmitMigration(ctx, fragment)
	if code, ok := AsError(submitErr); !ok || code.Code != CodeStaleBase {
		t.Fatalf("expected E_STALE_BASE, got %v", submitErr)
	}
	version, _ := wb.Store().ViewVersion(ctx)
	if version != 3 {
		t.Fatalf("stale base changed committed version: %d", version)
	}
}

func TestMigrationRaceIncludesBothIDsAndOverlap(t *testing.T) {
	wb := newTestWorkbench(t)
	ctx := context.Background()
	base := sample.FixedBaseTimeMS()
	if _, err := wb.SubmitSnapshot(ctx, sample.FixedSnapshot(base)); err != nil {
		t.Fatal(err)
	}
	migrations := sample.FixedMigrations(base)
	if _, err := wb.SubmitMigration(ctx, migrations.RaceFirst); err != nil {
		t.Fatalf("race first: %v", err)
	}
	_, err := wb.SubmitMigration(ctx, migrations.RaceSecond)
	bizErr, ok := AsError(err)
	if !ok || bizErr.Code != CodeMigrationRace {
		t.Fatalf("expected E_MIGRATION_RACE, got %v", err)
	}
	if bizErr.Context["migration_id"] != "mig-race-b" || bizErr.Context["other_migration_id"] != "mig-race-a" {
		t.Fatalf("race ids missing: %+v", bizErr.Context)
	}
	overlap, ok := bizErr.Context["overlap"].(map[string]any)
	if !ok || overlap["start_at_ms"] == nil || overlap["end_at_ms"] == nil {
		t.Fatalf("overlap interval missing: %+v", bizErr.Context)
	}
}

func TestConcurrentIdempotentCommitAndConflict(t *testing.T) {
	wb := newTestWorkbench(t)
	ctx := context.Background()
	snapshot := sample.FixedSnapshot(sample.FixedBaseTimeMS())

	const callers = 8
	var waitGroup sync.WaitGroup
	results := make([]*CommitResult, callers)
	errs := make([]*Error, callers)
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			<-start
			results[index], errs[index] = wb.SubmitSnapshot(ctx, snapshot)
		}(i)
	}
	close(start)
	waitGroup.Wait()

	committed := 0
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("concurrent submit %d: %v", i, errs[i])
		}
		if results[i].ViewVersion != 1 || results[i].CommitID != snapshot.CommitID {
			t.Fatalf("bad result %d: %+v", i, results[i])
		}
		if results[i].IdempotentReplay {
			committed++
		}
	}
	if committed != callers-1 {
		replayCount := 0
		for _, result := range results {
			if result.IdempotentReplay {
				replayCount++
			}
		}
		t.Fatalf("expected %d replay callers, got %d", callers-1, replayCount)
	}

	conflicting := snapshot
	conflicting.GeneratedAtMS++
	conflicting.Digest = ""
	_, conflictErr := wb.SubmitSnapshot(ctx, conflicting)
	if code, ok := AsError(conflictErr); !ok || code.Code != CodeConflictingLedger {
		t.Fatalf("expected E_CONFLICTING_LEDGER, got %v", conflictErr)
	}
}

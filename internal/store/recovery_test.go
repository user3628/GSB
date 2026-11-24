package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"local/sqlitepool/internal/domain"
)

func TestRecoverPendingAcquireReleaseAndConfig(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "meta.sqlite")
	st, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	cfg := domain.Config{MaxOpen: 2, MaxIdle: 2, AcquireTimeoutMS: 1000, MaxWaiters: 2, Version: 3}
	if _, err := st.SeedConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := st.InsertConnection(ctx, domain.ConnectionRecord{
		ConnectionID: "c-acquire", State: domain.StateAvailable, CreatedAt: now, LastUsedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertConnection(ctx, domain.ConnectionRecord{
		ConnectionID: "c-release", State: domain.StateInUse, CreatedAt: now, LastUsedAt: now, UseCount: 1,
	}); err != nil {
		t.Fatal(err)
	}
	acquireIntent, _, err := st.PrepareAcquire(ctx, "c-acquire", "pending-acquire")
	if err != nil {
		t.Fatal(err)
	}
	releaseIntent, err := st.PrepareRelease(ctx, "c-release", "pending-release", domain.StateAvailable, "")
	if err != nil {
		t.Fatal(err)
	}
	configIntent, err := st.BeginConfigIntent(ctx, "pending-config", domain.Config{
		MaxOpen: 1, MaxIdle: 1, AcquireTimeoutMS: 900, MaxWaiters: 1, Version: 4,
	}, cfg.Version)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	report, err := st.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.PendingIntentsFound != 3 {
		t.Fatalf("expected 3 pending intents, got %d: %+v", report.PendingIntentsFound, report)
	}
	if !report.MetadataStatesRestored || report.RuntimeHandlesPreserved || report.IdleQueueOrderRecoverable || !report.FactsOnly {
		t.Fatalf("recovery report capability flags are wrong: %+v", report)
	}
	byIntent := map[int64]domain.RecoveredIntent{}
	for _, intent := range report.Intents {
		byIntent[intent.IntentID] = intent
	}
	if intent := byIntent[acquireIntent]; intent.Action != "rollback_uncommitted_acquire" ||
		intent.FromState != domain.StateInUse || intent.ToState != domain.StateAvailable {
		t.Fatalf("acquire rollback mismatch: %+v", intent)
	}
	if intent := byIntent[releaseIntent]; intent.Action != "rollback_uncommitted_release" ||
		intent.FromState != domain.StateAvailable || intent.ToState != domain.StateInUse {
		t.Fatalf("release rollback mismatch: %+v", intent)
	}
	if intent := byIntent[configIntent]; intent.Action != "config_change_not_applied" {
		t.Fatalf("config rollback mismatch: %+v", intent)
	}
	acquired, err := st.GetConnection(ctx, "c-acquire")
	if err != nil {
		t.Fatal(err)
	}
	released, err := st.GetConnection(ctx, "c-release")
	if err != nil {
		t.Fatal(err)
	}
	if acquired.State != domain.StateAvailable {
		t.Fatalf("pending acquire was not rolled back to available: %+v", acquired)
	}
	if released.State != domain.StateInUse {
		t.Fatalf("pending release was not rolled back to in_use: %+v", released)
	}
	loaded, err := st.LoadConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version != cfg.Version || loaded.MaxOpen != cfg.MaxOpen {
		t.Fatalf("pending config was unexpectedly applied: %+v", loaded)
	}
	latest, err := st.LatestRecoveryReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if latest.ReportID != report.ReportID {
		t.Fatalf("latest report mismatch: %s vs %s", latest.ReportID, report.ReportID)
	}

	var pending int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM intents WHERE status=?`, domain.IntentPending).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Fatalf("pending intents were not consumed: %d", pending)
	}
}

func TestCommittedAcquireIntentIsNotRolledBack(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "meta.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.SeedConfig(ctx, domain.Config{MaxOpen: 1, MaxIdle: 1, AcquireTimeoutMS: 500, Version: 1}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := st.InsertConnection(ctx, domain.ConnectionRecord{
		ConnectionID: "committed", State: domain.StateAvailable, CreatedAt: now, LastUsedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	intentID, _, err := st.PrepareAcquire(ctx, "committed", "request")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.MarkIntentCommitted(ctx, intentID); err != nil {
		t.Fatal(err)
	}
	report, err := st.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.PendingIntentsFound != 0 {
		t.Fatalf("committed intent should not be recovered: %+v", report)
	}
	rec, err := st.GetConnection(ctx, "committed")
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != domain.StateInUse {
		t.Fatalf("committed acquisition should remain in_use in durable facts: %+v", rec)
	}
}

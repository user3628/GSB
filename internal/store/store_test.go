package store_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"mitmpipeline/internal/domain"
	"mitmpipeline/internal/store"
)

func TestRecoverAbortsDurableIntent(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "recovery.db")
	st, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	now := domain.NowMS()
	result, err := st.DB().ExecContext(ctx, `INSERT INTO commits(kind,status,created_at_ms) VALUES('import','PENDING',?)`, now)
	if err != nil {
		t.Fatal(err)
	}
	version, _ := result.LastInsertId()
	_, err = st.DB().ExecContext(ctx, `INSERT INTO intents(intent_id,commit_version,kind,status,batch_id,payload_json,created_at_ms,updated_at_ms)
		VALUES('intent-test',?,'import','started','batch-test','{}',?,?)`, version, now, now)
	if err != nil {
		t.Fatal(err)
	}

	report, err := st.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.AbortedIntents) != 1 || report.AbortedIntents[0].IntentID != "intent-test" {
		t.Fatalf("unexpected report: %+v", report)
	}
	pending, err := st.PendingIntents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending intents remain: %+v", pending)
	}
}

func TestSubmitCompletesCommitAtomically(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "submit.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	version, intentID, err := st.Submit(ctx, "test", "batch", "{}", func(tx *sql.Tx, commit int64) error {
		if commit <= 0 {
			t.Fatalf("invalid commit version: %d", commit)
		}
		return nil
	})
	_ = intentID
	if err != nil {
		t.Fatal(err)
	}
	latest, err := st.LatestCompleteVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if latest != version {
		t.Fatalf("latest=%d version=%d", latest, version)
	}
}

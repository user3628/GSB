package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestMigrateAddsEventsJSONToOldCheckpointTable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `
CREATE TABLE checkpoints (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  shard_id TEXT NOT NULL,
  bh INTEGER NOT NULL,
  row_block INTEGER NOT NULL,
  col_start INTEGER NOT NULL,
  col_end INTEGER NOT NULL,
  rows_count INTEGER NOT NULL,
  head_dim INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  strategy TEXT NOT NULL,
  row_max_blob BLOB NOT NULL,
  exp_sum_blob BLOB NOT NULL,
  lse_blob BLOB NOT NULL,
  acc_blob BLOB NOT NULL,
  acc_hash TEXT NOT NULL,
  input_fp TEXT NOT NULL,
  version_fp TEXT NOT NULL,
  prev_hash TEXT NOT NULL DEFAULT '',
  state_hash TEXT NOT NULL,
  active INTEGER NOT NULL DEFAULT 1,
  invalid_reason TEXT NOT NULL DEFAULT ''
)`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	var count int
	if err := st.RawDB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('checkpoints') WHERE name='events_json'`,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("events_json column count=%d, want 1", count)
	}
}

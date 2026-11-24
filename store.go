package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db   *sql.DB
	path string
	now  func() time.Time
}

type Intent struct {
	IntentID    string
	CommitID    string
	Kind        string
	Attempt     int
	PayloadJSON string
	Status      string
	ViewVersion int64
	CreatedAtMS int64
	UpdatedAtMS int64
}

type RecoveryReport struct {
	RecoveredAtMS             int64        `json:"recovered_at_ms"`
	DatabasePath              string       `json:"database_path"`
	CurrentViewVersion        int64        `json:"current_view_version"`
	CurrentSnapshotVersion    int64        `json:"current_snapshot_version"`
	UnfinishedIntents         []IntentFact `json:"unfinished_intents"`
	DiscardedStagedEntries    int          `json:"discarded_staged_entries"`
	RecoveredMigrationIDs     []string     `json:"recovered_migration_ids"`
	InterruptedProbeBatches   []string     `json:"interrupted_probe_batches"`
	PersistedProbeResultFacts int          `json:"persisted_probe_result_facts"`
	Statements                []string     `json:"statements"`
}

type IntentFact struct {
	IntentID    string `json:"intent_id"`
	CommitID    string `json:"commit_id"`
	Kind        string `json:"kind"`
	Status      string `json:"status"`
	Attempt     int    `json:"attempt"`
	CreatedAtMS int64  `json:"created_at_ms"`
}

func OpenStore(path string) (*Store, error) {
	if path == "" {
		path = ":memory:"
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	if path == ":memory:" {
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, `
		PRAGMA journal_mode=WAL;
		PRAGMA busy_timeout=5000;
		PRAGMA foreign_keys=ON;
		PRAGMA synchronous=NORMAL;
	`); err != nil {
		_ = db.Close()
		return nil, err
	}
	store := &Store{db: db, path: path, now: time.Now}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`INSERT OR IGNORE INTO meta(key,value) VALUES
			('view_version','0'),
			('snapshot_version','0'),
			('current_commit_id','')`,
		`CREATE TABLE IF NOT EXISTS idempotency (
			commit_id TEXT PRIMARY KEY,
			request_hash TEXT NOT NULL,
			kind TEXT NOT NULL,
			status TEXT NOT NULL,
			result_json TEXT,
			error_json TEXT,
			view_version INTEGER NOT NULL DEFAULT 0,
			attempt INTEGER NOT NULL DEFAULT 1,
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS intents (
			intent_id TEXT PRIMARY KEY,
			commit_id TEXT NOT NULL,
			kind TEXT NOT NULL,
			attempt INTEGER NOT NULL DEFAULT 1,
			payload_json TEXT NOT NULL,
			status TEXT NOT NULL,
			view_version INTEGER NOT NULL DEFAULT 0,
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_intents_commit ON intents(commit_id)`,
		`CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status)`,
		`CREATE TABLE IF NOT EXISTS ledger_entries (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			intent_id TEXT NOT NULL,
			commit_id TEXT NOT NULL,
			seq INTEGER NOT NULL,
			timestamp_ms INTEGER NOT NULL,
			route_key TEXT NOT NULL,
			client_id TEXT NOT NULL,
			hit_version TEXT NOT NULL,
			candidates_json TEXT NOT NULL,
			weights_json TEXT NOT NULL,
			request_weight INTEGER NOT NULL,
			state TEXT NOT NULL,
			view_version INTEGER NOT NULL DEFAULT 0,
			snapshot_version INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_entries_route_state ON ledger_entries(route_key,state)`,
		`CREATE INDEX IF NOT EXISTS idx_entries_commit ON ledger_entries(commit_id)`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			snapshot_version INTEGER PRIMARY KEY,
			generated_at_ms INTEGER NOT NULL,
			digest TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			commit_id TEXT NOT NULL,
			view_version INTEGER NOT NULL,
			created_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS route_weights (
			view_version INTEGER NOT NULL,
			snapshot_version INTEGER NOT NULL,
			route_key TEXT NOT NULL,
			version TEXT NOT NULL,
			weight_permille INTEGER NOT NULL,
			source TEXT NOT NULL,
			migration_id TEXT NOT NULL DEFAULT '',
			PRIMARY KEY(view_version,route_key,version)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_route_weights_route ON route_weights(route_key,view_version)`,
		`CREATE TABLE IF NOT EXISTS migrations (
			migration_id TEXT PRIMARY KEY,
			commit_id TEXT NOT NULL,
			base_version INTEGER NOT NULL,
			view_version INTEGER NOT NULL DEFAULT 0,
			route_key TEXT NOT NULL,
			from_version TEXT NOT NULL,
			to_version TEXT NOT NULL,
			ratio_permille INTEGER NOT NULL,
			start_at_ms INTEGER NOT NULL,
			transition_ms INTEGER NOT NULL,
			end_at_ms INTEGER NOT NULL,
			status TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			created_at_ms INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_migrations_route ON migrations(route_key,start_at_ms,end_at_ms)`,
		`CREATE TABLE IF NOT EXISTS route_views (
			view_version INTEGER NOT NULL,
			route_key TEXT NOT NULL,
			as_of_ms INTEGER NOT NULL,
			snapshot_version INTEGER NOT NULL,
			balanced INTEGER NOT NULL,
			view_hash TEXT NOT NULL,
			view_json TEXT NOT NULL,
			PRIMARY KEY(view_version,route_key,as_of_ms)
		)`,
		`CREATE TABLE IF NOT EXISTS derived_lineage (
			view_version INTEGER NOT NULL,
			route_key TEXT NOT NULL,
			snapshot_version INTEGER NOT NULL,
			ledger_entry_count INTEGER NOT NULL,
			ledger_total_weight INTEGER NOT NULL,
			conservation_hash TEXT NOT NULL,
			probe_batch_id TEXT NOT NULL DEFAULT '',
			probe_status TEXT NOT NULL DEFAULT '',
			valid INTEGER NOT NULL DEFAULT 1,
			invalidation_reason TEXT NOT NULL DEFAULT '',
			updated_at_ms INTEGER NOT NULL,
			PRIMARY KEY(view_version,route_key)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_lineage_route_valid ON derived_lineage(route_key,valid)`,
		`CREATE TABLE IF NOT EXISTS probe_batches (
			batch_id TEXT PRIMARY KEY,
			commit_id TEXT NOT NULL DEFAULT '',
			view_version INTEGER NOT NULL,
			snapshot_version INTEGER NOT NULL,
			endpoint_base TEXT NOT NULL,
			config_json TEXT NOT NULL,
			status TEXT NOT NULL,
			total_routes INTEGER NOT NULL,
			completed_routes INTEGER NOT NULL DEFAULT 0,
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS probe_results (
			batch_id TEXT NOT NULL,
			route_key TEXT NOT NULL,
			endpoint TEXT NOT NULL,
			observed_owner TEXT NOT NULL DEFAULT '',
			outcome_code TEXT NOT NULL,
			http_status INTEGER NOT NULL DEFAULT 0,
			elapsed_ms INTEGER NOT NULL,
			timed_out INTEGER NOT NULL DEFAULT 0,
			conn_closed INTEGER NOT NULL DEFAULT 0,
			slot_released INTEGER NOT NULL DEFAULT 1,
			detail_json TEXT NOT NULL DEFAULT '{}',
			is_current INTEGER NOT NULL DEFAULT 1,
			persisted_at_ms INTEGER NOT NULL,
			PRIMARY KEY(batch_id,route_key)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_probe_current_route ON probe_results(route_key,is_current)`,
		`CREATE TABLE IF NOT EXISTS recovery_reports (
			recovered_at_ms INTEGER PRIMARY KEY,
			report_json TEXT NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate sqlite: %w\nSQL: %s", err, statement)
		}
	}
	return nil
}

func (s *Store) metaInt(ctx context.Context, key string) (int64, error) {
	var value string
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key=?`, key).Scan(&value); err != nil {
		return 0, err
	}
	var parsed int64
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil {
		return 0, err
	}
	return parsed, nil
}

func (s *Store) ViewVersion(ctx context.Context) (int64, error) {
	return s.metaInt(ctx, "view_version")
}

func (s *Store) SnapshotVersion(ctx context.Context) (int64, error) {
	return s.metaInt(ctx, "snapshot_version")
}

func (s *Store) writeRecoveryReport(ctx context.Context, report RecoveryReport) error {
	buf, err := json.Marshal(report)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO recovery_reports(recovered_at_ms,report_json) VALUES(?,?)`,
		report.RecoveredAtMS, string(buf))
	return err
}

// Recover scans durable intent records. It never infers outcome from process
// memory: every reported identifier/count comes from SQLite rows.
func (s *Store) Recover(ctx context.Context, nowMS int64) (*RecoveryReport, error) {
	report := RecoveryReport{
		RecoveredAtMS:           nowMS,
		DatabasePath:            s.path,
		UnfinishedIntents:       []IntentFact{},
		RecoveredMigrationIDs:   []string{},
		InterruptedProbeBatches: []string{},
		Statements: []string{
			"scanned intents with status started/running/probing",
			"discarded staged ledger entries",
			"reset unfinished migrations to pending",
			"marked unfinished probe batches interrupted",
		},
	}
	viewVersion, err := s.ViewVersion(ctx)
	if err != nil {
		return nil, err
	}
	snapshotVersion, err := s.SnapshotVersion(ctx)
	if err != nil {
		return nil, err
	}
	report.CurrentViewVersion = viewVersion
	report.CurrentSnapshotVersion = snapshotVersion

	rows, err := s.db.QueryContext(ctx, `
		SELECT intent_id,commit_id,kind,attempt,payload_json,status,created_at_ms
		FROM intents
		WHERE status IN ('started','running','probing')
		ORDER BY created_at_ms,intent_id`)
	if err != nil {
		return nil, err
	}
	type pendingIntent struct {
		Intent
		Payload string
	}
	pending := []pendingIntent{}
	for rows.Next() {
		var intent Intent
		var payload string
		if err := rows.Scan(&intent.IntentID, &intent.CommitID, &intent.Kind, &intent.Attempt, &payload, &intent.Status, &intent.CreatedAtMS); err != nil {
			_ = rows.Close()
			return nil, err
		}
		report.UnfinishedIntents = append(report.UnfinishedIntents, IntentFact{
			IntentID: intent.IntentID, CommitID: intent.CommitID, Kind: intent.Kind,
			Status: intent.Status, Attempt: intent.Attempt, CreatedAtMS: intent.CreatedAtMS,
		})
		pending = append(pending, pendingIntent{Intent: intent, Payload: payload})
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	var stagedCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM ledger_entries WHERE state='staged'`).Scan(&stagedCount); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE ledger_entries SET state='discarded' WHERE state='staged'`); err != nil {
		return nil, err
	}
	report.DiscardedStagedEntries = stagedCount

	if _, err := tx.ExecContext(ctx, `UPDATE migrations SET status='pending' WHERE status IN ('applying','running')`); err != nil {
		return nil, err
	}

	for _, item := range pending {
		switch item.Kind {
		case "migration":
			var fragment MigrationFragment
			if err := json.Unmarshal([]byte(item.Payload), &fragment); err == nil {
				endAt := fragment.StartAtMS + fragment.TransitionMS
				_, err := tx.ExecContext(ctx, `
					INSERT INTO migrations(
						migration_id,commit_id,base_version,view_version,route_key,from_version,to_version,
						ratio_permille,start_at_ms,transition_ms,end_at_ms,status,payload_json,created_at_ms)
					VALUES(?,?,?,0,?,?,?,?,?,?,?,'pending',?,?)
					ON CONFLICT(migration_id) DO UPDATE SET status='pending'`,
					fragment.MigrationID, item.CommitID, fragment.BaseVersion, fragment.RouteKey,
					fragment.FromVersion, fragment.ToVersion, fragment.RatioPermille,
					fragment.StartAtMS, fragment.TransitionMS, endAt, item.Payload, nowMS)
				if err != nil {
					return nil, err
				}
				report.RecoveredMigrationIDs = append(report.RecoveredMigrationIDs, fragment.MigrationID)
			}
		case "probe":
			var batchID string
			var payload map[string]any
			if err := json.Unmarshal([]byte(item.Payload), &payload); err == nil {
				if value, ok := payload["batch_id"].(string); ok {
					batchID = value
				}
			}
			if batchID != "" {
				if _, err := tx.ExecContext(ctx,
					`UPDATE probe_batches SET status='interrupted',updated_at_ms=? WHERE batch_id=? AND status IN ('started','probing')`,
					nowMS, batchID); err != nil {
					return nil, err
				}
				report.InterruptedProbeBatches = append(report.InterruptedProbeBatches, batchID)
			}
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE intents SET status='recovered',updated_at_ms=? WHERE intent_id=?`,
			nowMS, item.IntentID); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE idempotency SET status='recovered',updated_at_ms=? WHERE commit_id=? AND status='pending'`,
			nowMS, item.CommitID); err != nil {
			return nil, err
		}
	}

	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM probe_results WHERE batch_id IN (SELECT batch_id FROM probe_batches WHERE status='interrupted')`).Scan(&report.PersistedProbeResultFacts); err != nil {
		return nil, err
	}
	if err := s.writeRecoveryReportTx(ctx, tx, report); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &report, nil
}

func (s *Store) writeRecoveryReportTx(ctx context.Context, tx *sql.Tx, report RecoveryReport) error {
	buf, err := json.Marshal(report)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO recovery_reports(recovered_at_ms,report_json) VALUES(?,?)`,
		report.RecoveredAtMS, string(buf))
	return err
}

func (s *Store) LatestRecoveryReport(ctx context.Context) (*RecoveryReport, error) {
	var payload string
	err := s.db.QueryRowContext(ctx, `SELECT report_json FROM recovery_reports ORDER BY recovered_at_ms DESC LIMIT 1`).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var report RecoveryReport
	if err := json.Unmarshal([]byte(payload), &report); err != nil {
		return nil, err
	}
	return &report, nil
}

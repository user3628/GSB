package configimpact

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

var memoryDatabaseCounter atomic.Int64

func openDatabase(path string) (*sql.DB, error) {
	var dsn string
	if path == ":memory:" {
		dsn = fmt.Sprintf("file:configimpact-memory-%d?mode=memory&cache=shared&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)", memoryDatabaseCounter.Add(1))
	} else {
		if path == "" {
			return nil, fmt.Errorf("database path is required")
		}
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
			return nil, err
		}
		// Pass a native filesystem path instead of a file: URI so spaces and
		// other platform-specific path characters do not require URI escaping.
		dsn = fmt.Sprintf("%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(ON)", absolute)
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(0)
	return db, nil
}

func migrate(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY NOT NULL,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS documents (
			version INTEGER PRIMARY KEY NOT NULL,
			doc_hash TEXT NOT NULL,
			payload BLOB NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS materializations (
			snapshot_version INTEGER NOT NULL,
			component_id TEXT NOT NULL,
			materialized_version INTEGER NOT NULL,
			config_json TEXT NOT NULL,
			path TEXT NOT NULL,
			PRIMARY KEY (snapshot_version, component_id)
		)`,
		`CREATE INDEX IF NOT EXISTS materializations_component_idx
			ON materializations(component_id, snapshot_version DESC)`,
		`CREATE TABLE IF NOT EXISTS impacts (
			snapshot_version INTEGER NOT NULL,
			component_id TEXT NOT NULL,
			impact_json TEXT NOT NULL,
			PRIMARY KEY (snapshot_version, component_id)
		)`,
		`CREATE TABLE IF NOT EXISTS refs (
			snapshot_version INTEGER NOT NULL,
			component_id TEXT NOT NULL,
			ref_kind TEXT NOT NULL,
			target_id TEXT NOT NULL,
			key_path TEXT NOT NULL,
			PRIMARY KEY (snapshot_version, component_id, ref_kind, target_id, key_path)
		)`,
		`CREATE INDEX IF NOT EXISTS refs_target_idx
			ON refs(target_id, ref_kind, key_path, snapshot_version)`,
		`CREATE TABLE IF NOT EXISTS patch_log (
			patch_id TEXT PRIMARY KEY NOT NULL,
			payload_hash TEXT NOT NULL,
			base_version INTEGER NOT NULL,
			document_version INTEGER,
			state TEXT NOT NULL,
			result_json TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS intents (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			kind TEXT NOT NULL,
			patch_id TEXT,
			base_version INTEGER NOT NULL,
			target_version INTEGER NOT NULL,
			payload_hash TEXT NOT NULL,
			status TEXT NOT NULL,
			detail_json TEXT,
			created_at TEXT NOT NULL,
			finished_at TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS intents_status_idx ON intents(status, id)`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate schema: %w", err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT OR IGNORE INTO meta(key, value) VALUES('current_version', '0')`); err != nil {
		return fmt.Errorf("initialize meta: %w", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', '1')`); err != nil {
		return fmt.Errorf("initialize schema version: %w", err)
	}
	return nil
}

func currentVersion(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (int64, error) {
	var raw string
	if err := queryer.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='current_version'`).Scan(&raw); err != nil {
		return 0, err
	}
	var version int64
	if _, err := fmt.Sscan(raw, &version); err != nil {
		return 0, fmt.Errorf("invalid current_version %q: %w", raw, err)
	}
	return version, nil
}

func setCurrentVersion(ctx context.Context, tx *sql.Tx, version int64) error {
	result, err := tx.ExecContext(ctx, `UPDATE meta SET value=? WHERE key='current_version'`, fmt.Sprint(version))
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("current_version meta row missing")
	}
	return nil
}

func loadDocumentAt(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, version int64) (*Document, string, []byte, error) {
	var payload []byte
	var hash string
	err := queryer.QueryRowContext(ctx, `SELECT payload, doc_hash FROM documents WHERE version=?`, version).Scan(&payload, &hash)
	if err != nil {
		return nil, "", nil, err
	}
	var doc Document
	if err := json.Unmarshal(payload, &doc); err != nil {
		return nil, "", nil, err
	}
	return &doc, hash, payload, nil
}

type snapshotQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadMaterializations(ctx context.Context, queryer snapshotQueryer, snapshotVersion int64) (map[string]MaterializedConfig, error) {
	rows, err := queryer.QueryContext(ctx, `SELECT config_json FROM materializations WHERE snapshot_version=? ORDER BY component_id`, snapshotVersion)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]MaterializedConfig)
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var config MaterializedConfig
		if err := json.Unmarshal([]byte(payload), &config); err != nil {
			return nil, err
		}
		result[config.ComponentID] = config
	}
	return result, rows.Err()
}

func persistSnapshotTx(
	ctx context.Context,
	tx *sql.Tx,
	doc *Document,
	payload []byte,
	hash string,
	materializations []MaterializedConfig,
	impacts []AffectedComponent,
	refs []PatchRef,
) error {
	createdAt := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `INSERT INTO documents(version, doc_hash, payload, created_at) VALUES(?,?,?,?)`, doc.Version, hash, payload, createdAt); err != nil {
		return fmt.Errorf("insert document: %w", err)
	}
	materializationStatement, err := tx.PrepareContext(ctx, `INSERT INTO materializations(snapshot_version, component_id, materialized_version, config_json, path) VALUES(?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer materializationStatement.Close()
	for _, config := range materializations {
		encoded, err := json.Marshal(config)
		if err != nil {
			return err
		}
		if _, err := materializationStatement.ExecContext(ctx, doc.Version, config.ComponentID, config.MaterializedVersion, string(encoded), config.Decision.Path); err != nil {
			return fmt.Errorf("insert materialization %s: %w", config.ComponentID, err)
		}
	}

	impactStatement, err := tx.PrepareContext(ctx, `INSERT INTO impacts(snapshot_version, component_id, impact_json) VALUES(?,?,?)`)
	if err != nil {
		return err
	}
	defer impactStatement.Close()
	for _, impact := range impacts {
		encoded, err := json.Marshal(impact)
		if err != nil {
			return err
		}
		if _, err := impactStatement.ExecContext(ctx, doc.Version, impact.ComponentID, string(encoded)); err != nil {
			return fmt.Errorf("insert impact %s: %w", impact.ComponentID, err)
		}
	}

	refStatement, err := tx.PrepareContext(ctx, `INSERT INTO refs(snapshot_version, component_id, ref_kind, target_id, key_path) VALUES(?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer refStatement.Close()
	for _, ref := range refs {
		if _, err := refStatement.ExecContext(ctx, doc.Version, ref.ComponentID, ref.Kind, ref.TargetID, ref.Key); err != nil {
			return fmt.Errorf("insert ref: %w", err)
		}
	}
	return setCurrentVersion(ctx, tx, doc.Version)
}

func recordIntent(ctx context.Context, db *sql.DB, kind, patchID string, baseVersion, targetVersion int64, payloadHash, detail string) (int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if patchID != "" {
		result, err := tx.ExecContext(ctx, `
			UPDATE patch_log SET state='in_progress', updated_at=?
			WHERE patch_id=? AND state IN ('aborted','failed')`, now, patchID)
		if err != nil {
			return 0, err
		}
		updated, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		if updated == 0 {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO patch_log(patch_id, payload_hash, base_version, state, created_at, updated_at)
				VALUES(?,?,?,'in_progress',?,?)`, patchID, payloadHash, baseVersion, now, now); err != nil {
				return 0, err
			}
		}
	}
	result, err := tx.ExecContext(ctx, `
		INSERT INTO intents(kind, patch_id, base_version, target_version, payload_hash, status, detail_json, created_at)
		VALUES(?,?,?,?,?,'pending',?,?)`, kind, nullableString(patchID), baseVersion, targetVersion, payloadHash, detail, now)
	if err != nil {
		return 0, err
	}
	intentID, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return intentID, nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

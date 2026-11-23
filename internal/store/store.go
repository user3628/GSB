package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"example.com/logicplan/editor/internal/model"
	"example.com/logicplan/editor/internal/planstate"
)

type Store struct {
	db *sql.DB
}

type Intent struct {
	ID          string
	Kind        string
	RequestHash string
	BaseVersion *uint64
	Target      uint64
	Detail      map[string]any
	StartedAt   time.Time
}

type IntentRecord struct {
	ID          string
	Kind        string
	RequestHash string
	BaseVersion *uint64
	Target      uint64
	Status      string
	Error       string
	StartedAt   time.Time
	FinishedAt  *time.Time
}

type CommitRecord struct {
	CommitID    string
	Kind        string
	RequestHash string
	Version     uint64
	ResultJSON  string
	CreatedAt   time.Time
}

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("database path is required")
	}
	if path != ":memory:" && !strings.HasPrefix(path, "file:") {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && filepath.Dir(path) != "." {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
	}
	dsn := path
	if path == ":memory:" {
		dsn = fmt.Sprintf("file:logicplan-editor-%d?mode=memory&cache=shared", time.Now().UnixNano())
	} else if !strings.HasPrefix(path, "file:") {
		dsn = "file:" + path
	}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	dsn += sep + "_busy_timeout=10000&_journal_mode=WAL&_synchronous=FULL&_foreign_keys=on"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	// API readers use immutable in-memory states. Keeping one SQLite connection
	// makes intent/commit/finish ordering explicit and avoids writer contention.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	store := &Store{db: db}
	if err := store.initSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) initSchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `PRAGMA foreign_keys = ON`); err != nil {
		return err
	}
	statements := []string{
		`CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS snapshot_versions (
			version INTEGER PRIMARY KEY,
			snapshot_seq INTEGER NOT NULL,
			format TEXT NOT NULL,
			format_version INTEGER NOT NULL,
			decision TEXT NOT NULL,
			generated_at TEXT NOT NULL,
			committed_at TEXT NOT NULL,
			tree_hash TEXT NOT NULL,
			root_id TEXT NOT NULL,
			node_count INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS operators (
			version INTEGER NOT NULL,
			node_id TEXT NOT NULL,
			type TEXT NOT NULL,
			children_json TEXT NOT NULL,
			columns_json TEXT NOT NULL,
			predicate_json TEXT,
			stats_json TEXT,
			PRIMARY KEY (version, node_id),
			FOREIGN KEY (version) REFERENCES snapshot_versions(version) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS derived_indexes (
			version INTEGER NOT NULL,
			kind TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			PRIMARY KEY (version, kind),
			FOREIGN KEY (version) REFERENCES snapshot_versions(version) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS degradations (
			version INTEGER NOT NULL,
			record_no INTEGER NOT NULL,
			operator_id TEXT NOT NULL,
			field TEXT NOT NULL,
			action TEXT NOT NULL,
			default_json TEXT,
			reason TEXT NOT NULL,
			PRIMARY KEY (version, record_no, operator_id, field, action),
			FOREIGN KEY (version) REFERENCES snapshot_versions(version) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS intents (
			id TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			request_hash TEXT NOT NULL,
			base_version INTEGER,
			target_version INTEGER NOT NULL,
			status TEXT NOT NULL,
			detail_json TEXT NOT NULL,
			started_at TEXT NOT NULL,
			finished_at TEXT,
			error TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS intents_status_idx ON intents(status, started_at)`,
		`CREATE TABLE IF NOT EXISTS commits (
			commit_id TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			request_hash TEXT NOT NULL,
			version INTEGER NOT NULL,
			result_json TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS recovery_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_json TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS recovery_reports (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			report_json TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("initialize sqlite schema: %w", err)
		}
	}
	return tx.Commit()
}

func (s *Store) InsertIntent(ctx context.Context, intent Intent) error {
	detail, err := json.Marshal(intent.Detail)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO intents
		(id, kind, request_hash, base_version, target_version, status, detail_json, started_at, error)
		VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, '')`,
		intent.ID, intent.Kind, intent.RequestHash, nullableUint(intent.BaseVersion), intent.Target,
		string(detail), formatTime(intent.StartedAt))
	return err
}

func (s *Store) FinishIntent(ctx context.Context, id, status, errorText string) error {
	if status != "completed" && status != "aborted" {
		return fmt.Errorf("invalid intent status %q", status)
	}
	_, err := s.db.ExecContext(ctx, `UPDATE intents SET status = ?, error = ?, finished_at = ? WHERE id = ?`,
		status, errorText, formatTime(time.Now()), id)
	return err
}

func (s *Store) FindCommit(ctx context.Context, commitID string) (*CommitRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT commit_id, kind, request_hash, version, result_json, created_at FROM commits WHERE commit_id = ?`, commitID)
	var record CommitRecord
	var created string
	if err := row.Scan(&record.CommitID, &record.Kind, &record.RequestHash, &record.Version, &record.ResultJSON, &created); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	record.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	return &record, nil
}

func (s *Store) SaveVersion(ctx context.Context, state *planstate.State, intentID string, commit *CommitRecord) error {
	if state == nil {
		return fmt.Errorf("cannot save nil state")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `INSERT INTO snapshot_versions
		(version, snapshot_seq, format, format_version, decision, generated_at, committed_at, tree_hash, root_id, node_count)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		state.Version, state.Version, state.Format, state.FormatVersion, state.Decision,
		formatTime(state.GeneratedAt), formatTime(state.CommittedAt), state.Derived.RootHash,
		state.Root, len(state.Nodes))
	if err != nil {
		return err
	}
	nodes := state.NodeList()
	for _, node := range nodes {
		children, _ := json.Marshal(node.Children)
		columns, _ := json.Marshal(node.Columns)
		var predicate any
		if node.Predicate != nil {
			b, _ := json.Marshal(node.Predicate)
			predicate = string(b)
		}
		var stats any
		if node.Stats != nil {
			b, _ := json.Marshal(node.Stats)
			stats = string(b)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO operators
			(version, node_id, type, children_json, columns_json, predicate_json, stats_json)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			state.Version, node.ID, node.Type, string(children), string(columns), predicate, stats); err != nil {
			return err
		}
	}
	derived := map[string]any{
		"tree_summary":    state.Derived.TreeSummary,
		"column_index":    state.Derived.ColumnIndex,
		"predicate_index": state.Derived.PredicateIndex,
		"cost_estimate":   state.Derived.CostEstimate,
	}
	kinds := make([]string, 0, len(derived))
	for kind := range derived {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		payload, err := json.Marshal(derived[kind])
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO derived_indexes (version, kind, payload_json) VALUES (?, ?, ?)`,
			state.Version, kind, string(payload)); err != nil {
			return err
		}
	}
	for _, degradation := range state.Degradations {
		def, _ := json.Marshal(degradation.Default)
		if _, err := tx.ExecContext(ctx, `INSERT INTO degradations
			(version, record_no, operator_id, field, action, default_json, reason)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			state.Version, degradation.Record, degradation.OperatorID, degradation.Field,
			degradation.Action, string(def), degradation.Reason); err != nil {
			return err
		}
	}
	if commit != nil {
		if _, err := tx.ExecContext(ctx, `INSERT INTO commits
			(commit_id, kind, request_hash, version, result_json, created_at)
			VALUES (?, ?, ?, ?, ?, ?)`,
			commit.CommitID, commit.Kind, commit.RequestHash, commit.Version, commit.ResultJSON,
			formatTime(commit.CreatedAt)); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO meta(key, value) VALUES('current_version', ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`, fmt.Sprintf("%d", state.Version)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status = 'completed', error = '', finished_at = ? WHERE id = ?`,
		formatTime(time.Now()), intentID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) CurrentVersion(ctx context.Context) (uint64, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = 'current_version'`).Scan(&raw)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var version uint64
	_, err = fmt.Sscanf(raw, "%d", &version)
	return version, err
}

func (s *Store) LoadState(ctx context.Context, version uint64) (*planstate.State, error) {
	if version == 0 {
		return nil, nil
	}
	var state planstate.State
	var generated, committed, storedTreeHash string
	err := s.db.QueryRowContext(ctx, `SELECT snapshot_seq, format, format_version, decision, generated_at, committed_at, root_id, tree_hash
		FROM snapshot_versions WHERE version = ?`, version).Scan(
		&state.Version, &state.Format, &state.FormatVersion, &state.Decision, &generated, &committed, &state.Root, &storedTreeHash)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	state.GeneratedAt, _ = time.Parse(time.RFC3339Nano, generated)
	state.CommittedAt, _ = time.Parse(time.RFC3339Nano, committed)

	rows, err := s.db.QueryContext(ctx, `SELECT node_id, type, children_json, columns_json, predicate_json, stats_json
		FROM operators WHERE version = ? ORDER BY node_id`, version)
	if err != nil {
		return nil, err
	}
	var nodes []*model.Node
	for rows.Next() {
		var node model.Node
		var nodeType, childrenJSON, columnsJSON string
		var predicateJSON, statsJSON sql.NullString
		if err := rows.Scan(&node.ID, &nodeType, &childrenJSON, &columnsJSON, &predicateJSON, &statsJSON); err != nil {
			rows.Close()
			return nil, err
		}
		node.Type = model.OperatorType(nodeType)
		if err := json.Unmarshal([]byte(childrenJSON), &node.Children); err != nil {
			rows.Close()
			return nil, err
		}
		if err := json.Unmarshal([]byte(columnsJSON), &node.Columns); err != nil {
			rows.Close()
			return nil, err
		}
		if predicateJSON.Valid {
			node.Predicate = &model.Predicate{}
			if err := json.Unmarshal([]byte(predicateJSON.String), node.Predicate); err != nil {
				rows.Close()
				return nil, err
			}
		}
		if statsJSON.Valid {
			node.Stats = &model.StatsBinding{}
			if err := json.Unmarshal([]byte(statsJSON.String), node.Stats); err != nil {
				rows.Close()
				return nil, err
			}
		}
		nodes = append(nodes, &node)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	plan, err := model.Analyze(nodes)
	if err != nil {
		return nil, fmt.Errorf("persisted operator tree is invalid: %w", err)
	}
	state.Nodes = plan.Nodes
	state.Outputs = plan.Outputs

	derived, err := s.loadDerived(ctx, version, state.Root)
	if err != nil {
		return nil, err
	}
	state.Derived = derived
	if state.Derived.RootHash != storedTreeHash {
		return nil, fmt.Errorf("persisted tree hash %s does not match derived root hash %s", storedTreeHash, state.Derived.RootHash)
	}
	state.Degradations, err = s.loadDegradations(ctx, version)
	if err != nil {
		return nil, err
	}
	return &state, nil
}

func (s *Store) loadDerived(ctx context.Context, version uint64, root string) (planstate.Derived, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT kind, payload_json FROM derived_indexes WHERE version = ?`, version)
	if err != nil {
		return planstate.Derived{}, err
	}
	defer rows.Close()
	var derived planstate.Derived
	seen := map[string]bool{}
	for rows.Next() {
		var kind, payload string
		if err := rows.Scan(&kind, &payload); err != nil {
			return planstate.Derived{}, err
		}
		seen[kind] = true
		switch kind {
		case "tree_summary":
			if err := json.Unmarshal([]byte(payload), &derived.TreeSummary); err != nil {
				return planstate.Derived{}, err
			}
		case "column_index":
			if err := json.Unmarshal([]byte(payload), &derived.ColumnIndex); err != nil {
				return planstate.Derived{}, err
			}
		case "predicate_index":
			if err := json.Unmarshal([]byte(payload), &derived.PredicateIndex); err != nil {
				return planstate.Derived{}, err
			}
		case "cost_estimate":
			if err := json.Unmarshal([]byte(payload), &derived.CostEstimate); err != nil {
				return planstate.Derived{}, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return planstate.Derived{}, err
	}
	for _, kind := range []string{"tree_summary", "column_index", "predicate_index", "cost_estimate"} {
		if !seen[kind] {
			return planstate.Derived{}, fmt.Errorf("version %d is missing derived index %s", version, kind)
		}
	}
	derived.RootHash = derived.TreeSummary[root]
	return derived, nil
}

func (s *Store) loadDegradations(ctx context.Context, version uint64) ([]model.Degradation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT record_no, operator_id, field, action, default_json, reason
		FROM degradations WHERE version = ? ORDER BY record_no, operator_id, field`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Degradation
	for rows.Next() {
		var d model.Degradation
		var defaultJSON string
		if err := rows.Scan(&d.Record, &d.OperatorID, &d.Field, &d.Action, &defaultJSON, &d.Reason); err != nil {
			return nil, err
		}
		if defaultJSON != "" && defaultJSON != "null" {
			_ = json.Unmarshal([]byte(defaultJSON), &d.Default)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func nullableUint(v *uint64) any {
	if v == nil {
		return nil
	}
	return *v
}

func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

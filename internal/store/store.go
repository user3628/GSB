package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"deployverify/internal/derive"
	"deployverify/internal/model"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

type Intent struct {
	ID           string `json:"id"`
	Kind         string `json:"kind"`
	AggregateKey string `json:"aggregateKey"`
	Status       string `json:"status"`
	LastError    string `json:"lastError,omitempty"`
	CreatedAt    string `json:"createdAt"`
	UpdatedAt    string `json:"updatedAt"`
}

type RecoveryReport struct {
	PendingIntents   []string `json:"pendingIntents"`
	AbortedIntents   []string `json:"abortedIntents"`
	InterruptedRuns  []string `json:"interruptedRuns"`
	RestoredVersion  int64    `json:"restoredVersion"`
	RecoveryRecordID int64    `json:"recoveryRecordId"`
	RecoveredAt      string   `json:"recoveredAt"`
}

func DSN(path string) string {
	if path == ":memory:" {
		return ":memory:?_pragma=busy_timeout(5000)&_pragma=journal_mode(MEMORY)&_pragma=foreign_keys(ON)"
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}
	slashPath := filepath.ToSlash(abs)
	// Windows drive paths such as C:/tmp/platform.db become /C:/tmp/platform.db for file:///.
	if len(slashPath) >= 2 && slashPath[1] == ':' {
		slashPath = "/" + strings.TrimPrefix(slashPath, "/")
	}
	fileURL := url.URL{Scheme: "file", Path: slashPath}
	uri := fileURL.String()
	return uri + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)"
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate sqlite: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func NewIntentID(prefix string) string {
	return prefix + "-" + uuid.NewString()
}

func nowISO() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func (s *Store) CurrentVersion(ctx context.Context) (int64, error) {
	var value string
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='current_version'`).Scan(&value); err != nil {
		return 0, err
	}
	var version int64
	_, err := fmt.Sscan(value, &version)
	return version, err
}

func (s *Store) InsertIntent(ctx context.Context, kind, aggregateKey string, payload any) (string, error) {
	id := NewIntentID("intent")
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	now := nowISO()
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO intents(id, kind, aggregate_key, payload_json, status, created_at, updated_at)
		 VALUES(?, ?, ?, ?, 'pending', ?, ?)`,
		id, kind, aggregateKey, string(data), now, now)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (s *Store) finishIntent(ctx context.Context, id, status, lastError string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE intents SET status=?, last_error=?, updated_at=? WHERE id=?`,
		status, lastError, nowISO(), id)
	return err
}

func (s *Store) ListIntents(ctx context.Context, status string) ([]Intent, error) {
	query := `SELECT id, kind, aggregate_key, status, last_error, created_at, updated_at FROM intents`
	args := []any{}
	if status != "" {
		query += ` WHERE status=?`
		args = append(args, status)
	}
	query += ` ORDER BY created_at ASC, id ASC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Intent{}
	for rows.Next() {
		var item Intent
		var lastError sql.NullString
		if err := rows.Scan(&item.ID, &item.Kind, &item.AggregateKey, &item.Status, &lastError, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		item.LastError = lastError.String
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) Recover(ctx context.Context) (*RecoveryReport, error) {
	report := &RecoveryReport{PendingIntents: []string{}, AbortedIntents: []string{}, InterruptedRuns: []string{}, RecoveredAt: nowISO()}
	recoveryIntentID, err := s.InsertIntent(ctx, "recovery", "startup-or-admin", map[string]string{"at": report.RecoveredAt})
	if err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		_ = s.finishIntent(context.Background(), recoveryIntentID, "aborted", err.Error())
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id, kind, aggregate_key FROM intents WHERE status='pending' AND id <> ? ORDER BY created_at ASC`, recoveryIntentID)
	if err != nil {
		return nil, err
	}
	type pending struct{ id, kind, key string }
	pendingItems := []pending{}
	for rows.Next() {
		var item pending
		if err := rows.Scan(&item.id, &item.kind, &item.key); err != nil {
			rows.Close()
			return nil, err
		}
		pendingItems = append(pendingItems, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, item := range pendingItems {
		report.PendingIntents = append(report.PendingIntents, item.id)
		message := "process restarted before intent was committed; SQLite transaction was rolled back"
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='aborted', last_error=?, updated_at=? WHERE id=?`, message, nowISO(), item.id); err != nil {
			return nil, err
		}
		report.AbortedIntents = append(report.AbortedIntents, item.id)
		if item.kind == "probe_round" {
			result, err := tx.ExecContext(ctx,
				`UPDATE probe_runs SET status='interrupted', finished_at=?, report_json=json_set(report_json, '$.status', 'interrupted') WHERE id=? AND status IN ('running','queued')`,
				nowISO(), item.key)
			if err != nil {
				return nil, err
			}
			if n, _ := result.RowsAffected(); n > 0 {
				report.InterruptedRuns = append(report.InterruptedRuns, item.key)
			}
		}
	}
	var versionText string
	if err := tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='current_version'`).Scan(&versionText); err != nil {
		return nil, err
	}
	fmt.Sscan(versionText, &report.RestoredVersion)
	abortedJSON, _ := json.Marshal(report.AbortedIntents)
	res, err := tx.ExecContext(ctx, `INSERT INTO recovery_events(pending_count, aborted_json, restored_version, created_at) VALUES(?, ?, ?, ?)`,
		len(report.PendingIntents), string(abortedJSON), report.RestoredVersion, nowISO())
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	report.RecoveryRecordID = id
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='committed', updated_at=? WHERE id=?`, nowISO(), recoveryIntentID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return report, nil
}

func (s *Store) CommitSnapshot(ctx context.Context, snapshot *model.Snapshot) (bundle *derive.Bundle, err error) {
	intentID, err := s.InsertIntent(ctx, "snapshot", fmt.Sprintf("version:%d", snapshot.Version), snapshot)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = s.finishIntent(context.Background(), intentID, "aborted", err.Error())
		}
	}()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	current, err := currentVersionTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	if snapshot.Version <= current {
		err = model.NewError(model.CodeConflict, "snapshot version is not newer than current version", map[string]any{"currentVersion": current, "snapshotVersion": snapshot.Version})
		return nil, err
	}
	previous, err := loadSnapshotTx(ctx, tx, current)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if errors.Is(err, sql.ErrNoRows) || current == 0 {
		previous = nil
	}
	if err := replaceStateTx(ctx, tx, snapshot); err != nil {
		return nil, err
	}
	bundle = derive.BuildBundle(snapshot, previous, nil)
	if err := replaceDerivedTx(ctx, tx, snapshot, bundle); err != nil {
		return nil, err
	}
	payload, _ := json.Marshal(snapshot)
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO snapshots(version, kind, generated_at, checksum, payload_json, created_at) VALUES(?, 'snapshot', ?, ?, ?, ?)
		 ON CONFLICT(version) DO UPDATE SET kind=excluded.kind, generated_at=excluded.generated_at, checksum=excluded.checksum, payload_json=excluded.payload_json, created_at=excluded.created_at`,
		snapshot.Version, snapshot.GeneratedAt.UTC().Format(time.RFC3339Nano), snapshot.Checksum, string(payload), nowISO()); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE meta SET value=? WHERE key='current_version'`, fmt.Sprint(snapshot.Version)); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='committed', updated_at=? WHERE id=?`, nowISO(), intentID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return bundle, nil
}

type IncrementCommit struct {
	Increment *model.Increment
	Candidate *model.Snapshot
	Changes   []derive.Change
}

func (s *Store) CommitIncrement(ctx context.Context, commit IncrementCommit) (bundle *derive.Bundle, err error) {
	increment := commit.Increment
	candidate := commit.Candidate
	intentID, err := s.InsertIntent(ctx, "increment", fmt.Sprintf("base:%d", increment.BaseVersion), increment)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			code := model.CodeInternal
			var apiErr *model.APIError
			if errors.As(err, &apiErr) {
				code = apiErr.Code
			}
			_ = s.finishIntent(context.Background(), intentID, "aborted", code+": "+err.Error())
		}
	}()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	current, err := currentVersionTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	if current != increment.BaseVersion {
		err = model.NewError(model.CodeStaleBase, "increment base version does not match current version", map[string]any{
			"baseVersion": increment.BaseVersion, "currentVersion": current,
		})
		return nil, err
	}
	previous, err := loadSnapshotTx(ctx, tx, current)
	if err != nil {
		return nil, err
	}
	changes := commit.Changes
	if changes == nil {
		changes = []derive.Change{}
	}
	if err := replaceStateTx(ctx, tx, candidate); err != nil {
		return nil, err
	}
	bundle = derive.BuildBundle(candidate, previous, changes)
	if err := replaceDerivedTx(ctx, tx, candidate, bundle); err != nil {
		return nil, err
	}
	payload, _ := json.Marshal(increment)
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO snapshots(version, kind, generated_at, checksum, payload_json, created_at) VALUES(?, 'increment', ?, ?, ?, ?)`,
		candidate.Version, increment.GeneratedAt.UTC().Format(time.RFC3339Nano), increment.Checksum, string(payload), nowISO()); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE meta SET value=? WHERE key='current_version'`, fmt.Sprint(candidate.Version)); err != nil {
		return nil, err
	}
	if bundle.Cascade != nil {
		cascadeJSON, _ := json.Marshal(bundle.Cascade)
		if _, err := tx.ExecContext(ctx, `INSERT INTO cascade_events(version, payload_json, created_at) VALUES(?, ?, ?)`, candidate.Version, string(cascadeJSON), nowISO()); err != nil {
			return nil, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='committed', updated_at=? WHERE id=?`, nowISO(), intentID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return bundle, nil
}

func currentVersionTx(ctx context.Context, tx *sql.Tx) (int64, error) {
	var value string
	if err := tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='current_version'`).Scan(&value); err != nil {
		return 0, err
	}
	var version int64
	_, err := fmt.Sscan(value, &version)
	return version, err
}

func replaceStateTx(ctx context.Context, tx *sql.Tx, snapshot *model.Snapshot) error {
	for _, statement := range []string{
		`DELETE FROM dependencies`, `DELETE FROM config_kv`, `DELETE FROM units`,
		`DELETE FROM environments`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	for _, env := range snapshot.Environments {
		if _, err := tx.ExecContext(ctx, `INSERT INTO environments(name, priority) VALUES(?, ?)`, env.Name, env.Priority); err != nil {
			return err
		}
		for _, unit := range env.Units {
			probeJSON := "{}"
			if unit.Probe != nil {
				data, _ := json.Marshal(unit.Probe)
				probeJSON = string(data)
			}
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO units(environment, service, version, image_digest, desired_version, probe_json)
				 VALUES(?, ?, ?, ?, ?, ?)`,
				env.Name, unit.Service, snapshot.Version, unit.ImageDigest, unit.DesiredVersion, probeJSON); err != nil {
				return err
			}
			keys := make([]string, 0, len(unit.Config))
			for key := range unit.Config {
				keys = append(keys, key)
			}
			for _, key := range keys {
				if _, err := tx.ExecContext(ctx, `INSERT INTO config_kv(environment, service, config_key, config_value) VALUES(?, ?, ?, ?)`,
					env.Name, unit.Service, key, unit.Config[key]); err != nil {
					return err
				}
			}
			for ordinal, dep := range unit.DependsOn {
				if _, err := tx.ExecContext(ctx, `INSERT INTO dependencies(environment, service, depends_on, ordinal) VALUES(?, ?, ?, ?)`,
					env.Name, unit.Service, dep, ordinal); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func replaceDerivedTx(ctx context.Context, tx *sql.Tx, snapshot *model.Snapshot, bundle *derive.Bundle) error {
	for _, statement := range []string{`DELETE FROM artifact_refs`, `DELETE FROM plans`, `DELETE FROM topologies`, `DELETE FROM scores`} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	for _, plan := range bundle.Plans {
		data, _ := json.Marshal(plan)
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO plans(version, environment, service, layer, action, status, payload_json) VALUES(?, ?, ?, ?, ?, ?, ?)`,
			plan.Version, plan.Environment, plan.Service, plan.Layer, plan.Action, plan.Status, string(data)); err != nil {
			return err
		}
	}
	for _, topology := range bundle.Topologies {
		data, _ := json.Marshal(topology)
		if _, err := tx.ExecContext(ctx, `INSERT INTO topologies(version, environment, status, payload_json) VALUES(?, ?, ?, ?)`,
			bundle.Version, topology.Environment, topology.Status, string(data)); err != nil {
			return err
		}
	}
	for _, score := range bundle.Scores {
		data, _ := json.Marshal(score)
		if _, err := tx.ExecContext(ctx, `INSERT INTO scores(version, service, score, consistent, payload_json) VALUES(?, ?, ?, ?, ?)`,
			bundle.Version, score.Service, score.Score, boolToInt(score.Consistent), string(data)); err != nil {
			return err
		}
	}
	allRefs := append(append([]derive.ArtifactRef{}, bundle.Refs...), derive.BuildConfigRefs(bundle.Version, snapshot)...)
	for _, ref := range allRefs {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO artifact_refs(version, artifact_type, artifact_key, environment, service, unit_env, unit_service, config_key)
			 VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
			ref.Version, ref.ArtifactType, ref.ArtifactKey, ref.Environment, ref.Service, ref.UnitEnv, ref.UnitService, ref.ConfigKey); err != nil {
			return err
		}
	}
	return nil
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

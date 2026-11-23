// Package store provides the SQLite persistence boundary. Complete statistics
// snapshots are immutable rows; a release header points at exactly one complete
// version. This is what prevents readers from observing mixed quantile and
// difference-estimate generations.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"releasegate/domain"
)

const (
	BatchStaging   = "STAGING"
	BatchCommitted = "COMMITTED"
	BatchAborted   = "ABORTED"

	StatsComplete = "COMPLETE"
	StatsRetired  = "RETIRED"

	IntentPending   = "PENDING"
	IntentCompleted = "COMPLETED"
	IntentFailed    = "FAILED"
	IntentRecovered = "RECOVERED"

	RetainedVersions = 8
)

type DBTX interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type Store struct {
	db   *sql.DB
	path string
	mu   sync.Mutex // recovery gate
}

func (s *Store) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, query, args...)
}

func (s *Store) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, query, args...)
}

func (s *Store) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return s.db.QueryRowContext(ctx, query, args...)
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("sqlite path is required")
	}
	if path != ":memory:" {
		if err := ensureDir(path); err != nil {
			return nil, fmt.Errorf("create SQLite directory: %w", err)
		}
	}
	dsn := sqliteDSN(path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open SQLite: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping SQLite: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout=10000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set busy timeout: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set WAL: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA synchronous=FULL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set synchronous: %w", err)
	}
	if err := migrate(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, path: path}, nil
}

func sqliteDSN(path string) string {
	if path == ":memory:" {
		return "file:releasegate-memory?mode=memory&cache=shared&_pragma=busy_timeout(10000)&_pragma=foreign_keys(ON)"
	}
	abs, err := filepath.Abs(path)
	if err == nil {
		path = abs
	}
	slashPath := filepath.ToSlash(path)
	if !strings.HasPrefix(slashPath, "/") {
		slashPath = "/" + slashPath
	}
	u := url.URL{Scheme: "file", Path: slashPath}
	q := u.Query()
	q.Add("_pragma", "busy_timeout(10000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "synchronous(FULL)")
	u.RawQuery = q.Encode()
	return u.String()
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) BeginTx(ctx context.Context, readOnly bool) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: readOnly})
}

func (s *Store) CreateRelease(ctx context.Context, tx *sql.Tx, rel domain.Release) error {
	def, err := json.Marshal(rel.Definition)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO releases
		(release_id, definition_json, state, current_stage, traffic_per_mille, current_stats_version, created_at_ms, updated_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		rel.ReleaseID, string(def), rel.State, rel.CurrentStage, rel.TrafficPerMille, rel.CurrentStatsVersion, rel.CreatedAtMS, rel.UpdatedAtMS)
	if err != nil {
		return translateWriteErr(err)
	}
	return nil
}

func (s *Store) GetRelease(ctx context.Context, q DBTX, releaseID string) (domain.Release, error) {
	var rel domain.Release
	var defJSON string
	err := q.QueryRowContext(ctx, `SELECT release_id, definition_json, state, current_stage,
		traffic_per_mille, current_stats_version, created_at_ms, updated_at_ms
		FROM releases WHERE release_id = ?`, releaseID).Scan(
		&rel.ReleaseID, &defJSON, &rel.State, &rel.CurrentStage,
		&rel.TrafficPerMille, &rel.CurrentStatsVersion, &rel.CreatedAtMS, &rel.UpdatedAtMS)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Release{}, domain.WithContext(domain.NewError(domain.CodeNotFound, "release not found"), "release_id", releaseID)
	}
	if err != nil {
		return domain.Release{}, err
	}
	if err := json.Unmarshal([]byte(defJSON), &rel.Definition); err != nil {
		return domain.Release{}, fmt.Errorf("decode release definition: %w", err)
	}
	return rel, nil
}

func (s *Store) UpdateRelease(ctx context.Context, tx *sql.Tx, rel domain.Release) error {
	res, err := tx.ExecContext(ctx, `UPDATE releases SET state=?, current_stage=?, traffic_per_mille=?,
		current_stats_version=?, updated_at_ms=? WHERE release_id=?`,
		rel.State, rel.CurrentStage, rel.TrafficPerMille, rel.CurrentStatsVersion, rel.UpdatedAtMS, rel.ReleaseID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return domain.NewError(domain.CodeNotFound, "release disappeared during update")
	}
	return nil
}

type BatchRecord struct {
	ReleaseID        string
	BatchID          string
	Stage            int
	Status           string
	ContentHash      string
	ObservationCount int
	StatsVersion     int64
	ReceivedAtMS     int64
}

func (s *Store) GetBatch(ctx context.Context, q DBTX, releaseID, batchID string) (BatchRecord, bool, error) {
	var b BatchRecord
	err := q.QueryRowContext(ctx, `SELECT release_id, batch_id, stage, status, content_hash,
		observation_count, COALESCE(stats_version,0), received_at_ms
		FROM observation_batches WHERE release_id=? AND batch_id=?`, releaseID, batchID).Scan(
		&b.ReleaseID, &b.BatchID, &b.Stage, &b.Status, &b.ContentHash,
		&b.ObservationCount, &b.StatsVersion, &b.ReceivedAtMS)
	if errors.Is(err, sql.ErrNoRows) {
		return BatchRecord{}, false, nil
	}
	if err != nil {
		return BatchRecord{}, false, err
	}
	return b, true, nil
}

// InsertStagingBatch durably stores an unreachable batch. It is deliberately a
// separate transaction: a crash before the main commit leaves an orphan that
// recovery can identify and abort.
func (s *Store) InsertStagingBatch(ctx context.Context, batch domain.Batch, contentHash string, receivedAtMS int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO observation_batches
		(release_id,batch_id,stage,status,content_hash,observation_count,stats_version,received_at_ms)
		VALUES (?,?,?,?,?,?,0,?)`,
		batch.ReleaseID, batch.BatchID, batch.Stage, BatchStaging, contentHash, len(batch.Observations), receivedAtMS); err != nil {
		return translateWriteErr(err)
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO observations
		(release_id,batch_id,seq,stage,timestamp_ms,instance_id,group_name,success,latency_us)
		VALUES (?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, o := range batch.Observations {
		success := 0
		if o.Success {
			success = 1
		}
		if _, err := stmt.ExecContext(ctx, batch.ReleaseID, o.BatchID, o.Seq, o.Stage, o.TimestampMS, o.InstanceID, o.Group, success, o.LatencyUS); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) MarkBatchCommitted(ctx context.Context, tx *sql.Tx, releaseID, batchID string, version, committedAtMS int64) error {
	res, err := tx.ExecContext(ctx, `UPDATE observation_batches SET status=?, stats_version=?, committed_at_ms=?
		WHERE release_id=? AND batch_id=? AND status=?`, BatchCommitted, version, committedAtMS, releaseID, batchID, BatchStaging)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return domain.NewError(domain.CodeConflictingBatch, "batch is no longer in STAGING state")
	}
	return nil
}

func (s *Store) MarkBatchAborted(ctx context.Context, releaseID, batchID, reason string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE observation_batches SET status=?, abort_reason=?
		WHERE release_id=? AND batch_id=? AND status=?`, BatchAborted, reason, releaseID, batchID, BatchStaging)
	return err
}

// LoadCommittedObservations loads only COMMITTED batches. Rows are ordered by a
// canonical key so cross-stage processing cannot depend on arrival order.
func (s *Store) LoadCommittedObservations(ctx context.Context, q DBTX, releaseID string, maxStage int) ([]domain.Observation, error) {
	rows, err := q.QueryContext(ctx, `SELECT o.stage,o.timestamp_ms,o.instance_id,o.group_name,o.success,o.latency_us,o.batch_id,o.seq
		FROM observations o JOIN observation_batches b
		ON b.release_id=o.release_id AND b.batch_id=o.batch_id
		WHERE o.release_id=? AND b.status=? AND o.stage<=?
		ORDER BY o.stage, o.timestamp_ms, o.instance_id, o.group_name, o.batch_id, o.seq`,
		releaseID, BatchCommitted, maxStage)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Observation
	for rows.Next() {
		var o domain.Observation
		var success int
		if err := rows.Scan(&o.Stage, &o.TimestampMS, &o.InstanceID, &o.Group, &success, &o.LatencyUS, &o.BatchID, &o.Seq); err != nil {
			return nil, err
		}
		o.Success = success != 0
		out = append(out, o)
	}
	return out, rows.Err()
}

func (s *Store) MaxCommittedTimestamp(ctx context.Context, q DBTX, releaseID string, stage int) (int64, error) {
	var value sql.NullInt64
	err := q.QueryRowContext(ctx, `SELECT MAX(o.timestamp_ms)
		FROM observations o JOIN observation_batches b
		ON b.release_id=o.release_id AND b.batch_id=o.batch_id
		WHERE o.release_id=? AND b.status=? AND o.stage=?`, releaseID, BatchCommitted, stage).Scan(&value)
	if err != nil {
		return 0, err
	}
	if !value.Valid {
		return -1, nil
	}
	return value.Int64, nil
}

func (s *Store) InsertStatsVersion(ctx context.Context, tx *sql.Tx, snapshot domain.StatsSnapshot) error {
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO stats_versions
		(release_id,version,status,snapshot_json,committed_at_ms) VALUES (?,?,?,?,?)`,
		snapshot.ReleaseID, snapshot.Version, StatsComplete, string(payload), snapshot.CreatedAtMS)
	if err != nil {
		return translateWriteErr(err)
	}
	return nil
}

func (s *Store) RetireOldVersions(ctx context.Context, tx *sql.Tx, releaseID string, currentVersion int64) error {
	cutoff := currentVersion - RetainedVersions
	if cutoff <= 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, `UPDATE stats_versions SET status=?
		WHERE release_id=? AND version<=? AND status=?`, StatsRetired, releaseID, cutoff, StatsComplete)
	return err
}

// ReadSnapshot resolves an explicitly bound version or the current version in
// one SQLite read snapshot. A single JSON row carries metrics, estimates and
// error contracts, so readers can never observe a mixed generation.
func (s *Store) ReadSnapshot(ctx context.Context, releaseID string, requestedVersion int64) (domain.Release, domain.StatsSnapshot, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return domain.Release{}, domain.StatsSnapshot{}, err
	}
	defer tx.Rollback()
	rel, err := s.GetRelease(ctx, tx, releaseID)
	if err != nil {
		return domain.Release{}, domain.StatsSnapshot{}, err
	}
	version := requestedVersion
	if version == 0 {
		version = rel.CurrentStatsVersion
	}
	if version <= 0 {
		return rel, domain.StatsSnapshot{}, domain.WithContext(domain.NewError(domain.CodeInsufficientSamples, "release has no committed statistics version"), "stats_version", version)
	}
	if version > rel.CurrentStatsVersion {
		return rel, domain.StatsSnapshot{}, domain.WithContext(domain.NewError(domain.CodeVersionRetired, "requested statistics version has not been committed"), "stats_version", version)
	}
	var status, payload string
	err = tx.QueryRowContext(ctx, `SELECT status,snapshot_json FROM stats_versions WHERE release_id=? AND version=?`, releaseID, version).Scan(&status, &payload)
	if errors.Is(err, sql.ErrNoRows) || status == StatsRetired {
		return rel, domain.StatsSnapshot{}, domain.WithContext(domain.NewError(domain.CodeVersionRetired, "requested statistics version was retired or does not exist"), "stats_version", version)
	}
	if err != nil {
		return rel, domain.StatsSnapshot{}, err
	}
	var snap domain.StatsSnapshot
	if err := json.Unmarshal([]byte(payload), &snap); err != nil {
		return rel, domain.StatsSnapshot{}, fmt.Errorf("decode stats snapshot: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.Release{}, domain.StatsSnapshot{}, err
	}
	return rel, snap, nil
}

func (s *Store) SnapshotByVersion(ctx context.Context, q DBTX, releaseID string, version int64) (domain.StatsSnapshot, error) {
	var status, payload string
	err := q.QueryRowContext(ctx, `SELECT status,snapshot_json FROM stats_versions WHERE release_id=? AND version=?`, releaseID, version).Scan(&status, &payload)
	if errors.Is(err, sql.ErrNoRows) || status == StatsRetired {
		return domain.StatsSnapshot{}, domain.WithContext(domain.NewError(domain.CodeVersionRetired, "statistics version retired"), "stats_version", version)
	}
	if err != nil {
		return domain.StatsSnapshot{}, err
	}
	var snap domain.StatsSnapshot
	if err := json.Unmarshal([]byte(payload), &snap); err != nil {
		return domain.StatsSnapshot{}, err
	}
	return snap, nil
}

func (s *Store) InsertDecision(ctx context.Context, tx *sql.Tx, d *domain.GateDecision) error {
	measured, err := json.Marshal(d.Measured)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO decisions
		(release_id,stats_version,stage,scope,outcome,code,triggered_threshold,measured_json,from_state,to_state,from_stage,to_stage,from_traffic_per_mille,to_traffic_per_mille,created_at_ms)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		d.ReleaseID, d.StatsVersion, d.Stage, d.Scope, d.Outcome, d.Code, d.TriggeredThreshold, string(measured),
		d.FromState, d.ToState, d.FromStage, d.ToStage, d.FromTrafficPerMille, d.ToTrafficPerMille, d.CreatedAtMS)
	if err != nil {
		return err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	d.ID = id
	return nil
}

func (s *Store) ListDecisions(ctx context.Context, releaseID string) ([]domain.GateDecision, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,release_id,stats_version,stage,scope,outcome,code,triggered_threshold,measured_json,
		from_state,to_state,from_stage,to_stage,from_traffic_per_mille,to_traffic_per_mille,created_at_ms
		FROM decisions WHERE release_id=? ORDER BY id`, releaseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.GateDecision
	for rows.Next() {
		var d domain.GateDecision
		var measured string
		if err := rows.Scan(&d.ID, &d.ReleaseID, &d.StatsVersion, &d.Stage, &d.Scope, &d.Outcome, &d.Code, &d.TriggeredThreshold, &measured,
			&d.FromState, &d.ToState, &d.FromStage, &d.ToStage, &d.FromTrafficPerMille, &d.ToTrafficPerMille, &d.CreatedAtMS); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(measured), &d.Measured); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) Checkpoint(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}

func translateWriteErr(err error) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "unique constraint") || strings.Contains(msg, "constraint failed") {
		return domain.WithContext(domain.NewError(domain.CodeConflictingBatch, "unique constraint violated"), "detail", err.Error())
	}
	return err
}

func ensureDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

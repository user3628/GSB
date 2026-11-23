package store

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"recoveryengine/internal/fixed"
	"recoveryengine/internal/graph"
	"recoveryengine/internal/model"
	"recoveryengine/internal/spec"
)

var (
	ErrConflictingRequest = errors.New("idempotency key was already used with different content")
	ErrVersionConflict    = errors.New("committed version changed")
)

//go:embed schema.sql
var schemaFS embed.FS

type Store struct {
	db *sql.DB
}

type Submission struct {
	Key          string
	Kind         string
	RequestHash  string
	State        string
	Version      sql.NullInt64
	HTTPStatus   int
	ResponseJSON []byte
	ErrorJSON    []byte
	CreatedAt    string
	UpdatedAt    string
}

type Intent struct {
	ID            string
	SubmissionKey string
	Kind          string
	BaseVersion   int64
	TargetVersion int64
	PayloadJSON   []byte
	Phase         string
	State         string
	ErrorJSON     []byte
	CreatedAt     string
	CompletedAt   string
}

type IntentSummary struct {
	ID            string          `json:"id"`
	SubmissionKey string          `json:"submission_key"`
	Kind          string          `json:"kind"`
	BaseVersion   int64           `json:"base_version"`
	TargetVersion int64           `json:"target_version"`
	Phase         string          `json:"phase"`
	State         string          `json:"state"`
	Payload       json.RawMessage `json:"payload"`
	Error         json.RawMessage `json:"error,omitempty"`
	CreatedAt     string          `json:"created_at"`
	CompletedAt   string          `json:"completed_at,omitempty"`
}

type RebuildRecord struct {
	Trigger     any        `json:"trigger"`
	ChangedRefs []spec.Ref `json:"changed_refs"`
	Invalidated []string   `json:"invalidated"`
	Rebuilt     []string   `json:"rebuilt"`
	Strategy    string     `json:"strategy"`
	Status      string     `json:"status"`
}

type VersionCommit struct {
	ExpectedCurrent int64
	Mode            string
	Snapshot        model.Snapshot
	Duplicates      []model.DuplicateComponent
	Analysis        *graph.Analysis
	Consistency     fixed.Result
	Refs            []RefRecord
	Rebuild         RebuildRecord
	DerivedDigest   string
	SubmissionKey   string
	IntentID        string
	HTTPStatus      int
	ResponseJSON    []byte
	CreatedAt       string
}

type RefRecord struct {
	DerivedKind string
	DerivedKey  string
	RefKind     string
	RefKey      string
	Field       string
}

type DrillRecord struct {
	ID            string
	SubmissionKey string
	Version       int64
	State         string
	RequestJSON   []byte
	ResultJSON    []byte
	StartedAt     string
	CompletedAt   string
}

type ReservationAudit struct {
	ComponentID   string `json:"component_id"`
	CPUMillicores int64  `json:"cpu_millicores"`
	MemoryBytes   int64  `json:"memory_bytes"`
	State         string `json:"state"`
	Reason        string `json:"reason"`
}

type RecoveryReport struct {
	ID                   int64           `json:"id"`
	StartedAt            string          `json:"started_at"`
	LastCommittedVersion int64           `json:"last_committed_version"`
	ScannedIntents       int             `json:"scanned_intents"`
	Aborted              json.RawMessage `json:"aborted"`
	Facts                json.RawMessage `json:"facts"`
}

type LineageRef struct {
	DerivedKind string `json:"derived_kind"`
	DerivedKey  string `json:"derived_key"`
	RefKind     string `json:"ref_kind"`
	RefKey      string `json:"ref_key"`
	Field       string `json:"field"`
}

type SnapshotSummary struct {
	Version       int64  `json:"version"`
	GeneratedAt   string `json:"generated_at"`
	Checksum      string `json:"checksum_sha256"`
	DerivedDigest string `json:"derived_digest"`
	CreatedAt     string `json:"created_at"`
}

func Open(ctx context.Context, path string) (*Store, error) {
	dsn := path
	if !strings.Contains(path, "?") {
		dsn += "?"
	} else {
		dsn += "&"
	}
	dsn += "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(0)
	store := &Store{db: db}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	schema, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("read embedded schema: %w", err)
	}
	if _, err := db.ExecContext(ctx, string(schema)); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize schema: %w", err)
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) CurrentVersion(ctx context.Context) (int64, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = 'current_version'`).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var version int64
	if _, err := fmt.Sscan(value, &version); err != nil {
		return 0, fmt.Errorf("invalid current_version meta value: %w", err)
	}
	return version, nil
}

func (s *Store) CurrentSnapshot(ctx context.Context) (model.Snapshot, []model.DuplicateComponent, string, bool, error) {
	version, err := s.CurrentVersion(ctx)
	if err != nil {
		return model.Snapshot{}, nil, "", false, err
	}
	if version == 0 {
		return model.Snapshot{}, nil, "", false, nil
	}
	var snapshotJSON, duplicatesJSON, digest string
	err = s.db.QueryRowContext(ctx, `SELECT snapshot_json, duplicates_json, derived_digest FROM snapshots WHERE version = ?`, version).
		Scan(&snapshotJSON, &duplicatesJSON, &digest)
	if err != nil {
		return model.Snapshot{}, nil, "", false, err
	}
	var snapshot model.Snapshot
	if err := json.Unmarshal([]byte(snapshotJSON), &snapshot); err != nil {
		return model.Snapshot{}, nil, "", false, err
	}
	var duplicates []model.DuplicateComponent
	if err := json.Unmarshal([]byte(duplicatesJSON), &duplicates); err != nil {
		return model.Snapshot{}, nil, "", false, err
	}
	return snapshot, duplicates, digest, true, nil
}

func (s *Store) CommitVersion(ctx context.Context, commit VersionCommit) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var current int64
	var rawCurrent string
	err = tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = 'current_version'`).Scan(&rawCurrent)
	if errors.Is(err, sql.ErrNoRows) {
		current = 0
	} else if err != nil {
		return err
	} else if _, err := fmt.Sscan(rawCurrent, &current); err != nil {
		return err
	}
	if current != commit.ExpectedCurrent {
		return ErrVersionConflict
	}
	switch commit.Mode {
	case "increment":
		if commit.Snapshot.Version != current+1 {
			return ErrVersionConflict
		}
	case "snapshot":
		if commit.Snapshot.Version <= current {
			return ErrVersionConflict
		}
	default:
		return fmt.Errorf("unknown commit mode %q", commit.Mode)
	}

	snapshotJSON, err := json.Marshal(commit.Snapshot)
	if err != nil {
		return err
	}
	duplicatesJSON, err := json.Marshal(commit.Duplicates)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO snapshots(version, generated_at, checksum, snapshot_json, duplicates_json, derived_digest, status, created_at)
		VALUES(?, ?, ?, ?, ?, ?, 'committed', ?)`,
		commit.Snapshot.Version, commit.Snapshot.GeneratedAt, commit.Snapshot.ChecksumSHA256,
		string(snapshotJSON), string(duplicatesJSON), commit.DerivedDigest, commit.CreatedAt,
	); err != nil {
		return err
	}
	for _, component := range commit.Snapshot.SortedComponents() {
		componentJSON, err := json.Marshal(component)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO components(version, component_id, spec_json, cpu_millicores, memory_bytes, capacity_cpu_millicores, capacity_memory_bytes)
			VALUES(?, ?, ?, ?, ?, ?, ?)`,
			commit.Snapshot.Version, component.ID, string(componentJSON), component.Resources.CPUMillicores,
			component.Resources.MemoryBytes, component.Capacity.CPUMillicores, component.Capacity.MemoryBytes,
		); err != nil {
			return err
		}
	}
	for _, edge := range commit.Snapshot.SortedEdges() {
		if _, err := tx.ExecContext(ctx, `INSERT INTO edges(version, upstream, downstream) VALUES(?, ?, ?)`,
			commit.Snapshot.Version, edge.From, edge.To); err != nil {
			return err
		}
	}
	if err := insertDerived(ctx, tx, commit); err != nil {
		return err
	}
	if commit.SubmissionKey != "" {
		if _, err := tx.ExecContext(ctx, `
			UPDATE submissions SET state='committed', version=?, http_status=?, response_json=?, error_json=NULL, updated_at=?
			WHERE idempotency_key=?`,
			commit.Snapshot.Version, commit.HTTPStatus, string(commit.ResponseJSON), commit.CreatedAt, commit.SubmissionKey,
		); err != nil {
			return err
		}
	}
	if commit.IntentID != "" {
		if _, err := tx.ExecContext(ctx, `
			UPDATE intents SET state='committed', phase='published', completed_at=? WHERE id=?`,
			commit.CreatedAt, commit.IntentID,
		); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO meta(key, value) VALUES('current_version', ?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, fmt.Sprintf("%d", commit.Snapshot.Version),
	); err != nil {
		return err
	}
	return tx.Commit()
}

func insertDerived(ctx context.Context, tx *sql.Tx, commit VersionCommit) error {
	analysis := commit.Analysis
	if analysis == nil {
		return fmt.Errorf("analysis is required")
	}
	for _, step := range commit.Consistency.Steps {
		resourceSummary, _ := json.Marshal(map[string]any{
			"resources": componentResource(commit.Snapshot, step.ComponentID).Resources,
			"capacity":  componentResource(commit.Snapshot, step.ComponentID).Capacity,
		})
		refsJSON, _ := json.Marshal(refsForDerived(commit.Refs, "resource_summary", step.ComponentID))
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO derived_components(version, component_id, resource_summary_json, local_score_micro, refs_json)
			VALUES(?, ?, ?, ?, ?)`,
			commit.Snapshot.Version, step.ComponentID, string(resourceSummary), step.LocalScoreMicro, string(refsJSON),
		); err != nil {
			return err
		}
		stepJSON, _ := json.Marshal(step)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO derived_consistency(version, component_id, score_micro, decision, trace_json)
			VALUES(?, ?, ?, ?, ?)`,
			commit.Snapshot.Version, step.ComponentID, step.LocalScoreMicro, commit.Consistency.Decision, string(stepJSON),
		); err != nil {
			return err
		}
	}
	aggregateJSON, _ := json.Marshal(commit.Consistency)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO derived_consistency(version, component_id, score_micro, decision, trace_json)
		VALUES(?, '__aggregate__', ?, ?, ?)`,
		commit.Snapshot.Version, commit.Consistency.ScoreMicro, commit.Consistency.Decision, string(aggregateJSON),
	); err != nil {
		return err
	}

	for origin, impact := range analysis.Impacts {
		for _, row := range impact.Affected {
			var topology, recovery any
			if row.TopologyLevel != nil {
				topology = *row.TopologyLevel
			}
			if row.RecoveryBatch != nil {
				recovery = *row.RecoveryBatch
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO derived_impacts(version, origin, component_id, hop, topology_level, recovery_batch, blocked, blocked_reason, blocked_by_cycle)
				VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				commit.Snapshot.Version, origin, row.Component, row.Hop, topology, recovery,
				boolInt(row.Blocked), row.BlockedReason, row.BlockedByCycle,
			); err != nil {
				return err
			}
		}
	}
	for _, group := range analysis.BatchGroups {
		for _, component := range group.Components {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO derived_batches(version, batch_no, component_id, blocked, reason)
				VALUES(?, ?, ?, 0, '')`, commit.Snapshot.Version, group.Batch, component,
			); err != nil {
				return err
			}
		}
	}
	for component, block := range analysis.Blocked {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO derived_batches(version, batch_no, component_id, blocked, reason)
			VALUES(?, NULL, ?, 1, ?)`, commit.Snapshot.Version, component, block.Reason,
		); err != nil {
			return err
		}
	}
	for _, ref := range commit.Refs {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO derived_refs(version, derived_kind, derived_key, ref_kind, ref_key, field)
			VALUES(?, ?, ?, ?, ?, ?)`,
			commit.Snapshot.Version, ref.DerivedKind, ref.DerivedKey, ref.RefKind, ref.RefKey, ref.Field,
		); err != nil {
			return err
		}
	}
	triggerJSON, _ := json.Marshal(commit.Rebuild.Trigger)
	changedJSON, _ := json.Marshal(commit.Rebuild.ChangedRefs)
	invalidatedJSON, _ := json.Marshal(commit.Rebuild.Invalidated)
	rebuiltJSON, _ := json.Marshal(commit.Rebuild.Rebuilt)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO derived_rebuilds(version, trigger_json, changed_refs_json, invalidated_json, rebuilt_json, strategy, status, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
		commit.Snapshot.Version, string(triggerJSON), string(changedJSON), string(invalidatedJSON), string(rebuiltJSON),
		commit.Rebuild.Strategy, commit.Rebuild.Status, commit.CreatedAt,
	); err != nil {
		return err
	}
	return nil
}

func (s *Store) PrepareSubmission(ctx context.Context, key, kind, requestHash, now string) (Submission, bool, error) {
	if key == "" {
		return Submission{}, false, fmt.Errorf("idempotency key is required")
	}
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO submissions(idempotency_key, kind, request_hash, state, created_at, updated_at)
		VALUES(?, ?, ?, 'pending', ?, ?)
		ON CONFLICT(idempotency_key) DO NOTHING`, key, kind, requestHash, now, now)
	if err != nil {
		return Submission{}, false, err
	}
	rows, _ := result.RowsAffected()
	submission, err := s.GetSubmission(ctx, key)
	if err != nil {
		return Submission{}, false, err
	}
	if submission.Kind != kind || submission.RequestHash != requestHash {
		return submission, false, ErrConflictingRequest
	}
	if submission.State == "aborted" {
		if _, err := s.db.ExecContext(ctx, `
			UPDATE submissions SET state='pending', version=NULL, http_status=0, response_json=NULL, error_json=NULL, updated_at=?
			WHERE idempotency_key=?`, now, key); err != nil {
			return Submission{}, false, err
		}
		submission.State = "pending"
		submission.Version = sql.NullInt64{}
		submission.HTTPStatus = 0
		submission.ResponseJSON = nil
		submission.ErrorJSON = nil
	}
	return submission, rows > 0, nil
}

func (s *Store) GetSubmission(ctx context.Context, key string) (Submission, error) {
	var submission Submission
	var response, errorJSON sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT idempotency_key, kind, request_hash, state, version, http_status, response_json, error_json, created_at, updated_at
		FROM submissions WHERE idempotency_key=?`, key).Scan(
		&submission.Key, &submission.Kind, &submission.RequestHash, &submission.State, &submission.Version,
		&submission.HTTPStatus, &response, &errorJSON, &submission.CreatedAt, &submission.UpdatedAt,
	)
	if err != nil {
		return Submission{}, err
	}
	submission.ResponseJSON = []byte(response.String)
	submission.ErrorJSON = []byte(errorJSON.String)
	return submission, nil
}

func (s *Store) RejectSubmission(ctx context.Context, key string, httpStatus int, errorJSON []byte, now string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE submissions SET state='rejected', http_status=?, error_json=?, response_json=NULL, updated_at=?
		WHERE idempotency_key=?`, httpStatus, string(errorJSON), now, key)
	return err
}

func (s *Store) InsertIntent(ctx context.Context, intent Intent) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO intents(id, submission_key, kind, base_version, target_version, payload_json, phase, state, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		intent.ID, intent.SubmissionKey, intent.Kind, intent.BaseVersion, intent.TargetVersion,
		string(intent.PayloadJSON), intent.Phase, intent.State, intent.CreatedAt,
	)
	return err
}

func (s *Store) RejectIntent(ctx context.Context, id, errorJSON, now string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE intents SET state='aborted', phase='rejected', error_json=?, completed_at=? WHERE id=?`,
		errorJSON, now, id)
	return err
}

func (s *Store) FailIntent(ctx context.Context, id, errorJSON, now string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE intents SET state='failed', phase='failed', error_json=?, completed_at=? WHERE id=?`,
		errorJSON, now, id)
	return err
}

func (s *Store) ListIntents(ctx context.Context, state string, limit int) ([]IntentSummary, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `SELECT id, submission_key, kind, base_version, target_version, phase, state, payload_json, error_json, created_at, COALESCE(completed_at,'')
		FROM intents`
	args := make([]any, 0, 2)
	if state != "" {
		query += ` WHERE state=?`
		args = append(args, state)
	}
	query += ` ORDER BY created_at DESC, id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]IntentSummary, 0)
	for rows.Next() {
		var item IntentSummary
		var payload string
		var errorJSON sql.NullString
		if err := rows.Scan(&item.ID, &item.SubmissionKey, &item.Kind, &item.BaseVersion, &item.TargetVersion,
			&item.Phase, &item.State, &payload, &errorJSON, &item.CreatedAt, &item.CompletedAt); err != nil {
			return nil, err
		}
		item.Payload = json.RawMessage(payload)
		if errorJSON.Valid && errorJSON.String != "" {
			item.Error = json.RawMessage(errorJSON.String)
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) StartDrill(ctx context.Context, drill DrillRecord) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO drills(id, submission_key, version, state, request_json, result_json, started_at)
		VALUES(?, ?, ?, ?, ?, NULL, ?)`,
		drill.ID, drill.SubmissionKey, drill.Version, drill.State, string(drill.RequestJSON), drill.StartedAt,
	)
	return err
}

func (s *Store) CompleteDrill(ctx context.Context, drill DrillRecord, audits []ReservationAudit, intentID string, httpStatus int, now string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
		UPDATE drills SET state=?, result_json=?, completed_at=? WHERE id=?`,
		drill.State, string(drill.ResultJSON), now, drill.ID,
	); err != nil {
		return err
	}
	for _, audit := range audits {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO reservation_audit(drill_id, component_id, cpu_millicores, memory_bytes, state, reason)
			VALUES(?, ?, ?, ?, ?, ?)
			ON CONFLICT(drill_id, component_id) DO UPDATE SET state=excluded.state, reason=excluded.reason`,
			drill.ID, audit.ComponentID, audit.CPUMillicores, audit.MemoryBytes, audit.State, audit.Reason,
		); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE submissions SET state='committed', http_status=?, response_json=?, error_json=NULL, updated_at=?
		WHERE idempotency_key=?`, httpStatus, string(drill.ResultJSON), now, drill.SubmissionKey,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE intents SET state='committed', phase='published', completed_at=? WHERE id=?`, now, intentID,
	); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) AbortDrill(ctx context.Context, id, now string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE drills SET state='aborted', completed_at=? WHERE id=? AND state='running'`, now, id)
	return err
}

func (s *Store) GetDrill(ctx context.Context, id string) (DrillRecord, error) {
	var drill DrillRecord
	var result sql.NullString
	var completed sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT id, submission_key, version, state, request_json, result_json, started_at, completed_at
		FROM drills WHERE id=?`, id).Scan(
		&drill.ID, &drill.SubmissionKey, &drill.Version, &drill.State, &drill.RequestJSON, &result, &drill.StartedAt, &completed,
	)
	if err != nil {
		return DrillRecord{}, err
	}
	drill.ResultJSON = []byte(result.String)
	drill.CompletedAt = completed.String
	return drill, nil
}

func (s *Store) ListDrills(ctx context.Context, limit int) ([]DrillRecord, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, submission_key, version, state, request_json, COALESCE(result_json,''), started_at, COALESCE(completed_at,'')
		FROM drills ORDER BY started_at DESC, id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]DrillRecord, 0)
	for rows.Next() {
		var drill DrillRecord
		var result, completed string
		if err := rows.Scan(&drill.ID, &drill.SubmissionKey, &drill.Version, &drill.State,
			&drill.RequestJSON, &result, &drill.StartedAt, &completed); err != nil {
			return nil, err
		}
		drill.ResultJSON = []byte(result)
		drill.CompletedAt = completed
		out = append(out, drill)
	}
	return out, rows.Err()
}

func (s *Store) RecoverInterrupted(ctx context.Context, now string) (RecoveryReport, error) {
	lastVersion, err := s.CurrentVersion(ctx)
	if err != nil {
		return RecoveryReport{}, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, COALESCE(submission_key,''), kind, COALESCE(target_version,0)
		FROM intents WHERE state='pending' ORDER BY id`)
	if err != nil {
		return RecoveryReport{}, err
	}
	type aborted struct {
		IntentID      string `json:"intent_id"`
		SubmissionKey string `json:"submission_key,omitempty"`
		Kind          string `json:"kind"`
		TargetVersion int64  `json:"target_version"`
		Resolution    string `json:"resolution"`
	}
	abortedList := make([]aborted, 0)
	submissionKeys := map[string]struct{}{}
	for rows.Next() {
		var item aborted
		if err := rows.Scan(&item.IntentID, &item.SubmissionKey, &item.Kind, &item.TargetVersion); err != nil {
			rows.Close()
			return RecoveryReport{}, err
		}
		item.Resolution = "marked_aborted; committed database state retained"
		abortedList = append(abortedList, item)
		if item.SubmissionKey != "" {
			submissionKeys[item.SubmissionKey] = struct{}{}
		}
	}
	if err := rows.Close(); err != nil {
		return RecoveryReport{}, err
	}
	if err := rows.Err(); err != nil {
		return RecoveryReport{}, err
	}
	pendingRows, err := s.db.QueryContext(ctx, `SELECT idempotency_key FROM submissions WHERE state='pending' ORDER BY idempotency_key`)
	if err != nil {
		return RecoveryReport{}, err
	}
	for pendingRows.Next() {
		var key string
		if err := pendingRows.Scan(&key); err != nil {
			pendingRows.Close()
			return RecoveryReport{}, err
		}
		if _, exists := submissionKeys[key]; exists {
			continue
		}
		submissionKeys[key] = struct{}{}
		abortedList = append(abortedList, aborted{
			SubmissionKey: key,
			Kind:          "submission",
			Resolution:    "pending submission marked aborted; no committed state was published",
		})
	}
	if err := pendingRows.Close(); err != nil {
		return RecoveryReport{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RecoveryReport{}, err
	}
	defer func() { _ = tx.Rollback() }()
	errorJSON, _ := json.Marshal(map[string]any{
		"code": model.CodeProcessInterrupted, "message": "process terminated before atomic commit",
	})
	for _, item := range abortedList {
		if _, err := tx.ExecContext(ctx, `
			UPDATE intents SET state='aborted', phase='recovered_on_startup', error_json=?, completed_at=? WHERE id=?`,
			string(errorJSON), now, item.IntentID); err != nil {
			return RecoveryReport{}, err
		}
	}
	for key := range submissionKeys {
		if _, err := tx.ExecContext(ctx, `
			UPDATE submissions SET state='aborted', error_json=?, updated_at=? WHERE idempotency_key=? AND state='pending'`,
			string(errorJSON), now, key); err != nil {
			return RecoveryReport{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE drills SET state='aborted', completed_at=? WHERE state='running'`, now); err != nil {
		return RecoveryReport{}, err
	}
	abortedJSON, _ := json.Marshal(abortedList)
	factsJSON, _ := json.Marshal(map[string]any{
		"last_committed_version": lastVersion,
		"source_of_truth":        "sqlite committed snapshot",
		"pending_intents":        len(abortedList),
		"recovery_rule":          "discard uncommitted work and retain the last complete snapshot version",
		"report_basis":           "persisted intents and committed snapshot only",
	})
	result, err := tx.ExecContext(ctx, `
		INSERT INTO recovery_reports(started_at, last_committed_version, scanned_intents, aborted_json, facts_json)
		VALUES(?, ?, ?, ?, ?)`, now, lastVersion, len(abortedList), string(abortedJSON), string(factsJSON))
	if err != nil {
		return RecoveryReport{}, err
	}
	id, _ := result.LastInsertId()
	if err := tx.Commit(); err != nil {
		return RecoveryReport{}, err
	}
	return RecoveryReport{
		ID: id, StartedAt: now, LastCommittedVersion: lastVersion,
		ScannedIntents: len(abortedList), Aborted: abortedJSON, Facts: factsJSON,
	}, nil
}

func (s *Store) ListRecoveryReports(ctx context.Context, limit int) ([]RecoveryReport, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, started_at, last_committed_version, scanned_intents, aborted_json, facts_json
		FROM recovery_reports ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]RecoveryReport, 0)
	for rows.Next() {
		var report RecoveryReport
		var abortedJSON, factsJSON string
		if err := rows.Scan(&report.ID, &report.StartedAt, &report.LastCommittedVersion,
			&report.ScannedIntents, &abortedJSON, &factsJSON); err != nil {
			return nil, err
		}
		report.Aborted = json.RawMessage(abortedJSON)
		report.Facts = json.RawMessage(factsJSON)
		out = append(out, report)
	}
	return out, rows.Err()
}

func (s *Store) ListLineageRefs(ctx context.Context, version int64) ([]LineageRef, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT derived_kind, derived_key, ref_kind, ref_key, field
		FROM derived_refs WHERE version=? ORDER BY derived_kind, derived_key, ref_kind, ref_key, field`, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]LineageRef, 0)
	for rows.Next() {
		var ref LineageRef
		if err := rows.Scan(&ref.DerivedKind, &ref.DerivedKey, &ref.RefKind, &ref.RefKey, &ref.Field); err != nil {
			return nil, err
		}
		out = append(out, ref)
	}
	return out, rows.Err()
}

func (s *Store) GetRebuild(ctx context.Context, version int64) (json.RawMessage, error) {
	var trigger, changed, invalidated, rebuilt, strategy, status, createdAt string
	err := s.db.QueryRowContext(ctx, `
		SELECT trigger_json, changed_refs_json, invalidated_json, rebuilt_json, strategy, status, created_at
		FROM derived_rebuilds WHERE version=?`, version).Scan(
		&trigger, &changed, &invalidated, &rebuilt, &strategy, &status, &createdAt,
	)
	if err != nil {
		return nil, err
	}
	payload, _ := json.Marshal(map[string]any{
		"version": version, "trigger": json.RawMessage(trigger), "changed_refs": json.RawMessage(changed),
		"invalidated": json.RawMessage(invalidated), "rebuilt": json.RawMessage(rebuilt),
		"strategy": strategy, "status": status, "created_at": createdAt,
	})
	return payload, nil
}

func (s *Store) AbortPendingSubmission(ctx context.Context, key, now string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE submissions SET state='aborted', updated_at=? WHERE idempotency_key=? AND state='pending'`, now, key)
	return err
}

func (s *Store) SnapshotCount(ctx context.Context) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshots`).Scan(&count)
	return count, err
}

func (s *Store) ListSnapshots(ctx context.Context, limit int) ([]SnapshotSummary, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT version, generated_at, checksum, derived_digest, created_at
		FROM snapshots ORDER BY version DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]SnapshotSummary, 0)
	for rows.Next() {
		var summary SnapshotSummary
		if err := rows.Scan(&summary.Version, &summary.GeneratedAt, &summary.Checksum, &summary.DerivedDigest, &summary.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, summary)
	}
	return out, rows.Err()
}

func refsForDerived(refs []RefRecord, derivedKind, derivedKey string) []RefRecord {
	out := make([]RefRecord, 0)
	for _, ref := range refs {
		if ref.DerivedKind == derivedKind && ref.DerivedKey == derivedKey {
			out = append(out, ref)
		}
	}
	return out
}

func componentResource(snapshot model.Snapshot, id string) model.Component {
	for _, component := range snapshot.Components {
		if component.ID == id {
			return component
		}
	}
	return model.Component{}
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func Now() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

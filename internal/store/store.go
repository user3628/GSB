package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"example.com/replicabudget/internal/apperr"
	"example.com/replicabudget/internal/policy"
	_ "modernc.org/sqlite"
)

type Store struct {
	db        *sql.DB
	path      string
	failpoint func(stage string) error
	mu        sync.Mutex
}

type Reference struct {
	OwnerService string `json:"owner_service"`
	Kind         string `json:"ref_kind"`
	Target       string `json:"target"`
}

type RebuildEvent struct {
	Version          int      `json:"version"`
	EventType        string   `json:"event_type"`
	AffectedServices []string `json:"affected_services"`
	Reason           string   `json:"reason"`
	CreatedAt        string   `json:"created_at"`
}

type StateView struct {
	Version     int                      `json:"version"`
	Policies    []policy.DerivedService  `json:"policies"`
	QuotaGroups []policy.QuotaGroup      `json:"quota_groups"`
	Index       policy.WeightedIndex     `json:"weighted_index"`
	Threshold   policy.ThresholdDecision `json:"threshold"`
	References  []Reference              `json:"references"`
	Rebuilds    []RebuildEvent           `json:"rebuild_events"`
}

type storedPolicy struct {
	Strategy      policy.NewStrategy
	SourceJSON    []byte
	SourceFormat  string
	SourceVersion int
}

type snapshotData struct {
	Version           int
	Policies          []storedPolicy
	Quotas            []policy.QuotaGroup
	ThresholdMicros   int64
	TolerancePermille int
}

type RecoveredIntent struct {
	IntentID      string `json:"intent_id"`
	Kind          string `json:"kind"`
	BaseVersion   int    `json:"base_version"`
	TargetVersion int    `json:"target_version"`
	Status        string `json:"previous_status"`
	Details       any    `json:"details,omitempty"`
}

type RecoveryReport struct {
	ID                      int64             `json:"id"`
	CreatedAt               string            `json:"created_at"`
	CurrentVersion          int               `json:"current_version"`
	LastCompleteVersion     int               `json:"last_complete_version"`
	Action                  string            `json:"action"`
	RecoveredIntents        []RecoveredIntent `json:"recovered_intents"`
	AbortedMigrationBatches []string          `json:"aborted_migration_batches"`
}

func Open(ctx context.Context, path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("sqlite path is empty")
	}
	if path != ":memory:" && !strings.HasPrefix(path, "file:") {
		if dir := filepath.Dir(path); dir != "." && dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
		}
	}
	dsn := path
	if path == ":memory:" {
		dsn = "file::memory:?cache=shared"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout=5000"); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{db: db, path: path}
	if _, err := s.Recover(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) SetFailpoint(fn func(stage string) error) { s.failpoint = fn }

func (s *Store) callFailpoint(stage string) error {
	if s.failpoint != nil {
		return s.failpoint(stage)
	}
	return nil
}

func newID(prefix string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s%d", prefix, time.Now().UnixNano())
	}
	return prefix + hex.EncodeToString(b[:])
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func marshal(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func currentVersion(ctx context.Context, q queryer) (int, error) {
	var version int
	if err := q.QueryRowContext(ctx, "SELECT current_version FROM state WHERE id=1").Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) loadSnapshot(ctx context.Context, q queryer, version int) (snapshotData, error) {
	data := snapshotData{Version: version, TolerancePermille: policy.DefaultRelativeTolerancePermille, ThresholdMicros: policy.DefaultThresholdMicros}
	if version < 0 {
		return data, nil
	}
	rows, err := q.QueryContext(ctx, `SELECT service_id, spec_json, source_json, source_format, source_version FROM strategies WHERE version=? ORDER BY service_id`, version)
	if err != nil {
		return data, err
	}
	defer rows.Close()
	for rows.Next() {
		var item storedPolicy
		var specJSON, sourceJSON string
		if err := rows.Scan(&item.Strategy.ServiceID, &specJSON, &sourceJSON, &item.SourceFormat, &item.SourceVersion); err != nil {
			return data, err
		}
		if err := json.Unmarshal([]byte(specJSON), &item.Strategy); err != nil {
			return data, err
		}
		item.SourceJSON = []byte(sourceJSON)
		data.Policies = append(data.Policies, item)
	}
	if err := rows.Err(); err != nil {
		return data, err
	}
	qrows, err := q.QueryContext(ctx, `SELECT group_id, vram_bytes, compute_milli FROM quota_groups WHERE version=? ORDER BY group_id`, version)
	if err != nil {
		return data, err
	}
	defer qrows.Close()
	for qrows.Next() {
		var group policy.QuotaGroup
		var vram, compute int64
		if err := qrows.Scan(&group.ID, &vram, &compute); err != nil {
			return data, err
		}
		group.VRAMBytes = uint64(vram)
		group.ComputeMilli = uint64(compute)
		data.Quotas = append(data.Quotas, group)
	}
	if err := qrows.Err(); err != nil {
		return data, err
	}
	var threshold int64
	var tolerance int
	err = q.QueryRowContext(ctx, `SELECT threshold_micros, tolerance_permille FROM snapshot_state WHERE version=?`, version).Scan(&threshold, &tolerance)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return data, err
	}
	if err == nil {
		data.ThresholdMicros = threshold
		data.TolerancePermille = tolerance
	}
	return data, nil
}

func policyList(policies []storedPolicy) []policy.NewStrategy {
	out := make([]policy.NewStrategy, 0, len(policies))
	for _, p := range policies {
		out = append(out, p.Strategy)
	}
	return out
}

func (s *Store) loadState(ctx context.Context, version int) (StateView, error) {
	view := StateView{Version: version, Policies: []policy.DerivedService{}, QuotaGroups: []policy.QuotaGroup{}, References: []Reference{}, Rebuilds: []RebuildEvent{}}
	if version == 0 {
		view.Index = policy.WeightedIndex{Scale: policy.IndexScale, Steps: []policy.IndexStep{}, OrderedServiceIDs: []string{}}
		view.Threshold = policy.CompareThreshold(view.Index, policy.DefaultThresholdMicros, policy.DefaultRelativeTolerancePermille)
		return view, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT d.service_id, d.desired_replicas, d.concurrency_need, d.resource_cap, d.placements_json, s.spec_json
		FROM derived_services d JOIN strategies s ON s.version=d.version AND s.service_id=d.service_id
		WHERE d.version=? ORDER BY d.service_id`, version)
	if err != nil {
		return view, err
	}
	for rows.Next() {
		var item policy.DerivedService
		var placementsJSON, specJSON string
		var capValue int64
		if err := rows.Scan(&item.Strategy.ServiceID, &item.DesiredReplicas, &item.ConcurrencyNeed, &capValue, &placementsJSON, &specJSON); err != nil {
			rows.Close()
			return view, err
		}
		item.ResourceCap = uint64(capValue)
		if err := json.Unmarshal([]byte(specJSON), &item.Strategy); err != nil {
			rows.Close()
			return view, err
		}
		if err := json.Unmarshal([]byte(placementsJSON), &item.Placements); err != nil {
			rows.Close()
			return view, err
		}
		view.Policies = append(view.Policies, item)
	}
	if err := rows.Close(); err != nil {
		return view, err
	}
	if err := rows.Err(); err != nil {
		return view, err
	}
	data, err := s.loadSnapshot(ctx, s.db, version)
	if err != nil {
		return view, err
	}
	view.QuotaGroups = data.Quotas
	var indexJSON string
	if err := s.db.QueryRowContext(ctx, `SELECT index_json FROM derived_index WHERE version=?`, version).Scan(&indexJSON); err != nil {
		return view, err
	}
	if err := json.Unmarshal([]byte(indexJSON), &view.Index); err != nil {
		return view, err
	}
	view.Threshold = policy.CompareThreshold(view.Index, data.ThresholdMicros, data.TolerancePermille)
	refRows, err := s.db.QueryContext(ctx, `SELECT owner_service, ref_kind, target FROM references_v1 WHERE version=? ORDER BY owner_service, ref_kind, target`, version)
	if err != nil {
		return view, err
	}
	for refRows.Next() {
		var ref Reference
		if err := refRows.Scan(&ref.OwnerService, &ref.Kind, &ref.Target); err != nil {
			refRows.Close()
			return view, err
		}
		view.References = append(view.References, ref)
	}
	if err := refRows.Close(); err != nil {
		return view, err
	}
	if err := refRows.Err(); err != nil {
		return view, err
	}
	eventRows, err := s.db.QueryContext(ctx, `SELECT event_type, affected_services_json, reason, created_at FROM rebuild_events WHERE version=? ORDER BY id`, version)
	if err != nil {
		return view, err
	}
	defer eventRows.Close()
	for eventRows.Next() {
		var event RebuildEvent
		var affected string
		if err := eventRows.Scan(&event.EventType, &affected, &event.Reason, &event.CreatedAt); err != nil {
			return view, err
		}
		event.Version = version
		_ = json.Unmarshal([]byte(affected), &event.AffectedServices)
		view.Rebuilds = append(view.Rebuilds, event)
	}
	return view, eventRows.Err()
}

func (s *Store) State(ctx context.Context) (StateView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	version, err := currentVersion(ctx, s.db)
	if err != nil {
		return StateView{}, err
	}
	return s.loadState(ctx, version)
}

func terminateIntent(ctx context.Context, tx *sql.Tx, intentID, batchID, status, code string, details any) error {
	ts := now()
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET status=?, error_code=?, details_json=?, updated_at=? WHERE intent_id=?`, status, code, marshal(details), ts, intentID); err != nil {
		return err
	}
	if batchID != "" {
		batchStatus := "aborted"
		if status == "committed" {
			batchStatus = "accepted"
		}
		if _, err := tx.ExecContext(ctx, `UPDATE migration_batches SET status=?, decision_json=?, updated_at=? WHERE batch_id=?`, batchStatus, marshal(details), ts, batchID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Recover(ctx context.Context) (RecoveryReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RecoveryReport{}, err
	}
	defer tx.Rollback()
	report := RecoveryReport{CreatedAt: now(), RecoveredIntents: []RecoveredIntent{}, AbortedMigrationBatches: []string{}, Action: "no_unfinished_intent"}
	if err := tx.QueryRowContext(ctx, `SELECT current_version FROM state WHERE id=1`).Scan(&report.CurrentVersion); err != nil {
		return RecoveryReport{}, err
	}
	var lastComplete sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(version) FROM snapshots WHERE status='complete'`).Scan(&lastComplete); err != nil {
		return RecoveryReport{}, err
	}
	if lastComplete.Valid {
		report.LastCompleteVersion = int(lastComplete.Int64)
	}
	if report.CurrentVersion != report.LastCompleteVersion {
		if _, err := tx.ExecContext(ctx, `UPDATE state SET current_version=? WHERE id=1`, report.LastCompleteVersion); err != nil {
			return RecoveryReport{}, err
		}
		report.Action = "rolled_back_to_last_complete_snapshot"
		report.CurrentVersion = report.LastCompleteVersion
	}
	rows, err := tx.QueryContext(ctx, `SELECT intent_id, kind, base_version, target_version, status, details_json FROM intents WHERE status IN ('pending','applying') ORDER BY created_at`)
	if err != nil {
		return RecoveryReport{}, err
	}
	type pending struct {
		intentID, kind, status, details string
		base, target                    int
	}
	var pendingItems []pending
	for rows.Next() {
		var item pending
		if err := rows.Scan(&item.intentID, &item.kind, &item.base, &item.target, &item.status, &item.details); err != nil {
			rows.Close()
			return RecoveryReport{}, err
		}
		pendingItems = append(pendingItems, item)
	}
	if err := rows.Close(); err != nil {
		return RecoveryReport{}, err
	}
	if err := rows.Err(); err != nil {
		return RecoveryReport{}, err
	}
	if len(pendingItems) > 0 && report.Action == "no_unfinished_intent" {
		report.Action = "aborted_unfinished_intents"
	}
	for _, item := range pendingItems {
		var details any
		_ = json.Unmarshal([]byte(item.details), &details)
		report.RecoveredIntents = append(report.RecoveredIntents, RecoveredIntent{
			IntentID: item.intentID, Kind: item.kind, BaseVersion: item.base,
			TargetVersion: item.target, Status: item.status, Details: details,
		})
		ts := now()
		recoveryDetails := map[string]any{"reason": "process_terminated_before_commit", "recovered_at": ts, "last_complete_version": report.LastCompleteVersion}
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='recovered_aborted', error_code='E_PROCESS_TERMINATED', details_json=?, updated_at=? WHERE intent_id=?`, marshal(recoveryDetails), ts, item.intentID); err != nil {
			return RecoveryReport{}, err
		}
		batchRows, err := tx.QueryContext(ctx, `SELECT batch_id FROM migration_batches WHERE intent_id=? AND status IN ('pending','applying')`, item.intentID)
		if err != nil {
			return RecoveryReport{}, err
		}
		for batchRows.Next() {
			var batchID string
			if err := batchRows.Scan(&batchID); err != nil {
				batchRows.Close()
				return RecoveryReport{}, err
			}
			report.AbortedMigrationBatches = append(report.AbortedMigrationBatches, batchID)
			if _, err := tx.ExecContext(ctx, `UPDATE migration_batches SET status='aborted', decision_json=?, updated_at=? WHERE batch_id=?`, marshal(recoveryDetails), ts, batchID); err != nil {
				batchRows.Close()
				return RecoveryReport{}, err
			}
		}
		if err := batchRows.Close(); err != nil {
			return RecoveryReport{}, err
		}
	}
	if report.Action == "aborted_unfinished_intents" && len(report.AbortedMigrationBatches) > 0 {
		report.Action = "aborted_unfinished_intents_and_migration_batches"
	}
	encoded, _ := json.Marshal(report)
	res, err := tx.ExecContext(ctx, `INSERT INTO recovery_reports(created_at,current_version,last_complete_version,report_json) VALUES(?,?,?,?)`, report.CreatedAt, report.CurrentVersion, report.LastCompleteVersion, string(encoded))
	if err != nil {
		return RecoveryReport{}, err
	}
	id, _ := res.LastInsertId()
	report.ID = id
	if err := tx.Commit(); err != nil {
		return RecoveryReport{}, err
	}
	return report, nil
}

func (s *Store) RecoveryReport(ctx context.Context) (RecoveryReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var report RecoveryReport
	var raw string
	var persistedID int64
	err := s.db.QueryRowContext(ctx, `SELECT id, report_json FROM recovery_reports ORDER BY id DESC LIMIT 1`).Scan(&persistedID, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return RecoveryReport{}, apperr.New(apperr.NotFound, "no recovery report exists")
	}
	if err != nil {
		return RecoveryReport{}, err
	}
	if err := json.Unmarshal([]byte(raw), &report); err != nil {
		return RecoveryReport{}, err
	}
	report.ID = persistedID
	return report, nil
}

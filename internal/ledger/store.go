package ledger

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db      *sql.DB
	path    string
	writeMu sync.Mutex
	now     func() time.Time
}

type Meta struct {
	Version              int64
	AsOfMinute           int64
	LastSettledThroughMS int64
	StateDigest          string
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("database path is required")
	}
	if path != ":memory:" {
		dir := filepath.Dir(path)
		if dir != "." && dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("create database directory: %w", err)
			}
		}
	}
	dsn := sqliteDSN(path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(0)
	s := &Store{db: db, path: path, now: func() time.Time { return time.Now().UTC() }}
	if err := s.initSchema(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func sqliteDSN(path string) string {
	const pragmas = "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(ON)"
	if path == ":memory:" {
		return fmt.Sprintf("file:quota-ledger-memory-%d?mode=memory&cache=shared&%s", time.Now().UnixNano(), pragmas)
	}
	escaped := strings.NewReplacer("%", "%25", "?", "%3F", "#", "%23", " ", "%20").Replace(filepath.ToSlash(path))
	return "file:" + escaped + "?" + pragmas
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) initSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS schema_meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS requests (
			request_id TEXT PRIMARY KEY,
			kind TEXT NOT NULL,
			payload_digest TEXT NOT NULL,
			http_status INTEGER NOT NULL,
			response_json TEXT NOT NULL,
			ledger_version INTEGER NOT NULL,
			created_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS intents (
			intent_id TEXT PRIMARY KEY,
			request_id TEXT NOT NULL,
			kind TEXT NOT NULL,
			payload_digest TEXT NOT NULL,
			target_ledger_version INTEGER NOT NULL,
			status TEXT NOT NULL,
			phase TEXT NOT NULL,
			committed_ledger_version INTEGER,
			detail_json TEXT NOT NULL,
			created_ms INTEGER NOT NULL,
			updated_ms INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status)`,
		`CREATE TABLE IF NOT EXISTS ledger_versions (
			ledger_version INTEGER PRIMARY KEY,
			request_id TEXT NOT NULL UNIQUE,
			kind TEXT NOT NULL,
			payload_digest TEXT NOT NULL,
			state_json TEXT NOT NULL,
			state_digest TEXT NOT NULL,
			as_of_minute INTEGER NOT NULL,
			last_settled_through_ms INTEGER NOT NULL,
			created_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS snapshots (
			snapshot_version INTEGER PRIMARY KEY,
			generated_at_ms INTEGER NOT NULL,
			digest TEXT NOT NULL,
			body_json TEXT NOT NULL,
			effective_minute INTEGER NOT NULL,
			commit_version INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_snapshots_effective ON snapshots(effective_minute, snapshot_version)`,
		`CREATE TABLE IF NOT EXISTS events (
			settle_seq TEXT PRIMARY KEY,
			tenant_id TEXT NOT NULL,
			event_type TEXT NOT NULL,
			shares_milli INTEGER NOT NULL,
			timestamp_ms INTEGER NOT NULL,
			content_digest TEXT NOT NULL,
			body_json TEXT NOT NULL,
			commit_version INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_events_tenant_time ON events(tenant_id, timestamp_ms)`,
		`CREATE TABLE IF NOT EXISTS tenant_states (
			ledger_version INTEGER NOT NULL,
			tenant_id TEXT NOT NULL,
			pool_id TEXT NOT NULL,
			declaration_snapshot_version INTEGER NOT NULL,
			base_shares_milli INTEGER NOT NULL,
			credit_limit_milli INTEGER NOT NULL,
			rate_tier TEXT NOT NULL,
			repayment_priority INTEGER NOT NULL,
			principal_outstanding_milli INTEGER NOT NULL,
			unsettled_interest_micros INTEGER NOT NULL,
			available_credit_micros INTEGER NOT NULL,
			state_json TEXT NOT NULL,
			PRIMARY KEY(ledger_version, tenant_id)
		)`,
		`CREATE TABLE IF NOT EXISTS pool_states (
			ledger_version INTEGER NOT NULL,
			pool_id TEXT NOT NULL,
			used_ratio_fixed TEXT NOT NULL,
			used_ratio_ppm INTEGER NOT NULL,
			principal_outstanding_milli INTEGER NOT NULL,
			unsettled_interest_micros INTEGER NOT NULL,
			cross_tenant_exposure_micros INTEGER NOT NULL,
			overcommit_risk INTEGER NOT NULL,
			trigger_tenant_id TEXT NOT NULL,
			state_json TEXT NOT NULL,
			PRIMARY KEY(ledger_version, pool_id)
		)`,
		`CREATE TABLE IF NOT EXISTS reconstruction_audit (
			ledger_version INTEGER NOT NULL,
			affected_tenant_id TEXT NOT NULL,
			dependent_kind TEXT NOT NULL,
			dependent_key TEXT NOT NULL,
			reason TEXT NOT NULL,
			resolved_version INTEGER NOT NULL,
			PRIMARY KEY(ledger_version, affected_tenant_id, dependent_kind, dependent_key)
		)`,
		`CREATE TABLE IF NOT EXISTS settlement_reports (
			request_id TEXT PRIMARY KEY,
			ledger_version INTEGER NOT NULL,
			report_json TEXT NOT NULL,
			created_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS recovery_runs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			started_ms INTEGER NOT NULL,
			finished_ms INTEGER NOT NULL,
			report_json TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("initialize schema: %w", err)
		}
	}
	return nil
}

func (s *Store) nowMS() int64 { return s.now().UnixMilli() }

func (s *Store) CurrentMeta(ctx context.Context, q queryer) (Meta, error) {
	var m Meta
	err := q.QueryRowContext(ctx, `SELECT ledger_version, as_of_minute, last_settled_through_ms, state_digest
		FROM ledger_versions ORDER BY ledger_version DESC LIMIT 1`).Scan(&m.Version, &m.AsOfMinute, &m.LastSettledThroughMS, &m.StateDigest)
	if err == sql.ErrNoRows {
		return Meta{}, nil
	}
	if err != nil {
		return Meta{}, err
	}
	return m, nil
}

func (s *Store) loadSnapshots(ctx context.Context, q queryer) ([]FactSnapshot, error) {
	rows, err := q.QueryContext(ctx, `SELECT body_json, commit_version FROM snapshots ORDER BY snapshot_version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FactSnapshot
	for rows.Next() {
		var body string
		var commit int64
		if err := rows.Scan(&body, &commit); err != nil {
			return nil, err
		}
		var snap Snapshot
		if err := json.Unmarshal([]byte(body), &snap); err != nil {
			return nil, err
		}
		out = append(out, FactSnapshot{Snapshot: snap, CommitVersion: commit})
	}
	return out, rows.Err()
}

func (s *Store) loadEvents(ctx context.Context, q queryer) ([]FactEvent, error) {
	rows, err := q.QueryContext(ctx, `SELECT body_json, content_digest, commit_version FROM events ORDER BY timestamp_ms, settle_seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FactEvent
	for rows.Next() {
		var body, digest string
		var commit int64
		if err := rows.Scan(&body, &digest, &commit); err != nil {
			return nil, err
		}
		var ev Event
		if err := json.Unmarshal([]byte(body), &ev); err != nil {
			return nil, err
		}
		out = append(out, FactEvent{Event: ev, ContentDigest: digest, CommitVersion: commit})
	}
	return out, rows.Err()
}

func (s *Store) loadFacts(ctx context.Context, q queryer) ([]FactSnapshot, []FactEvent, error) {
	snaps, err := s.loadSnapshots(ctx, q)
	if err != nil {
		return nil, nil, err
	}
	events, err := s.loadEvents(ctx, q)
	if err != nil {
		return nil, nil, err
	}
	return snaps, events, nil
}

func (s *Store) maxSnapshotVersion(ctx context.Context, q queryer) (int64, error) {
	var v sql.NullInt64
	if err := q.QueryRowContext(ctx, `SELECT MAX(snapshot_version) FROM snapshots`).Scan(&v); err != nil {
		return 0, err
	}
	if !v.Valid {
		return 0, nil
	}
	return v.Int64, nil
}

func (s *Store) GetState(ctx context.Context, version *int64) (*LedgerState, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var body string
	if version != nil {
		err = tx.QueryRowContext(ctx, `SELECT state_json FROM ledger_versions WHERE ledger_version=?`, *version).Scan(&body)
	} else {
		err = tx.QueryRowContext(ctx, `SELECT state_json FROM ledger_versions ORDER BY ledger_version DESC LIMIT 1`).Scan(&body)
	}
	if err == sql.ErrNoRows {
		return nil, Biz(CodeNotFound, "ledger state not found", map[string]any{"version": version})
	}
	if err != nil {
		return nil, err
	}
	var state LedgerState
	if err := json.Unmarshal([]byte(body), &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (s *Store) GetSettlement(ctx context.Context, requestID string) (*SettlementReport, error) {
	var body string
	err := s.db.QueryRowContext(ctx, `SELECT report_json FROM settlement_reports WHERE request_id=?`, requestID).Scan(&body)
	if err == sql.ErrNoRows {
		return nil, Biz(CodeNotFound, "settlement report not found", map[string]any{"request_id": requestID})
	}
	if err != nil {
		return nil, err
	}
	var report SettlementReport
	if err := json.Unmarshal([]byte(body), &report); err != nil {
		return nil, err
	}
	return &report, nil
}

type RecoveryAction struct {
	IntentID         string `json:"intent_id"`
	RequestID        string `json:"request_id"`
	Kind             string `json:"kind"`
	TargetVersion    int64  `json:"target_ledger_version"`
	Status           string `json:"status"`
	Action           string `json:"action"`
	Reason           string `json:"reason"`
	CommittedVersion int64  `json:"committed_ledger_version,omitempty"`
}

type RecoveryReport struct {
	StartedMS             int64            `json:"started_ms"`
	FinishedMS            int64            `json:"finished_ms"`
	LastCompleteVersion   int64            `json:"last_complete_version"`
	LastStateDigest       string           `json:"last_state_digest,omitempty"`
	LastSettledThroughMS  int64            `json:"last_settled_through_ms"`
	ScannedIntentCount    int              `json:"scanned_intent_count"`
	AbortedIntentCount    int              `json:"aborted_intent_count"`
	ReconciledIntentCount int              `json:"reconciled_intent_count"`
	Actions               []RecoveryAction `json:"actions"`
	Facts                 map[string]any   `json:"facts"`
}

func (s *Store) Recover(ctx context.Context) (*RecoveryReport, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	started := s.nowMS()
	report := &RecoveryReport{StartedMS: started, Actions: []RecoveryAction{}, Facts: map[string]any{}}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	meta, err := s.CurrentMeta(ctx, tx)
	if err != nil {
		return nil, err
	}
	report.LastCompleteVersion = meta.Version
	report.LastStateDigest = meta.StateDigest
	report.LastSettledThroughMS = meta.LastSettledThroughMS
	rows, err := tx.QueryContext(ctx, `SELECT intent_id, request_id, kind, target_ledger_version, status, phase
		FROM intents WHERE status IN ('pending','processing') ORDER BY created_ms, intent_id`)
	if err != nil {
		return nil, err
	}
	type pendingIntent struct {
		intentID, requestID, kind, status, phase string
		target                                   int64
	}
	var pending []pendingIntent
	for rows.Next() {
		var p pendingIntent
		if err := rows.Scan(&p.intentID, &p.requestID, &p.kind, &p.target, &p.status, &p.phase); err != nil {
			rows.Close()
			return nil, err
		}
		pending = append(pending, p)
	}
	rows.Close()
	report.ScannedIntentCount = len(pending)
	for _, p := range pending {
		var committedVersion int64
		var digest string
		err := tx.QueryRowContext(ctx, `SELECT ledger_version, state_digest FROM ledger_versions WHERE request_id=?`, p.requestID).Scan(&committedVersion, &digest)
		action := RecoveryAction{IntentID: p.intentID, RequestID: p.requestID, Kind: p.kind, TargetVersion: p.target, Status: "pending"}
		if err == nil && committedVersion == p.target {
			reason := "a committed ledger version with the same request_id and target version is already durable; intent status reconciled"
			action.Action = "reconciled_committed"
			action.Status = "committed"
			action.Reason = reason
			action.CommittedVersion = committedVersion
			report.ReconciledIntentCount++
			detail, _ := json.Marshal(map[string]any{"recovered_committed_version": committedVersion, "state_digest": digest})
			if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='committed', phase='recovered', committed_ledger_version=?, detail_json=?, updated_ms=? WHERE intent_id=?`, committedVersion, string(detail), s.nowMS(), p.intentID); err != nil {
				return nil, err
			}
		} else {
			reason := "no committed ledger version for this request and target; uncommitted SQLite writes were rolled back"
			action.Action = "aborted_uncommitted_intent"
			action.Status = "aborted"
			action.Reason = reason
			report.AbortedIntentCount++
			detail, _ := json.Marshal(map[string]any{
				"last_complete_version": meta.Version,
				"last_state_digest":     meta.StateDigest,
				"observed_phase":        p.phase,
			})
			if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='aborted', phase='recovered', detail_json=?, updated_ms=? WHERE intent_id=?`, string(detail), s.nowMS(), p.intentID); err != nil {
				return nil, err
			}
		}
		report.Actions = append(report.Actions, action)
	}
	report.FinishedMS = s.nowMS()
	var snapCount, eventCount, versionCount, tenantRows, poolRows int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshots`).Scan(&snapCount); err != nil {
		return nil, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&eventCount); err != nil {
		return nil, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM ledger_versions`).Scan(&versionCount); err != nil {
		return nil, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM tenant_states WHERE ledger_version=?`, meta.Version).Scan(&tenantRows); err != nil {
		return nil, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pool_states WHERE ledger_version=?`, meta.Version).Scan(&poolRows); err != nil {
		return nil, err
	}
	report.Facts["persisted_snapshots"] = snapCount
	report.Facts["persisted_events"] = eventCount
	report.Facts["ledger_versions"] = versionCount
	report.Facts["tenant_rows_at_last_complete_version"] = tenantRows
	report.Facts["pool_rows_at_last_complete_version"] = poolRows
	report.Facts["source"] = "SQLite committed rows only"
	body, err := json.Marshal(report)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_runs(started_ms, finished_ms, report_json) VALUES(?,?,?)`, report.StartedMS, report.FinishedMS, string(body)); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return report, nil
}

func (s *Store) LatestRecovery(ctx context.Context) (*RecoveryReport, error) {
	var body string
	err := s.db.QueryRowContext(ctx, `SELECT report_json FROM recovery_runs ORDER BY id DESC LIMIT 1`).Scan(&body)
	if err == sql.ErrNoRows {
		return nil, Biz(CodeNotFound, "no recovery run has been recorded", nil)
	}
	if err != nil {
		return nil, err
	}
	var report RecoveryReport
	if err := json.Unmarshal([]byte(body), &report); err != nil {
		return nil, err
	}
	return &report, nil
}

func newIntentID(requestID string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s@%s", requestID, hex.EncodeToString(b[:]))
}

func statusForBizCode(code string) int {
	switch code {
	case CodeStaleSnapshot, CodeConflictingSettle, CodePoolOvercommit, CodePriorityConflict, CodeConflictingRequest, CodeStaleSettlement:
		return http.StatusConflict
	case CodeNotFound:
		return http.StatusNotFound
	case CodeInternal:
		return http.StatusInternalServerError
	case CodeBadRequest:
		return http.StatusBadRequest
	default:
		return http.StatusUnprocessableEntity
	}
}

// ForceCrashDuringSettlement is a deliberate fault-injection helper for the local
// verification script. The durable intent is committed first. The subsequent
// ledger transaction writes tenant rows, removes pool rows, and exits without
// committing, so SQLite rolls the partial transaction back on process death.
func (s *Store) ForceCrashDuringSettlement(ctx context.Context, req CrashRequest) error {
	if !idPattern.MatchString(req.RequestID) {
		return Biz(CodeBadRequest, "request_id is required and must use safe identifier characters", map[string]any{"request_id": req.RequestID})
	}
	s.writeMu.Lock()
	meta, err := s.CurrentMeta(ctx, s.db)
	if err != nil {
		s.writeMu.Unlock()
		return err
	}
	if req.ThroughMS < 0 || req.ThroughMS > MaxTimestampMS {
		s.writeMu.Unlock()
		return Biz(CodeBadRequest, "through_ms must be non-negative", nil)
	}
	throughMinute := MinuteOf(req.ThroughMS)
	if throughMinute < meta.AsOfMinute {
		s.writeMu.Unlock()
		return Biz(CodeStaleSettlement, "crash target through_ms precedes current as-of minute", map[string]any{"through_minute": throughMinute, "current_as_of_minute": meta.AsOfMinute})
	}
	payloadDigest, _ := CanonicalJSONDigest(req)
	newVersion := meta.Version + 1
	intentID := newIntentID(req.RequestID)
	detail, _ := json.Marshal(map[string]any{"target_ledger_version": newVersion, "fault_injection": "crash_during_settlement"})
	if _, err := s.db.ExecContext(ctx, `INSERT INTO intents(intent_id, request_id, kind, payload_digest, target_ledger_version, status, phase, detail_json, created_ms, updated_ms)
		VALUES(?,?,?,?,?,'pending','intent_recorded',?,?,?)`, intentID, req.RequestID, "settle", payloadDigest, newVersion, string(detail), s.nowMS(), s.nowMS()); err != nil {
		s.writeMu.Unlock()
		return err
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE intents SET phase='crash_rebuild_started', updated_ms=? WHERE intent_id=?`, s.nowMS(), intentID); err != nil {
		s.writeMu.Unlock()
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.writeMu.Unlock()
		return err
	}
	// Deliberately do not defer rollback: the process must die with the SQLite
	// transaction open, leaving only the durable intent as evidence.
	snaps, events, err := s.loadFacts(ctx, tx)
	if err != nil {
		_ = tx.Rollback()
		s.writeMu.Unlock()
		return err
	}
	result, err := BuildProjection(snaps, events, throughMinute, ProjectionMeta{LedgerVersion: newVersion, LastSettledThroughMS: req.ThroughMS}, true)
	if err != nil {
		_ = tx.Rollback()
		s.writeMu.Unlock()
		return err
	}
	affected := make([]string, 0, len(result.State.Tenants))
	for _, t := range result.State.Tenants {
		affected = append(affected, t.TenantID)
	}
	if err := s.writeProjection(ctx, tx, result.State, req.RequestID, "settle-fault-injection", "", "crash_after_tenant_rebuild_before_pool_commit", affected); err != nil {
		_ = tx.Rollback()
		s.writeMu.Unlock()
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM pool_states WHERE ledger_version=?`, newVersion); err != nil {
		_ = tx.Rollback()
		s.writeMu.Unlock()
		return err
	}
	fmt.Fprintf(os.Stderr, "CRASH_INJECTED request_id=%s intent_id=%s target_version=%d phase=crash_after_tenant_rebuild_before_pool_commit\n", req.RequestID, intentID, newVersion)
	os.Stdout.Sync()
	os.Stderr.Sync()
	os.Exit(137)
	return nil
}

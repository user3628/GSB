package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"upstreampool/internal/model"
	"upstreampool/internal/sim"
)

const (
	BatchPending = "PENDING"
	BatchApplied = "APPLIED"

	IntentPending = "PENDING"
	IntentApplied = "APPLIED"
	IntentAborted = "ABORTED"
)

type Store struct {
	db *sql.DB
}

type Batch struct {
	TrackID      string
	BatchID      string
	Digest       string
	Status       string
	ResponseJSON string
	CreatedAt    string
	AppliedAt    string
}

type Intent struct {
	ID        string `json:"id"`
	TrackID   string `json:"track_id"`
	Operation string `json:"operation"`
	Payload   string `json:"payload"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
	AppliedAt string `json:"applied_at,omitempty"`
}

type IOFact struct {
	RequestKey string
	Status     string
	ErrorCode  string
	Message    string
}

type BatchOutcome struct {
	BatchID string
	JSON    string
}

type PendingBatch struct {
	TrackID string
	BatchID string
}

type PlanWrites struct {
	TrackID            string
	HorizonMS          int64
	FinalSeq           int64
	CheckpointInterval int
	Snapshots          []sim.Snapshot
	Transitions        []sim.Transition
	Results            []sim.RequestResult
	Connections        []sim.FinalConnection
	Uses               []sim.ConnectionUse
	IO                 []IOFact
	Batches            []BatchOutcome
	IntentID           string
}

type RecoveryReport struct {
	ID             int64    `json:"id"`
	StartedAt      string   `json:"started_at"`
	AbortedIntents []Intent `json:"aborted_intents"`
	PendingBatches []string `json:"pending_batches"`
	ResumedBatches []string `json:"resumed_batches"`
	ErrorCode      string   `json:"error_code,omitempty"`
	Message        string   `json:"message,omitempty"`
}

func Open(path string) (*Store, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, err
	}
	uriPath := filepath.ToSlash(abs)
	if runtime.GOOS == "windows" && len(uriPath) >= 2 && uriPath[1] == ':' {
		uriPath = "/" + uriPath
	}
	u := &url.URL{Scheme: "file", Path: uriPath}
	q := u.Query()
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	q.Add("_pragma", "foreign_keys(1)")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(0)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS endpoints (
			id TEXT PRIMARY KEY,
			digest TEXT NOT NULL,
			spec_json TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS tracks (
			id TEXT PRIMARY KEY,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			horizon_ms INTEGER NOT NULL DEFAULT 0,
			final_seq INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS batches (
			track_id TEXT NOT NULL,
			batch_id TEXT NOT NULL,
			digest TEXT NOT NULL,
			status TEXT NOT NULL,
			response_json TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			applied_at TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (track_id, batch_id)
		)`,
		`CREATE TABLE IF NOT EXISTS requests (
			track_id TEXT NOT NULL,
			request_key TEXT NOT NULL,
			request_id TEXT NOT NULL,
			batch_id TEXT NOT NULL,
			timestamp_ms INTEGER NOT NULL,
			client_id TEXT NOT NULL,
			endpoint_id TEXT NOT NULL,
			request_bytes INTEGER NOT NULL,
			upload_rate_bps INTEGER NOT NULL,
			response_bytes INTEGER NOT NULL,
			ordinal INTEGER NOT NULL,
			created_at TEXT NOT NULL,
			PRIMARY KEY (track_id, request_key),
			UNIQUE (track_id, request_id)
		)`,
		`CREATE TABLE IF NOT EXISTS request_io (
			track_id TEXT NOT NULL,
			request_key TEXT NOT NULL,
			status TEXT NOT NULL,
			error_code TEXT NOT NULL DEFAULT '',
			message TEXT NOT NULL DEFAULT '',
			recorded_at TEXT NOT NULL,
			PRIMARY KEY (track_id, request_key)
		)`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
			track_id TEXT NOT NULL,
			seq INTEGER NOT NULL,
			at_ms INTEGER NOT NULL,
			state_json TEXT NOT NULL,
			checksum TEXT NOT NULL,
			is_checkpoint INTEGER NOT NULL,
			created_at TEXT NOT NULL,
			PRIMARY KEY (track_id, seq)
		)`,
		`CREATE INDEX IF NOT EXISTS checkpoints_by_time ON checkpoints(track_id, at_ms, seq)`,
		`CREATE TABLE IF NOT EXISTS transitions (
			track_id TEXT NOT NULL,
			connection_id TEXT NOT NULL,
			endpoint_id TEXT NOT NULL DEFAULT '',
			seq INTEGER NOT NULL,
			at_ms INTEGER NOT NULL,
			from_stage TEXT NOT NULL,
			to_stage TEXT NOT NULL,
			request_key TEXT NOT NULL DEFAULT '',
			reason TEXT NOT NULL,
			PRIMARY KEY (track_id, connection_id, seq)
		)`,
		`CREATE INDEX IF NOT EXISTS transitions_by_connection ON transitions(track_id, connection_id, seq)`,
		`CREATE TABLE IF NOT EXISTS request_results (
			track_id TEXT NOT NULL,
			request_key TEXT NOT NULL,
			request_id TEXT NOT NULL,
			batch_id TEXT NOT NULL,
			client_id TEXT NOT NULL,
			endpoint_id TEXT NOT NULL,
			status TEXT NOT NULL,
			error_code TEXT NOT NULL DEFAULT '',
			error_message TEXT NOT NULL DEFAULT '',
			connection_id TEXT NOT NULL DEFAULT '',
			queued_at_ms INTEGER NOT NULL DEFAULT 0,
			acquired_at_ms INTEGER NOT NULL DEFAULT 0,
			started_ms INTEGER NOT NULL DEFAULT 0,
			completed_ms INTEGER NOT NULL,
			waited_ms INTEGER NOT NULL DEFAULT 0,
			upload_duration_ms INTEGER NOT NULL DEFAULT 0,
			response_duration_ms INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (track_id, request_key)
		)`,
		`CREATE TABLE IF NOT EXISTS final_connections (
			track_id TEXT NOT NULL,
			connection_id TEXT NOT NULL,
			endpoint_id TEXT NOT NULL,
			stage TEXT NOT NULL,
			reuse_count INTEGER NOT NULL,
			total_uses INTEGER NOT NULL,
			created_ms INTEGER NOT NULL,
			last_used_ms INTEGER NOT NULL,
			current_request_key TEXT NOT NULL DEFAULT '',
			close_reason TEXT NOT NULL DEFAULT '',
			closed_ms INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (track_id, connection_id)
		)`,
		`CREATE TABLE IF NOT EXISTS connection_uses (
			track_id TEXT NOT NULL,
			use_seq INTEGER NOT NULL,
			connection_id TEXT NOT NULL,
			request_key TEXT NOT NULL,
			request_id TEXT NOT NULL,
			acquired_ms INTEGER NOT NULL,
			reused INTEGER NOT NULL,
			PRIMARY KEY (track_id, use_seq)
		)`,
		`CREATE TABLE IF NOT EXISTS intents (
			id TEXT PRIMARY KEY,
			track_id TEXT NOT NULL,
			operation TEXT NOT NULL,
			payload TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at TEXT NOT NULL,
			applied_at TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS intents_pending ON intents(status, created_at)`,
		`CREATE TABLE IF NOT EXISTS recovery_reports (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			started_at TEXT NOT NULL,
			aborted_intents_json TEXT NOT NULL,
			pending_batches_json TEXT NOT NULL,
			resumed_batches_json TEXT NOT NULL,
			error_code TEXT NOT NULL DEFAULT '',
			message TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	if err := s.ensureColumn(ctx, "transitions", "endpoint_id", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureColumn(ctx context.Context, table, column, definition string) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `ALTER TABLE `+table+` ADD COLUMN `+column+` `+definition)
	return err
}

func (s *Store) SaveEndpoints(ctx context.Context, specs []model.EndpointSpec) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, spec := range specs {
		raw, err := json.Marshal(spec)
		if err != nil {
			return err
		}
		digest := shaHex(raw)
		var oldDigest string
		err = tx.QueryRowContext(ctx, `SELECT digest FROM endpoints WHERE id=?`, spec.ID).Scan(&oldDigest)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if _, err := tx.ExecContext(ctx, `INSERT INTO endpoints(id,digest,spec_json,updated_at) VALUES(?,?,?,?)`, spec.ID, digest, string(raw), now); err != nil {
				return err
			}
		case err != nil:
			return err
		case oldDigest != digest:
			return model.NewError(400, model.ErrSpecInvalid, "endpoint specification changed for an existing database", map[string]any{"endpoint_id": spec.ID})
		}
	}
	return tx.Commit()
}

func (s *Store) CreateIntent(ctx context.Context, trackID, operation string, payload any) (string, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	id, err := randomID()
	if err != nil {
		return "", err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO intents(id,track_id,operation,payload,status,created_at) VALUES(?,?,?,?,?,?)`,
		id, trackID, operation, string(raw), IntentPending, time.Now().UTC().Format(time.RFC3339Nano))
	return id, err
}

func (s *Store) FindBatch(ctx context.Context, trackID, batchID string) (*Batch, error) {
	var b Batch
	err := s.db.QueryRowContext(ctx, `SELECT track_id,batch_id,digest,status,response_json,created_at,applied_at FROM batches WHERE track_id=? AND batch_id=?`, trackID, batchID).
		Scan(&b.TrackID, &b.BatchID, &b.Digest, &b.Status, &b.ResponseJSON, &b.CreatedAt, &b.AppliedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &b, err
}

func (s *Store) InsertBatch(ctx context.Context, trackID, batchID, digest string, requests []model.Request, intentID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `INSERT INTO tracks(id,created_at,updated_at) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at`, trackID, now, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO batches(track_id,batch_id,digest,status,created_at) VALUES(?,?,?,?,?)`, trackID, batchID, digest, BatchPending, now); err != nil {
		return err
	}
	var maxOrdinal int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(ordinal),0) FROM requests WHERE track_id=?`, trackID).Scan(&maxOrdinal); err != nil {
		return err
	}
	for i, req := range requests {
		_, err := tx.ExecContext(ctx, `INSERT INTO requests(track_id,request_key,request_id,batch_id,timestamp_ms,client_id,endpoint_id,request_bytes,upload_rate_bps,response_bytes,ordinal,created_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, trackID, req.RequestKey, req.RequestID, batchID, req.TimestampMS, req.ClientID, req.EndpointID, req.RequestBytes, req.UploadRateBPS, req.ResponseBytes, maxOrdinal+i+1, now)
		if err != nil {
			return model.NewError(409, model.ErrConflictingBatch, "request id or request key already exists in this track", map[string]any{"request_id": req.RequestID, "db_error": err.Error()})
		}
	}
	if intentID != "" {
		if err := markIntentTx(ctx, tx, intentID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) Requests(ctx context.Context, trackID string) ([]model.Request, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT track_id,batch_id,request_key,request_id,timestamp_ms,client_id,endpoint_id,request_bytes,upload_rate_bps,response_bytes,ordinal FROM requests WHERE track_id=? ORDER BY ordinal`, trackID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Request
	for rows.Next() {
		var r model.Request
		if err := rows.Scan(&r.TrackID, &r.BatchID, &r.RequestKey, &r.RequestID, &r.TimestampMS, &r.ClientID, &r.EndpointID, &r.RequestBytes, &r.UploadRateBPS, &r.ResponseBytes, &r.Ordinal); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) PendingBatches(ctx context.Context) ([]PendingBatch, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT track_id,batch_id FROM batches WHERE status=? ORDER BY created_at`, BatchPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingBatch
	for rows.Next() {
		var pending PendingBatch
		if err := rows.Scan(&pending.TrackID, &pending.BatchID); err != nil {
			return nil, err
		}
		out = append(out, pending)
	}
	return out, rows.Err()
}

func (s *Store) PendingBatchIDsForTrack(ctx context.Context, trackID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT batch_id FROM batches WHERE track_id=? AND status=? ORDER BY created_at,batch_id`, trackID, BatchPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Store) LoadIO(ctx context.Context, trackID string) ([]IOFact, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT request_key,status,error_code,message FROM request_io WHERE track_id=?`, trackID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []IOFact
	for rows.Next() {
		var f IOFact
		if err := rows.Scan(&f.RequestKey, &f.Status, &f.ErrorCode, &f.Message); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func (s *Store) ApplyPlan(ctx context.Context, w PlanWrites) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, table := range []string{"checkpoints", "transitions", "request_results", "final_connections", "connection_uses"} {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE track_id=?`, w.TrackID); err != nil {
			return err
		}
	}
	interval := w.CheckpointInterval
	if interval < 1 {
		interval = 1
	}
	for _, snap := range w.Snapshots {
		raw, err := json.Marshal(snap)
		if err != nil {
			return err
		}
		isCheckpoint := snap.Seq%int64(interval) == 0 || snap.Seq == w.FinalSeq
		if _, err := tx.ExecContext(ctx, `INSERT INTO checkpoints(track_id,seq,at_ms,state_json,checksum,is_checkpoint,created_at) VALUES(?,?,?,?,?,?,?)`,
			w.TrackID, snap.Seq, snap.AtMS, string(raw), shaHex(raw), boolInt(isCheckpoint), now); err != nil {
			return err
		}
	}
	for _, tr := range w.Transitions {
		if _, err := tx.ExecContext(ctx, `INSERT INTO transitions(track_id,connection_id,endpoint_id,seq,at_ms,from_stage,to_stage,request_key,reason) VALUES(?,?,?,?,?,?,?,?,?)`,
			w.TrackID, tr.ConnectionID, tr.EndpointID, tr.Seq, tr.AtMS, tr.From, tr.To, tr.RequestKey, tr.Reason); err != nil {
			return err
		}
	}
	for _, r := range w.Results {
		if _, err := tx.ExecContext(ctx, `INSERT INTO request_results(track_id,request_key,request_id,batch_id,client_id,endpoint_id,status,error_code,error_message,connection_id,queued_at_ms,acquired_at_ms,started_ms,completed_ms,waited_ms,upload_duration_ms,response_duration_ms)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, w.TrackID, r.RequestKey, r.RequestID, r.BatchID, r.ClientID, r.EndpointID, r.Status, r.ErrorCode, r.ErrorMessage, r.ConnectionID, r.QueuedAtMS, r.AcquiredAtMS, r.StartedMS, r.CompletedMS, r.WaitedMS, r.UploadDurationMS, r.ResponseDurationMS); err != nil {
			return err
		}
	}
	for _, c := range w.Connections {
		if _, err := tx.ExecContext(ctx, `INSERT INTO final_connections(track_id,connection_id,endpoint_id,stage,reuse_count,total_uses,created_ms,last_used_ms,current_request_key,close_reason,closed_ms)
			VALUES(?,?,?,?,?,?,?,?,?,?,?)`, w.TrackID, c.ID, c.EndpointID, c.Stage, c.ReuseCount, c.TotalUses, c.CreatedMS, c.LastUsedMS, c.CurrentRequestKey, c.CloseReason, c.ClosedMS); err != nil {
			return err
		}
	}
	for i, u := range w.Uses {
		if _, err := tx.ExecContext(ctx, `INSERT INTO connection_uses(track_id,use_seq,connection_id,request_key,request_id,acquired_ms,reused) VALUES(?,?,?,?,?,?,?)`,
			w.TrackID, i+1, u.ConnectionID, u.RequestKey, u.RequestID, u.AcquiredMS, boolInt(u.Reused)); err != nil {
			return err
		}
	}
	for _, fact := range w.IO {
		if _, err := tx.ExecContext(ctx, `INSERT INTO request_io(track_id,request_key,status,error_code,message,recorded_at) VALUES(?,?,?,?,?,?) ON CONFLICT(track_id,request_key) DO UPDATE SET status=excluded.status,error_code=excluded.error_code,message=excluded.message,recorded_at=excluded.recorded_at`,
			w.TrackID, fact.RequestKey, fact.Status, fact.ErrorCode, fact.Message, now); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tracks SET updated_at=?,horizon_ms=?,final_seq=? WHERE id=?`, now, w.HorizonMS, w.FinalSeq, w.TrackID); err != nil {
		return err
	}
	for _, b := range w.Batches {
		res, err := tx.ExecContext(ctx, `UPDATE batches SET status=?,response_json=?,applied_at=? WHERE track_id=? AND batch_id=?`, BatchApplied, b.JSON, now, w.TrackID, b.BatchID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return fmt.Errorf("pending batch disappeared: %s", b.BatchID)
		}
	}
	if w.IntentID != "" {
		if err := markIntentTx(ctx, tx, w.IntentID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) SnapshotView(ctx context.Context, trackID string, atMS *int64) (*sim.Snapshot, int64, int64, error) {
	query := `SELECT c.seq,c.state_json,c.checksum,t.horizon_ms,t.final_seq
		FROM checkpoints c JOIN tracks t ON t.id=c.track_id WHERE c.track_id=?`
	args := []any{trackID}
	if atMS != nil {
		query += ` AND c.at_ms<=? ORDER BY c.at_ms DESC,c.seq DESC LIMIT 1`
		args = append(args, *atMS)
	} else {
		query += ` AND c.seq=t.final_seq LIMIT 1`
	}
	var seq, horizon, finalSeq int64
	var raw, checksum string
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&seq, &raw, &checksum, &horizon, &finalSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, 0, nil
	}
	if err != nil {
		return nil, 0, 0, err
	}
	if shaHex([]byte(raw)) != checksum {
		return nil, 0, 0, model.NewError(500, model.ErrCheckpointCorrupt, "checkpoint checksum does not match persisted state", map[string]any{"track_id": trackID, "seq": seq})
	}
	var snap sim.Snapshot
	if err := json.Unmarshal([]byte(raw), &snap); err != nil {
		return nil, 0, 0, model.NewError(500, model.ErrCheckpointCorrupt, "checkpoint JSON is invalid", map[string]any{"track_id": trackID, "seq": seq, "error": err.Error()})
	}
	return &snap, horizon, finalSeq, nil
}

func (s *Store) Transitions(ctx context.Context, trackID, connectionID string) ([]sim.Transition, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT connection_id,endpoint_id,seq,at_ms,from_stage,to_stage,request_key,reason FROM transitions WHERE track_id=? AND connection_id=? ORDER BY seq`, trackID, connectionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []sim.Transition
	for rows.Next() {
		var t sim.Transition
		if err := rows.Scan(&t.ConnectionID, &t.EndpointID, &t.Seq, &t.AtMS, &t.From, &t.To, &t.RequestKey, &t.Reason); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) ListTracks(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM tracks ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Store) ValidateTrackCheckpoints(ctx context.Context, trackID string) error {
	rows, err := s.db.QueryContext(ctx, `SELECT seq,state_json,checksum FROM checkpoints WHERE track_id=? ORDER BY seq`, trackID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var seq int64
		var raw, checksum string
		if err := rows.Scan(&seq, &raw, &checksum); err != nil {
			return err
		}
		if shaHex([]byte(raw)) != checksum {
			return model.NewError(500, model.ErrCheckpointCorrupt, "checkpoint checksum does not match persisted state", map[string]any{"track_id": trackID, "seq": seq})
		}
	}
	return rows.Err()
}

func (s *Store) AbortPendingIntents(ctx context.Context) ([]Intent, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id,track_id,operation,payload,status,created_at,applied_at FROM intents WHERE status=? ORDER BY created_at,id`, IntentPending)
	if err != nil {
		return nil, err
	}
	var out []Intent
	for rows.Next() {
		var in Intent
		if err := rows.Scan(&in.ID, &in.TrackID, &in.Operation, &in.Payload, &in.Status, &in.CreatedAt, &in.AppliedAt); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, in)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, in := range out {
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status=?,applied_at=? WHERE id=? AND status=?`, IntentAborted, now, in.ID, IntentPending); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) SaveRecoveryReport(ctx context.Context, report RecoveryReport) error {
	aborted, _ := json.Marshal(report.AbortedIntents)
	pending, _ := json.Marshal(report.PendingBatches)
	resumed, _ := json.Marshal(report.ResumedBatches)
	_, err := s.db.ExecContext(ctx, `INSERT INTO recovery_reports(started_at,aborted_intents_json,pending_batches_json,resumed_batches_json,error_code,message,created_at) VALUES(?,?,?,?,?,?,?)`,
		report.StartedAt, string(aborted), string(pending), string(resumed), report.ErrorCode, report.Message, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) LatestRecoveryReport(ctx context.Context) (*RecoveryReport, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id,started_at,aborted_intents_json,pending_batches_json,resumed_batches_json,error_code,message FROM recovery_reports ORDER BY id DESC LIMIT 1`)
	var r RecoveryReport
	var aborted, pending, resumed string
	if err := row.Scan(&r.ID, &r.StartedAt, &aborted, &pending, &resumed, &r.ErrorCode, &r.Message); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	_ = json.Unmarshal([]byte(aborted), &r.AbortedIntents)
	_ = json.Unmarshal([]byte(pending), &r.PendingBatches)
	_ = json.Unmarshal([]byte(resumed), &r.ResumedBatches)
	return &r, nil
}

func (s *Store) DB() *sql.DB { return s.db }

func markIntentTx(ctx context.Context, tx *sql.Tx, intentID string) error {
	res, err := tx.ExecContext(ctx, `UPDATE intents SET status=?,applied_at=? WHERE id=? AND status=?`, IntentApplied, time.Now().UTC().Format(time.RFC3339Nano), intentID, IntentPending)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("intent %s is not pending", intentID)
	}
	return nil
}

func shaHex(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func randomID() (string, error) {
	var buf [12]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

func NormalizeBatchDigest(trackID, batchID string, reqs []model.RequestInput) (string, error) {
	cp := append([]model.RequestInput(nil), reqs...)
	normalizeInputs(cp)
	raw, err := json.Marshal(struct {
		TrackID  string               `json:"track_id"`
		BatchID  string               `json:"batch_id"`
		Requests []model.RequestInput `json:"requests"`
	}{trackID, batchID, cp})
	if err != nil {
		return "", err
	}
	return shaHex(raw), nil
}

func normalizeInputs(reqs []model.RequestInput) {
	for i := range reqs {
		reqs[i].RequestID = strings.TrimSpace(reqs[i].RequestID)
		reqs[i].ClientID = strings.TrimSpace(reqs[i].ClientID)
		reqs[i].EndpointID = strings.TrimSpace(reqs[i].EndpointID)
	}
	sortInputs(reqs)
}

func sortInputs(reqs []model.RequestInput) {
	for i := 1; i < len(reqs); i++ {
		for j := i; j > 0 && requestInputLess(reqs[j], reqs[j-1]); j-- {
			reqs[j], reqs[j-1] = reqs[j-1], reqs[j]
		}
	}
}

func requestInputLess(a, b model.RequestInput) bool {
	if a.TimestampMS != b.TimestampMS {
		return a.TimestampMS < b.TimestampMS
	}
	if a.RequestID != b.RequestID {
		return a.RequestID < b.RequestID
	}
	if a.ClientID != b.ClientID {
		return a.ClientID < b.ClientID
	}
	if a.EndpointID != b.EndpointID {
		return a.EndpointID < b.EndpointID
	}
	if a.RequestBytes != b.RequestBytes {
		return a.RequestBytes < b.RequestBytes
	}
	if a.UploadRateBPS != b.UploadRateBPS {
		return a.UploadRateBPS < b.UploadRateBPS
	}
	return a.ResponseBytes < b.ResponseBytes
}

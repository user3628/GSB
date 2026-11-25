package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"dialogalign/internal/domain"

	_ "modernc.org/sqlite"
)

const (
	StageParse   = "parse"
	StageUpgrade = "upgrade"
	StageEvolve  = "evolve"
	StageAlign   = "align"
)

var stageOrder = map[string]int{
	StageParse:   1,
	StageUpgrade: 2,
	StageEvolve:  3,
	StageAlign:   4,
}

type Checkpoint struct {
	SessionID     string
	TurnIndex     int
	Stage         string
	Seq           int64
	InputDigest   string
	OutputDigest  string
	MapVersion    string
	Payload       []byte
	PayloadDigest string
}

type PublishRequest struct {
	Checkpoint      Checkpoint
	ExpectedSeq     int64
	StateJSON       []byte
	StateDigest     string
	AlignmentJSON   []byte
	AlignmentDigest string
}

type PublishResult struct {
	Seq        int64 `json:"seq"`
	NewHeadSeq int64 `json:"new_head_seq"`
}

type InvalidateResult struct {
	Seq        int64 `json:"seq"`
	Deleted    int   `json:"deleted"`
	NewHeadSeq int64 `json:"new_head_seq"`
}

type HistoricalState struct {
	Seq         int64
	MapVersion  string
	StateJSON   []byte
	StateDigest string
}

type Store struct {
	root     string
	db       *sql.DB
	lockPath string
}

func Open(root string) (*Store, error) {
	dir := filepath.Join(root, ".dialogbench")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(dir, "state.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA busy_timeout=10000",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := db.Exec(pragma); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	s := &Store{root: root, db: db, lockPath: filepath.Join(dir, "publish.lock")}
	if err := s.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.VerifyIntegrity(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS meta (
			k TEXT PRIMARY KEY,
			v TEXT NOT NULL
		)`,
		`INSERT OR IGNORE INTO meta(k,v) VALUES
			('next_seq','1'),('checkpoint_count','0'),('history_count','0')`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
			session_id TEXT NOT NULL,
			turn_index INTEGER NOT NULL,
			stage TEXT NOT NULL,
			seq INTEGER NOT NULL,
			input_digest TEXT NOT NULL,
			output_digest TEXT NOT NULL,
			map_version TEXT NOT NULL,
			payload BLOB NOT NULL,
			payload_digest TEXT NOT NULL,
			record_digest TEXT NOT NULL,
			PRIMARY KEY(session_id,turn_index,stage)
		)`,
		`CREATE TABLE IF NOT EXISTS checkpoint_history (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			session_id TEXT NOT NULL,
			turn_index INTEGER NOT NULL,
			stage TEXT NOT NULL,
			seq INTEGER NOT NULL,
			input_digest TEXT NOT NULL,
			output_digest TEXT NOT NULL,
			map_version TEXT NOT NULL,
			payload BLOB NOT NULL,
			payload_digest TEXT NOT NULL,
			record_digest TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_history_key ON checkpoint_history(session_id,turn_index,stage,seq DESC)`,
		`CREATE TABLE IF NOT EXISTS turn_states (
			session_id TEXT NOT NULL,
			turn_index INTEGER NOT NULL,
			map_version TEXT NOT NULL,
			state_digest TEXT NOT NULL,
			state_json BLOB NOT NULL,
			seq INTEGER NOT NULL,
			PRIMARY KEY(session_id,turn_index)
		)`,
		`CREATE TABLE IF NOT EXISTS turn_state_history (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			session_id TEXT NOT NULL,
			turn_index INTEGER NOT NULL,
			map_version TEXT NOT NULL,
			state_digest TEXT NOT NULL,
			state_json BLOB NOT NULL,
			seq INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_state_history_key ON turn_state_history(session_id,turn_index,seq DESC)`,
		`CREATE TABLE IF NOT EXISTS alignments (
			session_id TEXT NOT NULL,
			turn_index INTEGER NOT NULL,
			report_digest TEXT NOT NULL,
			report_json BLOB NOT NULL,
			seq INTEGER NOT NULL,
			PRIMARY KEY(session_id,turn_index)
		)`,
		`CREATE TABLE IF NOT EXISTS alignment_history (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			session_id TEXT NOT NULL,
			turn_index INTEGER NOT NULL,
			report_digest TEXT NOT NULL,
			report_json BLOB NOT NULL,
			seq INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS invalidations (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			seq INTEGER NOT NULL,
			session_id TEXT NOT NULL,
			turn_index INTEGER NOT NULL,
			stage TEXT NOT NULL,
			reason TEXT NOT NULL,
			old_input_digest TEXT NOT NULL,
			new_input_digest TEXT NOT NULL,
			old_output_digest TEXT NOT NULL,
			new_output_digest TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS stop_control (
			id INTEGER PRIMARY KEY CHECK(id=1),
			requested INTEGER NOT NULL,
			reason TEXT NOT NULL DEFAULT ''
		)`,
		`INSERT OR IGNORE INTO stop_control(id,requested,reason) VALUES(1,0,'')`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

func (s *Store) HeadSeq(ctx context.Context) (int64, error) {
	return s.metaInt(ctx, "next_seq")
}

func (s *Store) metaInt(ctx context.Context, key string) (int64, error) {
	var raw string
	if err := s.db.QueryRowContext(ctx, `SELECT v FROM meta WHERE k=?`, key).Scan(&raw); err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid meta %s: %w", key, err)
	}
	return n, nil
}

func recordDigest(cp Checkpoint) string {
	return domain.DigestBytes([]byte(strings.Join([]string{
		cp.SessionID,
		strconv.Itoa(cp.TurnIndex),
		cp.Stage,
		strconv.FormatInt(cp.Seq, 10),
		cp.InputDigest,
		cp.OutputDigest,
		cp.MapVersion,
		cp.PayloadDigest,
	}, "\x00")))
}

func (s *Store) Publish(ctx context.Context, req PublishRequest) (PublishResult, error) {
	if err := validatePublishRequest(req); err != nil {
		return PublishResult{}, err
	}
	lock, err := acquireFileLock(s.lockPath)
	if err != nil {
		return PublishResult{}, err
	}
	defer lock.Release()

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return PublishResult{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := s.verifyTx(ctx, tx); err != nil {
		return PublishResult{}, err
	}
	next, err := metaIntTx(ctx, tx, "next_seq")
	if err != nil {
		return PublishResult{}, err
	}
	if next != req.ExpectedSeq {
		return PublishResult{}, staleError(req.Checkpoint, fmt.Sprintf("expected head sequence %d, actual %d", req.ExpectedSeq, next))
	}
	if err := validateOrderTx(ctx, tx, req.Checkpoint); err != nil {
		return PublishResult{}, err
	}

	cp := req.Checkpoint
	cp.Seq = next
	cp.PayloadDigest = domain.DigestBytes(cp.Payload)
	cpDigest := recordDigest(cp)
	_, err = tx.ExecContext(ctx, `INSERT INTO checkpoints
		(session_id,turn_index,stage,seq,input_digest,output_digest,map_version,payload,payload_digest,record_digest)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		cp.SessionID, cp.TurnIndex, cp.Stage, cp.Seq, cp.InputDigest, cp.OutputDigest, cp.MapVersion, cp.Payload, cp.PayloadDigest, cpDigest)
	if err != nil {
		return PublishResult{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO checkpoint_history
		(session_id,turn_index,stage,seq,input_digest,output_digest,map_version,payload,payload_digest,record_digest)
		VALUES(?,?,?,?,?,?,?,?,?,?)`,
		cp.SessionID, cp.TurnIndex, cp.Stage, cp.Seq, cp.InputDigest, cp.OutputDigest, cp.MapVersion, cp.Payload, cp.PayloadDigest, cpDigest)
	if err != nil {
		return PublishResult{}, err
	}
	if cp.Stage == StageEvolve {
		if len(req.StateJSON) == 0 || req.StateDigest == "" {
			return PublishResult{}, domain.BizError(domain.CodeMissingPrerequisite, "evolve checkpoint requires state artifact")
		}
		if domain.DigestBytes(req.StateJSON) != req.StateDigest {
			return PublishResult{}, domain.BizError(domain.CodeCheckpointTampered, "state digest does not match state payload")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO turn_states(session_id,turn_index,map_version,state_digest,state_json,seq)
			VALUES(?,?,?,?,?,?) ON CONFLICT(session_id,turn_index) DO UPDATE SET
			map_version=excluded.map_version,state_digest=excluded.state_digest,state_json=excluded.state_json,seq=excluded.seq`,
			cp.SessionID, cp.TurnIndex, cp.MapVersion, req.StateDigest, req.StateJSON, cp.Seq); err != nil {
			return PublishResult{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO turn_state_history(session_id,turn_index,map_version,state_digest,state_json,seq)
			VALUES(?,?,?,?,?,?)`, cp.SessionID, cp.TurnIndex, cp.MapVersion, req.StateDigest, req.StateJSON, cp.Seq); err != nil {
			return PublishResult{}, err
		}
	}
	if cp.Stage == StageAlign {
		if len(req.AlignmentJSON) == 0 || req.AlignmentDigest == "" {
			return PublishResult{}, domain.BizError(domain.CodeMissingPrerequisite, "align checkpoint requires alignment artifact")
		}
		if domain.DigestBytes(req.AlignmentJSON) != req.AlignmentDigest {
			return PublishResult{}, domain.BizError(domain.CodeCheckpointTampered, "alignment digest does not match alignment payload")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO alignments(session_id,turn_index,report_digest,report_json,seq)
			VALUES(?,?,?,?,?) ON CONFLICT(session_id,turn_index) DO UPDATE SET
			report_digest=excluded.report_digest,report_json=excluded.report_json,seq=excluded.seq`,
			cp.SessionID, cp.TurnIndex, req.AlignmentDigest, req.AlignmentJSON, cp.Seq); err != nil {
			return PublishResult{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO alignment_history(session_id,turn_index,report_digest,report_json,seq)
			VALUES(?,?,?,?,?)`, cp.SessionID, cp.TurnIndex, req.AlignmentDigest, req.AlignmentJSON, cp.Seq); err != nil {
			return PublishResult{}, err
		}
	}
	if err := bumpMeta(ctx, tx, "next_seq", next+1); err != nil {
		return PublishResult{}, err
	}
	if err := incMeta(ctx, tx, "checkpoint_count", 1); err != nil {
		return PublishResult{}, err
	}
	if err := incMeta(ctx, tx, "history_count", 1); err != nil {
		return PublishResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return PublishResult{}, err
	}
	committed = true
	return PublishResult{Seq: cp.Seq, NewHeadSeq: next + 1}, nil
}

func validatePublishRequest(req PublishRequest) error {
	cp := req.Checkpoint
	if cp.SessionID == "" || cp.TurnIndex < 1 {
		return domain.BizError(domain.CodeInvalidRequest, "checkpoint requires session_id and positive turn_index")
	}
	if _, ok := stageOrder[cp.Stage]; !ok {
		return domain.BizError(domain.CodeStageTransition, "unknown stage "+cp.Stage)
	}
	if cp.InputDigest == "" || cp.OutputDigest == "" || len(cp.Payload) == 0 {
		return domain.ContextError(domain.CodeMissingPrerequisite, "checkpoint input/output/payload is required", cp.SessionID, cp.TurnIndex, "")
	}
	return nil
}

func validateOrderTx(ctx context.Context, tx *sql.Tx, cp Checkpoint) error {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM checkpoints WHERE session_id=?`, cp.SessionID).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		if cp.Stage != StageParse {
			return stageError(cp, domain.CodeStageTransition, "first publication must be turn 1 parse")
		}
		if cp.TurnIndex != 1 {
			return staleError(cp, "cross-session or out-of-order first turn publication")
		}
		return nil
	}
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM checkpoints WHERE session_id=? AND turn_index=? AND stage=?`,
		cp.SessionID, cp.TurnIndex, cp.Stage).Scan(&exists); err != nil {
		return err
	}
	if exists > 0 {
		return staleError(cp, "duplicate checkpoint publication")
	}
	var lastTurn int
	var lastStage string
	if err := tx.QueryRowContext(ctx, `SELECT turn_index,stage FROM checkpoints WHERE session_id=? ORDER BY turn_index DESC, CASE stage
		WHEN 'parse' THEN 1 WHEN 'upgrade' THEN 2 WHEN 'evolve' THEN 3 WHEN 'align' THEN 4 ELSE 99 END DESC LIMIT 1`,
		cp.SessionID).Scan(&lastTurn, &lastStage); err != nil {
		return err
	}
	lastOrder := stageOrder[lastStage]
	wantTurn, wantStage := lastTurn, ""
	if lastOrder < 4 {
		for stage, order := range stageOrder {
			if order == lastOrder+1 {
				wantStage = stage
				break
			}
		}
	} else {
		wantTurn = lastTurn + 1
		wantStage = StageParse
	}
	if cp.TurnIndex != wantTurn || cp.Stage != wantStage {
		return stageError(cp, domain.CodeStaleCheckpoint,
			fmt.Sprintf("expected next publication %s turn %d, got %s turn %d", wantStage, wantTurn, cp.Stage, cp.TurnIndex))
	}
	return nil
}

func staleError(cp Checkpoint, detail string) *domain.BusinessError {
	turn := cp.TurnIndex
	return &domain.BusinessError{
		Code:      domain.CodeStaleCheckpoint,
		Message:   "checkpoint publication is stale or out of order",
		SessionID: cp.SessionID,
		TurnIndex: &turn,
		Stage:     cp.Stage,
		Detail:    detail,
	}
}

func stageError(cp Checkpoint, code, detail string) *domain.BusinessError {
	turn := cp.TurnIndex
	return &domain.BusinessError{Code: code, Message: "invalid checkpoint stage transition", SessionID: cp.SessionID, TurnIndex: &turn, Stage: cp.Stage, Detail: detail}
}

func (s *Store) InvalidateFrom(ctx context.Context, sessionID string, turnIndex int, stage, reason, newInputDigest, newOutputDigest string, expectedSeq int64) (InvalidateResult, error) {
	order, ok := stageOrder[stage]
	if !ok {
		return InvalidateResult{}, domain.BizError(domain.CodeStageTransition, "unknown stage "+stage)
	}
	lock, err := acquireFileLock(s.lockPath)
	if err != nil {
		return InvalidateResult{}, err
	}
	defer lock.Release()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return InvalidateResult{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := s.verifyTx(ctx, tx); err != nil {
		return InvalidateResult{}, err
	}
	next, err := metaIntTx(ctx, tx, "next_seq")
	if err != nil {
		return InvalidateResult{}, err
	}
	if next != expectedSeq {
		return InvalidateResult{}, &domain.BusinessError{Code: domain.CodeStaleCheckpoint, Message: "invalidation raced with another publication", SessionID: sessionID, TurnIndex: domain.IntPtr(turnIndex), Stage: stage, Detail: fmt.Sprintf("expected %d actual %d", expectedSeq, next)}
	}
	var oldInput, oldOutput string
	err = tx.QueryRowContext(ctx, `SELECT input_digest,output_digest FROM checkpoints WHERE session_id=? AND turn_index=? AND stage=?`,
		sessionID, turnIndex, stage).Scan(&oldInput, &oldOutput)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return InvalidateResult{}, err
	}
	var deleted int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM checkpoints WHERE session_id=? AND (turn_index>? OR (turn_index=? AND CASE stage
		WHEN 'parse' THEN 1 WHEN 'upgrade' THEN 2 WHEN 'evolve' THEN 3 WHEN 'align' THEN 4 ELSE 99 END >= ?))`,
		sessionID, turnIndex, turnIndex, order).Scan(&deleted); err != nil {
		return InvalidateResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM checkpoints WHERE session_id=? AND (turn_index>? OR (turn_index=? AND CASE stage
		WHEN 'parse' THEN 1 WHEN 'upgrade' THEN 2 WHEN 'evolve' THEN 3 WHEN 'align' THEN 4 ELSE 99 END >= ?))`,
		sessionID, turnIndex, turnIndex, order); err != nil {
		return InvalidateResult{}, err
	}
	if order <= stageOrder[StageEvolve] {
		if _, err := tx.ExecContext(ctx, `DELETE FROM turn_states WHERE session_id=? AND turn_index>=?`, sessionID, turnIndex); err != nil {
			return InvalidateResult{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM alignments WHERE session_id=? AND turn_index>=?`, sessionID, turnIndex); err != nil {
		return InvalidateResult{}, err
	}
	invalidationSeq := next
	if _, err := tx.ExecContext(ctx, `INSERT INTO invalidations
		(seq,session_id,turn_index,stage,reason,old_input_digest,new_input_digest,old_output_digest,new_output_digest)
		VALUES(?,?,?,?,?,?,?,?,?)`,
		invalidationSeq, sessionID, turnIndex, stage, reason, oldInput, newInputDigest, oldOutput, newOutputDigest); err != nil {
		return InvalidateResult{}, err
	}
	if err := bumpMeta(ctx, tx, "next_seq", next+1); err != nil {
		return InvalidateResult{}, err
	}
	if err := incMeta(ctx, tx, "checkpoint_count", -deleted); err != nil {
		return InvalidateResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return InvalidateResult{}, err
	}
	committed = true
	return InvalidateResult{Seq: invalidationSeq, Deleted: deleted, NewHeadSeq: next + 1}, nil
}

func (s *Store) LoadCheckpoint(ctx context.Context, sessionID string, turn int, stage, inputDigest string) (*Checkpoint, bool, error) {
	cp, ok, err := s.ReadCheckpoint(ctx, sessionID, turn, stage)
	if err != nil || !ok {
		return nil, ok, err
	}
	if cp.InputDigest != inputDigest {
		return nil, false, nil
	}
	return cp, true, nil
}

func (s *Store) ReadCheckpoint(ctx context.Context, sessionID string, turn int, stage string) (*Checkpoint, bool, error) {
	var cp Checkpoint
	var recordDig string
	err := s.db.QueryRowContext(ctx, `SELECT session_id,turn_index,stage,seq,input_digest,output_digest,map_version,payload,payload_digest,record_digest
		FROM checkpoints WHERE session_id=? AND turn_index=? AND stage=?`, sessionID, turn, stage).
		Scan(&cp.SessionID, &cp.TurnIndex, &cp.Stage, &cp.Seq, &cp.InputDigest, &cp.OutputDigest, &cp.MapVersion, &cp.Payload, &cp.PayloadDigest, &recordDig)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if cp.PayloadDigest != domain.DigestBytes(cp.Payload) || cp.OutputDigest != cp.PayloadDigest {
		return nil, false, &domain.BusinessError{Code: domain.CodeCheckpointTampered, Message: "checkpoint digest mismatch", SessionID: sessionID, TurnIndex: domain.IntPtr(turn), Stage: stage}
	}
	if recordDig != recordDigest(cp) {
		return nil, false, &domain.BusinessError{Code: domain.CodeCheckpointTampered, Message: "checkpoint record digest mismatch", SessionID: sessionID, TurnIndex: domain.IntPtr(turn), Stage: stage}
	}
	return &cp, true, nil
}

func (s *Store) ListCheckpoints(ctx context.Context, sessionID string) ([]Checkpoint, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT session_id,turn_index,stage,seq,input_digest,output_digest,map_version,payload,payload_digest,record_digest
		FROM checkpoints WHERE session_id=? ORDER BY turn_index,
		CASE stage WHEN 'parse' THEN 1 WHEN 'upgrade' THEN 2 WHEN 'evolve' THEN 3 WHEN 'align' THEN 4 ELSE 99 END`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Checkpoint
	for rows.Next() {
		var cp Checkpoint
		var recordDig string
		if err := rows.Scan(&cp.SessionID, &cp.TurnIndex, &cp.Stage, &cp.Seq, &cp.InputDigest, &cp.OutputDigest, &cp.MapVersion, &cp.Payload, &cp.PayloadDigest, &recordDig); err != nil {
			return nil, err
		}
		if cp.PayloadDigest != domain.DigestBytes(cp.Payload) || recordDig != recordDigest(cp) {
			return nil, tamperCheckpoint(cp, "checkpoint digest mismatch while listing")
		}
		out = append(out, cp)
	}
	return out, rows.Err()
}

func (s *Store) VerifyIntegrity(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return s.verifyTx(ctx, tx)
}

func (s *Store) verifyTx(ctx context.Context, tx *sql.Tx) error {
	next, err := metaIntTx(ctx, tx, "next_seq")
	if err != nil {
		return err
	}
	wantCheckpoints, err := metaIntTx(ctx, tx, "checkpoint_count")
	if err != nil {
		return err
	}
	wantHistory, err := metaIntTx(ctx, tx, "history_count")
	if err != nil {
		return err
	}
	var gotCheckpoints, gotHistory int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM checkpoints`).Scan(&gotCheckpoints); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM checkpoint_history`).Scan(&gotHistory); err != nil {
		return err
	}
	if gotCheckpoints != wantCheckpoints || gotHistory != wantHistory {
		return domain.BizError(domain.CodeCheckpointTampered, "checkpoint row count does not match committed metadata")
	}
	rows, err := tx.QueryContext(ctx, `SELECT session_id,turn_index,stage,seq,input_digest,output_digest,map_version,payload,payload_digest,record_digest FROM checkpoints`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cp Checkpoint
		var storedRecordDigest string
		if err := rows.Scan(&cp.SessionID, &cp.TurnIndex, &cp.Stage, &cp.Seq, &cp.InputDigest, &cp.OutputDigest, &cp.MapVersion, &cp.Payload, &cp.PayloadDigest, &storedRecordDigest); err != nil {
			return err
		}
		if cp.PayloadDigest != domain.DigestBytes(cp.Payload) {
			return tamperCheckpoint(cp, "payload digest mismatch")
		}
		if storedRecordDigest != recordDigest(cp) {
			return tamperCheckpoint(cp, "record digest mismatch")
		}
		if cp.Seq >= next {
			return rollbackCheckpoint(cp, "checkpoint sequence is not behind the committed head")
		}
		var histMax sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT MAX(seq) FROM checkpoint_history WHERE session_id=? AND turn_index=? AND stage=?`,
			cp.SessionID, cp.TurnIndex, cp.Stage).Scan(&histMax); err != nil {
			return err
		}
		if histMax.Valid && cp.Seq < histMax.Int64 {
			return rollbackCheckpoint(cp, fmt.Sprintf("checkpoint sequence regressed below history sequence %d", histMax.Int64))
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	var histInvalid int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM checkpoint_history WHERE seq>=?`, next).Scan(&histInvalid); err != nil {
		return err
	}
	if histInvalid > 0 {
		return domain.BizError(domain.CodeCheckpointRollback, "history contains a sequence at or beyond the committed head")
	}
	return nil
}

func tamperCheckpoint(cp Checkpoint, detail string) *domain.BusinessError {
	turn := cp.TurnIndex
	return &domain.BusinessError{Code: domain.CodeCheckpointTampered, Message: "checkpoint integrity check failed", SessionID: cp.SessionID, TurnIndex: &turn, Stage: cp.Stage, Detail: detail}
}

func rollbackCheckpoint(cp Checkpoint, detail string) *domain.BusinessError {
	turn := cp.TurnIndex
	return &domain.BusinessError{Code: domain.CodeCheckpointRollback, Message: "checkpoint sequence rollback detected", SessionID: cp.SessionID, TurnIndex: &turn, Stage: cp.Stage, Detail: detail}
}

func metaIntTx(ctx context.Context, tx *sql.Tx, key string) (int64, error) {
	var raw string
	if err := tx.QueryRowContext(ctx, `SELECT v FROM meta WHERE k=?`, key).Scan(&raw); err != nil {
		return 0, err
	}
	return strconv.ParseInt(raw, 10, 64)
}

func bumpMeta(ctx context.Context, tx *sql.Tx, key string, value int64) error {
	result, err := tx.ExecContext(ctx, `UPDATE meta SET v=? WHERE k=?`, strconv.FormatInt(value, 10), key)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return fmt.Errorf("failed to update meta %s", key)
	}
	return nil
}

func incMeta(ctx context.Context, tx *sql.Tx, key string, delta int) error {
	current, err := metaIntTx(ctx, tx, key)
	if err != nil {
		return err
	}
	next := current + int64(delta)
	if next < 0 {
		return fmt.Errorf("meta %s would become negative", key)
	}
	return bumpMeta(ctx, tx, key, next)
}

func (s *Store) LatestState(ctx context.Context, sessionID string, turn int) ([]byte, string, error) {
	var blob []byte
	var digest string
	err := s.db.QueryRowContext(ctx, `SELECT state_json,state_digest FROM turn_states WHERE session_id=? AND turn_index=?`, sessionID, turn).Scan(&blob, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, "", nil
	}
	return blob, digest, err
}

func (s *Store) PreviousState(ctx context.Context, sessionID string, turn int, currentDigest string) (*HistoricalState, error) {
	var h HistoricalState
	err := s.db.QueryRowContext(ctx, `SELECT seq,map_version,state_json,state_digest FROM turn_state_history
		WHERE session_id=? AND turn_index=? AND state_digest<>? ORDER BY seq DESC LIMIT 1`,
		sessionID, turn, currentDigest).Scan(&h.Seq, &h.MapVersion, &h.StateJSON, &h.StateDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &h, nil
}

func (s *Store) LatestAlignment(ctx context.Context, sessionID string, turn int) ([]byte, string, error) {
	var blob []byte
	var digest string
	err := s.db.QueryRowContext(ctx, `SELECT report_json,report_digest FROM alignments WHERE session_id=? AND turn_index=?`, sessionID, turn).Scan(&blob, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, "", nil
	}
	return blob, digest, err
}

func (s *Store) Sessions(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT session_id FROM checkpoints ORDER BY session_id`)
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

func (s *Store) TurnCount(ctx context.Context, sessionID string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM checkpoints WHERE session_id=? AND stage='align'`, sessionID).Scan(&n)
	return n, err
}

func (s *Store) ListInvalidations(ctx context.Context, sessionID string) ([]domain.InvalidationRecord, error) {
	query := `SELECT seq,session_id,turn_index,stage,reason,old_input_digest,new_input_digest,old_output_digest,new_output_digest FROM invalidations`
	args := []any{}
	if sessionID != "" {
		query += ` WHERE session_id=?`
		args = append(args, sessionID)
	}
	query += ` ORDER BY seq DESC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.InvalidationRecord
	for rows.Next() {
		var rec domain.InvalidationRecord
		if err := rows.Scan(&rec.Seq, &rec.SessionID, &rec.TurnIndex, &rec.Stage, &rec.Reason, &rec.OldInputDigest, &rec.NewInputDigest, &rec.OldOutputDigest, &rec.NewOutputDigest); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *Store) RequestStop(ctx context.Context, reason string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE stop_control SET requested=1,reason=? WHERE id=1`, reason)
	return err
}

func (s *Store) ClearStop(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `UPDATE stop_control SET requested=0,reason='' WHERE id=1`)
	return err
}

func (s *Store) StopRequested(ctx context.Context) (bool, string, error) {
	var requested int
	var reason string
	if err := s.db.QueryRowContext(ctx, `SELECT requested,reason FROM stop_control WHERE id=1`).Scan(&requested, &reason); err != nil {
		return false, "", err
	}
	return requested == 1, reason, nil
}

func (s *Store) Reset(ctx context.Context) error {
	lock, err := acquireFileLock(s.lockPath)
	if err != nil {
		return err
	}
	defer lock.Release()
	tables := []string{"checkpoints", "checkpoint_history", "turn_states", "turn_state_history", "alignments", "alignment_history", "invalidations"}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, table := range tables {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+table); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	for key, value := range map[string]int64{"next_seq": 1, "checkpoint_count": 0, "history_count": 0} {
		if err := bumpMeta(ctx, tx, key, value); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE stop_control SET requested=0,reason='' WHERE id=1`); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

package ledger

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Config struct {
	Store            *Store
	DBPath           string
	Now              func() time.Time
	DefaultTolerance string
	ProbeTimeout     time.Duration
	ProbeConcurrency int
	BeforeCommitHook func(kind, commitID string, viewVersion int64) error
}

type Workbench struct {
	store *Store
	db    *sql.DB
	cfg   Config
	now   func() time.Time

	writer chan struct{}

	idemMu   sync.Mutex
	inflight map[string]*inflightCall

	probeSlots chan struct{}
	conns      *connectionRegistry

	recovery *RecoveryReport
}

type inflightCall struct {
	done       chan struct{}
	hash       string
	resultJSON []byte
	bizErr     *Error
}

type writeContext struct {
	Ctx           context.Context
	Tx            *sql.Tx
	CommitID      string
	IntentID      string
	Attempt       int
	CurrentView   int64
	NextView      int64
	NowMS         int64
	IncrementView bool
}

type idempotencyRecord struct {
	RequestHash string
	Kind        string
	Status      string
	ResultJSON  sql.NullString
	ErrorJSON   sql.NullString
	ViewVersion int64
	Attempt     int
}

func NewWorkbench(cfg Config) (*Workbench, error) {
	store := cfg.Store
	var err error
	if store == nil {
		store, err = OpenStore(cfg.DBPath)
		if err != nil {
			return nil, err
		}
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	concurrency := cfg.ProbeConcurrency
	if concurrency <= 0 {
		concurrency = 4
	}
	recovery, err := store.Recover(context.Background(), now().UnixMilli())
	if err != nil {
		return nil, err
	}
	return &Workbench{
		store:      store,
		db:         store.db,
		cfg:        cfg,
		now:        now,
		writer:     make(chan struct{}, 1),
		inflight:   map[string]*inflightCall{},
		probeSlots: make(chan struct{}, concurrency),
		conns:      newConnectionRegistry(),
		recovery:   recovery,
	}, nil
}

func (w *Workbench) Store() *Store { return w.store }
func (w *Workbench) DB() *sql.DB   { return w.db }

func (w *Workbench) Close() error {
	return w.store.Close()
}

func (w *Workbench) NowMS() int64 { return w.now().UnixMilli() }

func (w *Workbench) RecoveryReport() *RecoveryReport { return w.recovery }

func randomSuffix(prefix string) string {
	var buf [8]byte
	_, _ = rand.Read(buf[:])
	return prefix + hex.EncodeToString(buf[:])
}

func hashRequest(request any) string {
	buf, _ := json.Marshal(request)
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:])
}

func txMetaInt(ctx context.Context, tx *sql.Tx, key string) (int64, error) {
	var value string
	if err := tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key=?`, key).Scan(&value); err != nil {
		return 0, err
	}
	var parsed int64
	_, err := fmt.Sscanf(value, "%d", &parsed)
	return parsed, err
}

type commitOptions struct {
	Kind          string
	IncrementView bool
}

func executeCommit[T any](
	ctx context.Context,
	w *Workbench,
	commitID string,
	request any,
	opts commitOptions,
	action func(wc *writeContext) (*T, *Error),
) (*T, *Error) {
	if commitID == "" {
		commitID = randomSuffix("auto-")
	}
	requestHash := hashRequest(request)

	w.idemMu.Lock()
	if call, ok := w.inflight[commitID]; ok {
		if call.hash != requestHash {
			w.idemMu.Unlock()
			return nil, NewError(CodeConflictingLedger, "commit_id was already used with different content").
				WithContextMap(map[string]any{"commit_id": commitID, "reason": "concurrent_inflight_content_mismatch"})
		}
		w.idemMu.Unlock()
		<-call.done
		if call.bizErr != nil {
			return nil, call.bizErr
		}
		var out T
		if err := json.Unmarshal(call.resultJSON, &out); err != nil {
			return nil, WrapError(CodeInternal, err)
		}
		markReplayIfPossible(&out)
		return &out, nil
	}
	call := &inflightCall{done: make(chan struct{}), hash: requestHash}
	w.inflight[commitID] = call
	w.idemMu.Unlock()

	result, bizErr := executeLeader[T](ctx, w, commitID, request, requestHash, opts, action)
	if bizErr != nil {
		call.bizErr = bizErr
	} else {
		call.resultJSON, _ = json.Marshal(result)
	}
	close(call.done)

	w.idemMu.Lock()
	delete(w.inflight, commitID)
	w.idemMu.Unlock()
	return result, bizErr
}

func executeLeader[T any](
	ctx context.Context,
	w *Workbench,
	commitID string,
	request any,
	requestHash string,
	opts commitOptions,
	action func(wc *writeContext) (*T, *Error),
) (*T, *Error) {
	select {
	case w.writer <- struct{}{}:
	case <-ctx.Done():
		return nil, WrapError(CodeInternal, ctx.Err())
	}
	defer func() { <-w.writer }()

	nowMS := w.NowMS()
	existing, bizErr := loadIdempotency(ctx, w.db, commitID)
	if bizErr != nil {
		return nil, bizErr
	}
	attempt := 1
	if existing != nil {
		if existing.RequestHash != requestHash {
			return nil, NewError(CodeConflictingLedger, "commit_id was already used with different content").
				WithContextMap(map[string]any{
					"commit_id": commitID,
					"reason":    "persisted_content_mismatch",
					"kind":      existing.Kind,
				})
		}
		switch existing.Status {
		case "committed":
			var result T
			if existing.ResultJSON.Valid {
				if err := json.Unmarshal([]byte(existing.ResultJSON.String), &result); err != nil {
					return nil, WrapError(CodeInternal, err)
				}
			}
			if marker, ok := any(&result).(interface{ markIdempotentReplay() }); ok {
				marker.markIdempotentReplay()
			}
			markReplayIfPossible(&result)
			return &result, nil
		case "rejected":
			if existing.ErrorJSON.Valid {
				var replay Error
				if err := json.Unmarshal([]byte(existing.ErrorJSON.String), &replay); err == nil {
					replay.httpStatus = DefaultHTTPStatus(replay.Code)
					return nil, &replay
				}
			}
			return nil, NewError(CodeConflictingLedger, "commit was previously rejected").WithContext("commit_id", commitID)
		case "pending", "recovered":
			attempt = existing.Attempt + 1
		}
	}

	intentID := fmt.Sprintf("intent:%s:%d", commitID, attempt)
	payloadJSON, _ := json.Marshal(map[string]any{"request_hash": requestHash, "request": json.RawMessage(mustJSON(request))})
	intentTx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	_, err = intentTx.ExecContext(ctx, `
		INSERT INTO idempotency(commit_id,request_hash,kind,status,attempt,created_at_ms,updated_at_ms)
		VALUES(?,?,?,'pending',?,?,?)
		ON CONFLICT(commit_id) DO UPDATE SET status='pending',attempt=excluded.attempt,updated_at_ms=excluded.updated_at_ms
	`, commitID, requestHash, opts.Kind, attempt, nowMS, nowMS)
	if err == nil {
		_, err = intentTx.ExecContext(ctx, `
			INSERT INTO intents(intent_id,commit_id,kind,attempt,payload_json,status,created_at_ms,updated_at_ms)
			VALUES(?,?,?,?,?,'started',?,?)
			ON CONFLICT(intent_id) DO UPDATE SET status='started',payload_json=excluded.payload_json,updated_at_ms=excluded.updated_at_ms
		`, intentID, commitID, opts.Kind, attempt, string(payloadJSON), nowMS, nowMS)
	}
	if err != nil {
		_ = intentTx.Rollback()
		return nil, WrapError(CodeInternal, err)
	}
	if err := intentTx.Commit(); err != nil {
		return nil, WrapError(CodeInternal, err)
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	currentView, err := txMetaInt(ctx, tx, "view_version")
	if err != nil {
		_ = tx.Rollback()
		return nil, WrapError(CodeInternal, err)
	}
	nextView := currentView
	if opts.IncrementView {
		nextView = currentView + 1
		if _, err := tx.ExecContext(ctx, `UPDATE meta SET value=? WHERE key='view_version'`, fmt.Sprintf("%d", nextView)); err != nil {
			_ = tx.Rollback()
			return nil, WrapError(CodeInternal, err)
		}
	}

	wc := &writeContext{
		Ctx: ctx, Tx: tx, CommitID: commitID, IntentID: intentID, Attempt: attempt,
		CurrentView: currentView, NextView: nextView, NowMS: nowMS, IncrementView: opts.IncrementView,
	}
	result, actionErr := action(wc)
	if actionErr != nil {
		_ = tx.Rollback()
		persistRejectedIntent(w, commitID, intentID, attempt, actionErr, nowMS)
		return nil, actionErr
	}
	if w.cfg.BeforeCommitHook != nil {
		if hookErr := w.cfg.BeforeCommitHook(opts.Kind, commitID, nextView); hookErr != nil {
			_ = tx.Rollback()
			bizErr := NewError(CodeRebuildFailed, hookErr.Error()).WithContext("commit_id", commitID)
			persistRejectedIntent(w, commitID, intentID, attempt, bizErr, nowMS)
			return nil, bizErr
		}
	}

	resultJSON, err := json.Marshal(result)
	if err != nil {
		_ = tx.Rollback()
		return nil, WrapError(CodeInternal, err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE intents SET status='committed',view_version=?,updated_at_ms=? WHERE intent_id=?
	`, nextView, nowMS, intentID); err != nil {
		_ = tx.Rollback()
		return nil, WrapError(CodeInternal, err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE idempotency SET status='committed',result_json=?,error_json=NULL,view_version=?,updated_at_ms=? WHERE commit_id=?
	`, string(resultJSON), nextView, nowMS, commitID); err != nil {
		_ = tx.Rollback()
		return nil, WrapError(CodeInternal, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE meta SET value=? WHERE key='current_commit_id'`, commitID); err != nil {
		_ = tx.Rollback()
		return nil, WrapError(CodeInternal, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	return result, nil
}

func markReplayIfPossible(result any) {
	type replayMarker interface{ MarkIdempotentReplay() }
	if marker, ok := result.(replayMarker); ok {
		marker.MarkIdempotentReplay()
	}
}

func (r *CommitResult) MarkIdempotentReplay() {
	if r != nil {
		r.IdempotentReplay = true
	}
}

func mustJSON(value any) []byte {
	buf, err := json.Marshal(value)
	if err != nil {
		return []byte(`"<marshal-error>"`)
	}
	return buf
}

func loadIdempotency(ctx context.Context, db *sql.DB, commitID string) (*idempotencyRecord, *Error) {
	var record idempotencyRecord
	err := db.QueryRowContext(ctx, `
		SELECT request_hash,kind,status,result_json,error_json,view_version,attempt
		FROM idempotency WHERE commit_id=?`, commitID).Scan(
		&record.RequestHash, &record.Kind, &record.Status, &record.ResultJSON,
		&record.ErrorJSON, &record.ViewVersion, &record.Attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, WrapError(CodeInternal, err)
	}
	return &record, nil
}

func persistRejectedIntent(w *Workbench, commitID, intentID string, attempt int, bizErr *Error, nowMS int64) {
	errJSON, _ := json.Marshal(bizErr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback() }()
	_, _ = tx.ExecContext(ctx, `UPDATE intents SET status='failed',updated_at_ms=? WHERE intent_id=?`, nowMS, intentID)
	_, _ = tx.ExecContext(ctx, `
		UPDATE idempotency SET status='rejected',error_json=?,attempt=?,updated_at_ms=? WHERE commit_id=?
	`, string(errJSON), attempt, nowMS, commitID)
	_ = tx.Commit()
}

func checkObservedVersion(ctx context.Context, db *sql.DB, observedVersion int64) (int64, *Error) {
	current, err := (&Store{db: db}).ViewVersion(ctx)
	if err != nil {
		return 0, WrapError(CodeInternal, err)
	}
	if observedVersion > current {
		return current, NewError(CodeStaleRead, "ledger attribution view is behind observed version").
			WithContextMap(map[string]any{"view_version": current, "observed_version": observedVersion})
	}
	return current, nil
}

package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"blocksnap/internal/apperr"

	_ "modernc.org/sqlite"
)

type Store struct {
	db     *sql.DB
	mutMu  sync.Mutex
	now    func() time.Time
	dbPath string
}

type Option func(*Store)

func WithClock(clock func() time.Time) Option {
	return func(s *Store) { s.now = clock }
}

func Open(path string, opts ...Option) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("database path is required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return nil, err
	}
	fileURL := (&url.URL{Scheme: "file", Path: filepath.ToSlash(abs)}).String()
	dsn := fmt.Sprintf("%s?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=synchronous(FULL)", fileURL)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{db: db, now: time.Now, dbPath: abs}
	for _, opt := range opts {
		opt(s)
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Now() time.Time { return s.now().UTC() }

func (s *Store) nowText() string { return s.now().UTC().Format(time.RFC3339Nano) }

// MutationResult is either a fresh success or an exact replay of a stored
// idempotent response.
type MutationResult struct {
	HTTPStatus int
	Body       []byte
}

type mutationApply func(context.Context, *sql.Tx, string) (any, error)

type requestRecord struct {
	Hash       string
	Status     string
	HTTPStatus int
	Body       string
}

func (s *Store) executeMutation(ctx context.Context, volumeID, operation, requestID, requestHash string, apply mutationApply) (MutationResult, error) {
	if strings.TrimSpace(requestID) == "" {
		return MutationResult{}, apperr.Invalid("request_id is required")
	}
	if strings.TrimSpace(requestHash) == "" {
		return MutationResult{}, apperr.Invalid("request hash is required")
	}
	s.mutMu.Lock()
	defer s.mutMu.Unlock()

	existing, found, err := s.readRequest(ctx, requestID)
	if err != nil {
		return MutationResult{}, err
	}
	if found {
		if existing.Hash != requestHash {
			return MutationResult{}, apperr.WithContext(apperr.New(apperr.CodeConflictingRequest, "request_id was already used with different content", 409), "request_id", requestID)
		}
		if existing.Status == "completed" {
			return s.replay(existing)
		}
		// A pending row can only remain here after an interrupted process. The
		// recovery pass normally handles it; fail closed if called directly.
		aborted := apperr.WithContext(apperr.New(apperr.CodeOperationAborted, "request was interrupted before commit", 409), "request_id", requestID)
		body, _ := marshalError(aborted)
		_ = s.finishAborted(context.Background(), requestID, "", aborted, body)
		aborted.ReplayStatus = aborted.HTTPStatus
		aborted.ReplayBody = body
		return MutationResult{}, aborted
	}

	intentID := intentID(requestID, operation)
	now := s.nowText()
	preTx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return MutationResult{}, apperr.Internal(err)
	}
	if _, err := preTx.ExecContext(ctx, `INSERT INTO requests(request_id,request_hash,volume_id,operation,status,created_at,updated_at) VALUES(?,?,?,?, 'pending',?,?)`,
		requestID, requestHash, volumeID, operation, now, now); err != nil {
		preTx.Rollback()
		return MutationResult{}, apperr.Internal(err)
	}
	detail, _ := json.Marshal(map[string]string{"request_hash": requestHash, "volume_id": volumeID})
	if _, err := preTx.ExecContext(ctx, `INSERT INTO intents(id,request_id,volume_id,operation,state,detail_json,created_at,updated_at) VALUES(?,?,?,?, 'pending',?,?,?)`,
		intentID, requestID, volumeID, operation, string(detail), now, now); err != nil {
		preTx.Rollback()
		return MutationResult{}, apperr.Internal(err)
	}
	if err := preTx.Commit(); err != nil {
		return MutationResult{}, apperr.Internal(err)
	}

	opTx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return MutationResult{}, s.abortBeforeApply(requestID, intentID, apperr.Internal(err))
	}
	result, applyErr := apply(ctx, opTx, intentID)
	if applyErr != nil {
		_ = opTx.Rollback()
		ae := apperr.As(applyErr)
		body, _ := marshalError(ae)
		finishErr := s.finishAborted(context.Background(), requestID, intentID, ae, body)
		if finishErr != nil {
			return MutationResult{}, apperr.Internal(fmt.Errorf("%v; additionally failed to record abort: %w", applyErr, finishErr))
		}
		ae.ReplayStatus = ae.HTTPStatus
		ae.ReplayBody = body
		return MutationResult{}, ae
	}
	body, err := json.Marshal(result)
	if err != nil {
		_ = opTx.Rollback()
		return MutationResult{}, s.abortBeforeApply(requestID, intentID, apperr.Internal(err))
	}
	now = s.nowText()
	if _, err := opTx.ExecContext(ctx, `UPDATE requests SET status='completed',http_status=?,response_json=?,updated_at=? WHERE request_id=?`,
		HTTPStatusOK, string(body), now, requestID); err != nil {
		_ = opTx.Rollback()
		return MutationResult{}, s.abortBeforeApply(requestID, intentID, apperr.Internal(err))
	}
	if _, err := opTx.ExecContext(ctx, `UPDATE intents SET state='committed',updated_at=? WHERE id=?`, now, intentID); err != nil {
		_ = opTx.Rollback()
		return MutationResult{}, s.abortBeforeApply(requestID, intentID, apperr.Internal(err))
	}
	if err := opTx.Commit(); err != nil {
		return MutationResult{}, s.abortBeforeApply(requestID, intentID, apperr.Internal(err))
	}
	return MutationResult{HTTPStatus: HTTPStatusOK, Body: body}, nil
}

const HTTPStatusOK = 200

func (s *Store) readRequest(ctx context.Context, requestID string) (requestRecord, bool, error) {
	var rec requestRecord
	err := s.db.QueryRowContext(ctx, `SELECT request_hash,status,http_status,response_json FROM requests WHERE request_id=?`, requestID).
		Scan(&rec.Hash, &rec.Status, &rec.HTTPStatus, &rec.Body)
	if errors.Is(err, sql.ErrNoRows) {
		return requestRecord{}, false, nil
	}
	if err != nil {
		return requestRecord{}, false, apperr.Internal(err)
	}
	return rec, true, nil
}

func (s *Store) replay(rec requestRecord) (MutationResult, error) {
	if rec.HTTPStatus >= 200 && rec.HTTPStatus < 300 {
		return MutationResult{HTTPStatus: rec.HTTPStatus, Body: []byte(rec.Body)}, nil
	}
	var envelope struct {
		Error *apperr.Error `json:"error"`
	}
	if err := json.Unmarshal([]byte(rec.Body), &envelope); err != nil || envelope.Error == nil {
		if err == nil {
			err = errors.New("stored error response has no error object")
		}
		return MutationResult{}, apperr.Internal(err)
	}
	envelope.Error.HTTPStatus = rec.HTTPStatus
	envelope.Error.ReplayStatus = rec.HTTPStatus
	envelope.Error.ReplayBody = []byte(rec.Body)
	return MutationResult{}, envelope.Error
}

func (s *Store) finishAborted(ctx context.Context, requestID, intentID string, ae *apperr.Error, body []byte) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := s.nowText()
	if _, err := tx.ExecContext(ctx, `UPDATE requests SET status='completed',http_status=?,response_json=?,updated_at=? WHERE request_id=?`,
		ae.HTTPStatus, string(body), now, requestID); err != nil {
		return err
	}
	if intentID != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE intents SET state='aborted',updated_at=? WHERE id=?`, now, intentID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) abortBeforeApply(requestID, intentID string, ae *apperr.Error) error {
	body, _ := marshalError(ae)
	_ = s.finishAborted(context.Background(), requestID, intentID, ae, body)
	ae.ReplayStatus = ae.HTTPStatus
	ae.ReplayBody = body
	return ae
}

func intentID(requestID, operation string) string {
	sum := sha256.Sum256([]byte(operation + "\x00" + requestID))
	return hex.EncodeToString(sum[:])
}

func marshalError(e *apperr.Error) ([]byte, error) {
	if e == nil {
		e = apperr.Internal(nil)
	}
	return json.Marshal(struct {
		Error *apperr.Error `json:"error"`
	}{Error: e})
}

// RequestHash creates a stable hash over the operation and canonical request
// payload. Callers should hash structs with json.Marshal or this helper.
func RequestHash(operation, volumeID string, payload any) string {
	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(append([]byte(operation+"\x00"+volumeID+"\x00"), raw...))
	return hex.EncodeToString(sum[:])
}

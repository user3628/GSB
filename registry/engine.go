package registry

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Engine serializes all writes while allowing concurrent complete-version
// readers. Network probes run under the writer gate but not under stateMu, so
// queries continue to read the last complete revision during a round.
type Engine struct {
	db *DB

	writeMu sync.Mutex
	stateMu sync.RWMutex

	reqMu sync.Mutex
	reqs  map[string]*idemEntry

	crashHook func(string)
}

// Option customizes a diagnostic Engine.
type Option func(*Engine)

// WithCrashHook installs a test-only hook after durable phase boundaries.
func WithCrashHook(fn func(string)) Option {
	return func(e *Engine) { e.crashHook = fn }
}

type idemEntry struct {
	operation string
	hash      string
	done      chan struct{}
	body      []byte
	err       *Error
}

// Open initializes the database, performs crash recovery, and returns an
// integration-ready engine.
func Open(path string, opts ...Option) (*Engine, error) {
	db, err := OpenDB(path)
	if err != nil {
		return nil, err
	}
	e := &Engine{db: db, reqs: make(map[string]*idemEntry)}
	for _, opt := range opts {
		opt(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := e.Recover(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return e, nil
}

func (e *Engine) Close() error { return e.db.Close() }

func (e *Engine) crash(phase string) {
	if e.crashHook != nil {
		e.crashHook(phase)
	}
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func requestHash(v any) string {
	b, _ := json.Marshal(v)
	return sha256Hex(b)
}

func snapshotHash(req SnapshotRequest) string {
	req.RequestID = ""
	req.Instances = SortedInstances(req.Instances)
	return requestHash(req)
}

func deltaHash(req DeltaRequest) string {
	req.RequestID = ""
	req.Upserts = SortedInstances(req.Upserts)
	sort.Slice(req.Removes, func(i, j int) bool {
		if req.Removes[i].ServiceName != req.Removes[j].ServiceName {
			return req.Removes[i].ServiceName < req.Removes[j].ServiceName
		}
		return req.Removes[i].InstanceID < req.Removes[j].InstanceID
	})
	return requestHash(req)
}

func roundHash(opts RoundOptions) string {
	opts.RequestID = ""
	return requestHash(opts)
}

// idempotent executes a request once. Concurrent duplicates wait for that
// execution and reuse its bytes; completed duplicates are served from SQLite.
func (e *Engine) idempotent(ctx context.Context, requestID, operation, hash string, fn func() ([]byte, *Error)) ([]byte, *Error) {
	if requestID == "" {
		return fn()
	}

	e.reqMu.Lock()
	if entry, ok := e.reqs[requestID]; ok {
		if entry.operation != operation || entry.hash != hash {
			e.reqMu.Unlock()
			return nil, newError(ErrConflictingRequest, "requestId was already used with different content",
				"requestId", requestID, "existingOperation", entry.operation, "receivedOperation", operation)
		}
		if entry.done != nil {
			select {
			case <-entry.done:
				e.reqMu.Unlock()
				return entry.body, entry.err
			default:
			}
		}
		done := entry.done
		e.reqMu.Unlock()
		select {
		case <-done:
			e.reqMu.Lock()
			body, opErr := entry.body, entry.err
			e.reqMu.Unlock()
			return body, opErr
		case <-ctx.Done():
			return nil, newError(ErrInternal, "waiting for duplicate request was canceled", "requestId", requestID)
		}
	}

	if body, opErr, found, conflict := e.lookupRequest(ctx, requestID, operation, hash); found {
		e.reqMu.Unlock()
		if conflict != nil {
			return nil, conflict
		}
		e.reqs[requestID] = &idemEntry{operation: operation, hash: hash, done: closedChan(), body: body, err: opErr}
		return body, opErr
	}

	entry := &idemEntry{operation: operation, hash: hash, done: make(chan struct{})}
	e.reqs[requestID] = entry
	e.reqMu.Unlock()

	body, opErr := fn()
	e.reqMu.Lock()
	entry.body, entry.err = body, opErr
	close(entry.done)
	e.reqMu.Unlock()

	if opErr != nil {
		e.persistFailure(ctx, requestID, operation, hash, opErr)
	}
	return body, opErr
}

func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (e *Engine) lookupRequest(ctx context.Context, requestID, operation, hash string) ([]byte, *Error, bool, *Error) {
	e.stateMu.RLock()
	var op, storedHash, status, response, errCode string
	err := readTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		var qerr error
		op, storedHash, status, response, errCode, qerr = getRequest(ctx, tx, requestID)
		return qerr
	})
	e.stateMu.RUnlock()
	if err == sql.ErrNoRows {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, newError(ErrInternal, "read request journal failed", "detail", err.Error())
	}
	if op != operation || storedHash != hash {
		return nil, nil, false, newError(ErrConflictingRequest, "requestId was already used with different content",
			"requestId", requestID, "existingOperation", op, "receivedOperation", operation)
	}
	if status == "COMPLETED" {
		var opErr *Error
		if errCode != "" {
			opErr = &Error{}
			if json.Unmarshal([]byte(response), opErr) != nil {
				opErr = newError(errCode, "replayed failed request")
			}
		}
		return []byte(response), opErr, true, nil
	}
	if status == "FAILED" {
		opErr := &Error{}
		if err := json.Unmarshal([]byte(response), opErr); err != nil || opErr.Code == "" {
			opErr = newError(errCode, "replayed failed request")
		}
		return nil, opErr, true, nil
	}
	// RUNNING/ABORTED records have no committed verdict. Recovery marks them
	// ABORTED; a retry may execute from the last complete state.
	return nil, nil, false, nil
}

func (e *Engine) persistFailure(ctx context.Context, requestID, operation, hash string, opErr *Error) {
	if requestID == "" {
		return
	}
	raw, _ := json.Marshal(opErr)
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	_ = writeTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		now := nowText()
		_, err := tx.ExecContext(ctx, `INSERT INTO requests(request_id,operation,payload_hash,status,response_json,http_status,error_code,created_at,updated_at)
			VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(request_id) DO UPDATE SET status='FAILED',response_json=excluded.response_json,
			http_status=excluded.http_status,error_code=excluded.error_code,updated_at=excluded.updated_at`,
			requestID, operation, hash, "FAILED", string(raw), opErr.HTTPStatus(), opErr.Code, now, now)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE intents SET status='ABORTED',updated_at=? WHERE request_id=? AND status='RUNNING'`, now, requestID)
		return err
	})
}

// Current returns one complete committed registration/health snapshot.
func (e *Engine) Current(ctx context.Context) (CurrentSnapshot, *Error) {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	var out CurrentSnapshot
	err := readTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		meta, err := readCurrentMeta(ctx, tx)
		if err != nil {
			return err
		}
		out.Version = meta.Version
		out.DerivedRevision = meta.DerivedRevision
		if meta.Version > 0 {
			generated, digest, _, err := snapshotMeta(ctx, tx, meta.Version)
			if err != nil {
				return err
			}
			out.GeneratedAt, out.Digest = generated, digest
		}
		out.Instances, err = loadRuntimeInstances(ctx, tx)
		return err
	})
	if err != nil {
		return out, newError(ErrInternal, "read current snapshot failed", "detail", err.Error())
	}
	return out, nil
}

// Derived reads the active derived view in the same read transaction as the
// current pointers.
func (e *Engine) Derived(ctx context.Context) (DerivedView, *Error) {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	var meta currentMeta
	var view DerivedView
	err := readTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		var err error
		meta, err = readCurrentMeta(ctx, tx)
		if err != nil {
			return err
		}
		view, err = loadDerivedView(ctx, tx, meta.DerivedRevision)
		return err
	})
	if err != nil {
		if de, ok := err.(*Error); ok {
			return view, de
		}
		return view, newError(ErrInternal, "read derived view failed", "detail", err.Error())
	}
	return view, nil
}

// CompleteView is a single-reader consistency unit: the committed snapshot,
// active instances and derived projections all come from one SQLite read
// transaction.
type CompleteView struct {
	Current CurrentSnapshot
	Derived DerivedView
}

// ReadComplete returns the registration snapshot and derived view at exactly
// one commit pointer. Writers cannot switch revisions halfway through it.
func (e *Engine) ReadComplete(ctx context.Context) (CompleteView, *Error) {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	var out CompleteView
	err := readTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		meta, err := readCurrentMeta(ctx, tx)
		if err != nil {
			return err
		}
		out.Current.Version = meta.Version
		out.Current.DerivedRevision = meta.DerivedRevision
		if meta.Version > 0 {
			generated, digest, _, err := snapshotMeta(ctx, tx, meta.Version)
			if err != nil {
				return err
			}
			out.Current.GeneratedAt, out.Current.Digest = generated, digest
		}
		out.Current.Instances, err = loadRuntimeInstances(ctx, tx)
		if err != nil {
			return err
		}
		out.Derived, err = loadDerivedView(ctx, tx, meta.DerivedRevision)
		return err
	})
	if err != nil {
		if de, ok := err.(*Error); ok {
			return out, de
		}
		return out, newError(ErrInternal, "read complete view failed", "detail", err.Error())
	}
	return out, nil
}

// Intents returns recent intent journal rows, newest first.
func (e *Engine) Intents(ctx context.Context, limit int) ([]IntentView, *Error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	var out []IntentView
	err := readTx(ctx, e.db.sql, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT id,kind,request_id,status,target_version,derived_revision,round_id,created_at,updated_at
			FROM intents ORDER BY created_at DESC LIMIT ?`, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var in IntentView
			if err := rows.Scan(&in.ID, &in.Kind, &in.RequestID, &in.Status, &in.TargetVersion,
				&in.DerivedRevision, &in.RoundID, &in.CreatedAt, &in.UpdatedAt); err != nil {
				return err
			}
			out = append(out, in)
		}
		return rows.Err()
	})
	if err != nil {
		return out, newError(ErrInternal, "read intent journal failed", "detail", err.Error())
	}
	return out, nil
}

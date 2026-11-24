package blockpool

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

type requestRow struct {
	Operation    string
	RequestHash  string
	Status       string
	Owner        string
	ResponseJSON []byte
	ErrorJSON    []byte
}

type mutationSpec struct {
	RequestID    string
	Operation    string
	AggregateKey string
	Payload      any
	PayloadHash  string
	LockKeys     []string
	BusyKey      string
}

func requestHash(operation string, payload any) (string, *Error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return "", Errorf(CodeSpecInvalid, "request payload is not serializable", "cause", err.Error())
	}
	sum := sha256.Sum256(append([]byte(operation+"\n"), b...))
	return hex.EncodeToString(sum[:]), nil
}

func (p *Pool) lookupRequest(ctx context.Context, requestID string) (*requestRow, error) {
	row := p.db.QueryRowContext(ctx, `SELECT operation, request_hash, status, owner, response_json, error_json
		FROM requests WHERE request_id = ?`, requestID)
	var r requestRow
	var response, errJSON sql.NullString
	if err := row.Scan(&r.Operation, &r.RequestHash, &r.Status, &r.Owner, &response, &errJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if response.Valid {
		r.ResponseJSON = []byte(response.String)
	}
	if errJSON.Valid {
		r.ErrorJSON = []byte(errJSON.String)
	}
	return &r, nil
}

// runMutation is the single idempotent mutation path. The request row is first
// claimed, then a durable intent row is committed, and only then does the
// business transaction run. The request result and intent completion are part
// of that same business transaction. Consequently, after a crash either the
// whole business change plus result exists, or it does not and the intent stays
// pending for the next opener.
func runMutation[T any](p *Pool, ctx context.Context, spec mutationSpec,
	work func(context.Context, *sql.Tx, int64) (T, *Error)) (T, *Error) {
	var zero T
	requestID := spec.RequestID
	if requestID == "" {
		requestID = randomID(16)
	}
	if spec.PayloadHash == "" {
		hash, err := requestHash(spec.Operation, spec.Payload)
		if err != nil {
			return zero, err
		}
		spec.PayloadHash = hash
	}

	unlockRequest, err := p.locks.Lock(ctx, []string{"q:" + requestID})
	if err != nil {
		return zero, err
	}
	defer unlockRequest()

	// A same-process concurrent duplicate waits on the request lock above and
	// then observes the completed row. The loop also supports a second Pool
	// instance in another goroutine/process sharing the same database.
	for {
		row, lookupErr := p.lookupRequest(ctx, requestID)
		if lookupErr != nil {
			return zero, Errorf(CodeInternal, "read idempotency record", "cause", lookupErr.Error())
		}
		if row != nil {
			if row.Operation != spec.Operation || row.RequestHash != spec.PayloadHash {
				return zero, Errorf(CodeConflictingRequest,
					"request_id was already used for a different operation or payload",
					"request_id", requestID, "existing_operation", row.Operation)
			}
			switch row.Status {
			case "completed":
				if len(row.ErrorJSON) > 0 {
					return zero, errorFromJSON(row.ErrorJSON)
				}
				if err := json.Unmarshal(row.ResponseJSON, &zero); err != nil {
					return zero, Errorf(CodeInternal, "decode idempotent result", "cause", err.Error())
				}
				return zero, nil
			case "in_progress":
				select {
				case <-ctx.Done():
					return zero, Errorf(CodeContextCanceled, "request canceled while waiting for duplicate request", "cause", ctx.Err().Error())
				case <-time.After(10 * time.Millisecond):
					continue
				}
			case "aborted":
				// The previous process never committed a result. Fall through
				// and claim it for a fresh attempt.
			default:
				return zero, Errorf(CodeInternal, "unknown idempotency status", "status", row.Status)
			}
		}

		claimed, claimErr := p.claimRequest(ctx, requestID, spec)
		if claimErr != nil {
			return zero, claimErr
		}
		if claimed {
			break
		}
	}

	if spec.BusyKey != "" {
		unlockBusy, ok := p.batchGate.TryLock([]string{spec.BusyKey})
		if !ok {
			busy := Errorf(CodeNodeBusy, "another batch replace is already in flight for this node",
				"aggregate_key", spec.AggregateKey)
			p.finishImmediateError(context.Background(), requestID, busy)
			return zero, busy
		}
		defer unlockBusy()
		if busyErr := p.acquirePersistentBatchLock(ctx, spec.BusyKey, requestID); busyErr != nil {
			p.finishImmediateError(context.Background(), requestID, busyErr)
			return zero, busyErr
		}
		defer p.releasePersistentBatchLock(spec.BusyKey, requestID)
	}

	intentID := randomID(16)
	payloadJSON, marshalErr := json.Marshal(spec.Payload)
	if marshalErr != nil {
		p.abortRequest(context.Background(), requestID, "payload serialization failed")
		return zero, Errorf(CodeInternal, "serialize intent payload", "cause", marshalErr.Error())
	}
	now := p.nowMS()
	intentTx, beginErr := p.beginWrite(ctx)
	if beginErr != nil {
		p.abortRequest(context.Background(), requestID, "intent transaction could not begin")
		return zero, Errorf(CodeInternal, "begin intent transaction", "cause", beginErr.Error())
	}
	if _, execErr := intentTx.ExecContext(ctx, `INSERT INTO intents
		(id, request_id, operation, aggregate_key, payload_json, state, created_at_ms)
		VALUES (?, ?, ?, ?, ?, 'pending', ?)`, intentID, requestID, spec.Operation,
		spec.AggregateKey, string(payloadJSON), now); execErr != nil {
		rollback(intentTx)
		p.abortRequest(context.Background(), requestID, "intent insert failed")
		return zero, Errorf(CodeInternal, "persist intent", "cause", execErr.Error())
	}
	if commitErr := intentTx.Commit(); commitErr != nil {
		p.abortRequest(context.Background(), requestID, "intent commit failed")
		return zero, Errorf(CodeInternal, "commit intent", "cause", commitErr.Error())
	}

	unlockKeys, lockErr := p.locks.Lock(ctx, spec.LockKeys)
	if lockErr != nil {
		p.finishWithError(context.Background(), requestID, intentID, lockErr, "lock acquisition canceled")
		return zero, lockErr
	}
	defer unlockKeys()

	workNow := p.nowMS()
	tx, beginErr := p.beginWrite(ctx)
	if beginErr != nil {
		p.finishWithError(context.Background(), requestID, intentID,
			Errorf(CodeInternal, "begin business transaction", "cause", beginErr.Error()), "business transaction begin failed")
		return zero, Errorf(CodeInternal, "begin business transaction", "cause", beginErr.Error())
	}

	result, workErr := work(ctx, tx, workNow)
	if workErr != nil {
		rollback(tx)
		p.finishWithError(context.Background(), requestID, intentID, workErr, workErr.Message)
		return zero, workErr
	}
	resultJSON, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		rollback(tx)
		internal := Errorf(CodeInternal, "serialize result", "cause", marshalErr.Error())
		p.finishWithError(context.Background(), requestID, intentID, internal, internal.Message)
		return zero, internal
	}
	if _, execErr := tx.ExecContext(ctx, `UPDATE requests
		SET status='completed', response_json=?, error_json=NULL, completed_at_ms=?
		WHERE request_id=? AND status='in_progress'`, string(resultJSON), p.nowMS(), requestID); execErr != nil {
		rollback(tx)
		p.abortRequest(context.Background(), requestID, "result update failed")
		return zero, Errorf(CodeInternal, "persist request result", "cause", execErr.Error())
	}
	if _, execErr := tx.ExecContext(ctx, `UPDATE intents
		SET state='committed', finished_at_ms=?, detail='mutation and result committed'
		WHERE id=? AND state='pending'`, p.nowMS(), intentID); execErr != nil {
		rollback(tx)
		p.abortRequest(context.Background(), requestID, "intent completion update failed")
		return zero, Errorf(CodeInternal, "complete intent", "cause", execErr.Error())
	}
	if commitErr := tx.Commit(); commitErr != nil {
		p.abortRequest(context.Background(), requestID, "business commit failed")
		return zero, Errorf(CodeInternal, "commit business transaction", "cause", commitErr.Error())
	}
	return result, nil
}

func (p *Pool) acquirePersistentBatchLock(ctx context.Context, nodeKey, requestID string) *Error {
	tx, err := p.beginWrite(ctx)
	if err != nil {
		return Errorf(CodeInternal, "begin batch lock transaction", "cause", err.Error())
	}
	defer rollback(tx)
	res, err := tx.ExecContext(ctx, `INSERT INTO batch_locks(node_key, request_id, acquired_at_ms)
		VALUES (?, ?, ?) ON CONFLICT(node_key) DO NOTHING`, nodeKey, requestID, p.nowMS())
	if err != nil {
		return Errorf(CodeInternal, "claim persistent batch lock", "cause", err.Error())
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return Errorf(CodeNodeBusy, "another batch replace is already in flight for this node",
			"node_key", nodeKey)
	}
	if err := tx.Commit(); err != nil {
		return Errorf(CodeInternal, "commit persistent batch lock", "cause", err.Error())
	}
	return nil
}

func (p *Pool) releasePersistentBatchLock(nodeKey, requestID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = p.db.ExecContext(ctx, `DELETE FROM batch_locks WHERE node_key=? AND request_id=?`, nodeKey, requestID)
}

func (p *Pool) finishImmediateError(ctx context.Context, requestID string, businessErr *Error) {
	errorJSON, _ := json.Marshal(businessErr)
	tx, err := p.beginWrite(ctx)
	if err != nil {
		return
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, `UPDATE requests SET status='completed', error_json=?, response_json=NULL,
		completed_at_ms=? WHERE request_id=? AND status='in_progress'`, string(errorJSON), p.nowMS(), requestID); err != nil {
		return
	}
	_ = tx.Commit()
}

func (p *Pool) claimRequest(ctx context.Context, requestID string, spec mutationSpec) (bool, *Error) {
	tx, err := p.beginWrite(ctx)
	if err != nil {
		return false, Errorf(CodeInternal, "begin request claim", "cause", err.Error())
	}
	defer rollback(tx)

	var status, owner, hash string
	err = tx.QueryRowContext(ctx, `SELECT status, owner, request_hash FROM requests WHERE request_id=?`, requestID).
		Scan(&status, &owner, &hash)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, Errorf(CodeInternal, "read request claim", "cause", err.Error())
	}
	if err == nil {
		if hash != spec.PayloadHash {
			return false, Errorf(CodeConflictingRequest, "request_id payload conflicts with existing request", "request_id", requestID)
		}
		if status == "completed" || status == "in_progress" {
			return false, nil
		}
	}

	now := p.nowMS()
	if err == nil {
		_, err = tx.ExecContext(ctx, `UPDATE requests SET status='in_progress', owner=?, status_code=0,
			response_json=NULL, error_json=NULL, completed_at_ms=NULL WHERE request_id=?`, p.instanceID, requestID)
	} else {
		_, err = tx.ExecContext(ctx, `INSERT INTO requests
			(request_id, operation, request_hash, status, owner, created_at_ms)
			VALUES (?, ?, ?, 'in_progress', ?, ?)`, requestID, spec.Operation, spec.PayloadHash, p.instanceID, now)
	}
	if err != nil {
		return false, Errorf(CodeInternal, "claim request", "cause", err.Error())
	}
	if err := tx.Commit(); err != nil {
		return false, Errorf(CodeInternal, "commit request claim", "cause", err.Error())
	}
	return true, nil
}

func (p *Pool) finishWithError(ctx context.Context, requestID, intentID string, businessErr *Error, detail string) {
	errorJSON, _ := json.Marshal(businessErr)
	tx, err := p.beginWrite(ctx)
	if err != nil {
		return
	}
	defer rollback(tx)
	now := p.nowMS()
	if _, err := tx.ExecContext(ctx, `UPDATE requests SET status='completed', error_json=?, response_json=NULL,
		completed_at_ms=? WHERE request_id=?`, string(errorJSON), now, requestID); err != nil {
		return
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET state='failed', finished_at_ms=?, detail=? WHERE id=?`,
		now, detail, intentID); err != nil {
		return
	}
	_ = tx.Commit()
}

func (p *Pool) abortRequest(ctx context.Context, requestID, detail string) {
	tx, err := p.beginWrite(ctx)
	if err != nil {
		return
	}
	defer rollback(tx)
	now := p.nowMS()
	if _, err := tx.ExecContext(ctx, `UPDATE requests SET status='aborted', completed_at_ms=? WHERE request_id=? AND status='in_progress'`,
		now, requestID); err != nil {
		return
	}
	if _, err := tx.ExecContext(ctx, `UPDATE intents SET state='aborted', finished_at_ms=?, detail=?
		WHERE request_id=? AND state='pending'`, now, detail, requestID); err != nil {
		return
	}
	_ = tx.Commit()
}

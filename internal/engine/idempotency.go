package engine

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"mvccengine/pkg/mvcc"
)

type RequestRun struct {
	ID          string
	call        *inflightCall
	completedTx bool
}

func hashRequest(scope string, payload any) string {
	b, _ := json.Marshal(struct {
		Scope   string `json:"scope"`
		Payload any    `json:"payload"`
	}{Scope: scope, Payload: payload})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (e *Engine) Execute(id, scope string, payload any, fn func(run *RequestRun) (any, error)) (any, error) {
	if id == "" {
		id = newRequestID()
	}
	hash := hashRequest(scope, payload)

	e.callsMu.Lock()
	if call, ok := e.calls[id]; ok {
		if call.scope != scope || call.hash != hash {
			e.callsMu.Unlock()
			return nil, mvcc.NewError(mvcc.CodeConflictingRequest, "request_id was already used with different content", map[string]any{
				"request_id":     id,
				"expected_scope": call.scope,
				"actual_scope":   scope,
			})
		}
		e.callsMu.Unlock()
		<-call.done
		return call.response, call.err
	}

	call := &inflightCall{done: make(chan struct{}), scope: scope, hash: hash}
	e.calls[id] = call
	e.callsMu.Unlock()

	var existingScope, existingHash, existingStatus string
	var responseJSON, errorJSON, intentID sql.NullString
	err := e.db.QueryRow(`SELECT scope, request_hash, status, response_json, error_json, intent_id
		FROM idempotent_requests WHERE request_id=?`, id).
		Scan(&existingScope, &existingHash, &existingStatus, &responseJSON, &errorJSON, &intentID)
	if err == nil {
		if existingScope != scope || existingHash != hash {
			call.err = mvcc.NewError(mvcc.CodeConflictingRequest, "request_id was already persisted with different content", map[string]any{
				"request_id":            id,
				"expected_scope":        existingScope,
				"actual_scope":          scope,
				"expected_request_hash": existingHash,
				"actual_request_hash":   hash,
			})
			close(call.done)
			return nil, call.err
		}
		switch existingStatus {
		case "COMPLETED":
			raw := json.RawMessage(responseJSON.String)
			call.response = raw
			close(call.done)
			return raw, nil
		case "FAILED":
			var me mvcc.Error
			if json.Unmarshal([]byte(errorJSON.String), &me) == nil && me.Code != "" {
				call.err = &me
			} else {
				call.err = mvcc.NewError(mvcc.CodeInterrupted, "request failed before completion", map[string]any{"request_id": id})
			}
			close(call.done)
			return nil, call.err
		default:
			call.err = mvcc.NewError(mvcc.CodeInterrupted, "request is interrupted; recovery has marked it unresolved", map[string]any{"request_id": id, "status": existingStatus})
			close(call.done)
			return nil, call.err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		call.err = err
		close(call.done)
		return nil, err
	}

	now := e.now()
	_, err = e.db.Exec(`INSERT INTO idempotent_requests(request_id,scope,request_hash,status,response_json,error_json,intent_id,created_at_ms,updated_at_ms)
		VALUES(?,?,?,'PENDING',NULL,NULL,'',?,?)`, id, scope, hash, now, now)
	if err != nil {
		call.err = err
		close(call.done)
		return nil, err
	}

	run := &RequestRun{ID: id, call: call}
	result, execErr := fn(run)
	if execErr != nil {
		errPayload := jsonMarshal(mvcc.AsError(execErr))
		_, _ = e.db.Exec(`UPDATE idempotent_requests SET status='FAILED', error_json=?, updated_at_ms=? WHERE request_id=? AND status='PENDING'`, errPayload, e.now(), id)
		call.err = execErr
		close(call.done)
		return result, execErr
	}

	if !run.completedTx {
		respPayload := jsonMarshal(result)
		_, execErr = e.db.Exec(`UPDATE idempotent_requests SET status='COMPLETED', response_json=?, updated_at_ms=? WHERE request_id=?`, respPayload, e.now(), id)
		if execErr != nil {
			call.err = execErr
			close(call.done)
			return nil, execErr
		}
	}
	call.response = result
	close(call.done)
	return result, nil
}

func executeAs[T any](e *Engine, id, scope string, payload any, fn func(run *RequestRun) (*T, error)) (*T, error) {
	raw, err := e.Execute(id, scope, payload, func(run *RequestRun) (any, error) {
		return fn(run)
	})
	if err != nil {
		return nil, err
	}
	switch value := raw.(type) {
	case *T:
		return value, nil
	case T:
		return &value, nil
	case json.RawMessage:
		var result T
		if err := json.Unmarshal(value, &result); err != nil {
			return nil, err
		}
		return &result, nil
	default:
		b, err := json.Marshal(raw)
		if err != nil {
			return nil, err
		}
		var result T
		if err := json.Unmarshal(b, &result); err != nil {
			return nil, err
		}
		return &result, nil
	}
}

func (r *RequestRun) CompleteTx(tx *sql.Tx, response any, intentID string) error {
	payload := jsonMarshal(response)
	_, err := tx.Exec(`UPDATE idempotent_requests
		SET status='COMPLETED', response_json=?, error_json=NULL, intent_id=?, updated_at_ms=?
		WHERE request_id=?`, payload, intentID, time.Now().UnixMilli(), r.ID)
	if err != nil {
		return err
	}
	r.completedTx = true
	return nil
}

func (e *Engine) createIntent(run *RequestRun, kind string, payload any) (string, error) {
	intentID := newIntentID(kind)
	now := e.now()
	tx, err := e.db.Begin()
	if err != nil {
		return "", err
	}
	if _, err := tx.Exec(`INSERT INTO intents(intent_id,kind,request_id,payload_json,status,created_at_ms,updated_at_ms)
		VALUES(?,?,?,?,'PENDING',?,?)`, intentID, kind, run.ID, jsonMarshal(payload), now, now); err != nil {
		_ = tx.Rollback()
		return "", err
	}
	if _, err := tx.Exec(`UPDATE idempotent_requests SET intent_id=?, updated_at_ms=? WHERE request_id=?`, intentID, now, run.ID); err != nil {
		_ = tx.Rollback()
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return intentID, nil
}

func completeIntentTx(tx *sql.Tx, intentID string, result any) error {
	_, err := tx.Exec(`UPDATE intents SET status='DONE', result_json=?, error_json=NULL, updated_at_ms=? WHERE intent_id=?`,
		jsonMarshal(result), time.Now().UnixMilli(), intentID)
	return err
}

func failIntentTx(tx *sql.Tx, intentID string, cause error) error {
	_, err := tx.Exec(`UPDATE intents SET status='FAILED', error_json=?, updated_at_ms=? WHERE intent_id=?`,
		jsonMarshal(mvcc.AsError(cause)), time.Now().UnixMilli(), intentID)
	return err
}

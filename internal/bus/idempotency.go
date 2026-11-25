package bus

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
)

type requestCall struct {
	operation string
	hash      string
	done      chan struct{}
	response  json.RawMessage
	err       error
}

func contentHash(operation string, content any) string {
	encoded, _ := json.Marshal(content)
	var generic any
	if json.Unmarshal(encoded, &generic) == nil {
		if object, ok := generic.(map[string]any); ok {
			delete(object, "request_id")
		}
		encoded, _ = json.Marshal(generic)
	}
	sum := sha256.Sum256(append([]byte(operation+"\x00"), encoded...))
	return hex.EncodeToString(sum[:])
}

// idempotentJSON executes fn exactly once for a request id and coalesces
// concurrent callers. The durable copy also survives a process restart.
func idempotentJSON[T any](
	s *Service,
	ctx context.Context,
	operation, requestID, hash string,
	fn func() (T, error),
) (T, error) {
	var zero T
	if requestID == "" {
		return fn()
	}

	s.requestsMu.Lock()
	if call := s.requests[requestID]; call != nil {
		if call.operation != operation || call.hash != hash {
			s.requestsMu.Unlock()
			return zero, NewError(CodeConflictingRequest, "request id was used with different content or operation")
		}
		done := call.done
		s.requestsMu.Unlock()
		select {
		case <-done:
			if call.err != nil {
				return zero, call.err
			}
			if err := json.Unmarshal(call.response, &zero); err != nil {
				return zero, WrapError(CodeInternal, "decode idempotent response", err)
			}
			return zero, nil
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}

	if response, err, found, loadErr := s.loadCompletedRequest(ctx, requestID, operation, hash); loadErr != nil {
		s.requestsMu.Unlock()
		return zero, loadErr
	} else if found {
		s.requestsMu.Unlock()
		if err != nil {
			return zero, err
		}
		if err := json.Unmarshal(response, &zero); err != nil {
			return zero, WrapError(CodeInternal, "decode durable idempotent response", err)
		}
		return zero, nil
	}

	call := &requestCall{operation: operation, hash: hash, done: make(chan struct{})}
	s.requests[requestID] = call
	s.requestsMu.Unlock()

	if err := s.claimRequest(ctx, requestID, operation, hash); err != nil {
		s.requestsMu.Lock()
		call.err = err
		delete(s.requests, requestID)
		close(call.done)
		s.requestsMu.Unlock()
		return zero, err
	}

	result, operationErr := fn()
	var response json.RawMessage
	var encodeErr error
	if operationErr == nil {
		response, encodeErr = json.Marshal(result)
		if encodeErr != nil {
			operationErr = WrapError(CodeInternal, "encode operation response", encodeErr)
		}
	}

	// A committed business operation must not lose its idempotent record merely
	// because the caller's request context was cancelled.
	persistCtx := context.WithoutCancel(ctx)
	if persistErr := s.completeRequest(persistCtx, requestID, operation, hash, response, operationErr); persistErr != nil && operationErr == nil {
		operationErr = persistErr
	}

	s.requestsMu.Lock()
	call.response = response
	call.err = operationErr
	close(call.done)
	s.requestsMu.Unlock()
	return result, operationErr
}

func (s *Service) loadCompletedRequest(ctx context.Context, requestID, operation, hash string) (json.RawMessage, error, bool, error) {
	var storedOperation, storedHash, status, response, errorCode, errorMessage string
	err := s.db.QueryRowContext(ctx, `
		SELECT operation, content_hash, status, response_json, error_code, error_message
		FROM idempotency WHERE request_id = ?`, requestID).
		Scan(&storedOperation, &storedHash, &status, &response, &errorCode, &errorMessage)
	if err == sql.ErrNoRows {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, WrapError(CodeInternal, "read idempotency record", err)
	}
	if storedOperation != operation || storedHash != hash {
		return nil, nil, false, NewError(CodeConflictingRequest, "request id was used with different content or operation")
	}
	if status != "DONE" {
		return nil, nil, false, WrapError(CodeInternal, "request id is incomplete after restart; retry with a new request id", nil)
	}
	if errorCode != "" {
		return nil, NewError(errorCode, errorMessage), true, nil
	}
	return json.RawMessage(response), nil, true, nil
}

func (s *Service) claimRequest(ctx context.Context, requestID, operation, hash string) error {
	now := s.nowMS()
	_, err := s.execWrite(ctx, `
		INSERT INTO idempotency(request_id, operation, content_hash, status, created_at_ms, updated_at_ms)
		VALUES(?, ?, ?, 'RUNNING', ?, ?)`, requestID, operation, hash, now, now)
	if err != nil {
		var existingOperation, existingHash, status string
		readErr := s.db.QueryRowContext(ctx,
			`SELECT operation, content_hash, status FROM idempotency WHERE request_id = ?`, requestID).
			Scan(&existingOperation, &existingHash, &status)
		if readErr == nil && (existingOperation != operation || existingHash != hash) {
			return NewError(CodeConflictingRequest, "request id was used with different content or operation")
		}
		if readErr == nil && status == "RUNNING" {
			return NewError(CodeInternal, "concurrent or interrupted request")
		}
		return WrapError(CodeInternal, "claim request id", err)
	}
	return nil
}

func (s *Service) completeRequest(
	ctx context.Context,
	requestID, operation, hash string,
	response json.RawMessage,
	operationErr error,
) error {
	errorCode := ""
	errorMessage := ""
	if operationErr != nil {
		errorCode = ErrorCode(operationErr)
		var busErr *BusError
		if errors.As(operationErr, &busErr) {
			errorMessage = busErr.Message
		} else {
			errorMessage = operationErr.Error()
		}
	}
	if len(response) == 0 {
		response = json.RawMessage("null")
	}
	_, err := s.execWrite(ctx, `
		UPDATE idempotency SET status = 'DONE', response_json = ?, error_code = ?, error_message = ?, updated_at_ms = ?
		WHERE request_id = ? AND operation = ? AND content_hash = ?`,
		string(response), errorCode, errorMessage, s.nowMS(), requestID, operation, hash)
	if err != nil {
		return WrapError(CodeInternal, "persist idempotent response", err)
	}
	return nil
}

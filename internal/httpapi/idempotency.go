package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"

	"local/objectstore/internal/model"
	"local/objectstore/internal/store"
)

type Outcome struct {
	Status int
	Body   []byte
	Code   string
}

type requestCall struct {
	fingerprint string
	done        chan struct{}
	outcome     Outcome
	err         *model.Error
}

type RequestCoordinator struct {
	store *store.Store
	now   func() time.Time
	mu    sync.Mutex
	calls map[string]*requestCall
}

func NewRequestCoordinator(database *store.Store, now func() time.Time) *RequestCoordinator {
	if now == nil {
		now = time.Now
	}
	return &RequestCoordinator{store: database, now: now, calls: make(map[string]*requestCall)}
}

func RequestFingerprint(operation string, value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		encoded = []byte(err.Error())
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(operation))
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write(encoded)
	return hex.EncodeToString(hasher.Sum(nil))
}

func (c *RequestCoordinator) Run(ctx context.Context, requestID, operation, fingerprint string, execute func() Outcome) (Outcome, bool, *model.Error) {
	c.mu.Lock()
	if existing := c.calls[requestID]; existing != nil {
		if existing.fingerprint != fingerprint {
			c.mu.Unlock()
			return Outcome{}, false, model.NewError(model.CodeConflictingRequest, "request_id is already executing with different content").
				WithContext("request_id", requestID)
		}
		done := existing.done
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return Outcome{}, false, model.WrapError(model.CodeInternal, "request was cancelled while waiting for duplicate", ctx.Err())
		case <-done:
			if existing.err != nil {
				return Outcome{}, false, existing.err
			}
			return existing.outcome, true, nil
		}
	}
	call := &requestCall{fingerprint: fingerprint, done: make(chan struct{})}
	c.calls[requestID] = call
	c.mu.Unlock()

	finish := func(outcome Outcome, business *model.Error) {
		c.mu.Lock()
		call.outcome = outcome
		call.err = business
		delete(c.calls, requestID)
		close(call.done)
		c.mu.Unlock()
	}

	record, err := c.store.ClaimRequest(ctx, requestID, operation, fingerprint, c.now().UnixMilli())
	if err != nil {
		business := model.AsError(err)
		finish(Outcome{}, business)
		return Outcome{}, false, business
	}
	if record.Status == "completed" {
		outcome := Outcome{Status: record.HTTPStatus, Body: record.ResponseBody, Code: record.Code}
		finish(outcome, nil)
		return outcome, true, nil
	}

	outcome := execute()
	if err := c.store.CompleteRequest(ctx, requestID, outcome.Status, outcome.Body, outcome.Code, c.now().UnixMilli()); err != nil {
		// The mutation is already complete and operation methods are retry-safe. A failed
		// response cache must not turn a committed mutation into a visible failure.
		finish(outcome, nil)
		return outcome, false, nil
	}
	finish(outcome, nil)
	return outcome, false, nil
}

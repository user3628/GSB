package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"local/sqlitepool/internal/domain"
)

type idempotencyEntry struct {
	digest string
	done   chan struct{}
	result apiResult
}

type idempotencyCoordinator struct {
	mu      sync.Mutex
	entries map[string]*idempotencyEntry
}

func newIdempotencyCoordinator() *idempotencyCoordinator {
	return &idempotencyCoordinator{entries: make(map[string]*idempotencyEntry)}
}

func digestFor(route, canonicalBody string) string {
	sum := sha256.Sum256([]byte(route + "\n" + canonicalBody))
	return hex.EncodeToString(sum[:])
}

// execute serializes concurrent requests sharing a request ID. The winner runs
// the operation exactly once; followers wait for and reuse the exact byte
// response. A different digest under the same ID is rejected.
func (c *idempotencyCoordinator) execute(ctx context.Context, requestID, digest string, fn func() apiResult) apiResult {
	c.mu.Lock()
	if existing, ok := c.entries[requestID]; ok {
		if existing.digest != digest {
			c.mu.Unlock()
			return faultResult(domain.CodeConflictingRequest, "request ID was reused with different content", map[string]any{
				"request_id":       requestID,
				"existing_digest":  existing.digest,
				"submitted_digest": digest,
			}, requestID)
		}
		c.mu.Unlock()
		select {
		case <-existing.done:
			return existing.result
		case <-ctx.Done():
			return faultResult(domain.CodeRequestCancelled, "duplicate request stopped waiting", map[string]any{
				"request_id": requestID,
				"error":      ctx.Err().Error(),
			}, requestID)
		}
	}
	entry := &idempotencyEntry{digest: digest, done: make(chan struct{})}
	c.entries[requestID] = entry
	c.mu.Unlock()

	result := safeExecute(requestID, fn)
	c.mu.Lock()
	entry.result = result
	close(entry.done)
	c.mu.Unlock()
	return result
}

func safeExecute(requestID string, fn func() apiResult) (result apiResult) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = faultResult(domain.CodeInternal, "request handler panicked", map[string]any{
				"request_id": requestID,
				"panic":      fmt.Sprint(recovered),
			}, requestID)
		}
	}()
	return fn()
}

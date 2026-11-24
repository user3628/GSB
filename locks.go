package blockpool

import (
	"context"
	"sort"
	"sync"
)

type lockEntry struct {
	mu   sync.Mutex
	refs int
}

// keyedLocks provides in-process deterministic single-writer locks. Callers
// must always pass the full key set to Lock/TryLock so keys can be sorted before
// acquisition, which prevents lock-order deadlocks between batch operations.
type keyedLocks struct {
	mu      sync.Mutex
	entries map[string]*lockEntry
}

func newKeyedLocks() *keyedLocks {
	return &keyedLocks{entries: make(map[string]*lockEntry)}
}

func normalizeKeys(keys []string) []string {
	seen := make(map[string]struct{}, len(keys))
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func (l *keyedLocks) acquire(key string) func() {
	l.mu.Lock()
	e := l.entries[key]
	if e == nil {
		e = &lockEntry{}
		l.entries[key] = e
	}
	e.refs++
	l.mu.Unlock()

	e.mu.Lock()
	return func() {
		e.mu.Unlock()
		l.mu.Lock()
		e.refs--
		if e.refs == 0 {
			delete(l.entries, key)
		}
		l.mu.Unlock()
	}
}

func (l *keyedLocks) Lock(ctx context.Context, keys []string) (func(), *Error) {
	keys = normalizeKeys(keys)
	unlocks := make([]func(), 0, len(keys))
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			for i := len(unlocks) - 1; i >= 0; i-- {
				unlocks[i]()
			}
			return nil, Errorf(CodeContextCanceled, "request canceled while waiting for lock", "cause", err.Error())
		}
		unlock := l.acquire(key)
		unlocks = append(unlocks, unlock)
	}
	return func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}, nil
}

// TryLock follows the same ordering rule but fails without blocking if any key
// is already held.
func (l *keyedLocks) TryLock(keys []string) (func(), bool) {
	keys = normalizeKeys(keys)
	unlocks := make([]func(), 0, len(keys))
	for _, key := range keys {
		l.mu.Lock()
		e := l.entries[key]
		if e == nil {
			e = &lockEntry{}
			l.entries[key] = e
		}
		e.refs++
		l.mu.Unlock()

		if !e.mu.TryLock() {
			l.mu.Lock()
			e.refs--
			if e.refs == 0 {
				delete(l.entries, key)
			}
			l.mu.Unlock()
			for i := len(unlocks) - 1; i >= 0; i-- {
				unlocks[i]()
			}
			return nil, false
		}
		unlocks = append(unlocks, func() {
			e.mu.Unlock()
			l.mu.Lock()
			e.refs--
			if e.refs == 0 {
				delete(l.entries, key)
			}
			l.mu.Unlock()
		})
	}
	return func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}, true
}

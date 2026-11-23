package arbiter

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// panelCache is the cached basis of an editor panel: the newest row version
// and the lease state observed together. Every mutation invalidates the entry
// so the fast path can never serve a cross-snapshot mixed conclusion.
// panelData is the lock-free cached observation.
type panelData struct {
	rowVersion   int64
	commitVer    int64
	leaseVersion int64
	holder       string
	leaseState   string
	expiresAtMs  int64
	cachedAtMs   int64
	token        string
}

type panelCache struct {
	mu   sync.Mutex
	data panelData
}

func (e *Engine) cacheEntry(rowID string) *panelCache {
	e.cacheMu.Lock()
	defer e.cacheMu.Unlock()
	c, ok := e.cache[rowID]
	if !ok {
		c = &panelCache{}
		e.cache[rowID] = c
	}
	return c
}

func (e *Engine) invalidateCache(rowID string) {
	e.cacheMu.Lock()
	if c, ok := e.cache[rowID]; ok {
		c.mu.Lock()
		c.data.rowVersion = 0
		c.data.token = ""
		c.mu.Unlock()
	}
	e.cacheMu.Unlock()
}

// refreshCache snapshots latest row version and lease state into the panel
// cache. It is called by the full-read panel-open path and by precheck when
// the budget allows a lease refresh.
func (e *Engine) refreshCache(rowID string) (*panelCache, error) {
	c := e.cacheEntry(rowID)
	latest, err := e.latestCommitted(rowID)
	if err != nil {
		return nil, err
	}
	lease, err := e.leaseOrNil(rowID)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = panelData{
		rowVersion: latest.RowVersion,
		commitVer:  latest.CommitVer,
		cachedAtMs: time.Now().UnixMilli(),
		token:      c.data.token,
	}
	if c.data.token == "" {
		c.data.token = newToken()
	}
	if lease != nil {
		c.data.leaseVersion = lease.LeaseVersion
		c.data.holder = lease.Holder
		c.data.leaseState = string(lease.State)
		c.data.expiresAtMs = lease.ExpiresAtMs
	}
	return c, nil
}

func newToken() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (c *panelCache) snapshot() panelData {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data
}

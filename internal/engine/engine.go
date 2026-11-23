package engine

import "sync"

// Engine is the local device authorization and trust-scoring engine.
type Engine struct {
	key      []byte
	policies map[string]*TenantPolicy
	params   Params
	store    *Store

	states           map[string]*DeviceState
	latestContinuous int64
	headHash         string
	chain            []SnapshotRecord
	fault            *FaultRecord
	round            int

	lockMu   sync.Mutex
	locks    map[string]chan struct{}
	sessions map[string]*Session
	sem      chan struct{}
}

// NewEngine opens the store, verifies the snapshot chain and restores
// committed device states. A broken chain is reported through the engine
// fault record and the engine falls back to the latest continuous version.
func NewEngine(dbPath string, policies []TenantPolicy, params Params, key []byte) (*Engine, error) {
	store, err := OpenStore(dbPath)
	if err != nil {
		return nil, err
	}
	maxInFlight := params.MaxInFlight
	if maxInFlight < 1 {
		maxInFlight = 1
	}
	e := &Engine{
		key:      append([]byte(nil), key...),
		policies: map[string]*TenantPolicy{},
		params:   params,
		store:    store,
		states:   map[string]*DeviceState{},
		locks:    map[string]chan struct{}{},
		sessions: map[string]*Session{},
		sem:      make(chan struct{}, maxInFlight),
	}
	for i := range policies {
		p := policies[i]
		e.policies[p.TenantID] = &p
	}
	chain, latest, head, ferr := store.LoadChain()
	e.chain = chain
	e.latestContinuous = latest
	e.headHash = head
	if ferr != nil {
		e.fault = &FaultRecord{Code: ferr.Code, LatestContinuous: latest, Detail: ferr.Msg}
	}
	states, err := store.LoadStates()
	if err != nil {
		store.Close()
		return nil, err
	}
	for i := range states {
		st := states[i]
		e.states[deviceKey(st.TenantID, st.DeviceID)] = &st
	}
	return e, nil
}

// Close releases the store.
func (e *Engine) Close() error { return e.store.Close() }

// Store exposes the persistence layer for scenario drivers and tests.
func (e *Engine) Store() *Store { return e.store }

// Fault returns the load-time fault record, if any.
func (e *Engine) Fault() *FaultRecord { return e.fault }

// SnapshotVersion returns the latest continuous committed version.
func (e *Engine) SnapshotVersion() int64 { return e.latestContinuous }

// State returns the committed state of a device, or nil.
func (e *Engine) State(tenantID, deviceID string) *DeviceState {
	return e.states[deviceKey(tenantID, deviceID)]
}

func deviceKey(tenantID, deviceID string) string { return tenantID + "/" + deviceID }

// Session is a per-tenant evaluation session. Score ledgers are isolated
// between sessions; committed state merges only at snapshot commit.
type Session struct {
	TenantID string
	mu       sync.Mutex
	ledger   map[string]int64
}

// NewSession returns the isolated session for a tenant, creating it on
// first use.
func (e *Engine) NewSession(tenantID string) *Session {
	e.lockMu.Lock()
	defer e.lockMu.Unlock()
	s, ok := e.sessions[tenantID]
	if !ok {
		s = &Session{TenantID: tenantID, ledger: map[string]int64{}}
		e.sessions[tenantID] = s
	}
	return s
}

// Record writes a score into the session-local ledger.
func (s *Session) Record(deviceID string, score int64) {
	s.mu.Lock()
	s.ledger[deviceID] = score
	s.mu.Unlock()
}

// Ledger returns a copy of the session-local score ledger.
func (s *Session) Ledger() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64, len(s.ledger))
	for k, v := range s.ledger {
		out[k] = v
	}
	return out
}

func (e *Engine) tryLock(key string) bool {
	e.lockMu.Lock()
	ch, ok := e.locks[key]
	if !ok {
		ch = make(chan struct{}, 1)
		e.locks[key] = ch
	}
	e.lockMu.Unlock()
	select {
	case ch <- struct{}{}:
		return true
	default:
		return false
	}
}

func (e *Engine) unlock(key string) {
	e.lockMu.Lock()
	ch := e.locks[key]
	e.lockMu.Unlock()
	<-ch
}

// Authorize handles one authorization request. Requests for the same
// device are mutually exclusive at device granularity: the first request
// proceeds, concurrent contenders get E_AUTH_CONTENTION.
func (e *Engine) Authorize(c *DeviceClaim, score, scoreVersion, now int64) (*Credential, Code) {
	key := deviceKey(c.TenantID, c.DeviceID)
	if !e.tryLock(key) {
		return nil, CodeAuthContention
	}
	defer e.unlock(key)
	policy := e.policies[c.TenantID]
	for _, cap := range c.Capabilities {
		if !containsStr(policy.AllowedCaps, cap) {
			return nil, CodeCapabilityDenied
		}
	}
	if score < policy.TrustThreshold {
		return nil, CodeTrustInsufficient
	}
	return IssueCredential(e.key, c.DeviceID, c.TenantID, c.Capabilities, scoreVersion, now, policy.CredTTL), CodeOK
}

// ContentionRecord summarizes a concurrent-authorization contention check.
type ContentionRecord struct {
	DeviceKey string `json:"device_key"`
	Requests  int    `json:"requests"`
	Succeeded int    `json:"succeeded"`
	Contended int    `json:"contended"`
}

// RunContentionCheck holds the device lock as the winning request, then
// lets n-1 concurrent requests contend. Exactly one request succeeds and
// the rest observe E_AUTH_CONTENTION, deterministically.
func (e *Engine) RunContentionCheck(key string, n int) *ContentionRecord {
	rec := &ContentionRecord{DeviceKey: key, Requests: n}
	if n <= 0 {
		return rec
	}
	if !e.tryLock(key) {
		rec.Contended = n
		return rec
	}
	rec.Succeeded = 1
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 1; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e.tryLock(key) {
				mu.Lock()
				rec.Succeeded++
				mu.Unlock()
				e.unlock(key)
				return
			}
			mu.Lock()
			rec.Contended++
			mu.Unlock()
		}()
	}
	wg.Wait()
	e.unlock(key)
	return rec
}

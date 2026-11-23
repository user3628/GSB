package arbiter

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"leasearbiter/internal/store"
)

var errWoken = errors.New("lease wait woken by release/expiry")

// AcquireRequest is the write-lease grant request.
type AcquireRequest struct {
	RowID     string `json:"row_id"`
	Holder    string `json:"holder"`
	TTLMs     int64  `json:"ttl_ms"`
	WaitMs    int64  `json:"wait_ms"`
	RequestID string `json:"request_id,omitempty"`
}

// AcquireResponse describes the granted lease.
type AcquireResponse struct {
	RowID        string `json:"row_id"`
	Holder       string `json:"holder"`
	LeaseVersion int64  `json:"lease_version"`
	GrantedAtMs  int64  `json:"granted_at_ms"`
	ExpiresAtMs  int64  `json:"expires_at_ms"`
	Mode         string `json:"mode"`
}

// RenewRequest extends a held lease.
type RenewRequest struct {
	RowID        string `json:"row_id"`
	Holder       string `json:"holder"`
	LeaseVersion int64  `json:"lease_version"`
	TTLMs        int64  `json:"ttl_ms"`
	RequestID    string `json:"request_id,omitempty"`
}

// ReleaseRequest surrenders a held lease before its TTL elapses.
type ReleaseRequest struct {
	RowID        string `json:"row_id"`
	Holder       string `json:"holder"`
	LeaseVersion int64  `json:"lease_version"`
	RequestID    string `json:"request_id,omitempty"`
}

// Acquire grants the exclusive row write lease or queues a bounded,
// interruptible wait. The wait parks on a channel and is woken by release or
// expiry; it never busy-polls.
func (e *Engine) Acquire(ctx context.Context, req AcquireRequest) (*AcquireResponse, error) {
	if req.RowID == "" || req.Holder == "" {
		return nil, apiError(CodeBadRequest, "row_id and holder are required", nil)
	}
	if req.TTLMs <= 0 {
		return nil, apiError(CodeBadRequest, "ttl_ms must be positive", nil)
	}
	g := e.gates.get(req.RowID)
	g.mu.Lock()
	defer g.mu.Unlock()

	deadline := e.now().Add(time.Duration(req.WaitMs) * time.Millisecond)
	mode := "immediate"
	for {
		e.expireIfDueLocked(req.RowID)
		resp, err := e.tryGrant(req)
		if err == nil {
			resp.Mode = mode
			return resp, nil
		}
		if !errors.Is(err, errLeaseBusy) {
			return nil, err
		}
		if req.WaitMs == 0 {
			return nil, e.leaseHeldError(req.RowID)
		}
		remaining := deadline.Sub(e.now())
		if remaining <= 0 {
			return nil, apiError(CodeLeaseTimeout, "bounded wait elapsed before the lease became free",
				map[string]any{"row_id": req.RowID, "wait_ms": req.WaitMs})
		}
		waiter := g.enqueue(req.Holder)
		g.mu.Unlock()
		werr := e.park(ctx, waiter.ch, remaining)
		g.mu.Lock()
		g.remove(waiter)
		if errors.Is(werr, context.Canceled) || errors.Is(werr, context.DeadlineExceeded) {
			return nil, werr
		}
		mode = "waited"
	}
}

var errLeaseBusy = errors.New("row lease currently held")

// tryGrant performs the durable grant transaction. It re-reads the lease
// inside the transaction so a grant from another process cannot be silently
// overwritten.
func (e *Engine) tryGrant(req AcquireRequest) (*AcquireResponse, error) {
	nowMs := store.Milli(e.now())
	var resp *AcquireResponse
	err := e.st.TxFunc(func(tx *sql.Tx) error {
		lease, err := store.ScanLease(tx, req.RowID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if lease != nil && lease.State == store.LeaseGranted && lease.ExpiresAtMs > nowMs {
			return errLeaseBusy
		}
		newVer := int64(1)
		if lease != nil {
			newVer = lease.LeaseVersion + 1
		}
		intentID, err := store.InsertIntent(tx, store.WriteIntent{
			Kind:         store.KindAcquire,
			RowID:        req.RowID,
			RequestID:    req.RequestID,
			LeaseVersion: newVer,
			Payload:      req.Holder,
			CreatedAtMs:  nowMs,
		})
		if err != nil {
			return err
		}
		lease = &store.Lease{
			RowID:        req.RowID,
			LeaseVersion: newVer,
			Holder:       req.Holder,
			State:        store.LeaseGranted,
			GrantedAtMs:  nowMs,
			RenewedAtMs:  nowMs,
			ExpiresAtMs:  nowMs + req.TTLMs,
		}
		if err := store.UpsertLease(tx, *lease); err != nil {
			return err
		}
		if err := store.CompleteIntent(tx, intentID, nowMs); err != nil {
			return err
		}
		resp = &AcquireResponse{
			RowID:        req.RowID,
			Holder:       req.Holder,
			LeaseVersion: newVer,
			GrantedAtMs:  nowMs,
			ExpiresAtMs:  lease.ExpiresAtMs,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	e.invalidateCache(req.RowID)
	return resp, nil
}

func (e *Engine) leaseHeldError(rowID string) error {
	lease, err := store.ScanLease(e.st.DB, rowID)
	if err != nil {
		return err
	}
	return apiError(CodeLeaseHeld, "row has an unexpired write lease", map[string]any{
		"row_id":        rowID,
		"holder":        lease.Holder,
		"lease_version": lease.LeaseVersion,
		"expires_at_ms": lease.ExpiresAtMs,
		"granted_at_ms": lease.GrantedAtMs,
	})
}

// park blocks on the sleeper abstraction and the waiter channel. The sleep is
// a real interruptible wait (time.Timer for the system clock, a parked channel
// for the deterministic manual clock); wake from release/expiry cancels it.
// There is no polling and no leaked goroutine: the helper goroutine exits via
// the deferred cancel when this call returns.
func (e *Engine) park(ctx context.Context, wake <-chan struct{}, d time.Duration) error {
	inner, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = e.sleep.Sleep(inner, d)
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wake:
		return errWoken
	case <-done:
		select {
		case <-wake:
			return errWoken
		default:
			return nil
		}
	}
}

// Renew extends the lease TTL after validating the presented lease version.
func (e *Engine) Renew(ctx context.Context, req RenewRequest) (*AcquireResponse, error) {
	if req.RowID == "" || req.Holder == "" || req.LeaseVersion <= 0 {
		return nil, apiError(CodeBadRequest, "row_id, holder and positive lease_version are required", nil)
	}
	if req.TTLMs <= 0 {
		return nil, apiError(CodeBadRequest, "ttl_ms must be positive", nil)
	}
	g := e.gates.get(req.RowID)
	g.mu.Lock()
	defer g.mu.Unlock()

	e.expireIfDueLocked(req.RowID)
	lease, err := store.ScanLease(e.st.DB, req.RowID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, e.leaseLost(req.RowID, "no_lease", "no write lease exists for the row")
	}
	if err != nil {
		return nil, err
	}
	if lease.LeaseVersion != req.LeaseVersion {
		return nil, apiError(CodeLeaseStale, "presented lease_version is not current", map[string]any{
			"row_id":              req.RowID,
			"presented_lease_ver": req.LeaseVersion,
			"current_lease_ver":   lease.LeaseVersion,
			"current_holder":      lease.Holder,
		})
	}
	if lease.State != store.LeaseGranted {
		return nil, e.leaseLost(req.RowID, "state_"+string(lease.State), "lease is no longer granted")
	}
	if lease.ExpiresAtMs <= store.Milli(e.now()) {
		return nil, e.leaseLost(req.RowID, "expired", "lease reached its expiry time")
	}
	if lease.Holder != req.Holder {
		return nil, e.leaseLost(req.RowID, "holder_mismatch", "lease is held by another client")
	}

	nowMs := store.Milli(e.now())
	newExpiry := nowMs + req.TTLMs
	err = e.st.TxFunc(func(tx *sql.Tx) error {
		intentID, err := store.InsertIntent(tx, store.WriteIntent{
			Kind:         store.KindRenew,
			RowID:        req.RowID,
			RequestID:    req.RequestID,
			LeaseVersion: req.LeaseVersion,
			Payload:      req.Holder,
			CreatedAtMs:  nowMs,
		})
		if err != nil {
			return err
		}
		res, err := tx.Exec(`UPDATE write_leases SET renewed_at=?, expires_at=?
			WHERE row_id=? AND lease_version=? AND state=?`,
			nowMs, newExpiry, req.RowID, req.LeaseVersion, string(store.LeaseGranted))
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return e.leaseLost(req.RowID, "concurrent_change", "lease changed during renewal")
		}
		return store.CompleteIntent(tx, intentID, nowMs)
	})
	if err != nil {
		var ae *APIError
		if errors.As(err, &ae) {
			return nil, err
		}
		return nil, err
	}
	e.invalidateCache(req.RowID)
	return &AcquireResponse{
		RowID:        req.RowID,
		Holder:       lease.Holder,
		LeaseVersion: lease.LeaseVersion,
		GrantedAtMs:  lease.GrantedAtMs,
		ExpiresAtMs:  newExpiry,
		Mode:         "renewed",
	}, nil
}

// Release surrenders a lease and wakes every queued waiter.
func (e *Engine) Release(ctx context.Context, req ReleaseRequest) error {
	if req.RowID == "" || req.LeaseVersion <= 0 {
		return apiError(CodeBadRequest, "row_id and positive lease_version are required", nil)
	}
	g := e.gates.get(req.RowID)
	g.mu.Lock()
	defer g.mu.Unlock()

	lease, err := store.ScanLease(e.st.DB, req.RowID)
	if errors.Is(err, sql.ErrNoRows) {
		return e.leaseLost(req.RowID, "no_lease", "no write lease exists for the row")
	}
	if err != nil {
		return err
	}
	if lease.LeaseVersion != req.LeaseVersion {
		return apiError(CodeLeaseStale, "presented lease_version is not current", map[string]any{
			"row_id":              req.RowID,
			"presented_lease_ver": req.LeaseVersion,
			"current_lease_ver":   lease.LeaseVersion,
		})
	}
	if lease.State != store.LeaseGranted {
		return e.leaseLost(req.RowID, "state_"+string(lease.State), "lease is not granted")
	}
	if err := e.persistState(req.RowID, req.LeaseVersion, store.KindRelease, store.LeaseReleased, req.RequestID, nowOf(e)); err != nil {
		return err
	}
	e.invalidateCache(req.RowID)
	g.wakeAll()
	return nil
}

func nowOf(e *Engine) int64 { return store.Milli(e.now()) }

func (e *Engine) leaseLost(rowID, reason, msg string) *APIError {
	details := map[string]any{"row_id": rowID, "reason": reason}
	if lease, err := store.ScanLease(e.st.DB, rowID); err == nil {
		details["current_holder"] = lease.Holder
		details["current_lease_ver"] = lease.LeaseVersion
		details["lease_state"] = string(lease.State)
		details["expires_at_ms"] = lease.ExpiresAtMs
	}
	return apiError(CodeLeaseLost, msg, details)
}

// persistExpiry writes the expire intent and state transition atomically.
func (e *Engine) persistExpiry(rowID string, leaseVer, nowMs int64) error {
	return e.persistState(rowID, leaseVer, store.KindExpire, store.LeaseExpired, "", nowMs)
}

func (e *Engine) persistState(rowID string, leaseVer int64, kind store.IntentKind,
	state store.LeaseState, requestID string, nowMs int64) error {
	return e.st.TxFunc(func(tx *sql.Tx) error {
		intentID, err := store.InsertIntent(tx, store.WriteIntent{
			Kind:         kind,
			RowID:        rowID,
			RequestID:    requestID,
			LeaseVersion: leaseVer,
			CreatedAtMs:  nowMs,
		})
		if err != nil {
			return err
		}
		ok, err := store.UpdateLeaseState(tx, rowID, leaseVer, state, nowMs)
		if err != nil {
			return err
		}
		if !ok {
			return e.leaseLost(rowID, "concurrent_change", "lease changed during state transition")
		}
		return store.CompleteIntent(tx, intentID, nowMs)
	})
}

// expireIfDueLocked is the gate-held variant of expireIfDue.
func (e *Engine) expireIfDueLocked(rowID string) {
	nowMs := store.Milli(e.now())
	lease, err := store.ScanLease(e.st.DB, rowID)
	if err != nil || lease.State != store.LeaseGranted || lease.ExpiresAtMs > nowMs {
		return
	}
	if err := e.persistExpiry(rowID, lease.LeaseVersion, nowMs); err != nil {
		return
	}
	e.invalidateCache(rowID)
	e.gates.get(rowID).wakeAll()
}

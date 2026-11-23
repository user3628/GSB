package arbiter

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"leasearbiter/internal/store"
)

// PrecheckRequest is the fast editor-panel verdict request.
type PrecheckRequest struct {
	RowID        string `json:"row_id"`
	Holder       string `json:"holder"`
	LeaseVersion int64  `json:"lease_version"`
	SnapshotVer  int64  `json:"snapshot_ver"`
	BudgetMs     int64  `json:"budget_ms"`
	AllowRefresh bool   `json:"allow_lease_refresh"`
}

// PrecheckResponse carries the verdict, the mode that produced it and the
// factual basis so the full commit can be compared field by field.
type PrecheckResponse struct {
	RowID          string `json:"row_id"`
	Editable       bool   `json:"editable"`
	Completed      bool   `json:"completed"`
	Mode           string `json:"mode"`
	Basis          string `json:"basis"`
	Token          string `json:"token"`
	RowVersion     int64  `json:"row_version"`
	CommitVer      int64  `json:"commit_ver"`
	LeaseVersion   int64  `json:"lease_version"`
	Holder         string `json:"holder"`
	LeaseState     string `json:"lease_state"`
	ExpiresAtMs    int64  `json:"expires_at_ms"`
	CachedAtMs     int64  `json:"cached_at_ms"`
	CheckedAtMs    int64  `json:"checked_at_ms"`
	SnapshotVer    int64  `json:"snapshot_ver"`
	IncompleteCode string `json:"incomplete_code,omitempty"`
}

// Precheck returns within the declared latency budget. When the budget is too
// small to finish even the cached check it returns completed=false instead of
// mixing observations from different snapshots.
func (e *Engine) Precheck(ctx context.Context, req PrecheckRequest) (*PrecheckResponse, error) {
	if req.RowID == "" || req.Holder == "" {
		return nil, apiError(CodeBadRequest, "row_id and holder are required", nil)
	}
	budget := time.Duration(req.BudgetMs) * time.Millisecond
	if budget == 0 {
		// An explicit zero budget cannot tolerate even a cold-cache durable
		// read: answer with the incomplete marker rather than guessing.
		return e.incomplete(req, "zero_budget", time.Now()), nil
	}
	if budget < 0 {
		return nil, apiError(CodeBadRequest, "budget_ms must be non-negative", nil)
	}
	deadline := time.Now().Add(budget)
	started := time.Now()

	c := e.cacheEntry(req.RowID)
	cached := c.snapshot()
	if cached.rowVersion == 0 {
		// Cold cache: only a durable read can produce a verdict. Honor the
		// budget; if the read cannot start in time, report incompleteness.
		if time.Until(deadline) <= 0 {
			return e.incomplete(req, "cold_cache_budget_exhausted", started), nil
		}
		refreshed, err := e.refreshCache(req.RowID)
		if err != nil {
			return nil, err
		}
		cached = refreshed.snapshot()
		if time.Now().After(deadline) {
			return e.incomplete(req, "refresh_exceeded_budget", started), nil
		}
		return e.verdictFromCache(req, cached, "fast_refresh", "durable read filled cold cache", started), nil
	}

	if req.AllowRefresh && time.Until(deadline) > 0 {
		refreshed, err := e.refreshCache(req.RowID)
		if err == nil {
			cached = refreshed.snapshot()
			if time.Now().After(deadline) {
				// The refresh crossed the budget: never return a half-observed
				// state, explicitly mark the precheck incomplete.
				return e.incomplete(req, "refresh_exceeded_budget", started), nil
			}
			return e.verdictFromCache(req, cached, "fast_lease_refresh",
				"durable lease and latest row version re-read within budget", started), nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
	}
	if time.Until(deadline) <= 0 {
		return e.incomplete(req, "budget_exhausted_before_verdict", started), nil
	}
	return e.verdictFromCache(req, cached, "fast_cache",
		"cached latest row version and lease state; no cross-snapshot mixing", started), nil
}

func (e *Engine) verdictFromCache(req PrecheckRequest, c panelData, mode, basis string, started time.Time) *PrecheckResponse {
	nowMs := store.Milli(e.now())
	leaseValid := c.leaseState == string(store.LeaseGranted) &&
		c.holder == req.Holder && c.leaseVersion == req.LeaseVersion &&
		c.expiresAtMs > nowMs
	// A non-zero snapshot older than the latest commit cannot support an
	// edit on the newest row version; the caller must reopen at a current
	// snapshot rather than receiving an optimistic editable verdict.
	snapshotFresh := req.SnapshotVer == 0 || c.commitVer <= req.SnapshotVer
	editable := leaseValid && snapshotFresh
	return &PrecheckResponse{
		RowID:        req.RowID,
		Editable:     editable,
		Completed:    true,
		Mode:         mode,
		Basis:        basis,
		Token:        c.token,
		RowVersion:   c.rowVersion,
		CommitVer:    c.commitVer,
		LeaseVersion: c.leaseVersion,
		Holder:       c.holder,
		LeaseState:   c.leaseState,
		ExpiresAtMs:  c.expiresAtMs,
		CachedAtMs:   c.cachedAtMs,
		CheckedAtMs:  time.Now().UnixMilli(),
		SnapshotVer:  req.SnapshotVer,
	}
}

func (e *Engine) incomplete(req PrecheckRequest, reason string, started time.Time) *PrecheckResponse {
	return &PrecheckResponse{
		RowID:          req.RowID,
		Editable:       false,
		Completed:      false,
		Mode:           "incomplete",
		Basis:          reason,
		CheckedAtMs:    time.Now().UnixMilli(),
		SnapshotVer:    req.SnapshotVer,
		IncompleteCode: string(CodePrecheckIncomplete),
	}
}

// PanelOpen is the editor-panel open path: one consistent durable read of row
// content plus lease state, which also primes the fast-path cache.
type PanelOpenRequest struct {
	RowID       string `json:"row_id"`
	SnapshotVer int64  `json:"snapshot_ver"`
	Holder      string `json:"holder"`
}

// PanelOpenResponse combines the visible row content and lease state.
type PanelOpenResponse struct {
	Row          *RowView          `json:"row"`
	Lease        *LeaseView        `json:"lease"`
	Editable     bool              `json:"editable"`
	PrecheckHint *PrecheckResponse `json:"precheck_hint,omitempty"`
}

// LeaseView is the wire form of lease state used by panel open.
type LeaseView struct {
	Holder       string `json:"holder"`
	LeaseVersion int64  `json:"lease_version"`
	State        string `json:"state"`
	GrantedAtMs  int64  `json:"granted_at_ms"`
	ExpiresAtMs  int64  `json:"expires_at_ms"`
}

// PanelOpen reads the row at the snapshot (or latest when snapshot is zero),
// attaches current lease state and warms the cache for the fast precheck.
func (e *Engine) PanelOpen(ctx context.Context, req PanelOpenRequest) (*PanelOpenResponse, error) {
	if req.RowID == "" {
		return nil, apiError(CodeBadRequest, "row_id is required", nil)
	}
	snapshot := req.SnapshotVer
	current, err := e.st.CommitVer()
	if err != nil {
		return nil, err
	}
	if snapshot == 0 {
		snapshot = current
	}
	if snapshot > current {
		return nil, apiError(CodeSnapshotAhead, "snapshot is newer than any committed version", map[string]any{
			"requested_snapshot_ver": snapshot,
			"current_commit_ver":     current,
		})
	}
	rv, err := store.RowVersionAt(e.st.DB, req.RowID, snapshot)
	if errors.Is(err, store.ErrVersionRetired) {
		return nil, apiError(CodeVersionRetired, "panel row version was reclaimed", map[string]any{
			"row_id": req.RowID, "snapshot_ver": snapshot,
		})
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(CodeNotFound, "row not found at snapshot", map[string]any{
			"row_id": req.RowID, "snapshot_ver": snapshot,
		})
	}
	if err != nil {
		return nil, err
	}
	lease, err := e.leaseOrNil(req.RowID)
	if err != nil {
		return nil, err
	}
	// Warm the cache with a single consistent observation.
	c, err := e.refreshCache(req.RowID)
	if err != nil {
		return nil, err
	}
	resp := &PanelOpenResponse{
		Row: &RowView{
			RowID: rv.RowID, RowVersion: rv.RowVersion, CommitVer: rv.CommitVer,
			ContentHash: rv.ContentHash, Content: rv.Content, Deleted: rv.Deleted,
			SnapshotVer: snapshot, CurrentVer: current,
		},
	}
	if lease != nil {
		resp.Lease = &LeaseView{
			Holder: lease.Holder, LeaseVersion: lease.LeaseVersion,
			State: string(lease.State), GrantedAtMs: lease.GrantedAtMs,
			ExpiresAtMs: lease.ExpiresAtMs,
		}
		nowMs := store.Milli(e.now())
		resp.Editable = lease.State == store.LeaseGranted &&
			lease.Holder == req.Holder && lease.ExpiresAtMs > nowMs &&
			rv.RowVersion == c.snapshot().rowVersion
	}
	return resp, nil
}

func (e *Engine) leaseOrNil(rowID string) (*store.Lease, error) {
	lease, err := store.ScanLease(e.st.DB, rowID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return lease, err
}

func (e *Engine) latestCommitted(rowID string) (*store.RowVersion, error) {
	rv, err := store.LatestRowVersion(e.st.DB, rowID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(CodeNotFound, "row has no committed version", map[string]any{"row_id": rowID})
	}
	return rv, err
}

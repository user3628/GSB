package prefixcache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
)

// HitQuery carries the version the caller observed. Request may be supplied
// inline; otherwise RequestID is loaded from the imported request set.
type HitQuery struct {
	ObservedVersion int64    `json:"observed_version"`
	RequestID       string   `json:"request_id,omitempty"`
	Request         *Request `json:"request,omitempty"`
}

// Hit checks the observed version first and then adjudicates the cache hit. A
// non-current observation never returns a possibly stale hit.
func (e *Engine) Hit(ctx context.Context, query HitQuery) (HitResult, error) {
	var result HitResult
	err := e.withReadTransaction(ctx, func(tx *sql.Tx) error {
		head, err := loadHead(tx)
		if err != nil {
			return err
		}
		if query.ObservedVersion != head.Version {
			return staleError(query.ObservedVersion, head.Version, "hit query observation is not the current effective version")
		}
		if head.Version == 0 {
			return &Error{Code: CodeNotFound, Message: "cache has no effective policy version", RequestID: query.RequestID}
		}
		row, err := loadPolicyRow(tx, head.Version)
		if err != nil {
			return err
		}
		var request Request
		if query.Request != nil {
			request = *query.Request
			if query.RequestID != "" && query.RequestID != request.ID {
				return invalidRequestError(query.RequestID, "inline request id disagrees with request_id field")
			}
		} else {
			if query.RequestID == "" {
				return invalidRequestError("", "request_id or inline request is required")
			}
			request, err = loadRequest(tx, query.RequestID)
			if err != nil {
				return err
			}
		}
		plan, err := BuildRequestPlan(request, row.Policy)
		if err != nil {
			return err
		}
		slots, err := loadSlots(tx, head.Version)
		if err != nil {
			return err
		}
		result = DecideHit(head.Version, query.ObservedVersion, request.ID, plan, row.Policy, slots)
		return nil
	})
	if err != nil {
		return HitResult{}, err
	}
	return result, nil
}

// Status returns the effective head and stage/version fingerprints.
func (e *Engine) Status(ctx context.Context) (Status, error) {
	status := Status{DatabasePath: e.dbPath}
	err := e.withReadTransaction(ctx, func(tx *sql.Tx) error {
		head, err := loadHead(tx)
		if err != nil {
			return err
		}
		status.CurrentVersion = head.Version
		status.AllocationSequence = head.AllocationSequence
		if head.Version == 0 {
			return nil
		}
		row, err := loadPolicyRow(tx, head.Version)
		if err != nil {
			return err
		}
		status.Policy = row.Policy
		status.PolicyDigest = row.PolicyDigest
		status.VersionFingerprint = row.StageDigests.Version
		status.StageDigests = row.StageDigests
		status.SlotDigest = row.SlotDigest
		status.OccupancyBudget = row.Policy.OccupancyBudget
		status.SlotCount = row.Policy.SlotCount
		if err := tx.QueryRow(`SELECT COUNT(*) FROM slots WHERE version=?`, head.Version).Scan(&status.SlotOccupancy); err != nil {
			return err
		}
		if err := tx.QueryRow(`SELECT COUNT(*) FROM request_states WHERE version=?`, head.Version).Scan(&status.RequestCount); err != nil {
			return err
		}
		if err := tx.QueryRow(`SELECT COUNT(*) FROM invalidations WHERE version=?`, head.Version).Scan(&status.InvalidationCount); err != nil {
			return err
		}
		if err := tx.QueryRow(`SELECT COUNT(*) FROM evictions WHERE version=?`, head.Version).Scan(&status.EvictionCount); err != nil {
			return err
		}
		return nil
	})
	return status, err
}

// Snapshot returns one immutable version. Version 0 selects the current head.
func (e *Engine) Snapshot(ctx context.Context, version int64) (Snapshot, error) {
	var snapshot Snapshot
	// One read transaction sees either all rows of a version or none of them.
	err := e.withReadTransaction(ctx, func(tx *sql.Tx) error {
		if version == 0 {
			head, err := loadHead(tx)
			if err != nil {
				return err
			}
			if head.Version == 0 {
				return &Error{Code: CodeNotFound, Message: "cache has no committed version"}
			}
			version = head.Version
		}
		row, err := loadPolicyRow(tx, version)
		if err != nil {
			return err
		}
		slots, err := loadSlots(tx, version)
		if err != nil {
			return err
		}
		states, err := loadStates(tx, version)
		if err != nil {
			return err
		}
		invalidations, err := loadInvalidations(tx, version, "")
		if err != nil {
			return err
		}
		evictions, err := loadEvictions(tx, version, "")
		if err != nil {
			return err
		}
		snapshot = Snapshot{
			Version:            row.Version,
			Kind:               row.Kind,
			Policy:             row.Policy,
			PolicyDigest:       row.PolicyDigest,
			ParentVersion:      row.ParentVersion,
			ParentDigest:       row.ParentDigest,
			VersionFingerprint: row.StageDigests.Version,
			StageDigests:       row.StageDigests,
			SlotDigest:         row.SlotDigest,
			AllocationSequence: row.AllocationSequence,
			Slots:              slots,
			States:             states,
			Invalidations:      invalidations,
			Evictions:          evictions,
			CreatedAt:          row.CreatedAt,
		}
		return nil
	})
	return snapshot, err
}

// Invalidations lists traceable invalidation records for a version. Version 0
// selects the current head.
func (e *Engine) Invalidations(ctx context.Context, version int64, requestID string) ([]Invalidation, error) {
	var result []Invalidation
	err := e.withReadTransaction(ctx, func(tx *sql.Tx) error {
		if version == 0 {
			head, err := loadHead(tx)
			if err != nil {
				return err
			}
			version = head.Version
		}
		if version == 0 {
			return &Error{Code: CodeNotFound, Message: "cache has no committed version"}
		}
		var err error
		result, err = loadInvalidations(tx, version, requestID)
		return err
	})
	return result, err
}

// Evictions lists budget, limit, and slot-conflict decisions for a version.
func (e *Engine) Evictions(ctx context.Context, version int64, requestID string) ([]Eviction, error) {
	var result []Eviction
	err := e.withReadTransaction(ctx, func(tx *sql.Tx) error {
		if version == 0 {
			head, err := loadHead(tx)
			if err != nil {
				return err
			}
			version = head.Version
		}
		if version == 0 {
			return &Error{Code: CodeNotFound, Message: "cache has no committed version"}
		}
		var err error
		result, err = loadEvictions(tx, version, requestID)
		return err
	})
	return result, err
}

// Requests returns the current imported request set sorted by id.
func (e *Engine) Requests(ctx context.Context) ([]Request, error) {
	var result []Request
	err := e.withReadTransaction(ctx, func(tx *sql.Tx) error {
		requests, err := loadRequests(tx)
		if err != nil {
			return err
		}
		ids := make([]string, 0, len(requests))
		for id := range requests {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		result = make([]Request, 0, len(ids))
		for _, id := range ids {
			result = append(result, requests[id])
		}
		return nil
	})
	return result, err
}

// State returns the unique effective request state for a version.
func (e *Engine) State(ctx context.Context, version int64, requestID string) (RequestState, error) {
	var result RequestState
	err := e.withReadTransaction(ctx, func(tx *sql.Tx) error {
		if version == 0 {
			head, err := loadHead(tx)
			if err != nil {
				return err
			}
			version = head.Version
		}
		var body string
		err := tx.QueryRow(`SELECT state_json FROM request_states WHERE version=? AND request_id=?`, version, requestID).Scan(&body)
		if errors.Is(err, sql.ErrNoRows) {
			return &Error{Code: CodeNotFound, Message: fmt.Sprintf("request state %s@%d does not exist", requestID, version), RequestID: requestID}
		}
		if err != nil {
			return err
		}
		state, err := decodeState(body)
		if err != nil {
			return err
		}
		result = state
		return nil
	})
	return result, err
}

func decodeState(body string) (RequestState, error) {
	var state RequestState
	if err := jsonUnmarshal([]byte(body), &state); err != nil {
		return RequestState{}, err
	}
	return state, nil
}

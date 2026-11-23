package store

import "database/sql"

const (
	RequestRunning   = "running"
	RequestCompleted = "completed"
	RequestAborted   = "aborted"
)

// LookupRequest returns the stored request record and whether it exists.
func (s *Store) LookupRequest(requestID string) (*RequestRecord, bool, error) {
	if requestID == "" {
		return nil, false, nil
	}
	row := s.DB.QueryRow(`SELECT request_id, kind, request_key, payload_hash, status,
		COALESCE(response,''), created_at, COALESCE(completed_at,0)
		FROM inflight_requests WHERE request_id=?`, requestID)
	var r RequestRecord
	if err := row.Scan(&r.RequestID, &r.Kind, &r.RequestKey, &r.PayloadHash,
		&r.Status, &r.ResponseJSON, &r.CreatedAtMs, &r.CompletedAtMs); err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &r, true, nil
}

// CompleteRequest stores the final response for reuse by duplicate callers.
func (s *Store) CompleteRequest(requestID, responseJSON string, nowMs int64) error {
	if requestID == "" {
		return nil
	}
	_, err := s.DB.Exec(`UPDATE inflight_requests SET status=?, response=?, completed_at=?
		WHERE request_id=?`, RequestCompleted, responseJSON, nowMs, requestID)
	return err
}

// AbortRunningRequests marks running requests left by a crashed process as
// aborted so a retry can reclaim them.
func (s *Store) AbortRunningRequests(nowMs int64) (int64, error) {
	res, err := s.DB.Exec(`UPDATE inflight_requests SET status=?, completed_at=?
		WHERE status=?`, RequestAborted, nowMs, RequestRunning)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// ClaimRunningLeader atomically transitions a request row into running when it
// is absent or aborted. It returns (true, false) when this caller won the
// durable leader election, (false, true) when another caller holds it, and
// (false, false) on a conflicting payload. The single SQL transaction makes
// the election safe across OS processes sharing the SQLite file.
func (s *Store) ClaimRunningLeader(r RequestRecord) (won bool, conflict bool, err error) {
	err = s.TxFunc(func(tx *sql.Tx) error {
		var existingHash, existingKey, existingStatus string
		lookupErr := tx.QueryRow(`SELECT payload_hash, request_key, status
			FROM inflight_requests WHERE request_id=?`, r.RequestID).Scan(
			&existingHash, &existingKey, &existingStatus)
		if lookupErr == nil {
			if existingHash != r.PayloadHash || existingKey != r.RequestKey {
				conflict = true
				return nil
			}
			if existingStatus == RequestRunning {
				won = false
				return nil
			}
			res, execErr := tx.Exec(`UPDATE inflight_requests SET status=?, kind=?, created_at=?
				WHERE request_id=? AND status<>?`,
				RequestRunning, r.Kind, r.CreatedAtMs, r.RequestID, RequestRunning)
			if execErr != nil {
				return execErr
			}
			n, _ := res.RowsAffected()
			won = n == 1
			return nil
		}
		if lookupErr != sql.ErrNoRows {
			return lookupErr
		}
		if _, execErr := tx.Exec(`INSERT INTO inflight_requests
			(request_id, kind, request_key, payload_hash, status, response, created_at, completed_at)
			VALUES(?,?,?,?,?,'',?,NULL)`,
			r.RequestID, r.Kind, r.RequestKey, r.PayloadHash, RequestRunning, r.CreatedAtMs); execErr != nil {
			return execErr
		}
		won = true
		return nil
	})
	if err != nil || conflict {
		return false, conflict, err
	}
	return won, false, nil
}

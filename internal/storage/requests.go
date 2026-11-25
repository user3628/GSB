package storage

import "database/sql"

// Request statuses.
const (
	RequestStarted   = "STARTED"
	RequestCompleted = "COMPLETED"
)

// RequestRecord is the idempotency ledger row.
type RequestRecord struct {
	RequestID   string
	Kind        string
	PayloadHash string
	Status      string
	ResultJSON  string
}

// LookupRequest returns the stored record and whether it exists.
func LookupRequest(q Querier, requestID string) (RequestRecord, bool, error) {
	var r RequestRecord
	err := q.QueryRow(
		`SELECT request_id,request_kind,payload_hash,status,result_json
		 FROM requests WHERE request_id=?`, requestID).
		Scan(&r.RequestID, &r.Kind, &r.PayloadHash, &r.Status, &r.ResultJSON)
	if err == sql.ErrNoRows {
		return RequestRecord{}, false, nil
	}
	if err != nil {
		return RequestRecord{}, false, err
	}
	return r, true, nil
}

// InsertRequest reserves a request id inside tx.
func InsertRequest(tx *sql.Tx, id, kind, payloadHash string, nowMS int64) error {
	_, err := tx.Exec(
		`INSERT INTO requests(request_id,request_kind,payload_hash,status,result_json,created_at_ms)
		 VALUES(?,?,?,?,?,?)`, id, kind, payloadHash, RequestStarted, "{}", nowMS)
	return err
}

// CompleteRequest stores the final result inside tx.
func CompleteRequest(tx *sql.Tx, id, resultJSON string) error {
	_, err := tx.Exec(
		`UPDATE requests SET status=?, result_json=? WHERE request_id=?`,
		RequestCompleted, resultJSON, id)
	return err
}

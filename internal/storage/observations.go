package storage

import (
	"database/sql"
	"encoding/json"

	"alarmengine/internal/model"
)

// InsertObservation stores one observation row inside tx and returns its seq.
func InsertObservation(tx *sql.Tx, o model.Observation, batchID string, baseVersion, commitVersion int64) (int64, error) {
	labels, err := json.Marshal(o.Labels)
	if err != nil {
		return 0, err
	}
	res, err := tx.Exec(
		`INSERT INTO observations
		 (timestamp_ms,service_id,metric,value,labels_json,batch_id,base_version,commit_version)
		 VALUES(?,?,?,?,?,?,?,?)`,
		o.TimestampMS, o.ServiceID, o.Metric, o.Value, string(labels),
		batchID, baseVersion, commitVersion)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// ObservationRow adds bookkeeping fields to an observation.
type ObservationRow struct {
	model.Observation
	Seq           int64
	BatchID       string
	BaseVersion   int64
	CommitVersion int64
}

// LoadObservations reads all observations ordered by sequence.
func LoadObservations(q Querier) ([]ObservationRow, error) {
	rows, err := q.Query(
		`SELECT seq,timestamp_ms,service_id,metric,value,labels_json,batch_id,base_version,commit_version
		 FROM observations ORDER BY seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ObservationRow
	for rows.Next() {
		var r ObservationRow
		var labelsJSON string
		if err := rows.Scan(&r.Seq, &r.TimestampMS, &r.ServiceID, &r.Metric, &r.Value,
			&labelsJSON, &r.BatchID, &r.BaseVersion, &r.CommitVersion); err != nil {
			return nil, err
		}
		r.Labels = map[string]string{}
		if err := json.Unmarshal([]byte(labelsJSON), &r.Labels); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// CountObservations returns the total number of stored observations.
func CountObservations(q Querier) (int, error) {
	var n int
	err := q.QueryRow(`SELECT COUNT(*) FROM observations`).Scan(&n)
	return n, err
}

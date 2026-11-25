package storage

import (
	"database/sql"
	"encoding/json"

	"alarmengine/internal/model"
)

// InsertAlert writes one alert inside tx.
func InsertAlert(tx *sql.Tx, a model.Alert, batchID string) error {
	labels, err := json.Marshal(a.Labels)
	if err != nil {
		return err
	}
	sup, err := json.Marshal(a.SuppressedBy)
	if err != nil {
		return err
	}
	src, err := json.Marshal(a.SourceObservations)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		`INSERT INTO alerts
		 (id,version,fingerprint,service_id,zone,metric,labels_json,first_seen_ms,last_seen_ms,
		  window_start_ms,window_end_ms,state,merged_count,suppressed_by_json,primary_reason,
		  release_at_ms,source_obs_json,last_obs_seq,batch_id)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		a.ID, a.Version, a.Fingerprint, a.ServiceID, a.Zone, a.Metric, string(labels),
		a.FirstSeenMS, a.LastSeenMS, a.WindowStartMS, a.WindowEndMS, string(a.State),
		a.MergedCount, string(sup), a.PrimaryReason, a.ReleaseAtMS, string(src),
		a.LastObservationSeq, batchID)
	return err
}

// UpdateAlertState changes state (and suppression bookkeeping) inside tx.
func UpdateAlertState(tx *sql.Tx, a model.Alert) error {
	sup, err := json.Marshal(a.SuppressedBy)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		`UPDATE alerts SET state=?, suppressed_by_json=?, primary_reason=?, release_at_ms=?
		 WHERE id=?`, string(a.State), string(sup), a.PrimaryReason, a.ReleaseAtMS, a.ID)
	return err
}

// LoadAlerts reads all alerts for a committed version, ordered deterministically.
func LoadAlerts(q Querier, version int64) ([]model.Alert, error) {
	rows, err := q.Query(
		`SELECT id,version,fingerprint,service_id,zone,metric,labels_json,first_seen_ms,last_seen_ms,
		        window_start_ms,window_end_ms,state,merged_count,suppressed_by_json,primary_reason,
		        release_at_ms,source_obs_json,last_obs_seq
		 FROM alerts WHERE version=?
		 ORDER BY first_seen_ms, id`, version)
	if err == sql.ErrNoRows {
		return []model.Alert{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Alert
	for rows.Next() {
		var a model.Alert
		var labelsJSON, supJSON, srcJSON, state string
		if err := rows.Scan(&a.ID, &a.Version, &a.Fingerprint, &a.ServiceID, &a.Zone, &a.Metric,
			&labelsJSON, &a.FirstSeenMS, &a.LastSeenMS, &a.WindowStartMS, &a.WindowEndMS,
			&state, &a.MergedCount, &supJSON, &a.PrimaryReason, &a.ReleaseAtMS, &srcJSON,
			&a.LastObservationSeq); err != nil {
			return nil, err
		}
		a.State = model.AlertState(state)
		a.Labels = map[string]string{}
		if err := json.Unmarshal([]byte(labelsJSON), &a.Labels); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(supJSON), &a.SuppressedBy); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(srcJSON), &a.SourceObservations); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// DeleteAlertsForVersion removes derived alerts of a version (used inside
// atomic rebuild transactions).
func DeleteAlertsForVersion(tx *sql.Tx, version int64) error {
	_, err := tx.Exec(`DELETE FROM alerts WHERE version=?`, version)
	return err
}

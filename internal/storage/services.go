package storage

import (
	"database/sql"
	"encoding/json"

	"alarmengine/internal/model"
)

// UpsertService writes one service row inside tx.
func UpsertService(tx *sql.Tx, s model.Service, version int64) error {
	up, err := json.Marshal(s.Upstreams)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		`INSERT INTO services(id,zone,weight_mille,upstreams,sensitivity,silence_window,version)
		 VALUES(?,?,?,?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET
		   zone=excluded.zone, weight_mille=excluded.weight_mille,
		   upstreams=excluded.upstreams, sensitivity=excluded.sensitivity,
		   silence_window=excluded.silence_window, version=excluded.version`,
		s.ID, s.Zone, s.WeightPerMille, string(up), int(s.Sensitivity), s.SilenceWindow, version)
	return err
}

// ServiceRow is a stored service.
type ServiceRow struct {
	Service model.Service
	Version int64
}

// LoadServices reads all services.
func LoadServices(q Querier) ([]ServiceRow, error) {
	rows, err := q.Query(
		`SELECT id,zone,weight_mille,upstreams,sensitivity,silence_window,version FROM services ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ServiceRow
	for rows.Next() {
		var r ServiceRow
		var upJSON string
		if err := rows.Scan(&r.Service.ID, &r.Service.Zone, &r.Service.WeightPerMille,
			&upJSON, &r.Service.Sensitivity, &r.Service.SilenceWindow, &r.Version); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(upJSON), &r.Service.Upstreams); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

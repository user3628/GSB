package engine

type UnitStatus struct {
	UnitID         int    `json:"unit_id"`
	CurrentVersion int64  `json:"current_version"`
	StateDigest    string `json:"state_digest"`
}

type Status struct {
	Config              Config       `json:"config"`
	ConfigDigest        string       `json:"config_digest"`
	Generation          int64        `json:"generation"`
	CurrentModelVersion int64        `json:"current_model_version"`
	EventSequence       int64        `json:"event_sequence"`
	DatasetDigest       string       `json:"dataset_digest"`
	Fingerprint         string       `json:"fingerprint"`
	CommittedBatches    []string     `json:"committed_batches"`
	Units               []UnitStatus `json:"units"`
	ValidCacheEntries   int          `json:"valid_cache_entries"`
	InvalidCacheEntries int          `json:"invalid_cache_entries"`
}

func (e *Engine) Status() (*Status, error) {
	cp, err := e.LoadCheckpoint()
	if err != nil {
		return nil, err
	}
	batches, err := e.CommittedBatches()
	if err != nil {
		return nil, err
	}
	st := &Status{
		Config:              e.cfg,
		ConfigDigest:        cp.ConfigDigest,
		Generation:          cp.Generation,
		CurrentModelVersion: cp.ModelVersion,
		EventSequence:       cp.EventSequence,
		DatasetDigest:       cp.DatasetDigest,
		Fingerprint:         cp.Fingerprint,
		CommittedBatches:    batches,
		Units:               []UnitStatus{},
	}
	rows, err := e.db.Query("SELECT unit_id,current_version,state_digest FROM units ORDER BY unit_id")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var u UnitStatus
		if err := rows.Scan(&u.UnitID, &u.CurrentVersion, &u.StateDigest); err != nil {
			_ = rows.Close()
			return nil, err
		}
		st.Units = append(st.Units, u)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	_ = e.db.QueryRow("SELECT COUNT(*) FROM inference_cache WHERE valid=1").Scan(&st.ValidCacheEntries)
	_ = e.db.QueryRow("SELECT COUNT(*) FROM inference_cache WHERE valid=0").Scan(&st.InvalidCacheEntries)
	return st, nil
}

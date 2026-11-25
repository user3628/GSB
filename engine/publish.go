package engine

import (
	"database/sql"
	"fmt"
)

type PublishUnitRequest struct {
	UnitID          int             `json:"unit_id"`
	ExpectedVersion int64           `json:"expected_version,omitempty"`
	NewVersion      int64           `json:"new_version,omitempty"`
	ModelVersion    int64           `json:"model_version,omitempty"`
	State           *UnitCheckpoint `json:"state,omitempty"`
	Producer        string          `json:"producer,omitempty"`
}

type PublishUnitResult struct {
	UnitID     int   `json:"unit_id"`
	OldVersion int64 `json:"old_version"`
	NewVersion int64 `json:"new_version"`
}

// PublishUnit uses a file lock plus a SQLite compare-and-set update. Versions
// must advance by exactly one for the target unit.
func (e *Engine) PublishUnit(req PublishUnitRequest) (*PublishUnitResult, error) {
	result := &PublishUnitResult{UnitID: req.UnitID}
	err := e.withLock(func() error {
		tx, err := e.db.Begin()
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		var current int64
		err = tx.QueryRow("SELECT current_version FROM units WHERE unit_id=?", req.UnitID).Scan(&current)
		if err == sql.ErrNoRows {
			uid := req.UnitID
			return &Error{Code: CodeStaleCheckpoint, UnitID: &uid, Message: "cross-unit or unknown unit publication"}
		}
		if err != nil {
			return err
		}
		result.OldVersion = current
		if req.ExpectedVersion != 0 && req.ExpectedVersion != current {
			uid := req.UnitID
			return &Error{Code: CodeStaleCheckpoint, UnitID: &uid, ExpectedVersion: req.ExpectedVersion, ActualVersion: current,
				Message: "publish expected version does not match current unit checkpoint"}
		}
		newVersion := req.NewVersion
		if newVersion == 0 {
			newVersion = current + 1
		}
		if newVersion == current {
			return &Error{Code: CodeStaleCheckpoint, UnitID: &req.UnitID, ExpectedVersion: current + 1, ActualVersion: current,
				Message: fmt.Sprintf("duplicate publication for unit %d version %d", req.UnitID, newVersion)}
		}
		if newVersion < current {
			return &Error{Code: CodeStaleCheckpoint, UnitID: &req.UnitID, ExpectedVersion: current + 1, ActualVersion: newVersion,
				Message: "stale/behind publication rejected"}
		}
		if newVersion != current+1 {
			return &Error{Code: CodeStaleCheckpoint, UnitID: &req.UnitID, ExpectedVersion: current + 1, ActualVersion: newVersion,
				Message: "version gap rejected; only continuous advancement is allowed"}
		}
		if req.State != nil && req.State.UnitID != req.UnitID {
			wrong := req.State.UnitID
			uid := req.UnitID
			return &Error{Code: CodeStaleCheckpoint, UnitID: &uid, ExpectedVersion: int64(req.UnitID), ActualVersion: int64(wrong),
				Message: "cross-unit state placement rejected"}
		}

		unit := req.State
		if unit == nil {
			cp, err := loadCheckpointTx(tx)
			if err != nil {
				return err
			}
			idx := cp.unitIndex(req.UnitID)
			if idx < 0 {
				uid := req.UnitID
				return &Error{Code: CodeStaleCheckpoint, UnitID: &uid, Message: "unit missing in checkpoint"}
			}
			clone, err := CloneUnit(&cp.Units[idx])
			if err != nil {
				return err
			}
			unit = clone
		}
		unit.UnitID = req.UnitID
		unit.UnitVersion = newVersion
		unitBody, err := CanonicalJSON(struct {
			UnitID int        `json:"unit_id"`
			Dims   []DimState `json:"dims"`
		}{unit.UnitID, unit.Dims})
		if err != nil {
			return err
		}
		unit.StateDigest = DigestBytes(unitBody)
		stateJSON, err := CanonicalJSON(unit)
		if err != nil {
			return err
		}
		modelVersion := req.ModelVersion
		if modelVersion == 0 {
			var currentModel int64
			raw, ok, err := getMetaTx(tx, "current_model_version")
			if err != nil {
				return err
			}
			if ok {
				_, _ = fmt.Sscan(raw, &currentModel)
			}
			modelVersion = currentModel
		}

		cas, err := tx.Exec("UPDATE units SET current_version=?,state_digest=? WHERE unit_id=? AND current_version=?",
			newVersion, unit.StateDigest, req.UnitID, current)
		if err != nil {
			return err
		}
		if n, _ := cas.RowsAffected(); n != 1 {
			return &Error{Code: CodeStaleCheckpoint, UnitID: &req.UnitID, Message: "conditional unit update lost race"}
		}
		if _, err := tx.Exec("UPDATE unit_versions SET is_current=0 WHERE unit_id=? AND is_current=1", req.UnitID); err != nil {
			return err
		}
		producer := req.Producer
		if producer == "" {
			producer = "publisher"
		}
		_, err = tx.Exec(`INSERT INTO unit_versions
			(unit_id,version,model_version,sample_seq,batch_id,state_json,update_json,state_digest,valid,is_current)
			VALUES(?,?,?,0,?,?,'{}',?,1,1)`,
			req.UnitID, newVersion, modelVersion, "publish:"+producer, string(stateJSON), unit.StateDigest)
		if err != nil {
			return staleOrInternal(err, req.UnitID)
		}
		result.NewVersion = newVersion
		return tx.Commit()
	})
	return result, err
}

type PublishCacheRequest struct {
	CacheKey     string  `json:"cache_key"`
	RequestID    string  `json:"request_id"`
	ModelVersion int64   `json:"model_version"`
	ResultJSON   string  `json:"result_json"`
	Fingerprint  string  `json:"fingerprint"`
	ScoreQ       int64   `json:"score_q"`
	Score        float64 `json:"score"`
}

func (e *Engine) PublishCache(req PublishCacheRequest) error {
	return e.withLock(func() error {
		var existingVersion int64
		err := e.db.QueryRow("SELECT model_version FROM inference_cache WHERE cache_key=?", req.CacheKey).Scan(&existingVersion)
		if err == nil {
			if req.ModelVersion == existingVersion {
				return &Error{Code: CodeStaleCheckpoint, Message: "duplicate cache publication rejected"}
			}
			if req.ModelVersion < existingVersion {
				return &Error{Code: CodeStaleCheckpoint, ExpectedVersion: existingVersion, ActualVersion: req.ModelVersion, Message: "stale cache publication rejected"}
			}
		} else if err != sql.ErrNoRows {
			return err
		}
		_, err = e.db.Exec(`INSERT INTO inference_cache
			(cache_key,request_id,model_version,score_q,score,result_json,fingerprint,valid,created_logical)
			VALUES(?,?,?,?,?,?,?,1,0)
			ON CONFLICT(cache_key) DO UPDATE SET
				model_version=excluded.model_version,score_q=excluded.score_q,score=excluded.score,
				result_json=excluded.result_json,fingerprint=excluded.fingerprint,valid=1
			WHERE inference_cache.model_version < excluded.model_version`,
			req.CacheKey, req.RequestID, req.ModelVersion, req.ScoreQ, req.Score, req.ResultJSON, req.Fingerprint)
		return err
	})
}

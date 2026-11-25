package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func (e *Engine) LoadCheckpointAt(version int64) (*Checkpoint, error) {
	current, err := e.LoadCheckpoint()
	if err != nil {
		return nil, err
	}
	if version == 0 || version == current.ModelVersion {
		return current, nil
	}

	var generation int64
	var datasetDigest, configDigest, fingerprint string
	err = e.db.QueryRow(`SELECT generation,dataset_digest,config_digest,fingerprint
		FROM model_versions WHERE version=?`, version).Scan(&generation, &datasetDigest, &configDigest, &fingerprint)
	if err != nil {
		return nil, newError(CodeNotFound, "model version %d: %v", version, err)
	}
	cp, err := NewCheckpoint(e.cfg, datasetDigest, generation)
	if err != nil {
		return nil, err
	}
	cp.ModelVersion = version
	cp.ConfigDigest = configDigest
	cp.Fingerprint = fingerprint

	for unitID := 0; unitID < e.cfg.Units; unitID++ {
		var stateJSON string
		err := e.db.QueryRow(`SELECT state_json FROM unit_versions
			WHERE unit_id=? AND valid=1 AND model_version<=?
			ORDER BY model_version DESC, version DESC LIMIT 1`, unitID, version).Scan(&stateJSON)
		if err != nil {
			continue // zero state for a unit that has not published yet
		}
		var unit UnitCheckpoint
		if err := json.Unmarshal([]byte(stateJSON), &unit); err != nil {
			return nil, err
		}
		idx := cp.unitIndex(unitID)
		if idx >= 0 {
			cp.Units[idx] = unit
		}
	}
	return cp, nil
}

// Score featurizes and scores against the requested model version (0 means current).
func (e *Engine) Score(req ScoreRequest) (*ScoreResult, error) {
	var result *ScoreResult
	err := e.withLock(func() error {
		cp, err := e.LoadCheckpointAt(req.ModelVersion)
		if err != nil {
			return err
		}
		scoreReq := req
		if req.ModelVersion == 0 {
			scoreReq.ModelVersion = cp.ModelVersion
		}
		result, err = ScoreCheckpoint(cp, scoreReq)
		if err != nil {
			return err
		}

		tx, err := e.db.Begin()
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		resultJSON, err := CanonicalJSON(result)
		if err != nil {
			return err
		}
		logical, err := nextLogicalTx(tx)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`INSERT INTO inference_cache
			(cache_key,request_id,model_version,unit_id,unit_version,score_q,score,result_json,fingerprint,valid,created_logical)
			VALUES(?,?,?, -1,0,?,?,?, ?,1,?)
			ON CONFLICT(cache_key) DO UPDATE SET
				request_id=excluded.request_id,
				model_version=excluded.model_version,
				score_q=excluded.score_q,
				score=excluded.score,
				result_json=excluded.result_json,
				fingerprint=excluded.fingerprint,
				valid=1`,
			result.CacheKey, result.RequestID, result.ModelVersion, result.ScoreQ, result.Score, string(resultJSON), result.VersionFingerprint, logical)
		if err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM inference_dependencies WHERE cache_key=?", result.CacheKey); err != nil {
			return err
		}
		seenUnits := map[int]struct{}{}
		for _, db := range result.DimBounds {
			if _, ok := seenUnits[db.UnitID]; ok {
				continue
			}
			seenUnits[db.UnitID] = struct{}{}
			var unitVersion int64
			_ = tx.QueryRow("SELECT current_version FROM units WHERE unit_id=?", db.UnitID).Scan(&unitVersion)
			if _, err := tx.Exec(`INSERT INTO inference_dependencies
				(cache_key,unit_id,unit_version,model_version) VALUES(?,?,?,?)`,
				result.CacheKey, db.UnitID, unitVersion, result.ModelVersion); err != nil {
				return err
			}
		}
		return tx.Commit()
	})
	return result, err
}

func (e *Engine) CachedScore(cacheKey string) (*ScoreResult, error) {
	var raw string
	var valid int
	err := e.db.QueryRow("SELECT result_json,valid FROM inference_cache WHERE cache_key=?", cacheKey).Scan(&raw, &valid)
	if err != nil {
		return nil, newError(CodeNotFound, "cache key %q: %v", cacheKey, err)
	}
	if valid == 0 {
		return nil, newError(CodeNotFound, "cache key %q is invalidated", cacheKey)
	}
	var out ScoreResult
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	out.CacheHit = true
	return &out, nil
}

type ReportRecord struct {
	ReportID string `json:"report_id"`
	Kind     string `json:"kind"`
	Status   string `json:"status"`
	RawJSON  string `json:"-"`
}

func (e *Engine) LatestReport() (*BackfillReport, error) {
	var raw string
	err := e.db.QueryRow("SELECT report_json FROM rebuild_reports WHERE kind='backfill' ORDER BY created_logical DESC LIMIT 1").Scan(&raw)
	if err != nil {
		return nil, newError(CodeNotFound, "no rebuild report: %v", err)
	}
	var out BackfillReport
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (e *Engine) ExportReport(reportID, outPath string) (string, error) {
	var raw string
	var err error
	if reportID == "" || reportID == "latest" {
		raw, err = e.LatestReportJSON()
	} else {
		err = e.db.QueryRow("SELECT report_json FROM rebuild_reports WHERE report_id=?", reportID).Scan(&raw)
	}
	if err != nil {
		return "", err
	}
	if outPath == "" {
		return raw, nil
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil && outPath != filepath.Base(outPath) {
		return "", err
	}
	if err := os.WriteFile(outPath, []byte(raw+"\n"), 0o644); err != nil {
		return "", err
	}
	return outPath, nil
}

func (e *Engine) LatestReportJSON() (string, error) {
	var raw string
	err := e.db.QueryRow("SELECT report_json FROM rebuild_reports WHERE kind='backfill' ORDER BY created_logical DESC LIMIT 1").Scan(&raw)
	if err != nil {
		return "", newError(CodeNotFound, "no rebuild report: %v", err)
	}
	return raw, nil
}

func (e *Engine) ListModelVersions(limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := e.db.Query(`SELECT version,generation,kind,sample_id,fingerprint,parent_version
		FROM model_versions ORDER BY version DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var version, generation, parent int64
		var kind, sampleID, fp string
		if err := rows.Scan(&version, &generation, &kind, &sampleID, &fp, &parent); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"version": version, "generation": generation, "kind": kind,
			"sample_id": sampleID, "fingerprint": fp, "parent_version": parent,
		})
	}
	return out, rows.Err()
}

var _ = fmt.Sprintf

package engine

import (
	"database/sql"
	"encoding/json"
	"fmt"
)

type BackfillRequest struct {
	SampleID             string            `json:"sample_id"`
	Label                *int              `json:"label,omitempty"`
	Fields               map[string]string `json:"fields,omitempty"`
	ExpectedModelVersion int64             `json:"expected_model_version,omitempty"`
	ReportID             string            `json:"report_id,omitempty"`
}

type BackfillReport struct {
	ReportID               string   `json:"report_id"`
	SampleID               string   `json:"sample_id"`
	Revision               int64    `json:"revision"`
	OldContentDigest       string   `json:"old_content_digest"`
	NewContentDigest       string   `json:"new_content_digest"`
	OldVectorDigest        string   `json:"old_vector_digest"`
	NewVectorDigest        string   `json:"new_vector_digest"`
	AffectedUnits          []int    `json:"affected_units"`
	RebuiltUnits           []int    `json:"rebuilt_units"`
	ReusedUnits            []int    `json:"reused_units"`
	DependencySampleCount  int      `json:"dependency_sample_count"`
	OldModelVersion        int64    `json:"old_model_version"`
	NewModelVersion        int64    `json:"new_model_version"`
	IncrementalFingerprint string   `json:"incremental_fingerprint"`
	FullFingerprint        string   `json:"full_fingerprint"`
	ByteEqual              bool     `json:"byte_equal"`
	FirstDiffByte          int64    `json:"first_diff_byte"`
	InvalidatedCacheKeys   []string `json:"invalidated_cache_keys"`
	Published              bool     `json:"published"`
}

func mergeFields(old map[string]string, patch map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range old {
		out[k] = v
	}
	for k, v := range patch {
		out[k] = v
	}
	return out
}

func unitsForVector(fv *FeatureVector, units int) []int {
	set := map[int]struct{}{}
	for _, f := range fv.Features {
		set[UnitOfDim(f.Dim, units)] = struct{}{}
	}
	out := make([]int, 0, len(set))
	for u := range set {
		out = append(out, u)
	}
	sortInts(out)
	return out
}

func (e *Engine) Backfill(req BackfillRequest) (*BackfillReport, error) {
	report := &BackfillReport{SampleID: req.SampleID, FirstDiffByte: -1}
	err := e.withLock(func() error {
		cp, err := e.LoadCheckpoint()
		if err != nil {
			return err
		}
		if req.ExpectedModelVersion != 0 && req.ExpectedModelVersion != cp.ModelVersion {
			actual := cp.ModelVersion
			expected := req.ExpectedModelVersion
			return &Error{Code: CodeStaleCheckpoint, ExpectedVersion: expected, ActualVersion: actual,
				Message: fmt.Sprintf("backfill expected model version %d but current is %d", expected, actual)}
		}

		samples, err := e.ListSamples()
		if err != nil {
			return err
		}
		targetIdx := -1
		for i := range samples {
			if samples[i].ID == req.SampleID {
				targetIdx = i
				break
			}
		}
		if targetIdx < 0 {
			return newError(CodeNotFound, "sample %q not found", req.SampleID)
		}
		old := samples[targetIdx]
		report.Revision = old.Revision + 1
		report.OldContentDigest = old.ContentDigest
		report.OldVectorDigest = old.VectorDigest
		report.OldModelVersion = cp.ModelVersion
		report.NewModelVersion = cp.ModelVersion + 1
		if req.ReportID != "" {
			report.ReportID = req.ReportID
		} else {
			report.ReportID = fmt.Sprintf("report-v%d", report.NewModelVersion)
		}

		newInput := old.SampleInput
		newInput.Fields = mergeFields(old.Fields, req.Fields)
		if req.Label != nil {
			newInput.Label = *req.Label
		}
		if err := ValidateSample(newInput); err != nil {
			return err
		}
		newDigest, err := SampleContentDigest(newInput)
		if err != nil {
			return err
		}
		newFV, err := Featurize(newInput, e.cfg)
		if err != nil {
			return err
		}
		var oldVectorJSON string
		if err := e.db.QueryRow("SELECT vector_json FROM samples WHERE id=?", req.SampleID).Scan(&oldVectorJSON); err != nil {
			return err
		}
		var oldFV FeatureVector
		if err := json.Unmarshal([]byte(oldVectorJSON), &oldFV); err != nil {
			return err
		}
		report.NewContentDigest = newDigest
		report.NewVectorDigest = newFV.Digest

		affectedSet := map[int]struct{}{}
		for _, u := range unitsForVector(&oldFV, e.cfg.Units) {
			affectedSet[u] = struct{}{}
		}
		for _, u := range unitsForVector(newFV, e.cfg.Units) {
			affectedSet[u] = struct{}{}
		}
		affected := make([]int, 0, len(affectedSet))
		for u := range affectedSet {
			affected = append(affected, u)
		}
		sortInts(affected)
		report.AffectedUnits = affected

		corrected := append([]StoredSample(nil), samples...)
		corrected[targetIdx].SampleInput = newInput
		corrected[targetIdx].ContentDigest = newDigest
		corrected[targetIdx].VectorDigest = newFV.Digest
		corrected[targetIdx].Revision = report.Revision
		datasetDigest := DatasetDigest(corrected)

		rebuilt := map[int]*UnitCheckpoint{}
		dependencyCount := 0
		for _, unitID := range affected {
			rows, err := e.db.Query("SELECT sample_seq FROM sample_units WHERE unit_id=?", unitID)
			if err != nil {
				return err
			}
			seqSet := map[int64]struct{}{old.Seq: {}}
			for rows.Next() {
				var seq int64
				if err := rows.Scan(&seq); err != nil {
					_ = rows.Close()
					return err
				}
				seqSet[seq] = struct{}{}
			}
			if err := rows.Close(); err != nil {
				return err
			}
			deps := make([]StoredSample, 0, len(seqSet))
			for _, s := range corrected {
				if _, ok := seqSet[s.Seq]; ok {
					deps = append(deps, s)
				}
			}
			dependencyCount += len(deps)
			unit, _, err := RebuildUnit(unitID, deps, e.cfg)
			if err != nil {
				return err
			}
			oldUnitIdx := cp.unitIndex(unitID)
			if oldUnitIdx < 0 {
				return newError(CodeConfigInvalid, "unit %d missing from checkpoint", unitID)
			}
			unit.UnitVersion = cp.Units[oldUnitIdx].UnitVersion + 1
			rebuilt[unitID] = unit
			report.RebuiltUnits = append(report.RebuiltUnits, unitID)
		}
		report.DependencySampleCount = dependencyCount

		reused := make([]int, 0, e.cfg.Units-len(report.RebuiltUnits))
		for u := 0; u < e.cfg.Units; u++ {
			if _, ok := rebuilt[u]; !ok {
				reused = append(reused, u)
			}
		}
		report.ReusedUnits = reused

		incrementalCP, err := AssembleIncremental(cp, rebuilt, datasetDigest, report.NewModelVersion, cp.Generation)
		if err != nil {
			return err
		}
		fullCP, err := RebuildAll(corrected, e.cfg, datasetDigest, report.NewModelVersion, cp.Generation)
		if err != nil {
			return err
		}
		for i := range fullCP.Units {
			ui := cp.unitIndex(fullCP.Units[i].UnitID)
			if ui >= 0 {
				fullCP.Units[i].UnitVersion = cp.Units[ui].UnitVersion
			}
		}
		for _, unitID := range affected {
			fullCP.Units[fullCP.unitIndex(unitID)].UnitVersion = cp.Units[cp.unitIndex(unitID)].UnitVersion + 1
		}
		if err := fullCP.RefreshFingerprint(); err != nil {
			return err
		}
		report.IncrementalFingerprint = incrementalCP.Fingerprint
		report.FullFingerprint = fullCP.Fingerprint
		if err := EnsureCheckpointsEqual(incrementalCP, fullCP); err != nil {
			if be, ok := asError(err); ok {
				report.FirstDiffByte = be.FirstDiffByte
				report.ByteEqual = false
				raw, _ := CanonicalJSON(report)
				_ = e.persistReport(report.ReportID, "backfill_mismatch", "mismatch", affected, string(raw), report.FirstDiffByte)
				return err
			}
			return err
		}
		report.ByteEqual = true

		invalidated, err := e.invalidateDependencies(affected)
		if err != nil {
			return err
		}
		report.InvalidatedCacheKeys = invalidated

		tx, err := e.db.Begin()
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		res, err := tx.Exec(`UPDATE meta SET value=? WHERE key='current_model_version' AND value=?`,
			fmt.Sprint(report.NewModelVersion), fmt.Sprint(report.OldModelVersion))
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return &Error{Code: CodeStaleCheckpoint, ExpectedVersion: report.OldModelVersion + 1, ActualVersion: report.OldModelVersion,
				Message: "conditional model version publication failed"}
		}

		fieldsJSON, _ := CanonicalJSON(newInput.Fields)
		newVectorJSON, _ := CanonicalJSON(newFV)
		_, err = tx.Exec(`UPDATE samples SET label=?,text=?,fields_json=?,revision=?,content_digest=?,vector_json=?,vector_digest=? WHERE id=? AND revision=?`,
			newInput.Label, newInput.Text, string(fieldsJSON), report.Revision, newDigest, string(newVectorJSON), newFV.Digest, req.SampleID, old.Revision)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`INSERT INTO sample_revisions
			(id,revision,seq,label,text,fields_json,content_digest,vector_json,vector_digest,change_kind)
			VALUES(?,?,?,?,?,?,?,?,?, 'backfill')`,
			req.SampleID, report.Revision, old.Seq, newInput.Label, newInput.Text, string(fieldsJSON), newDigest, string(newVectorJSON), newFV.Digest)
		if err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM sample_units WHERE sample_seq=?", old.Seq); err != nil {
			return err
		}
		newUnitDims := map[int][]int{}
		for _, f := range newFV.Features {
			u := UnitOfDim(f.Dim, e.cfg.Units)
			newUnitDims[u] = append(newUnitDims[u], f.Dim)
		}
		for u, dims := range newUnitDims {
			dimsJSON, _ := CanonicalJSON(dims)
			if _, err := tx.Exec("INSERT INTO sample_units(sample_seq,unit_id,dims_json) VALUES(?,?,?)", old.Seq, u, string(dimsJSON)); err != nil {
				return err
			}
		}

		unitMap := make([]map[string]any, 0, len(rebuilt))
		for unitID, unit := range rebuilt {
			stateJSON, err := CanonicalJSON(unit)
			if err != nil {
				return err
			}
			if _, err := tx.Exec("UPDATE unit_versions SET is_current=0 WHERE unit_id=? AND is_current=1", unitID); err != nil {
				return err
			}
			_, err = tx.Exec(`INSERT INTO unit_versions
				(unit_id,version,model_version,sample_seq,batch_id,state_json,update_json,state_digest,valid,is_current)
				VALUES(?,?,?,?,'backfill',?,'{}',?,1,1)
				ON CONFLICT(unit_id,version) DO UPDATE SET valid=1,is_current=1,state_json=excluded.state_json`,
				unitID, unit.UnitVersion, report.NewModelVersion, old.Seq, string(stateJSON), unit.StateDigest)
			if err != nil {
				return staleOrInternal(err, unitID)
			}
			if _, err := tx.Exec(`INSERT INTO units(unit_id,current_version,state_digest) VALUES(?,?,?)
				ON CONFLICT(unit_id) DO UPDATE SET current_version=excluded.current_version,state_digest=excluded.state_digest`,
				unitID, unit.UnitVersion, unit.StateDigest); err != nil {
				return err
			}
			unitMap = append(unitMap, map[string]any{"unit_id": unitID, "version": unit.UnitVersion, "digest": unit.StateDigest})
		}
		unitMapJSON, _ := CanonicalJSON(unitMap)
		_, err = tx.Exec(`INSERT INTO model_versions
			(version,generation,event_sequence,batch_id,sample_id,kind,fingerprint,unit_map_json,dataset_digest,config_digest,parent_version)
			VALUES(?,?,?,'',?,'backfill',?,?,?,?,?)`,
			report.NewModelVersion, cp.Generation, report.NewModelVersion, req.SampleID, incrementalCP.Fingerprint, string(unitMapJSON), datasetDigest, cp.ConfigDigest, report.OldModelVersion)
		if err != nil {
			return err
		}
		if err := saveCheckpointTx(tx, incrementalCP); err != nil {
			return err
		}
		if err := setMetaTx(tx, "dataset_digest", datasetDigest); err != nil {
			return err
		}
		if err := setMetaTx(tx, "current_fingerprint", incrementalCP.Fingerprint); err != nil {
			return err
		}
		if err := setMetaTx(tx, "event_sequence", fmt.Sprint(report.NewModelVersion)); err != nil {
			return err
		}
		if err := insertVersionEventTx(tx, report.NewModelVersion, "backfill", cp.Generation, datasetDigest, cp.ConfigDigest, report); err != nil {
			return err
		}
		report.Published = true
		rawReport, _ := CanonicalJSON(report)
		logical, err := nextLogicalTx(tx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT OR REPLACE INTO rebuild_reports
			(report_id,kind,status,affected_units_json,report_json,first_diff_byte,created_logical)
			VALUES(?,'backfill','committed',?,?,?,?)`,
			report.ReportID, mustJSON(affected), string(rawReport), report.FirstDiffByte, logical); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	})
	return report, err
}

func (e *Engine) invalidateDependencies(units []int) ([]string, error) {
	if len(units) == 0 {
		return nil, nil
	}
	placeholders := ""
	args := make([]any, len(units))
	for i, u := range units {
		if i > 0 {
			placeholders += ","
		}
		placeholders += "?"
		args[i] = u
	}
	rows, err := e.db.Query("SELECT DISTINCT cache_key FROM inference_dependencies WHERE unit_id IN ("+placeholders+")", args...)
	if err != nil {
		return nil, err
	}
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			_ = rows.Close()
			return nil, err
		}
		keys = append(keys, key)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	_, err = e.db.Exec("UPDATE inference_cache SET valid=0 WHERE cache_key IN (SELECT cache_key FROM inference_dependencies WHERE unit_id IN ("+placeholders+"))", args...)
	if err != nil {
		return nil, err
	}
	return keys, nil
}

func (e *Engine) persistReport(id, kind, status string, units []int, raw string, diff int64) error {
	_, err := e.db.Exec(`INSERT OR REPLACE INTO rebuild_reports
		(report_id,kind,status,affected_units_json,report_json,first_diff_byte,created_logical)
		VALUES(?,?,?,?,?,?,0)`, id, kind, status, mustJSON(units), raw, diff)
	return err
}

func mustJSON(v any) string {
	b, _ := CanonicalJSON(v)
	return string(b)
}

func staleOrInternal(err error, unitID int) error {
	// SQLite primary-key conflict means a duplicate/stale publication attempt.
	if err != nil && (contains(err.Error(), "UNIQUE constraint failed") || contains(err.Error(), "PRIMARY KEY constraint failed")) {
		uid := unitID
		return &Error{Code: CodeStaleCheckpoint, UnitID: &uid, Message: err.Error()}
	}
	return err
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}

var _ = sql.ErrNoRows

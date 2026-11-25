package service

import (
	"context"
	"database/sql"
	"encoding/json"

	"tm-sync/internal/tm"
)

func (s *Service) reconcileUnit(unitID string, effectiveVersion int64) (bool, error) {
	wrote := false
	err := s.Store.WithTx(func(tx *sql.Tx) error {
		var existingJSON, existingStatus, sourceLang, targetLang, sourceText string
		var winningSeq int64
		err := tx.QueryRow(`SELECT unit_json,status,source_lang,target_lang,source_text FROM units WHERE unit_id=?`, unitID).
			Scan(&existingJSON, &existingStatus, &sourceLang, &targetLang, &sourceText)
		haveUnit := err == nil
		if err != nil && err != sql.ErrNoRows {
			return err
		}

		rows, err := tx.Query(`SELECT payload_json FROM unit_occurrences WHERE unit_id=? ORDER BY segment_id, source_start, target_start`, unitID)
		if err != nil {
			return err
		}
		var occurrences []tm.Occurrence
		for rows.Next() {
			var payload string
			if err := rows.Scan(&payload); err != nil {
				_ = rows.Close()
				return err
			}
			var occ tm.Occurrence
			if err := json.Unmarshal([]byte(payload), &occ); err != nil {
				_ = rows.Close()
				return err
			}
			occurrences = append(occurrences, occ)
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		if len(occurrences) == 0 {
			if !haveUnit {
				return nil
			}
			if _, err := tx.Exec(`UPDATE units SET status='retired', updated_at=datetime('now') WHERE unit_id=?`, unitID); err != nil {
				return err
			}
			if _, err := tx.Exec(`UPDATE suggestions SET status='invalid' WHERE unit_id=? AND status='active'`, unitID); err != nil {
				return err
			}
			_, err = putCheckpointTx(tx, CheckpointInput{
				Stage: tm.StageUnit, UnitID: unitID, Version: effectiveVersion,
				Status: "retired", ErrorCode: "E_UNIT_RETIRED", ArtifactDigest: tm.SHA256Hex([]byte("retired:" + unitID)),
			})
			wrote = true
			return err
		}

		if !haveUnit {
			var lang, tlang string
			if err := tx.QueryRow(`SELECT source_lang,target_lang FROM segments WHERE segment_id=?`, occurrences[0].SegmentID).Scan(&lang, &tlang); err != nil {
				return err
			}
			sourceLang, targetLang = lang, tlang
			sourceText = occurrences[0].SourceText
		}

		unit := tm.Unit{
			UnitID:      unitID,
			SourceLang:  sourceLang,
			TargetLang:  targetLang,
			SourceText:  sourceText,
			Occurrences: occurrences,
		}
		tm.FinalizeUnit(&unit)
		winningSeq = unit.WinningUpdateSeq

		cp, err := getActiveCheckpointTx(tx, tm.StageUnit, "", unitID)
		if err != nil {
			return err
		}
		if haveUnit && existingStatus == "active" && cp != nil && cp.ArtifactDigest == unit.Digest {
			var activeSug int
			if err := tx.QueryRow(`SELECT COUNT(*) FROM suggestions WHERE unit_id=? AND status='active' AND digest=?`, unitID, mustSuggestionDigest(unit)).Scan(&activeSug); err != nil {
				return err
			}
			if activeSug == 1 {
				return nil
			}
		}

		unitJSON, err := tm.CanonicalJSON(unit)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`INSERT INTO units
			(unit_id,source_lang,target_lang,source_text,target_text,winning_segment_id,decision_basis,score,
			 occurrence_count,distinct_segment_count,digest,status,version,unit_json)
			VALUES(?,?,?,?,?,?,?,?,?,?,?, 'active', ?, ?)
			ON CONFLICT(unit_id) DO UPDATE SET
			 target_text=excluded.target_text, winning_segment_id=excluded.winning_segment_id,
			 decision_basis=excluded.decision_basis, score=excluded.score,
			 occurrence_count=excluded.occurrence_count, distinct_segment_count=excluded.distinct_segment_count,
			 digest=excluded.digest, status='active', version=excluded.version,
			 unit_json=excluded.unit_json, previous_digest=units.digest, updated_at=datetime('now')`,
			unitID, unit.SourceLang, unit.TargetLang, unit.SourceText, unit.TargetText,
			unit.WinningSegmentID, unit.DecisionBasis, unit.Score, unit.OccurrenceCount,
			unit.DistinctSegmentCount, unit.Digest, winningSeq, string(unitJSON))
		if err != nil {
			return err
		}

		targetDigest := tm.SHA256Hex([]byte(unit.TargetText))
		suggestion := tm.Suggestion{
			SuggestionID:    tm.SuggestionIDFor(unitID, targetDigest),
			LogicalID:       "s_" + unitID[2:],
			UnitID:          unitID,
			Version:         unit.WinningUpdateSeq,
			SourceText:      unit.SourceText,
			TargetText:      unit.TargetText,
			Score:           unit.Score,
			OccurrenceCount: unit.OccurrenceCount,
		}
		suggestion.Digest, _, err = tm.DigestCanonical(suggestion)
		if err != nil {
			return err
		}
		sugJSON, _ := tm.CanonicalJSON(suggestion)
		if _, err := tx.Exec(`UPDATE suggestions SET status='superseded' WHERE logical_id=? AND status='active'`, suggestion.LogicalID); err != nil {
			return err
		}
		_, err = tx.Exec(`INSERT INTO suggestions
			(suggestion_uid,logical_id,unit_id,version,source_text,target_text,score,occurrence_count,digest,status,suggestion_json)
			VALUES(?,?,?,?,?,?,?,?,?, 'active', ?)
			ON CONFLICT(suggestion_uid) DO UPDATE SET
			 version=excluded.version, source_text=excluded.source_text, target_text=excluded.target_text,
			 score=excluded.score, occurrence_count=excluded.occurrence_count, digest=excluded.digest,
			 status='active', suggestion_json=excluded.suggestion_json`,
			suggestion.SuggestionID, suggestion.LogicalID, unitID, suggestion.Version,
			suggestion.SourceText, suggestion.TargetText, suggestion.Score, suggestion.OccurrenceCount,
			suggestion.Digest, string(sugJSON))
		if err != nil {
			return err
		}

		_, err = putCheckpointTx(tx, CheckpointInput{
			Stage: tm.StageUnit, UnitID: unitID, ContentFP: tm.SHA256Hex([]byte(unit.SourceText)),
			Version: effectiveVersion, ArtifactDigest: unit.Digest,
			Detail: map[string]any{"suggestion_id": suggestion.SuggestionID, "occurrence_count": unit.OccurrenceCount},
		})
		if err == nil {
			wrote = true
		}
		return err
	})
	if err == nil && wrote {
		s.maybeCrashAfterCheckpoint()
	}
	return wrote, err
}

func mustSuggestionDigest(unit tm.Unit) string {
	sug := tm.Suggestion{
		SuggestionID:    tm.SuggestionIDFor(unit.UnitID, tm.SHA256Hex([]byte(unit.TargetText))),
		LogicalID:       "s_" + unit.UnitID[2:],
		UnitID:          unit.UnitID,
		Version:         unit.WinningUpdateSeq,
		SourceText:      unit.SourceText,
		TargetText:      unit.TargetText,
		Score:           unit.Score,
		OccurrenceCount: unit.OccurrenceCount,
	}
	d, _, _ := tm.DigestCanonical(sug)
	return d
}

func (s *Service) ExportBundle(ctx context.Context) (tm.ExportBundle, []byte, error) {
	version, err := s.CurrentVersion(ctx)
	if err != nil {
		return tm.ExportBundle{}, nil, err
	}
	units, err := s.activeUnits()
	if err != nil {
		return tm.ExportBundle{}, nil, err
	}
	suggestions, err := s.activeSuggestions()
	if err != nil {
		return tm.ExportBundle{}, nil, err
	}
	bundle := tm.ExportBundle{SchemaVersion: "tm.export.v1", Version: version, Units: units, Suggestions: suggestions}
	b, err := tm.CanonicalJSON(bundle)
	return bundle, b, err
}

func (s *Service) FullRebuildExport(ctx context.Context) (tm.ExportBundle, []byte, error) {
	version, err := s.CurrentVersion(ctx)
	if err != nil {
		return tm.ExportBundle{}, nil, err
	}
	records, err := s.listSegmentRecords()
	if err != nil {
		return tm.ExportBundle{}, nil, err
	}
	oracle := tm.BuildCorpus(records, version, s.Config)
	bundle := oracle.Export()
	b, err := tm.CanonicalJSON(bundle)
	return bundle, b, err
}

func (s *Service) activeUnits() ([]tm.Unit, error) {
	rows, err := s.Store.DB.Query(`SELECT unit_json FROM units WHERE status='active' ORDER BY unit_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []tm.Unit
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var u tm.Unit
		if err := json.Unmarshal([]byte(raw), &u); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func (s *Service) activeSuggestions() ([]tm.Suggestion, error) {
	rows, err := s.Store.DB.Query(`SELECT suggestion_json FROM suggestions WHERE status='active' ORDER BY suggestion_uid`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []tm.Suggestion
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var sug tm.Suggestion
		if err := json.Unmarshal([]byte(raw), &sug); err != nil {
			return nil, err
		}
		out = append(out, sug)
	}
	return out, rows.Err()
}

type SuggestionSearchResult struct {
	Query      string                  `json:"query"`
	Normalized string                  `json:"normalized"`
	Active     []tm.Suggestion         `json:"active"`
	Invalid    []InvalidSuggestionView `json:"invalid"`
}

type InvalidSuggestionView struct {
	tm.Suggestion
	ReasonChain []tm.ReasonChainEntry `json:"reason_chain"`
}

func (s *Service) SearchSuggestions(ctx context.Context, query string, includeInvalid bool) (*SuggestionSearchResult, error) {
	normalized := tm.NormalizeText(query)
	res := &SuggestionSearchResult{Query: query, Normalized: normalized}
	like := "%" + normalized + "%"
	rows, err := s.Store.DB.Query(`SELECT suggestion_json FROM suggestions
		WHERE status='active' AND (source_text = ? OR source_text LIKE ?)
		ORDER BY suggestion_uid`, normalized, like)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			_ = rows.Close()
			return nil, err
		}
		var sug tm.Suggestion
		if err := json.Unmarshal([]byte(raw), &sug); err != nil {
			_ = rows.Close()
			return nil, err
		}
		res.Active = append(res.Active, sug)
	}
	_ = rows.Close()

	if includeInvalid {
		irows, err := s.Store.DB.Query(`SELECT suggestion_uid,suggestion_json FROM suggestions
			WHERE status<>'active' AND (source_text = ? OR source_text LIKE ?)
			ORDER BY suggestion_uid DESC`, normalized, like)
		if err != nil {
			return nil, err
		}
		type invalidRow struct{ uid, raw string }
		var rowsToEnrich []invalidRow
		for irows.Next() {
			var uid, raw string
			if err := irows.Scan(&uid, &raw); err != nil {
				_ = irows.Close()
				return nil, err
			}
			rowsToEnrich = append(rowsToEnrich, invalidRow{uid: uid, raw: raw})
		}
		if err := irows.Close(); err != nil {
			return nil, err
		}
		for _, row := range rowsToEnrich {
			var sug tm.Suggestion
			if err := json.Unmarshal([]byte(row.raw), &sug); err != nil {
				return nil, err
			}
			chain, err := s.reasonChainFor("suggestion", row.uid)
			if err != nil {
				return nil, err
			}
			res.Invalid = append(res.Invalid, InvalidSuggestionView{Suggestion: sug, ReasonChain: chain})
		}
	}
	return res, nil
}

func (s *Service) reasonChainFor(entityType, entityID string) ([]tm.ReasonChainEntry, error) {
	rows, err := s.Store.DB.Query(`SELECT chain_json FROM invalidations WHERE entity_type=? AND entity_id=? ORDER BY id DESC LIMIT 1`, entityType, entityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var chain []tm.ReasonChainEntry
		if err := json.Unmarshal([]byte(raw), &chain); err != nil {
			return nil, err
		}
		return chain, nil
	}
	return nil, nil
}

func (s *Service) UnitReasonChain(unitID string) ([]tm.ReasonChainEntry, error) {
	return s.reasonChainFor("unit", unitID)
}

var _ = sql.ErrNoRows

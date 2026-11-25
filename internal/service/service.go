package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"syscall"

	"tm-sync/internal/store"
	"tm-sync/internal/tm"
)

type Service struct {
	Store                *store.Store
	Config               tm.AlignConfig
	CrashAfterCheckpoint int64
	checkpointWrites     int64
}

func New(st *store.Store, cfg tm.AlignConfig) *Service {
	return &Service{Store: st, Config: cfg}
}

func (s *Service) CurrentVersion(ctx context.Context) (int64, error) {
	var version int64
	err := s.Store.WithTx(func(tx *sql.Tx) error {
		v, err := store.GetMeta(tx, "effective_version")
		if err != nil {
			return err
		}
		if v == "" {
			version = 0
			return nil
		}
		_, err = fmt.Sscanf(v, "%d", &version)
		return err
	})
	return version, err
}

type ImportResult struct {
	Classification  string `json:"classification"`
	Version         int64  `json:"version"`
	PreviousVersion int64  `json:"previous_version"`
	ContentDigest   string `json:"content_digest"`
	PreviousDigest  string `json:"previous_digest,omitempty"`
	SegmentCount    int    `json:"segment_count"`
	InvalidatedAll  bool   `json:"invalidated_all,omitempty"`
	Message         string `json:"message,omitempty"`
}

func segmentFP(r tm.SegmentRecord) string {
	b, _ := tm.CanonicalJSON(r)
	return tm.SHA256Hex(b)
}

func (s *Service) ImportSources(ctx context.Context, data []byte, requestedVersion int64) (*ImportResult, error) {
	records, err := tm.ParseSegmentJSONL(data)
	if err != nil {
		return nil, &tm.BizError{Code: "E_INVALID_JSONL", Message: err.Error(), Stage: "import", HTTPStatus: 400}
	}
	if len(records) == 0 {
		return nil, &tm.BizError{Code: "E_EMPTY_IMPORT", Message: "no segment records", Stage: "import", HTTPStatus: 400}
	}
	digestBytes, err := tm.CanonicalJSON(records)
	if err != nil {
		return nil, err
	}
	digest := tm.SHA256Hex(digestBytes)
	result := &ImportResult{ContentDigest: digest, SegmentCount: len(records)}

	err = s.Store.WithTx(func(tx *sql.Tx) error {
		currentStr, err := store.GetMeta(tx, "effective_version")
		if err != nil {
			return err
		}
		var current int64
		if currentStr != "" {
			_, _ = fmt.Sscanf(currentStr, "%d", &current)
		}
		oldDigest, _ := store.GetMeta(tx, "source_digest")
		result.PreviousVersion = current
		result.PreviousDigest = oldDigest

		switch {
		case current == 0:
			if requestedVersion <= 0 {
				requestedVersion = 1
			}
			result.Classification = "initial_import"
			result.Version = requestedVersion
		case requestedVersion < current:
			return tm.NewRollback(requestedVersion, current)
		case requestedVersion > current:
			result.Classification = "normal_advance"
			result.Version = requestedVersion
			result.InvalidatedAll = true
		case digest == oldDigest:
			result.Classification = "unchanged"
			result.Version = current
			result.Message = "source digest already effective"
			return nil
		default:
			result.Classification = "whole_replacement"
			result.Version = current
			result.InvalidatedAll = true
		}

		if result.InvalidatedAll {
			if err := invalidateAllTx(tx, result.Version, "SOURCE_VERSION_REPLACED", digest, oldDigest); err != nil {
				return err
			}
		}
		if err := replaceSegmentsTx(tx, records, result.Version); err != nil {
			return err
		}
		if err := store.SetMeta(tx, "effective_version", fmt.Sprintf("%d", result.Version)); err != nil {
			return err
		}
		if err := store.SetMeta(tx, "source_digest", digest); err != nil {
			return err
		}
		detail, _ := tm.CanonicalJSON(map[string]any{"record_count": len(records)})
		_, err = tx.Exec(`INSERT INTO version_events(stream,version,content_digest,previous_digest,classification,detail_json)
			VALUES('source',?,?,?,?,?)`, result.Version, digest, oldDigest, result.Classification, string(detail))
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func replaceSegmentsTx(tx *sql.Tx, records []tm.SegmentRecord, version int64) error {
	if _, err := tx.Exec(`DELETE FROM segments`); err != nil {
		return err
	}
	for _, r := range records {
		if r.SegmentID == "" {
			return &tm.BizError{Code: "E_SEGMENT_ID_REQUIRED", Message: "segment_id is required", Stage: "import", HTTPStatus: 400}
		}
		fp := segmentFP(r)
		quality, _ := tm.CanonicalJSON(r.Quality)
		raw, _ := tm.CanonicalJSON(r)
		_, err := tx.Exec(`INSERT INTO segments
			(segment_id,source_lang,target_lang,source_text,target_text,update_seq,quality_json,source_file,content_fp,version,raw_json)
			VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
			r.SegmentID, r.SourceLang, r.TargetLang, r.SourceText, r.TargetText, r.UpdateSeq,
			string(quality), r.SourceFile, fp, version, string(raw))
		if err != nil {
			return err
		}
		_, err = tx.Exec(`INSERT OR REPLACE INTO segment_versions(segment_id,version,content_fp,raw_json)
			VALUES(?,?,?,?)`, r.SegmentID, version, fp, string(raw))
		if err != nil {
			return err
		}
	}
	return nil
}

func invalidateAllTx(tx *sql.Tx, version int64, code, newDigest, oldDigest string) error {
	chain := []tm.ReasonChainEntry{{
		Code: code, Message: "all units and suggestions invalidated by version replacement",
		EntityType: "version", EntityID: fmt.Sprintf("v%d", version), Version: version,
		Details: map[string]any{"new_digest": newDigest, "previous_digest": oldDigest},
	}}
	rows, err := tx.Query(`SELECT unit_id, digest FROM units WHERE status='active'`)
	if err != nil {
		return err
	}
	type unitRow struct{ id, digest string }
	var units []unitRow
	for rows.Next() {
		var u unitRow
		if err := rows.Scan(&u.id, &u.digest); err != nil {
			_ = rows.Close()
			return err
		}
		units = append(units, u)
	}
	_ = rows.Close()
	if _, err := tx.Exec(`UPDATE units SET status='invalid', previous_digest=digest WHERE status='active'`); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE suggestions SET status='invalid' WHERE status='active'`); err != nil {
		return err
	}
	for _, u := range units {
		c := append([]tm.ReasonChainEntry{}, chain...)
		c = append(c, tm.ReasonChainEntry{Code: "UNIT_INVALIDATED", EntityType: "unit", EntityID: u.id, Version: version, Details: map[string]any{"digest": u.digest}})
		if err := insertInvalidationTx(tx, version, "unit", u.id, "", u.id, "", code, "unit invalidated by version replacement", c); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`UPDATE checkpoints SET status='superseded' WHERE status='active'`); err != nil {
		return err
	}
	return nil
}

func insertInvalidationTx(tx *sql.Tx, version int64, entityType, entityID, segmentID, unitID, suggestionID, code, message string, chain []tm.ReasonChainEntry) error {
	chainJSON, err := tm.CanonicalJSON(chain)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO invalidations
		(version,entity_type,entity_id,unit_id,segment_id,suggestion_uid,code,message,chain_json)
		VALUES(?,?,?,?,?,?,?,?,?)`,
		version, entityType, entityID, unitID, segmentID, suggestionID, code, message, string(chainJSON))
	return err
}

type IncrementResult struct {
	IncrementID        string                           `json:"increment_id"`
	BaseVersion        int64                            `json:"base_version"`
	Version            int64                            `json:"effective_version"`
	ContentDigest      string                           `json:"content_digest"`
	ChangedSegments    []string                         `json:"changed_segments"`
	InvalidSegments    []string                         `json:"invalid_segments"`
	InvalidUnits       []string                         `json:"invalid_units"`
	InvalidSuggestions []string                         `json:"invalid_suggestions"`
	Reasons            map[string][]tm.ReasonChainEntry `json:"reasons"`
}

func (s *Service) ApplyIncrement(ctx context.Context, data []byte) (*IncrementResult, error) {
	inc, err := tm.ParseIncrementJSONL(data)
	if err != nil {
		return nil, &tm.BizError{Code: "E_INVALID_JSONL", Message: err.Error(), Stage: "increment", HTTPStatus: 400}
	}
	if len(inc.Changes) == 0 {
		return nil, &tm.BizError{Code: "E_EMPTY_INCREMENT", Message: "increment has no changes", Stage: "increment", HTTPStatus: 400}
	}
	digestBytes, _ := tm.CanonicalJSON(inc)
	digest := tm.SHA256Hex(digestBytes)
	out := &IncrementResult{IncrementID: inc.IncrementID, BaseVersion: inc.BaseVersion, ContentDigest: digest, Reasons: map[string][]tm.ReasonChainEntry{}}

	err = s.Store.WithTx(func(tx *sql.Tx) error {
		current, err := currentVersionTx(tx)
		if err != nil {
			return err
		}
		if inc.BaseVersion != current {
			return tm.NewStaleBase(current, inc.BaseVersion)
		}
		targetVersion := current + 1
		out.Version = targetVersion

		var raw string
		changedIDs := map[string]bool{}
		invalidSegmentSet := map[string]bool{}
		invalidUnitSet := map[string]bool{}
		invalidSuggestionSet := map[string]bool{}
		reasons := map[string][]tm.ReasonChainEntry{}

		var nextUpdateSeq int64
		if err := tx.QueryRow(`SELECT COALESCE(MAX(update_seq),0)+1 FROM segments`).Scan(&nextUpdateSeq); err != nil {
			return err
		}
		for _, change := range inc.Changes {
			if change.BaseVersion != 0 && change.BaseVersion != current {
				return tm.NewStaleBase(current, change.BaseVersion)
			}
			err := tx.QueryRow(`SELECT raw_json FROM segments WHERE segment_id=?`, change.SegmentID).Scan(&raw)
			if err == sql.ErrNoRows {
				return &tm.BizError{Code: "E_SEGMENT_NOT_FOUND", Message: "increment references unknown segment", SegmentID: change.SegmentID, Stage: "increment", HTTPStatus: 404}
			}
			if err != nil {
				return err
			}
			var oldRecord tm.SegmentRecord
			if err := json.Unmarshal([]byte(raw), &oldRecord); err != nil {
				return err
			}
			newRecord := oldRecord
			if change.NewTargetText != "" || change.ChangeType == "target_cleared" || change.ChangeType == "delete" {
				newRecord.TargetText = change.NewTargetText
			}
			if change.UpdateSeq != 0 {
				newRecord.UpdateSeq = change.UpdateSeq
			} else {
				newRecord.UpdateSeq = nextUpdateSeq
				nextUpdateSeq++
			}
			if change.Quality != nil {
				newRecord.Quality = *change.Quality
			} else {
				newRecord.Quality = tm.QualityMark{Label: "preferred", Rank: 3, Valid: true}
			}
			newFP := segmentFP(newRecord)
			earliest := earliestStage(oldRecord, newRecord, s.Config)
			changedIDs[change.SegmentID] = true
			invalidSegmentSet[change.SegmentID] = true

			changeChain := []tm.ReasonChainEntry{{
				Code: "TRANSLATION_CHANGED", Message: "increment changed segment translation",
				EntityType: "segment", EntityID: change.SegmentID, Version: targetVersion,
				Details: map[string]any{
					"change_type":    change.ChangeType,
					"earliest_stage": earliest,
					"old_target_fp":  tm.SHA256Hex([]byte(oldRecord.TargetText)),
					"new_target_fp":  tm.SHA256Hex([]byte(newRecord.TargetText)),
				},
			}}
			reasons["segment:"+change.SegmentID] = append(reasons["segment:"+change.SegmentID], changeChain...)

			unitRows, err := tx.Query(`SELECT DISTINCT unit_id FROM unit_occurrences WHERE segment_id=?`, change.SegmentID)
			if err != nil {
				return err
			}
			var unitIDs []string
			for unitRows.Next() {
				var id string
				if err := unitRows.Scan(&id); err != nil {
					_ = unitRows.Close()
					return err
				}
				unitIDs = append(unitIDs, id)
			}
			_ = unitRows.Close()

			for _, unitID := range unitIDs {
				invalidUnitSet[unitID] = true
				unitChain := append([]tm.ReasonChainEntry{}, changeChain...)
				unitChain = append(unitChain, tm.ReasonChainEntry{Code: "SHARED_UNIT_AFFECTED", Message: "changed occurrence belongs to shared unit", EntityType: "unit", EntityID: unitID, Version: targetVersion})
				reasons["unit:"+unitID] = appendUniqueEntries(reasons["unit:"+unitID], unitChain)
				if err := insertInvalidationTx(tx, targetVersion, "unit", unitID, change.SegmentID, unitID, "", "SHARED_UNIT_AFFECTED", "shared memory unit affected by translation change", unitChain); err != nil {
					return err
				}
				if _, err := tx.Exec(`UPDATE units SET status='invalid', previous_digest=digest WHERE unit_id=? AND status='active'`, unitID); err != nil {
					return err
				}

				sugRows, err := tx.Query(`SELECT suggestion_uid FROM suggestions WHERE unit_id=? AND status='active'`, unitID)
				if err != nil {
					return err
				}
				var sugIDs []string
				for sugRows.Next() {
					var id string
					if err := sugRows.Scan(&id); err != nil {
						_ = sugRows.Close()
						return err
					}
					sugIDs = append(sugIDs, id)
				}
				_ = sugRows.Close()
				for _, sugID := range sugIDs {
					invalidSuggestionSet[sugID] = true
					sugChain := append(unitChain, tm.ReasonChainEntry{Code: "SUGGESTION_INVALIDATED", Message: "exported suggestion can no longer be reused", EntityType: "suggestion", EntityID: sugID, Version: targetVersion})
					reasons["suggestion:"+sugID] = appendUniqueEntries(reasons["suggestion:"+sugID], sugChain)
					if _, err := tx.Exec(`UPDATE suggestions SET status='invalid' WHERE suggestion_uid=?`, sugID); err != nil {
						return err
					}
					if err := insertInvalidationTx(tx, targetVersion, "suggestion", sugID, change.SegmentID, unitID, sugID, "SUGGESTION_INVALIDATED", "exported translation suggestion invalidated", sugChain); err != nil {
						return err
					}
				}

				peerRows, err := tx.Query(`SELECT DISTINCT segment_id FROM unit_occurrences WHERE unit_id=? AND segment_id<>?`, unitID, change.SegmentID)
				if err != nil {
					return err
				}
				for peerRows.Next() {
					var peer string
					if err := peerRows.Scan(&peer); err != nil {
						_ = peerRows.Close()
						return err
					}
					invalidSegmentSet[peer] = true
					peerChain := append(unitChain, tm.ReasonChainEntry{Code: "PEER_SEGMENT_INVALIDATED", Message: "segment shares the affected memory unit", EntityType: "segment", EntityID: peer, Version: targetVersion})
					reasons["segment:"+peer] = appendUniqueEntries(reasons["segment:"+peer], peerChain)
					if err := insertInvalidationTx(tx, targetVersion, "segment", peer, peer, unitID, "", "PEER_SEGMENT_INVALIDATED", "peer segment hit ruling must be rebuilt", peerChain); err != nil {
						return err
					}
				}
				_ = peerRows.Close()
			}

			if _, err := tx.Exec(`UPDATE checkpoints SET status='superseded' WHERE segment_id=? AND status='active'`, change.SegmentID); err != nil {
				return err
			}
			if _, err := tx.Exec(`UPDATE segments SET source_lang=?,target_lang=?,source_text=?,target_text=?,update_seq=?,quality_json=?,content_fp=?,version=?,raw_json=? WHERE segment_id=?`,
				newRecord.SourceLang, newRecord.TargetLang, newRecord.SourceText, newRecord.TargetText, newRecord.UpdateSeq,
				mustJSON(newRecord.Quality), newFP, targetVersion, mustJSON(newRecord), change.SegmentID); err != nil {
				return err
			}
			if _, err := tx.Exec(`INSERT OR REPLACE INTO segment_versions(segment_id,version,content_fp,raw_json) VALUES(?,?,?,?)`,
				change.SegmentID, targetVersion, newFP, mustJSON(newRecord)); err != nil {
				return err
			}
		}

		for segID := range invalidSegmentSet {
			if _, err := tx.Exec(`UPDATE checkpoints SET status='superseded' WHERE segment_id=? AND status='active'`, segID); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(`INSERT INTO increments(increment_id,base_version,target_version,digest,raw_json)
			VALUES(?,?,?,?,?)`, inc.IncrementID, inc.BaseVersion, targetVersion, digest, string(mustJSON(inc))); err != nil {
			return err
		}
		if err := store.SetMeta(tx, "effective_version", fmt.Sprintf("%d", targetVersion)); err != nil {
			return err
		}
		detail := map[string]any{"changed_segments": keys(changedIDs), "invalid_units": keys(invalidUnitSet)}
		_, err = tx.Exec(`INSERT INTO version_events(stream,version,content_digest,classification,detail_json)
			VALUES('increment',?,?, 'normal_advance',?)`, targetVersion, digest, string(mustJSON(detail)))
		if err != nil {
			return err
		}

		out.ChangedSegments = keys(changedIDs)
		out.InvalidSegments = keys(invalidSegmentSet)
		out.InvalidUnits = keys(invalidUnitSet)
		out.InvalidSuggestions = keys(invalidSuggestionSet)
		out.Reasons = serializableReasons(reasons)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func earliestStage(old, new tm.SegmentRecord, cfg tm.AlignConfig) string {
	oldSource := tm.SegmentText(old.SourceText)
	newSource := tm.SegmentText(new.SourceText)
	oldTarget := tm.SegmentText(old.TargetText)
	newTarget := tm.SegmentText(new.TargetText)
	if oldSource.ContentFP != newSource.ContentFP || oldTarget.ContentFP != newTarget.ContentFP {
		return tm.StageSegmentation
	}
	oldAlign, err1 := tm.AlignSegment(old.SegmentID, old.SourceLang, old.TargetLang, oldSource, oldTarget, cfg)
	newAlign, err2 := tm.AlignSegment(new.SegmentID, new.SourceLang, new.TargetLang, newSource, newTarget, cfg)
	if err1 != nil || err2 != nil || oldAlign.Digest != newAlign.Digest {
		return tm.StageAlignment
	}
	return tm.StageMerge
}

func currentVersionTx(tx *sql.Tx) (int64, error) {
	v, err := store.GetMeta(tx, "effective_version")
	if err != nil {
		return 0, err
	}
	var version int64
	if v != "" {
		_, err = fmt.Sscanf(v, "%d", &version)
	}
	return version, err
}

func mustJSON(v any) string {
	b, err := tm.CanonicalJSON(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func appendUniqueEntries(existing []tm.ReasonChainEntry, additions []tm.ReasonChainEntry) []tm.ReasonChainEntry {
	seen := map[string]bool{}
	for _, e := range existing {
		seen[e.Code+e.EntityType+e.EntityID] = true
	}
	for _, e := range additions {
		key := e.Code + e.EntityType + e.EntityID
		if !seen[key] {
			existing = append(existing, e)
			seen[key] = true
		}
	}
	return existing
}

func serializableReasons(in map[string][]tm.ReasonChainEntry) map[string][]tm.ReasonChainEntry {
	out := map[string][]tm.ReasonChainEntry{}
	for k, v := range in {
		cp := append([]tm.ReasonChainEntry{}, v...)
		sort.SliceStable(cp, func(i, j int) bool { return reasonKey(cp[i]) < reasonKey(cp[j]) })
		out[k] = cp
	}
	return out
}

func reasonKey(e tm.ReasonChainEntry) string {
	return e.Code + "|" + e.EntityType + "|" + e.EntityID
}

func (s *Service) maybeCrashAfterCheckpoint() {
	s.checkpointWrites++
	if s.CrashAfterCheckpoint > 0 && s.checkpointWrites >= s.CrashAfterCheckpoint {
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	}
}

package service

import (
	"context"
	"database/sql"
	"strconv"

	"tm-sync/internal/tm"
)

type CheckpointView struct {
	SeqNo          int64  `json:"seq_no"`
	Stage          string `json:"stage"`
	SegmentID      string `json:"segment_id,omitempty"`
	UnitID         string `json:"unit_id,omitempty"`
	ContentFP      string `json:"content_fp,omitempty"`
	Version        int64  `json:"version"`
	ArtifactDigest string `json:"artifact_digest,omitempty"`
	Status         string `json:"status"`
	ErrorCode      string `json:"error_code,omitempty"`
}

type StatusResponse struct {
	Version            int64             `json:"effective_version"`
	VersionFingerprint string            `json:"version_fingerprint"`
	StageFingerprints  map[string]string `json:"stage_fingerprints"`
	Checkpoints        []CheckpointView  `json:"checkpoints"`
	Counts             map[string]int    `json:"counts"`
}

func (s *Service) Status(ctx context.Context) (*StatusResponse, error) {
	version, err := s.CurrentVersion(ctx)
	if err != nil {
		return nil, err
	}
	out := &StatusResponse{Version: version, StageFingerprints: map[string]string{}, Counts: map[string]int{}}
	err = s.Store.WithTx(func(tx *sql.Tx) error {
		rows, err := tx.Query(`SELECT seq_no,stage,segment_id,unit_id,content_fp,version,artifact_digest,status,error_code
			FROM checkpoints WHERE status='active' ORDER BY stage, segment_id, unit_id, seq_no`)
		if err != nil {
			return err
		}
		defer rows.Close()
		byStage := map[string][]map[string]any{}
		for rows.Next() {
			var c CheckpointView
			if err := rows.Scan(&c.SeqNo, &c.Stage, &c.SegmentID, &c.UnitID, &c.ContentFP, &c.Version, &c.ArtifactDigest, &c.Status, &c.ErrorCode); err != nil {
				return err
			}
			out.Checkpoints = append(out.Checkpoints, c)
			out.Counts["checkpoints"]++
			byStage[c.Stage] = append(byStage[c.Stage], map[string]any{
				"segment_id": c.SegmentID, "unit_id": c.UnitID, "content_fp": c.ContentFP,
				"version": c.Version, "artifact_digest": c.ArtifactDigest, "seq_no": c.SeqNo,
			})
		}
		for stage, items := range byStage {
			out.StageFingerprints[stage] = stageDigest(stage, items)
		}
		overall := map[string]any{"version": version, "stages": out.StageFingerprints, "checkpoints": out.Checkpoints}
		out.VersionFingerprint = stageDigest("version-fingerprint", overall)

		for _, table := range []struct{ name, key string }{
			{"segments", "segments"}, {"units", "units_total"},
			{"unit_occurrences", "occurrences"}, {"suggestions", "suggestions_total"},
			{"invalidations", "invalidations"}, {"publications", "publications"},
		} {
			var n int
			if err := tx.QueryRow("SELECT COUNT(*) FROM " + table.name).Scan(&n); err != nil {
				return err
			}
			out.Counts[table.key] = n
		}
		out.Counts["units_active"] = countWhereTx(tx, `SELECT COUNT(*) FROM units WHERE status='active'`)
		out.Counts["units_invalid"] = countWhereTx(tx, `SELECT COUNT(*) FROM units WHERE status<>'active'`)
		out.Counts["suggestions_active"] = countWhereTx(tx, `SELECT COUNT(*) FROM suggestions WHERE status='active'`)
		return nil
	})
	return out, err
}

func countWhereTx(tx *sql.Tx, query string) int {
	var n int
	_ = tx.QueryRow(query).Scan(&n)
	return n
}

type VersionRecordRequest struct {
	Stream        string `json:"stream"`
	Version       int64  `json:"version"`
	ContentDigest string `json:"content_digest"`
	Content       any    `json:"content,omitempty"`
}

type VersionRecordResult struct {
	Classification  string `json:"classification"`
	CurrentVersion  int64  `json:"current_version"`
	RejectedVersion int64  `json:"rejected_version,omitempty"`
	ContentDigest   string `json:"content_digest"`
	PreviousDigest  string `json:"previous_digest,omitempty"`
}

func (s *Service) RecordVersion(ctx context.Context, req VersionRecordRequest) (*VersionRecordResult, error) {
	if req.Stream == "" {
		req.Stream = "generic"
	}
	digest := req.ContentDigest
	if digest == "" {
		digest = stageDigest(req.Stream, req.Content)
	}
	res := &VersionRecordResult{ContentDigest: digest}
	err := s.Store.WithTx(func(tx *sql.Tx) error {
		current, err := currentVersionTx(tx)
		if err != nil {
			return err
		}
		res.CurrentVersion = current
		var previousDigest string
		_ = tx.QueryRow(`SELECT content_digest FROM version_events WHERE stream=? AND version=? ORDER BY id DESC LIMIT 1`,
			req.Stream, req.Version).Scan(&previousDigest)
		res.PreviousDigest = previousDigest

		if req.Version < current {
			res.RejectedVersion = req.Version
			return tm.NewRollback(req.Version, current)
		}
		classification := "normal_advance"
		invalidateAll := false
		if req.Version == current {
			if previousDigest != "" && previousDigest != digest {
				classification = "whole_replacement"
				invalidateAll = true
			} else if previousDigest == digest {
				classification = "unchanged"
			} else {
				classification = "recorded_current"
			}
		}
		res.Classification = classification
		if invalidateAll {
			if err := invalidateAllTx(tx, current, "VERSION_RECORD_REPLACED", digest, previousDigest); err != nil {
				return err
			}
		}
		if req.Version > current {
			if err := storeSetVersionTx(tx, req.Version); err != nil {
				return err
			}
			res.CurrentVersion = req.Version
		}
		detail, _ := tm.CanonicalJSON(map[string]any{"request": req})
		_, err = tx.Exec(`INSERT INTO version_events(stream,version,content_digest,previous_digest,classification,detail_json)
			VALUES(?,?,?,?,?,?)`, req.Stream, req.Version, digest, previousDigest, classification, string(detail))
		return err
	})
	return res, err
}

func storeSetVersionTx(tx *sql.Tx, version int64) error {
	_, err := tx.Exec(`INSERT INTO meta(key,value) VALUES('effective_version',?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, fmtVersion(version))
	return err
}

func fmtVersion(v int64) string {
	return strconv.FormatInt(v, 10)
}

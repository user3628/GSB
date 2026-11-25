// Package store is the transactional SQLite state of the pipeline. Every
// stage artifact is committed in one transaction; a forced process kill can
// only leave fully committed or fully absent facts, which is exactly what the
// recovery boundary relies on. No CGO is required (modernc.org/sqlite).
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"

	"ticketpipeline/internal/model"
)

// Store wraps a SQLite database handle and its workspace paths.
type Store struct {
	db   *sql.DB
	path string
}

// Checkpoint is one persisted stage fact for one ticket.
type Checkpoint struct {
	Seq           int64
	StageSeq      int64
	Stage         string
	TicketID      string
	PolicyVersion int
	InputFP       string
	ConfigFP      string
	ArtifactFP    string
	Summary       string
	ErrorCode     string
	Status        string
}

// PublishOutcome reports how a publish request was classified.
type PublishOutcome struct {
	Kind     string // "accepted", "duplicate", "stale"
	Seq      int64
	StageSeq int64
	Error    *model.BizError
}

const (
	StatusOK       = "ok"
	StatusBizError = "biz_error"
)

// Open opens (creating if needed) the database and applies the schema.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "pipeline.db")
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // serialize access; cross-process safety comes from WAL + file lock
	s := &Store{db: db, path: path}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	_, err := s.db.Exec(schema)
	return err
}

const schema = `
CREATE TABLE IF NOT EXISTS checkpoints (
	seq            INTEGER PRIMARY KEY AUTOINCREMENT,
	stage_seq      INTEGER NOT NULL,
	stage          TEXT NOT NULL,
	ticket_id      TEXT NOT NULL,
	policy_version INTEGER NOT NULL,
	input_fp       TEXT NOT NULL,
	config_fp      TEXT NOT NULL,
	artifact_fp    TEXT NOT NULL,
	summary        TEXT NOT NULL,
	error_code     TEXT NOT NULL DEFAULT '',
	status         TEXT NOT NULL,
	UNIQUE(stage, ticket_id)
);
CREATE INDEX IF NOT EXISTS idx_checkpoints_stage ON checkpoints(stage, stage_seq);
CREATE TABLE IF NOT EXISTS stage_runs (
	id             INTEGER PRIMARY KEY AUTOINCREMENT,
	stage          TEXT NOT NULL,
	generation     INTEGER NOT NULL,
	policy_version INTEGER NOT NULL,
	config_fp      TEXT NOT NULL,
	input_fp       TEXT NOT NULL,
	artifact_fp    TEXT NOT NULL,
	started_at     INTEGER NOT NULL,
	committed_at   INTEGER NOT NULL,
	UNIQUE(stage, generation)
);
`

// RunTx runs fn inside a single transaction, committing on nil error.
func (s *Store) RunTx(fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// maxStageSeq returns the highest committed stage_seq for a stage (0 if none).
func maxStageSeq(tx *sql.Tx, stage string) (int64, error) {
	var max sql.NullInt64
	if err := tx.QueryRow(
		`SELECT MAX(stage_seq) FROM checkpoints WHERE stage=?`, stage).Scan(&max); err != nil {
		return 0, err
	}
	if !max.Valid {
		return 0, nil
	}
	return max.Int64, nil
}

// Publish inserts or conditionally replaces one checkpoint. The expected
// stage_seq must be exactly max+1; equal content is an idempotent duplicate,
// while a sequence regression or fingerprint/summary change on an existing row
// is classified as a wholesale replacement or rollback and rejected with
// E_STALE_CHECKPOINT.
func (s *Store) Publish(cp Checkpoint) PublishOutcome {
	out := PublishOutcome{}
	err := s.RunTx(func(tx *sql.Tx) error {
		maxSeq, err := maxStageSeq(tx, cp.Stage)
		if err != nil {
			return err
		}
		var existing Checkpoint
		row := tx.QueryRow(
			`SELECT seq, stage_seq, input_fp, config_fp, artifact_fp, summary, status, error_code
			 FROM checkpoints WHERE stage=? AND ticket_id=?`, cp.Stage, cp.TicketID)
		scanErr := row.Scan(&existing.Seq, &existing.StageSeq, &existing.InputFP,
			&existing.ConfigFP, &existing.ArtifactFP, &existing.Summary,
			&existing.Status, &existing.ErrorCode)
		switch {
		case scanErr == nil:
			kind := classify(existing, cp, maxSeq)
			if kind != "accepted" && kind != "duplicate" {
				out.Kind = "stale"
				out.Error = model.NewBizError(model.CodeStaleCheckpoint, cp.Stage, cp.TicketID,
					"checkpoint publish rejected: "+kind, map[string]string{
						"existing_stage_seq": fmt.Sprint(existing.StageSeq),
						"incoming_stage_seq": fmt.Sprint(cp.StageSeq),
						"max_stage_seq":      fmt.Sprint(maxSeq),
					})
				return out.Error
			}
			if kind == "duplicate" {
				out.Kind = "duplicate"
				out.Seq = existing.Seq
				out.StageSeq = existing.StageSeq
				return nil
			}
			// Normal advance on the same ticket must move stage_seq to max+1.
			res, err := tx.Exec(`UPDATE checkpoints SET
				stage_seq=?, policy_version=?, input_fp=?, config_fp=?,
				artifact_fp=?, summary=?, error_code=?, status=?
				WHERE stage=? AND ticket_id=? AND stage_seq=?`,
				cp.StageSeq, cp.PolicyVersion, cp.InputFP, cp.ConfigFP, cp.ArtifactFP,
				cp.Summary, cp.ErrorCode, cp.Status, cp.Stage, cp.TicketID, existing.StageSeq)
			if err != nil {
				return err
			}
			if n, _ := res.RowsAffected(); n != 1 {
				return model.NewBizError(model.CodeStaleCheckpoint, cp.Stage, cp.TicketID,
					"conditional update lost the race", nil)
			}
			row := tx.QueryRow(`SELECT seq FROM checkpoints WHERE stage=? AND ticket_id=?`,
				cp.Stage, cp.TicketID)
			if err := row.Scan(&out.Seq); err != nil {
				return err
			}
			out.StageSeq = cp.StageSeq
			out.Kind = "accepted"
			return nil
		case errors.Is(scanErr, sql.ErrNoRows):
			if cp.StageSeq != maxSeq+1 {
				out.Kind = "stale"
				out.Error = model.NewBizError(model.CodeStaleCheckpoint, cp.Stage, cp.TicketID,
					"new checkpoint out of order", map[string]string{
						"expected_stage_seq": fmt.Sprint(maxSeq + 1),
						"incoming_stage_seq": fmt.Sprint(cp.StageSeq),
					})
				return out.Error
			}
			res, err := tx.Exec(`INSERT INTO checkpoints
				(stage_seq, stage, ticket_id, policy_version, input_fp, config_fp,
				 artifact_fp, summary, error_code, status)
				VALUES(?,?,?,?,?,?,?,?,?,?)`,
				cp.StageSeq, cp.Stage, cp.TicketID, cp.PolicyVersion, cp.InputFP,
				cp.ConfigFP, cp.ArtifactFP, cp.Summary, cp.ErrorCode, cp.Status)
			if err != nil {
				return err
			}
			id, err := res.LastInsertId()
			if err != nil {
				return err
			}
			out.Seq, out.StageSeq, out.Kind = id, cp.StageSeq, "accepted"
			return nil
		default:
			return scanErr
		}
	})
	if err != nil && out.Error == nil {
		out.Kind = "stale"
		out.Error = model.NewBizError(model.CodeStaleCheckpoint, cp.Stage, cp.TicketID, err.Error(), nil)
	}
	return out
}

// classify distinguishes the three required publish situations.
func classify(existing, incoming Checkpoint, maxSeq int64) string {
	same := existing.InputFP == incoming.InputFP &&
		existing.ConfigFP == incoming.ConfigFP &&
		existing.ArtifactFP == incoming.ArtifactFP &&
		existing.Summary == incoming.Summary &&
		existing.Status == incoming.Status &&
		existing.ErrorCode == incoming.ErrorCode
	if same && existing.StageSeq == incoming.StageSeq {
		return "duplicate"
	}
	switch {
	case incoming.StageSeq < existing.StageSeq:
		return "rollback"
	case incoming.StageSeq == existing.StageSeq && !same:
		return "replacement"
	case incoming.StageSeq != maxSeq+1:
		return "out_of_order"
	default:
		return "accepted"
	}
}

// DeleteStage removes every checkpoint of a stage and its downstream stages,
// used when incremental rebuild identifies the earliest changed stage.
func (s *Store) DeleteStage(fromStage string) error {
	idx := stageIndex(fromStage)
	if idx < 0 {
		return fmt.Errorf("unknown stage %q", fromStage)
	}
	return s.RunTx(func(tx *sql.Tx) error {
		for _, stage := range model.Stages[idx:] {
			// Only ticket checkpoints are cleared. Historical stage_runs stay
			// in place so the next committed run gets generation max+1, which
			// is how replay distinguishes "reused" from "rebuilt".
			if _, err := tx.Exec(`DELETE FROM checkpoints WHERE stage=?`, stage); err != nil {
				return err
			}
		}
		// Reset the AUTOINCREMENT high-water mark so rebuilt global seq values
		// match a fresh workspace while remaining monotonic within each run.
		if _, err := tx.Exec(`DELETE FROM sqlite_sequence WHERE name='checkpoints'`); err != nil {
			return err
		}
		return nil
	})
}

// ListCheckpoints returns all checkpoints of a stage ordered by stage_seq.
func (s *Store) ListCheckpoints(stage string) ([]Checkpoint, error) {
	rows, err := s.db.Query(
		`SELECT seq, stage_seq, stage, ticket_id, policy_version, input_fp,
		        config_fp, artifact_fp, summary, error_code, status
		 FROM checkpoints WHERE stage=? ORDER BY stage_seq ASC`, stage)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Checkpoint
	for rows.Next() {
		var cp Checkpoint
		if err := rows.Scan(&cp.Seq, &cp.StageSeq, &cp.Stage, &cp.TicketID,
			&cp.PolicyVersion, &cp.InputFP, &cp.ConfigFP, &cp.ArtifactFP,
			&cp.Summary, &cp.ErrorCode, &cp.Status); err != nil {
			return nil, err
		}
		out = append(out, cp)
	}
	return out, rows.Err()
}

// StageArtifactFP returns the artifact fingerprint recorded in stage_runs for
// the latest generation, or "" when the stage has no committed run.
func (s *Store) StageArtifactFP(stage string) (string, error) {
	var fp string
	err := s.db.QueryRow(
		`SELECT artifact_fp FROM stage_runs WHERE stage=? ORDER BY generation DESC LIMIT 1`,
		stage).Scan(&fp)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return fp, err
}

// RecordStageRun commits one generation-level run marker.
func (s *Store) RecordStageRun(stage string, generation int, policyVersion int, configFP, inputFP, artifactFP string, startedAt, committedAt int64) error {
	return s.RunTx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO stage_runs
			(stage, generation, policy_version, config_fp, input_fp, artifact_fp, started_at, committed_at)
			VALUES(?,?,?,?,?,?,?,?)`,
			stage, generation, policyVersion, configFP, inputFP, artifactFP, startedAt, committedAt)
		return err
	})
}

// LatestGeneration returns the highest committed generation for a stage.
func (s *Store) LatestGeneration(stage string) (int, error) {
	var gen sql.NullInt64
	err := s.db.QueryRow(
		`SELECT MAX(generation) FROM stage_runs WHERE stage=?`, stage).Scan(&gen)
	if err != nil {
		return 0, err
	}
	return int(gen.Int64), nil
}

// AssertContiguous verifies stage_seq values are 1..N without gaps or dupes;
// it is the recovery/invariant check used by tests and the verify command.
func (s *Store) AssertContiguous(stage string) error {
	cps, err := s.ListCheckpoints(stage)
	if err != nil {
		return err
	}
	seen := map[int64]bool{}
	for i, cp := range cps {
		want := int64(i + 1)
		if cp.StageSeq != want {
			return model.NewBizError(model.CodeStaleCheckpoint, stage, cp.TicketID,
				"non-contiguous stage sequence",
				map[string]string{"expected": fmt.Sprint(want), "got": fmt.Sprint(cp.StageSeq)})
		}
		if seen[cp.StageSeq] {
			return model.NewBizError(model.CodeStaleCheckpoint, stage, cp.TicketID,
				"duplicate stage sequence", map[string]string{"stage_seq": fmt.Sprint(cp.StageSeq)})
		}
		seen[cp.StageSeq] = true
	}
	return nil
}

func stageIndex(stage string) int {
	for i, s := range model.Stages {
		if s == stage {
			return i
		}
	}
	return -1
}

// MaxStageSeqDB returns the current max stage_seq without an open transaction.
func (s *Store) MaxStageSeqDB(stage string) (int64, error) {
	var max sql.NullInt64
	if err := s.db.QueryRow(
		`SELECT MAX(stage_seq) FROM checkpoints WHERE stage=?`, stage).Scan(&max); err != nil {
		return 0, err
	}
	if !max.Valid {
		return 0, nil
	}
	return max.Int64, nil
}

// LatestStageRun returns the policy version and artifact fingerprint of the
// latest generation marker for a stage.
func (s *Store) LatestStageRun(stage string) (int, string, error) {
	var version int
	var fp string
	err := s.db.QueryRow(
		`SELECT policy_version, artifact_fp FROM stage_runs
		 WHERE stage=? ORDER BY generation DESC LIMIT 1`, stage).Scan(&version, &fp)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", err
	}
	return version, fp, nil
}

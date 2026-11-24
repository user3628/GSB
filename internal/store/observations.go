package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"replicationlag/internal/apperr"
	"replicationlag/internal/domain"
)

type queryContext interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (s *Store) loadObservations(ctx context.Context) ([]domain.Observation, error) {
	return loadObservationsFrom(ctx, s.db)
}

func loadObservationsFrom(ctx context.Context, queryer queryContext) ([]domain.Observation, error) {
	rows, err := queryer.QueryContext(ctx, `SELECT replica_id, sampled_at_ms, gtid_json, position_name,
		lag_ms, reported_version, format_version, last_write_ms FROM observations
		ORDER BY replica_id, sampled_at_ms`)
	if err != nil {
		return nil, fmt.Errorf("query observations: %w", err)
	}
	defer rows.Close()
	observations := make([]domain.Observation, 0)
	for rows.Next() {
		observation, err := scanObservation(rows)
		if err != nil {
			return nil, err
		}
		observations = append(observations, observation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate observations: %w", err)
	}
	return observations, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanObservation(row scanner) (domain.Observation, error) {
	var observation domain.Observation
	var gtidJSON, versionText string
	if err := row.Scan(&observation.ReplicaID, &observation.SampledAtMS, &gtidJSON, &observation.PositionName,
		&observation.LagMS, &versionText, &observation.FormatVersion, &observation.LastWriteMS); err != nil {
		return domain.Observation{}, fmt.Errorf("scan observation: %w", err)
	}
	if err := json.Unmarshal([]byte(gtidJSON), &observation.GTID); err != nil {
		return domain.Observation{}, fmt.Errorf("decode stored GTID set: %w", err)
	}
	version, err := domain.ParseVersion(versionText)
	if err != nil {
		return domain.Observation{}, fmt.Errorf("decode stored reported version: %w", err)
	}
	observation.ReportedVersion = version
	return observation, nil
}

func replaceObservations(ctx context.Context, tx *sql.Tx, observations []domain.Observation) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM observations`); err != nil {
		return fmt.Errorf("clear observations: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, `INSERT INTO observations
		(replica_id, sampled_at_ms, gtid_json, gtid_text, position_name, lag_ms, reported_version,
		 format_version, last_write_ms, fingerprint) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare observation insert: %w", err)
	}
	defer statement.Close()
	for _, observation := range observations {
		gtidJSON, err := json.Marshal(observation.GTID.Normalized())
		if err != nil {
			return fmt.Errorf("encode GTID set: %w", err)
		}
		if _, err := statement.ExecContext(ctx,
			observation.ReplicaID, observation.SampledAtMS, string(gtidJSON), observation.GTID.Normalized().String(),
			observation.PositionName, observation.LagMS, observation.ReportedVersion.String(),
			observation.FormatVersion, observation.LastWriteMS, observation.ContentFingerprint()); err != nil {
			return fmt.Errorf("insert observation: %w", err)
		}
	}
	return nil
}

func persistReport(ctx context.Context, tx *sql.Tx, report domain.MergeReport) error {
	for _, table := range []string{"evolution_steps", "evolution_sequences", "replica_states"} {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+table); err != nil {
			return fmt.Errorf("clear %s: %w", table, err)
		}
	}
	for _, sequence := range report.Sequences {
		raw, err := json.Marshal(sequence)
		if err != nil {
			return fmt.Errorf("encode evolution sequence: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO evolution_sequences
			(replica_id, sequence_json, current_state, last_sampled_at_ms, stall_count, lag_level)
			VALUES (?, ?, ?, ?, ?, ?)`, sequence.ReplicaID, string(raw), sequence.CurrentState,
			sequence.LastSampledAtMS, sequence.StallCount, sequence.CurrentLagLevel); err != nil {
			return fmt.Errorf("persist evolution sequence: %w", err)
		}
		for _, step := range sequence.Steps {
			stepJSON, err := json.Marshal(step)
			if err != nil {
				return fmt.Errorf("encode evolution step: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO evolution_steps
				(replica_id, seq, sampled_at_ms, step_json) VALUES (?, ?, ?, ?)`,
				sequence.ReplicaID, step.Seq, step.Observation.SampledAtMS, string(stepJSON)); err != nil {
				return fmt.Errorf("persist evolution step: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO replica_states
			(replica_id, state, lag_level, suggested_action, updated_sampled_at_ms)
			VALUES (?, ?, ?, ?, ?)`, sequence.ReplicaID, sequence.CurrentState,
			sequence.CurrentLagLevel, sequence.SuggestedAction, sequence.LastSampledAtMS); err != nil {
			return fmt.Errorf("persist replica state: %w", err)
		}
	}
	return nil
}

type StateView struct {
	ReplicaID          string              `json:"replica_id"`
	State              domain.ReplicaState `json:"state"`
	LagLevel           domain.LagLevel     `json:"lag_level"`
	SuggestedAction    string              `json:"suggested_action"`
	UpdatedSampledAtMS int64               `json:"updated_sampled_at_ms"`
}

// ObservationSet reads observations and version from one SQLite snapshot.
func (s *Store) ObservationSet(ctx context.Context) ([]domain.Observation, int64, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, 0, fmt.Errorf("begin read transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	version, err := versionFrom(ctx, tx)
	if err != nil {
		return nil, 0, err
	}
	observations, err := loadObservationsFrom(ctx, tx)
	if err != nil {
		return nil, 0, err
	}
	if err := tx.Commit(); err != nil {
		return nil, 0, fmt.Errorf("commit read transaction: %w", err)
	}
	return observations, version, nil
}

func versionFrom(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (int64, error) {
	var raw string
	if err := queryer.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='version'`).Scan(&raw); err != nil {
		return 0, fmt.Errorf("read version in snapshot: %w", err)
	}
	var version int64
	if _, err := fmt.Sscan(raw, &version); err != nil {
		return 0, fmt.Errorf("parse version in snapshot: %w", err)
	}
	return version, nil
}

func (s *Store) ListSequences(ctx context.Context) ([]domain.EvolutionSequence, int64, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, 0, fmt.Errorf("begin sequence read: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	version, err := versionFrom(ctx, tx)
	if err != nil {
		return nil, 0, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT sequence_json FROM evolution_sequences ORDER BY replica_id`)
	if err != nil {
		return nil, 0, fmt.Errorf("query sequences: %w", err)
	}
	defer rows.Close()
	sequences := make([]domain.EvolutionSequence, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, 0, fmt.Errorf("scan sequence: %w", err)
		}
		var sequence domain.EvolutionSequence
		if err := json.Unmarshal([]byte(raw), &sequence); err != nil {
			return nil, 0, fmt.Errorf("decode sequence: %w", err)
		}
		sequences = append(sequences, sequence)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate sequences: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, 0, fmt.Errorf("commit sequence read: %w", err)
	}
	return sequences, version, nil
}

func (s *Store) GetSequence(ctx context.Context, replicaID string) (domain.EvolutionSequence, int64, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return domain.EvolutionSequence{}, 0, fmt.Errorf("begin sequence read: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	version, err := versionFrom(ctx, tx)
	if err != nil {
		return domain.EvolutionSequence{}, 0, err
	}
	var raw string
	err = tx.QueryRowContext(ctx, `SELECT sequence_json FROM evolution_sequences WHERE replica_id=?`, replicaID).Scan(&raw)
	if err == sql.ErrNoRows {
		return domain.EvolutionSequence{}, version, apperr.New(apperr.CodeNotFound, "replica sequence not found", map[string]any{
			"replica_id": replicaID,
		})
	}
	if err != nil {
		return domain.EvolutionSequence{}, 0, fmt.Errorf("query replica sequence: %w", err)
	}
	var sequence domain.EvolutionSequence
	if err := json.Unmarshal([]byte(raw), &sequence); err != nil {
		return domain.EvolutionSequence{}, 0, fmt.Errorf("decode replica sequence: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.EvolutionSequence{}, 0, fmt.Errorf("commit sequence read: %w", err)
	}
	return sequence, version, nil
}

func (s *Store) GetState(ctx context.Context, replicaID string) (StateView, int64, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return StateView{}, 0, fmt.Errorf("begin state read: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	version, err := versionFrom(ctx, tx)
	if err != nil {
		return StateView{}, 0, err
	}
	var view StateView
	err = tx.QueryRowContext(ctx, `SELECT replica_id, state, lag_level, suggested_action, updated_sampled_at_ms
		FROM replica_states WHERE replica_id=?`, replicaID).
		Scan(&view.ReplicaID, &view.State, &view.LagLevel, &view.SuggestedAction, &view.UpdatedSampledAtMS)
	if err == sql.ErrNoRows {
		return StateView{}, version, apperr.New(apperr.CodeNotFound, "replica state not found", map[string]any{"replica_id": replicaID})
	}
	if err != nil {
		return StateView{}, 0, fmt.Errorf("query replica state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return StateView{}, 0, fmt.Errorf("commit state read: %w", err)
	}
	return view, version, nil
}

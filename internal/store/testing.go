package store

import (
	"database/sql"

	"ticketpipeline/internal/model"
)

// ResetFromStage wipes checkpoints and stage_runs from a stage onward and
// resets the checkpoint autoincrement. It reproduces the durable state left by
// a process killed before the named stage ever ran, and is used by the
// crash-recovery tests.
func (s *Store) ResetFromStage(stage string) error {
	idx := stageIndex(stage)
	if idx < 0 {
		return nil
	}
	return s.RunTx(func(tx *sql.Tx) error {
		for _, st := range model.Stages[idx:] {
			if _, err := tx.Exec(`DELETE FROM checkpoints WHERE stage=?`, st); err != nil {
				return err
			}
			if _, err := tx.Exec(`DELETE FROM stage_runs WHERE stage=?`, st); err != nil {
				return err
			}
		}
		_, err := tx.Exec(`DELETE FROM sqlite_sequence WHERE name='checkpoints'`)
		return err
	})
}

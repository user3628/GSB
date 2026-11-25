package engine

import (
	"database/sql"

	"ticketpipeline/internal/model"
)

// truncateNormalize deletes normalize checkpoints beyond keep, leaving the
// earliest committed prefix exactly as a forced kill mid-stage would.
func truncateNormalize(eng *Engine, keep int) error {
	return eng.Store().RunTx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`DELETE FROM checkpoints WHERE stage=? AND stage_seq > ?`,
			model.StageNormalize, keep)
		return err
	})
}

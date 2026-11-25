package engine

import (
	"database/sql"

	"alarmengine/internal/errcode"
	"alarmengine/internal/model"
	"alarmengine/internal/storage"
)

// TransitionAlert changes one alert's lifecycle state. Illegal transitions
// return E_INVALID_TRANSITION and never touch persisted state. The write is
// guarded by a durable intent.
func (e *Engine) TransitionAlert(alertID string, to model.AlertState) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()

	version := e.Version()
	var current model.Alert
	row := e.store.ReadDB().QueryRow(
		`SELECT id,state FROM alerts WHERE id=? AND version=?`, alertID, version)
	var state string
	if err := row.Scan(&current.ID, &state); err != nil {
		if err == sql.ErrNoRows {
			return errcode.New(errcode.ESampleInvalid, "alert %s not found at version %d", alertID, version)
		}
		return err
	}
	current.State = model.AlertState(state)
	if !model.CanTransition(current.State, to) {
		return errcode.New(errcode.EInvalidTransition,
			"illegal transition %s -> %s for alert %s", current.State, to, alertID)
	}

	now := e.cfg.Now()
	intentID := e.nextIntentID("intent-transition-" + alertID)
	tx, err := e.store.BeginWrite()
	if err != nil {
		return err
	}
	if err := storage.InsertIntent(tx, intentID, storage.IntentTransitionAlert, version,
		map[string]string{"alert_id": alertID, "to": string(to)}, now); err != nil {
		_ = tx.Rollback()
		return err
	}
	_, err = tx.Exec(
		`UPDATE alerts SET state=? WHERE id=? AND version=?`, string(to), alertID, version)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := storage.CompleteIntent(tx, intentID); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

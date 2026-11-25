package engine

import (
	"alarmengine/internal/model"
	"alarmengine/internal/storage"
)

// RecoveryReportOf returns the report produced at startup recovery.
func (e *Engine) RecoveryReportOf() RecoveryReport { return e.recoveryReport }

// QueryAlerts is a concurrent-safe read against one committed version.
func (e *Engine) QueryAlerts(version int64) ([]model.Alert, error) {
	return storage.LoadAlerts(e.store.ReadDB(), version)
}

// QuerySuppressionHits is a concurrent-safe read against one committed version.
func (e *Engine) QuerySuppressionHits(version int64) ([]model.SuppressionHit, error) {
	return storage.LoadSuppressionHits(e.store.ReadDB(), version)
}

// QueryRootCauses is a concurrent-safe read against one committed version.
func (e *Engine) QueryRootCauses(version int64) ([]model.RootCauseScore, error) {
	return storage.LoadRootCauseScores(e.store.ReadDB(), version)
}

// QueryDerivedRefs exposes the derived-structure reference registry.
func (e *Engine) QueryDerivedRefs(version int64) ([]storage.DerivedRef, error) {
	return storage.LoadDerivedRefs(e.store.ReadDB(), version)
}

package service

import (
	"context"

	"netpolcompiler/internal/store"
)

type IntentRecoveryFact struct {
	store.Intent
	SnapshotPresent bool `json:"snapshotPresent"`
	ReadyViewRows   int  `json:"readyViewRows"`
}

type CurrentViewFact struct {
	Namespace string `json:"namespace"`
	Version   int64  `json:"version"`
	RuleCount int    `json:"ruleCount"`
	Digest    string `json:"digest"`
}

type RecoveryReport struct {
	Status                     string               `json:"status"`
	StartedAt                  string               `json:"startedAt"`
	FinishedAt                 string               `json:"finishedAt"`
	DatabasePath               string               `json:"databasePath"`
	LastCompleteVersion        int64                `json:"lastCompleteVersion"`
	UnfinishedIntents          []IntentRecoveryFact `json:"unfinishedIntents"`
	AbortedIntentIDs           []string             `json:"abortedIntentIds"`
	AbortedRequestIDs          []string             `json:"abortedRequestIds"`
	DeletedNonReadyViewRows    int64                `json:"deletedNonReadyViewRows"`
	DeletedOrphanReferenceRows int64                `json:"deletedOrphanReferenceRows"`
	CurrentViews               []CurrentViewFact    `json:"currentViews"`
	RecoveryRule               string               `json:"recoveryRule"`
}

func (s *Service) Recover(ctx context.Context) (*RecoveryReport, error) {
	started := store.NowRFC3339()
	unfinished, err := s.store.ListUnfinishedIntents(ctx)
	if err != nil {
		return nil, err
	}
	report := &RecoveryReport{
		Status:            "clean",
		StartedAt:         started,
		DatabasePath:      s.store.Path,
		UnfinishedIntents: []IntentRecoveryFact{},
		AbortedIntentIDs:  []string{},
		AbortedRequestIDs: []string{},
		CurrentViews:      []CurrentViewFact{},
		RecoveryRule:      "unfinished prepared intents are aborted; atomic SQLite transactions retain the last ready version",
	}

	tx, err := s.store.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	for _, intent := range unfinished {
		fact := IntentRecoveryFact{Intent: intent}
		_ = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM snapshots WHERE version=?)`, intent.TargetVersion).Scan(&fact.SnapshotPresent)
		_ = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM compile_views WHERE snapshot_version=? AND status='ready'`, intent.TargetVersion).Scan(&fact.ReadyViewRows)
		report.UnfinishedIntents = append(report.UnfinishedIntents, fact)

		if _, err := tx.ExecContext(ctx, `UPDATE intents SET status='aborted', reason='restart_scanned_unfinished_intent', updated_at=? WHERE intent_id=?`, store.NowRFC3339(), intent.IntentID); err != nil {
			return nil, err
		}
		report.AbortedIntentIDs = append(report.AbortedIntentIDs, intent.IntentID)
		if intent.RequestID != "" {
			result, err := tx.ExecContext(ctx, `UPDATE requests SET status='aborted', error_code='E_RECOVERED_ABORTED_INTENT', completed_at=? WHERE request_id=? AND status='processing'`, store.NowRFC3339(), intent.RequestID)
			if err != nil {
				return nil, err
			}
			if count, _ := result.RowsAffected(); count > 0 {
				report.AbortedRequestIDs = append(report.AbortedRequestIDs, intent.RequestID)
			}
		}
	}

	result, err := tx.ExecContext(ctx, `DELETE FROM compile_views WHERE status!='ready'`)
	if err != nil {
		return nil, err
	}
	report.DeletedNonReadyViewRows, _ = result.RowsAffected()

	result, err = tx.ExecContext(ctx, `DELETE FROM view_references WHERE NOT EXISTS (SELECT 1 FROM compile_views v WHERE v.snapshot_version=view_references.snapshot_version AND v.namespace=view_references.namespace AND v.status='ready')`)
	if err != nil {
		return nil, err
	}
	report.DeletedOrphanReferenceRows, _ = result.RowsAffected()

	var version string
	_ = tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='current_version'`).Scan(&version)
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	currentVersion, err := s.store.CurrentVersion(ctx)
	if err != nil {
		return nil, err
	}
	report.LastCompleteVersion = currentVersion
	views, err := s.store.LoadCacheViews(ctx, currentVersion)
	if err != nil {
		return nil, err
	}
	for namespace, view := range views {
		report.CurrentViews = append(report.CurrentViews, CurrentViewFact{
			Namespace: namespace, Version: view.Version, RuleCount: view.RuleCount, Digest: view.CompiledDigest,
		})
	}
	sortViewFacts(report.CurrentViews)
	report.FinishedAt = store.NowRFC3339()
	if len(report.AbortedIntentIDs) > 0 || report.DeletedNonReadyViewRows > 0 || report.DeletedOrphanReferenceRows > 0 {
		report.Status = "recovered_unfinished_intents"
	}
	s.recovery.Store(report)
	return report, nil
}

func (s *Service) LastRecovery() *RecoveryReport {
	return s.recovery.Load()
}

func sortViewFacts(facts []CurrentViewFact) {
	for i := 0; i < len(facts); i++ {
		for j := i + 1; j < len(facts); j++ {
			if facts[j].Namespace < facts[i].Namespace {
				facts[i], facts[j] = facts[j], facts[i]
			}
		}
	}
}

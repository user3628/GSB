package store

import (
	"context"

	"mitmpipeline/internal/domain"
)

func (s *Store) PendingIntents(ctx context.Context) ([]domain.IntentRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT intent_id,commit_version,kind,status,COALESCE(batch_id,''),COALESCE(payload_json,''),created_at_ms,updated_at_ms
		FROM intents WHERE status IN ('started','committing') ORDER BY commit_version`)
	if err != nil {
		return nil, domain.WrapError(domain.CodeInternal, err, "list pending intents")
	}
	defer rows.Close()
	out := []domain.IntentRecord{}
	for rows.Next() {
		intent, err := scanIntent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, intent)
	}
	return out, nil
}

func (s *Store) ListPolicyVersions(ctx context.Context) ([]domain.StoredPolicyVersion, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT version,revision,mode,document_json,status,available_from_ms,COALESCE(retired_at_ms,0),COALESCE(replaced_by,'')
		FROM policy_versions ORDER BY revision`)
	if err != nil {
		return nil, domain.WrapError(domain.CodeInternal, err, "list policy versions")
	}
	defer rows.Close()
	out := []domain.StoredPolicyVersion{}
	for rows.Next() {
		var stored domain.StoredPolicyVersion
		var documentJSON string
		var revision int64
		if err := rows.Scan(&stored.Version, &revision, &stored.Mode, &documentJSON, &stored.Status,
			&stored.AvailableFromMS, &stored.RetiredAtMS, &stored.ReplacedBy); err != nil {
			return nil, domain.WrapError(domain.CodeInternal, err, "scan policy version")
		}
		out = append(out, stored)
	}
	return out, nil
}

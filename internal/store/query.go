package store

import (
	"context"
	"database/sql"
	"encoding/json"

	"mitmpipeline/internal/domain"
)

func (s *Store) GetConclusion(ctx context.Context, connectionID string, asOfCommit int64) (*domain.Conclusion, error) {
	latest, err := s.LatestCompleteVersion(ctx)
	if err != nil {
		return nil, err
	}
	if asOfCommit <= 0 {
		asOfCommit = latest
	}
	if asOfCommit > latest {
		return nil, domain.NewError(domain.CodeNotFound, "requested commit is not a complete visible version").
			WithContext("requested_commit", asOfCommit).WithContext("latest_complete_commit", latest)
	}
	var raw string
	var commit int64
	err = s.db.QueryRowContext(ctx, `SELECT report_json, commit_version FROM conclusion_versions
		WHERE connection_id=? AND commit_version<=? ORDER BY commit_version DESC LIMIT 1`,
		connectionID, asOfCommit).Scan(&raw, &commit)
	if err == sql.ErrNoRows {
		return nil, domain.NewError(domain.CodeNotFound, "conclusion not found at visible commit").
			WithContext("connection_id", connectionID).WithContext("as_of_commit", asOfCommit)
	}
	if err != nil {
		return nil, domain.WrapError(domain.CodeInternal, err, "query conclusion")
	}
	var conclusion domain.Conclusion
	if err := json.Unmarshal([]byte(raw), &conclusion); err != nil {
		return nil, domain.WrapError(domain.CodeInternal, err, "parse conclusion")
	}
	conclusion.CommitVersion = commit
	return &conclusion, nil
}

func (s *Store) ListConclusions(ctx context.Context, asOfCommit int64) ([]*domain.Conclusion, int64, error) {
	latest, err := s.LatestCompleteVersion(ctx)
	if err != nil {
		return nil, 0, err
	}
	if asOfCommit <= 0 {
		asOfCommit = latest
	}
	if asOfCommit > latest {
		return nil, 0, domain.NewError(domain.CodeNotFound, "requested commit is not complete").
			WithContext("requested_commit", asOfCommit).WithContext("latest_complete_commit", latest)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT report_json FROM conclusion_versions WHERE commit_version<=? ORDER BY connection_id, commit_version DESC`, asOfCommit)
	if err != nil {
		return nil, 0, domain.WrapError(domain.CodeInternal, err, "list conclusions")
	}
	defer rows.Close()
	seen := map[string]bool{}
	out := []*domain.Conclusion{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, 0, domain.WrapError(domain.CodeInternal, err, "scan conclusion")
		}
		var conclusion domain.Conclusion
		if err := json.Unmarshal([]byte(raw), &conclusion); err != nil {
			return nil, 0, domain.WrapError(domain.CodeInternal, err, "parse conclusion")
		}
		if seen[conclusion.CanonicalConnectionID] {
			continue
		}
		seen[conclusion.CanonicalConnectionID] = true
		out = append(out, &conclusion)
	}
	return out, asOfCommit, nil
}

func (s *Store) ListConnectionIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT canonical_id FROM connections ORDER BY canonical_id`)
	if err != nil {
		return nil, domain.WrapError(domain.CodeInternal, err, "list connections")
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, domain.WrapError(domain.CodeInternal, err, "scan connection id")
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *Store) ConnectionSwitches(ctx context.Context, connectionID string) ([]domain.PolicyTimelineEntry, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT document_json,effective_at_ms FROM connection_policy_switches WHERE connection_id=? ORDER BY effective_at_ms`, connectionID)
	if err != nil {
		return nil, domain.WrapError(domain.CodeInternal, err, "query policy switches")
	}
	defer rows.Close()
	out := []domain.PolicyTimelineEntry{}
	for rows.Next() {
		var raw string
		var at int64
		if err := rows.Scan(&raw, &at); err != nil {
			return nil, domain.WrapError(domain.CodeInternal, err, "scan policy switch")
		}
		var policy domain.Policy
		if err := json.Unmarshal([]byte(raw), &policy); err != nil {
			return nil, domain.WrapError(domain.CodeInternal, err, "parse policy switch")
		}
		out = append(out, domain.PolicyTimelineEntry{Policy: policy, EffectiveAtMS: at})
	}
	return out, nil
}

func (s *Store) GetPolicyVersion(ctx context.Context, version string) (*domain.StoredPolicyVersion, *domain.Error) {
	var stored domain.StoredPolicyVersion
	var documentJSON string
	var retiredAt sql.NullInt64
	var replacedBy sql.NullString
	var revision int64
	err := s.db.QueryRowContext(ctx, `SELECT version,revision,mode,document_json,status,available_from_ms,retired_at_ms,replaced_by FROM policy_versions WHERE version=?`, version).
		Scan(&stored.Version, &revision, &stored.Mode, &documentJSON, &stored.Status, &stored.AvailableFromMS, &retiredAt, &replacedBy)
	if err == sql.ErrNoRows {
		return nil, domain.NewError(domain.CodeNotFound, "policy version not found").WithContext("version", version)
	}
	if err != nil {
		return nil, domain.WrapError(domain.CodeInternal, err, "query policy version")
	}
	if err := json.Unmarshal([]byte(documentJSON), &stored.Policy); err != nil {
		return nil, domain.WrapError(domain.CodeInternal, err, "parse policy document")
	}
	stored.RetiredAtMS = retiredAt.Int64
	stored.ReplacedBy = replacedBy.String

	if stored.Status == "retired" {
		active, minRevision, maxRevision, listErr := s.activeVersions(ctx)
		if listErr != nil {
			return nil, listErr
		}
		perr := domain.NewError(domain.CodeVersionRetired, "policy version has been retired; retrospective adjudication under it is forbidden").
			WithContext("requested_version", version).
			WithContext("retired_at_ms", stored.RetiredAtMS).
			WithContext("replaced_by", stored.ReplacedBy).
			WithContext("current_available_versions", active).
			WithContext("available_revision_range", []int64{minRevision, maxRevision})
		return &stored, perr
	}
	return &stored, nil
}

func (s *Store) QueryVersionConclusions(ctx context.Context, version string) ([]*domain.Conclusion, *domain.Error) {
	if _, perr := s.GetPolicyVersion(ctx, version); perr != nil {
		return nil, perr
	}
	all, _, err := s.ListConclusions(ctx, 0)
	if err != nil {
		if de, ok := domain.AsError(err); ok {
			return nil, de
		}
		return nil, domain.WrapError(domain.CodeInternal, err, "list conclusions")
	}
	out := []*domain.Conclusion{}
	for _, conclusion := range all {
		for _, candidate := range conclusion.PolicyVersions {
			if candidate == version {
				out = append(out, conclusion)
				break
			}
		}
	}
	return out, nil
}

func (s *Store) activeVersions(ctx context.Context) ([]string, int64, int64, *domain.Error) {
	rows, err := s.db.QueryContext(ctx, `SELECT version,revision FROM policy_versions WHERE status='active' ORDER BY revision`)
	if err != nil {
		return nil, 0, 0, domain.WrapError(domain.CodeInternal, err, "list active policies")
	}
	defer rows.Close()
	versions := []string{}
	var minRevision, maxRevision int64
	first := true
	for rows.Next() {
		var version string
		var revision int64
		if err := rows.Scan(&version, &revision); err != nil {
			return nil, 0, 0, domain.WrapError(domain.CodeInternal, err, "scan active policy")
		}
		if first {
			minRevision = revision
			first = false
		}
		maxRevision = revision
		versions = append(versions, version)
	}
	if first {
		minRevision, maxRevision = 0, 0
	}
	return versions, minRevision, maxRevision, nil
}

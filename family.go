package tokenforge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type PathMode string

const (
	PathAuto    PathMode = "auto"
	PathPrecise PathMode = "precise_per_family"
	PathStream  PathMode = "batch_stream"
)

const familyIndexName = "idx_tokens_tenant_family"

// FamilyQueryOptions controls the adaptive family-genealogy query. A threshold
// override of zero uses the service default. ForcePath exists for parity tests
// and for hosts that need to pin a path; normal calls use PathAuto.
type FamilyQueryOptions struct {
	Threshold int      `json:"threshold"`
	ForcePath PathMode `json:"force_path"`
}

type familyRow struct {
	TenantID        string
	FamilyID        string
	UserID          string
	CurrentSequence int64
	Status          string
	RevokeReason    string
	CreatedAt       int64
	UpdatedAt       int64
}

type familyRead struct {
	Family    familyRow
	Path      PathMode
	Threshold int
	IndexUsed bool
}

type FamilyAuditMember struct {
	TokenID   string    `json:"token_id"`
	Digest    string    `json:"digest"`
	Sequence  int64     `json:"sequence"`
	Kind      TokenKind `json:"kind"`
	Status    string    `json:"status"`
	IssuedAt  int64     `json:"issued_at_unix_nano"`
	ExpiresAt int64     `json:"expires_at_unix_nano"`
}

type FamilyAuditEntry struct {
	FamilyID        string              `json:"family_id"`
	UserID          string              `json:"user_id"`
	CurrentSequence int64               `json:"current_sequence"`
	Status          string              `json:"status"`
	RevokeReason    string              `json:"revoke_reason"`
	Members         []FamilyAuditMember `json:"members"`
}

type FamilyAuditResult struct {
	TenantID      string             `json:"tenant_id"`
	Path          PathMode           `json:"path"`
	Threshold     int                `json:"threshold"`
	FamilyCount   int                `json:"family_count"`
	IndexName     string             `json:"index_name,omitempty"`
	IndexReady    bool               `json:"index_ready"`
	Families      []FamilyAuditEntry `json:"families"`
	MembersDigest string             `json:"members_digest"`
}

type familyQuerier interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Service) familyThreshold(opts FamilyQueryOptions) int {
	if opts.Threshold > 0 {
		return opts.Threshold
	}
	return s.cfg.DefaultFamilyScanThreshold
}

func (s *Service) ensureFamilyIndex(ctx context.Context, q familyQuerier) error {
	_, err := q.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_tokens_tenant_family
		ON tokens(tenant_id, family_id, sequence, kind, token_id)`)
	return err
}

func (s *Service) choosePath(ctx context.Context, q familyQuerier, tenantID string, opts FamilyQueryOptions) (PathMode, int, int, error) {
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM families WHERE tenant_id = ?`, tenantID).Scan(&count); err != nil {
		return "", 0, 0, fmt.Errorf("count tenant families: %w", err)
	}
	path, threshold, err := s.pathForCount(count, opts)
	return path, threshold, count, err
}

func (s *Service) pathForCount(count int, opts FamilyQueryOptions) (PathMode, int, error) {
	threshold := s.familyThreshold(opts)
	switch opts.ForcePath {
	case PathPrecise, PathStream:
		return opts.ForcePath, threshold, nil
	case "", PathAuto:
		if count < threshold {
			return PathPrecise, threshold, nil
		}
		return PathStream, threshold, nil
	default:
		return "", 0, codeError(CodeInvalidArgument, "unknown family query path")
	}
}

func (s *Service) readFamily(ctx context.Context, q familyQuerier, tenantID, familyID string, opts FamilyQueryOptions) (familyRead, error) {
	path, threshold, _, err := s.choosePath(ctx, q, tenantID, opts)
	if err != nil {
		return familyRead{}, err
	}
	out := familyRead{Path: path, Threshold: threshold}
	if path == PathStream {
		if err := s.ensureFamilyIndex(ctx, q); err != nil {
			return out, fmt.Errorf("build family index: %w", err)
		}
		out.IndexUsed = true
		rows, err := q.QueryContext(ctx, `SELECT tenant_id, family_id, user_id, current_sequence,
			status, revoke_reason, created_at, updated_at
			FROM families WHERE tenant_id = ? ORDER BY family_id`, tenantID)
		if err != nil {
			return out, fmt.Errorf("stream families: %w", err)
		}
		defer rows.Close()
		found := false
		for rows.Next() {
			var f familyRow
			if err := scanFamily(rows, &f); err != nil {
				return out, err
			}
			if f.FamilyID == familyID {
				out.Family = f
				found = true
			}
		}
		if err := rows.Err(); err != nil {
			return out, fmt.Errorf("stream families: %w", err)
		}
		if !found {
			return out, codeError(CodeTokenInvalid, "token family not found")
		}
		return out, nil
	}

	f, err := queryOneFamily(ctx, q, tenantID, familyID)
	if err != nil {
		if err == sql.ErrNoRows {
			return out, codeError(CodeTokenInvalid, "token family not found")
		}
		return out, err
	}
	out.Family = f
	return out, nil
}

func queryOneFamily(ctx context.Context, q familyQuerier, tenantID, familyID string) (familyRow, error) {
	var f familyRow
	err := q.QueryRowContext(ctx, `SELECT tenant_id, family_id, user_id, current_sequence,
		status, revoke_reason, created_at, updated_at
		FROM families WHERE tenant_id = ? AND family_id = ?`, tenantID, familyID).
		Scan(&f.TenantID, &f.FamilyID, &f.UserID, &f.CurrentSequence, &f.Status, &f.RevokeReason, &f.CreatedAt, &f.UpdatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return f, sql.ErrNoRows
		}
		return f, fmt.Errorf("load family: %w", err)
	}
	return f, nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanFamily(r rowScanner, f *familyRow) error {
	if err := r.Scan(&f.TenantID, &f.FamilyID, &f.UserID, &f.CurrentSequence,
		&f.Status, &f.RevokeReason, &f.CreatedAt, &f.UpdatedAt); err != nil {
		return fmt.Errorf("scan family: %w", err)
	}
	return nil
}

// AuditFamilies executes the adaptive genealogy read. The two paths emit the
// same canonical Families payload and MembersDigest for the same database
// state; only Path/Index metadata differs.
func (s *Service) AuditFamilies(ctx context.Context, tenantID string, opts FamilyQueryOptions) (FamilyAuditResult, error) {
	if strings.TrimSpace(tenantID) == "" {
		return FamilyAuditResult{}, codeError(CodeInvalidArgument, "tenant_id is required")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return FamilyAuditResult{}, fmt.Errorf("begin family audit: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	families, err := loadFamiliesOrdered(ctx, tx, tenantID)
	if err != nil {
		return FamilyAuditResult{}, err
	}
	// The schema migration creates the family index before any audit can run.
	// Keeping DDL out of this read-only path avoids taking a write lock while
	// refreshes are committing.
	path, threshold, err := s.pathForCount(len(families), opts)
	if err != nil {
		return FamilyAuditResult{}, err
	}
	out := FamilyAuditResult{
		TenantID:    tenantID,
		Path:        path,
		Threshold:   threshold,
		FamilyCount: len(families),
		Families:    make([]FamilyAuditEntry, 0, len(families)),
	}
	if path == PathStream {
		out.IndexReady = true
		out.IndexName = familyIndexName
	}

	if path == PathStream {
		membersByFamily := make(map[string][]FamilyAuditMember)
		rows, err := tx.QueryContext(ctx, `SELECT token_id, token_hash, family_id, sequence, kind,
			issued_at, expires_at, status FROM tokens WHERE tenant_id = ?
			ORDER BY family_id, sequence, kind, token_id`, tenantID)
		if err != nil {
			return out, fmt.Errorf("stream family members: %w", err)
		}
		for rows.Next() {
			var tokenID, digest, familyID, kind, status string
			var sequence, issuedAt, expiresAt int64
			if err := rows.Scan(&tokenID, &digest, &familyID, &sequence, &kind, &issuedAt, &expiresAt, &status); err != nil {
				_ = rows.Close()
				return out, fmt.Errorf("scan streamed family member: %w", err)
			}
			membersByFamily[familyID] = append(membersByFamily[familyID], FamilyAuditMember{
				TokenID: tokenID, Digest: digest, Sequence: sequence,
				Kind: TokenKind(kind), Status: status, IssuedAt: issuedAt, ExpiresAt: expiresAt,
			})
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return out, fmt.Errorf("stream family members: %w", err)
		}
		if err := rows.Close(); err != nil {
			return out, err
		}
		for _, f := range families {
			members := membersByFamily[f.FamilyID]
			if members == nil {
				members = []FamilyAuditMember{}
			}
			out.Families = append(out.Families, FamilyAuditEntry{
				FamilyID: f.FamilyID, UserID: f.UserID, CurrentSequence: f.CurrentSequence,
				Status: f.Status, RevokeReason: f.RevokeReason, Members: members,
			})
		}
	} else {
		for _, f := range families {
			members, err := loadFamilyMembers(ctx, tx, tenantID, f.FamilyID)
			if err != nil {
				return out, err
			}
			out.Families = append(out.Families, FamilyAuditEntry{
				FamilyID: f.FamilyID, UserID: f.UserID, CurrentSequence: f.CurrentSequence,
				Status: f.Status, RevokeReason: f.RevokeReason, Members: members,
			})
		}
	}
	digest, err := digestFamilyAudit(out.Families)
	if err != nil {
		return out, err
	}
	out.MembersDigest = digest
	return out, nil
}

func loadFamiliesOrdered(ctx context.Context, q familyQuerier, tenantID string) ([]familyRow, error) {
	rows, err := q.QueryContext(ctx, `SELECT tenant_id, family_id, user_id, current_sequence,
		status, revoke_reason, created_at, updated_at
		FROM families WHERE tenant_id = ? ORDER BY family_id`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("load families: %w", err)
	}
	defer rows.Close()
	families := make([]familyRow, 0)
	for rows.Next() {
		var f familyRow
		if err := scanFamily(rows, &f); err != nil {
			return nil, err
		}
		families = append(families, f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load families: %w", err)
	}
	return families, nil
}

func loadFamilyMembers(ctx context.Context, q familyQuerier, tenantID, familyID string) ([]FamilyAuditMember, error) {
	rows, err := q.QueryContext(ctx, `SELECT token_id, token_hash, sequence, kind, status, issued_at, expires_at
		FROM tokens WHERE tenant_id = ? AND family_id = ?
		ORDER BY sequence, kind, token_id`, tenantID, familyID)
	if err != nil {
		return nil, fmt.Errorf("load family members: %w", err)
	}
	defer rows.Close()
	members := make([]FamilyAuditMember, 0, 4)
	for rows.Next() {
		var m FamilyAuditMember
		var kind, status string
		if err := rows.Scan(&m.TokenID, &m.Digest, &m.Sequence, &kind, &status, &m.IssuedAt, &m.ExpiresAt); err != nil {
			return nil, fmt.Errorf("scan family member: %w", err)
		}
		m.Kind = TokenKind(kind)
		m.Status = status
		members = append(members, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load family members: %w", err)
	}
	return members, nil
}

func digestFamilyAudit(families []FamilyAuditEntry) (string, error) {
	if families == nil {
		families = []FamilyAuditEntry{}
	}
	b, err := json.Marshal(families)
	if err != nil {
		return "", fmt.Errorf("marshal family audit: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// SortFamilyAudit provides deterministic ordering for hosts assembling reports
// from multiple query results.
func SortFamilyAudit(families []FamilyAuditEntry) {
	sort.Slice(families, func(i, j int) bool { return families[i].FamilyID < families[j].FamilyID })
}

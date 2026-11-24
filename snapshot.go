package tokenforge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
)

const snapshotSchemaVersion = 1

type SnapshotFamily struct {
	FamilyID        string `json:"family_id"`
	UserID          string `json:"user_id"`
	CurrentSequence int64  `json:"current_sequence"`
	Status          string `json:"status"`
	RevokeReason    string `json:"revoke_reason"`
	CreatedAt       int64  `json:"created_at_unix_nano"`
	UpdatedAt       int64  `json:"updated_at_unix_nano"`
}

type SnapshotToken struct {
	TokenID    string    `json:"token_id"`
	Digest     string    `json:"digest"`
	FamilyID   string    `json:"family_id"`
	UserID     string    `json:"user_id"`
	Sequence   int64     `json:"sequence"`
	Kind       TokenKind `json:"kind"`
	IssuedAt   int64     `json:"issued_at_unix_nano"`
	ExpiresAt  int64     `json:"expires_at_unix_nano"`
	Status     string    `json:"status"`
	ConsumedAt int64     `json:"consumed_at_unix_nano"`
	RevokedAt  int64     `json:"revoked_at_unix_nano"`
}

type SnapshotState struct {
	SchemaVersion int              `json:"schema_version"`
	Version       int64            `json:"version"`
	TenantID      string           `json:"tenant_id"`
	Quota         QuotaState       `json:"quota"`
	Families      []SnapshotFamily `json:"families"`
	Tokens        []SnapshotToken  `json:"tokens"`
	Charges       []ChargeRecord   `json:"charges"`
}

type SnapshotRecord struct {
	TenantID    string        `json:"tenant_id"`
	Version     int64         `json:"version"`
	PrevHash    string        `json:"prev_hash"`
	ContentHash string        `json:"content_hash"`
	Operation   string        `json:"operation"`
	CreatedAt   int64         `json:"created_at_unix_nano"`
	State       SnapshotState `json:"state"`
}

type SnapshotComparison struct {
	VersionA          int64    `json:"version_a"`
	VersionB          int64    `json:"version_b"`
	DigestA           string   `json:"digest_a"`
	DigestB           string   `json:"digest_b"`
	VersionChanged    bool     `json:"version_changed"`
	SameVersion       bool     `json:"same_version"`
	ContentChanged    bool     `json:"content_changed"`
	VersionOnlyChange bool     `json:"version_only_change"`
	WholeReplacement  bool     `json:"whole_replacement"`
	SharedFamilies    int      `json:"shared_families"`
	SharedTokens      int      `json:"shared_tokens"`
	ChangedSections   []string `json:"changed_sections"`
}

type SnapshotReplayResult struct {
	TenantID                string               `json:"tenant_id"`
	Code                    Code                 `json:"code"`
	Snapshots               []SnapshotRecord     `json:"snapshots"`
	LatestContiguousVersion int64                `json:"latest_contiguous_version"`
	StoredVersion           int64                `json:"stored_version"`
	StoredContentHash       string               `json:"stored_content_hash"`
	Comparisons             []SnapshotComparison `json:"comparisons"`
}

func (s *Service) tenantCurrentVersion(ctx context.Context, q queryRower, tenantID string) (int64, string, error) {
	var version int64
	var hash string
	err := q.QueryRowContext(ctx, `SELECT version, content_hash FROM tenant_state WHERE tenant_id = ?`, tenantID).
		Scan(&version, &hash)
	if err == sql.ErrNoRows {
		return 0, "", codeError(CodeTenantNotFound, "tenant state not found")
	}
	if err != nil {
		return 0, "", fmt.Errorf("load tenant state: %w", err)
	}
	return version, hash, nil
}

// prepareSnapshotVersion reserves the next content version for this successful
// operation. InjectNextSnapshotGap advances the recorded version by one extra,
// intentionally leaving a hole for ReplaySnapshots to detect.
func (s *Service) prepareSnapshotVersion(ctx context.Context, q queryRower, tenantID string) (current, next int64, err error) {
	current, _, err = s.tenantCurrentVersion(ctx, q, tenantID)
	if err != nil {
		return 0, 0, err
	}
	if current == math.MaxInt64 {
		return 0, 0, codeError(CodeQuotaOverflow, "snapshot version overflow")
	}
	next = current + 1
	if s.skipSnapshotOnce.Swap(false) {
		if next == math.MaxInt64 {
			return 0, 0, codeError(CodeQuotaOverflow, "snapshot version overflow")
		}
		next++
	}
	return current, next, nil
}

func (s *Service) writeSnapshot(ctx context.Context, q familyQuerier, tenantID string, currentVersion, nextVersion int64, operation string, createdAt int64) (string, error) {
	state, err := loadSnapshotState(ctx, q, tenantID, nextVersion)
	if err != nil {
		return "", err
	}
	content, err := json.Marshal(state)
	if err != nil {
		return "", fmt.Errorf("marshal snapshot content: %w", err)
	}
	sum := sha256.Sum256(content)
	contentHash := hex.EncodeToString(sum[:])
	var prevHash string
	var storedVersion int64
	err = q.QueryRowContext(ctx, `SELECT version, content_hash FROM tenant_state WHERE tenant_id = ?`, tenantID).
		Scan(&storedVersion, &prevHash)
	if err != nil {
		return "", fmt.Errorf("load previous snapshot hash: %w", err)
	}
	if storedVersion != currentVersion {
		return "", codeError(CodeAborted, "snapshot version changed concurrently")
	}
	if _, err := q.ExecContext(ctx, `INSERT INTO snapshots
		(tenant_id, version, prev_hash, content_hash, content, operation, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, tenantID, nextVersion, prevHash, contentHash, string(content), operation, createdAt); err != nil {
		return "", fmt.Errorf("insert snapshot: %w", err)
	}
	res, err := q.ExecContext(ctx, `UPDATE tenant_state SET version = ?, content_hash = ?
		WHERE tenant_id = ? AND version = ?`, nextVersion, contentHash, tenantID, currentVersion)
	if err != nil {
		return "", fmt.Errorf("update tenant snapshot pointer: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("read snapshot pointer rows affected: %w", err)
	}
	if n != 1 {
		return "", codeError(CodeAborted, "snapshot pointer changed concurrently")
	}
	return contentHash, nil
}

func loadSnapshotState(ctx context.Context, q familyQuerier, tenantID string, version int64) (SnapshotState, error) {
	t, err := loadTenantRow(ctx, q, tenantID)
	if err != nil {
		return SnapshotState{}, err
	}
	quota, err := t.quotaState()
	if err != nil {
		return SnapshotState{}, err
	}
	families, err := loadSnapshotFamilies(ctx, q, tenantID)
	if err != nil {
		return SnapshotState{}, err
	}
	tokens, err := loadSnapshotTokens(ctx, q, tenantID)
	if err != nil {
		return SnapshotState{}, err
	}
	charges, err := loadCharges(ctx, q, tenantID)
	if err != nil {
		return SnapshotState{}, err
	}
	return SnapshotState{
		SchemaVersion: snapshotSchemaVersion,
		Version:       version,
		TenantID:      tenantID,
		Quota:         quota,
		Families:      families,
		Tokens:        tokens,
		Charges:       charges,
	}, nil
}

func loadSnapshotFamilies(ctx context.Context, q familyQuerier, tenantID string) ([]SnapshotFamily, error) {
	rows, err := q.QueryContext(ctx, `SELECT family_id, user_id, current_sequence, status,
		revoke_reason, created_at, updated_at FROM families WHERE tenant_id = ? ORDER BY family_id`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("load snapshot families: %w", err)
	}
	defer rows.Close()
	out := make([]SnapshotFamily, 0)
	for rows.Next() {
		var f SnapshotFamily
		if err := rows.Scan(&f.FamilyID, &f.UserID, &f.CurrentSequence, &f.Status,
			&f.RevokeReason, &f.CreatedAt, &f.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan snapshot family: %w", err)
		}
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load snapshot families: %w", err)
	}
	return out, nil
}

func loadSnapshotTokens(ctx context.Context, q familyQuerier, tenantID string) ([]SnapshotToken, error) {
	rows, err := q.QueryContext(ctx, `SELECT token_id, token_hash, family_id, user_id, sequence,
		kind, issued_at, expires_at, status, consumed_at, revoked_at FROM tokens
		WHERE tenant_id = ? ORDER BY family_id, sequence, kind, token_id`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("load snapshot tokens: %w", err)
	}
	defer rows.Close()
	out := make([]SnapshotToken, 0)
	for rows.Next() {
		var t SnapshotToken
		var kind string
		if err := rows.Scan(&t.TokenID, &t.Digest, &t.FamilyID, &t.UserID, &t.Sequence,
			&kind, &t.IssuedAt, &t.ExpiresAt, &t.Status, &t.ConsumedAt, &t.RevokedAt); err != nil {
			return nil, fmt.Errorf("scan snapshot token: %w", err)
		}
		t.Kind = TokenKind(kind)
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load snapshot tokens: %w", err)
	}
	return out, nil
}

func (s *Service) ReplaySnapshots(ctx context.Context, tenantID string) (SnapshotReplayResult, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return SnapshotReplayResult{}, fmt.Errorf("begin snapshot replay: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	version, storedHash, err := s.tenantCurrentVersion(ctx, tx, tenantID)
	if err != nil {
		return SnapshotReplayResult{}, err
	}
	result := SnapshotReplayResult{
		TenantID:      tenantID,
		Code:          CodeOK,
		Snapshots:     make([]SnapshotRecord, 0),
		StoredVersion: version, StoredContentHash: storedHash,
		Comparisons: make([]SnapshotComparison, 0),
	}
	rows, err := tx.QueryContext(ctx, `SELECT version, prev_hash, content_hash, content, operation, created_at
		FROM snapshots WHERE tenant_id = ? ORDER BY version`, tenantID)
	if err != nil {
		return result, fmt.Errorf("load snapshots: %w", err)
	}
	var expected int64 = 1
	priorHash := ""
	var replayErr error
	for rows.Next() {
		var rec SnapshotRecord
		var content string
		if err := rows.Scan(&rec.Version, &rec.PrevHash, &rec.ContentHash, &content, &rec.Operation, &rec.CreatedAt); err != nil {
			_ = rows.Close()
			return result, fmt.Errorf("scan snapshot: %w", err)
		}
		rec.TenantID = tenantID
		if rec.Version != expected {
			result.Code = CodeSnapshotVersion
			replayErr = codeError(CodeSnapshotVersion, fmt.Sprintf("snapshot version gap: expected %d, found %d", expected, rec.Version))
			break
		}
		sum := sha256.Sum256([]byte(content))
		if hex.EncodeToString(sum[:]) != rec.ContentHash {
			result.Code = CodeSnapshotIntegrity
			replayErr = codeError(CodeSnapshotIntegrity, fmt.Sprintf("snapshot %d content hash mismatch", rec.Version))
			break
		}
		if rec.PrevHash != priorHash {
			result.Code = CodeSnapshotIntegrity
			replayErr = codeError(CodeSnapshotIntegrity, fmt.Sprintf("snapshot %d previous hash mismatch", rec.Version))
			break
		}
		if err := json.Unmarshal([]byte(content), &rec.State); err != nil {
			result.Code = CodeSnapshotIntegrity
			replayErr = codeError(CodeSnapshotIntegrity, fmt.Sprintf("snapshot %d content is invalid", rec.Version))
			break
		}
		if rec.State.Version != rec.Version || rec.State.TenantID != tenantID {
			result.Code = CodeSnapshotIntegrity
			replayErr = codeError(CodeSnapshotIntegrity, fmt.Sprintf("snapshot %d body identity mismatch", rec.Version))
			break
		}
		result.Snapshots = append(result.Snapshots, rec)
		priorHash = rec.ContentHash
		expected++
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return result, fmt.Errorf("load snapshots: %w", err)
	}
	_ = rows.Close()
	if replayErr == nil {
		last := len(result.Snapshots)
		if int64(last) != version {
			result.Code = CodeSnapshotVersion
			replayErr = codeError(CodeSnapshotVersion, fmt.Sprintf("snapshot pointer version %d has only %d contiguous rows", version, last))
		} else if last > 0 && result.Snapshots[last-1].ContentHash != storedHash {
			result.Code = CodeSnapshotIntegrity
			replayErr = codeError(CodeSnapshotIntegrity, "snapshot pointer content hash mismatch")
		}
	}
	if len(result.Snapshots) > 0 {
		result.LatestContiguousVersion = result.Snapshots[len(result.Snapshots)-1].Version
	}
	for i := 1; i < len(result.Snapshots); i++ {
		result.Comparisons = append(result.Comparisons, CompareSnapshots(result.Snapshots[i-1], result.Snapshots[i]))
	}
	return result, replayErr
}

// CompareSnapshots compares content, not just version numbers. Same-version
// body replacement and disjoint-identity full replacement are both surfaced.
func CompareSnapshots(a, b SnapshotRecord) SnapshotComparison {
	cmp := SnapshotComparison{
		VersionA: a.Version, VersionB: b.Version,
		DigestA: a.ContentHash, DigestB: b.ContentHash,
		VersionChanged:  a.Version != b.Version,
		SameVersion:     a.Version == b.Version,
		ContentChanged:  a.ContentHash != b.ContentHash,
		ChangedSections: make([]string, 0),
	}
	cmp.VersionOnlyChange = cmp.VersionChanged && !cmp.ContentChanged
	if !reflect.DeepEqual(a.State.Quota, b.State.Quota) {
		cmp.ChangedSections = append(cmp.ChangedSections, "quota")
	}
	if !reflect.DeepEqual(a.State.Families, b.State.Families) {
		cmp.ChangedSections = append(cmp.ChangedSections, "families")
	}
	if !reflect.DeepEqual(a.State.Tokens, b.State.Tokens) {
		cmp.ChangedSections = append(cmp.ChangedSections, "tokens")
	}
	if !reflect.DeepEqual(a.State.Charges, b.State.Charges) {
		cmp.ChangedSections = append(cmp.ChangedSections, "charges")
	}
	af := make(map[string]struct{}, len(a.State.Families))
	for _, f := range a.State.Families {
		af[f.FamilyID] = struct{}{}
	}
	at := make(map[string]struct{}, len(a.State.Tokens))
	for _, t := range a.State.Tokens {
		at[t.TokenID] = struct{}{}
	}
	for _, f := range b.State.Families {
		if _, ok := af[f.FamilyID]; ok {
			cmp.SharedFamilies++
		}
	}
	for _, t := range b.State.Tokens {
		if _, ok := at[t.TokenID]; ok {
			cmp.SharedTokens++
		}
	}
	cmp.WholeReplacement = cmp.ContentChanged && (cmp.SameVersion || (cmp.SharedFamilies == 0 && cmp.SharedTokens == 0))
	sort.Strings(cmp.ChangedSections)
	return cmp
}

// SnapshotChainSummary is a compact deterministic report view.
type SnapshotChainSummary struct {
	TenantID                string               `json:"tenant_id"`
	StoredVersion           int64                `json:"stored_version"`
	LatestContiguousVersion int64                `json:"latest_contiguous_version"`
	Code                    Code                 `json:"code"`
	Versions                []int64              `json:"versions"`
	Hashes                  []string             `json:"hashes"`
	Comparisons             []SnapshotComparison `json:"comparisons"`
}

func SummarizeSnapshotChain(r SnapshotReplayResult) SnapshotChainSummary {
	out := SnapshotChainSummary{
		TenantID: r.TenantID, StoredVersion: r.StoredVersion,
		LatestContiguousVersion: r.LatestContiguousVersion, Code: r.Code,
		Versions:    make([]int64, 0, len(r.Snapshots)),
		Hashes:      make([]string, 0, len(r.Snapshots)),
		Comparisons: r.Comparisons,
	}
	for _, s := range r.Snapshots {
		out.Versions = append(out.Versions, s.Version)
		out.Hashes = append(out.Hashes, s.ContentHash)
	}
	return out
}

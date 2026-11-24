package tokenforge

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"
)

type RefreshRequest struct {
	RefreshToken string             `json:"refresh_token"`
	FamilyQuery  FamilyQueryOptions `json:"family_query"`
}

type RefreshResult struct {
	Code              Code       `json:"code"`
	TenantID          string     `json:"tenant_id"`
	UserID            string     `json:"user_id"`
	FamilyID          string     `json:"family_id"`
	OldSequence       int64      `json:"old_sequence"`
	NewSequence       int64      `json:"new_sequence"`
	Pair              *TokenPair `json:"pair,omitempty"`
	ChargedFixed      int64      `json:"charged_fixed"`
	RemainderUnits    int64      `json:"remainder_units"`
	BalanceAfterFixed int64      `json:"balance_after_fixed"`
	SpentAfterFixed   int64      `json:"spent_after_fixed"`
	ErrorBoundFixed   int64      `json:"error_bound_fixed"`
	SnapshotVersion   int64      `json:"snapshot_version"`
	QueryPath         PathMode   `json:"query_path,omitempty"`
	Threshold         int        `json:"threshold,omitempty"`
	IndexUsed         bool       `json:"index_used"`
	RevokedCount      int64      `json:"revoked_count"`
	QueueNanos        int64      `json:"queue_nanos"`
	InFlight          int64      `json:"in_flight"`
}

// Refresh verifies a refresh token, rotates the family atomically, applies the
// fixed-point quota charge, writes the charge/error ledger, and appends a
// snapshot in the same SQLite transaction.
func (s *Service) Refresh(ctx context.Context, req RefreshRequest) (RefreshResult, error) {
	req.RefreshToken = strings.TrimSpace(req.RefreshToken)
	claims, err := s.verifyToken(req.RefreshToken)
	if err != nil {
		return RefreshResult{Code: CodeOf(err)}, err
	}
	if claims.Kind != TokenKindRefresh {
		err = codeError(CodeTokenInvalid, "refresh endpoint requires a refresh token")
		return RefreshResult{Code: CodeTokenInvalid}, err
	}
	nowNanos := s.now().UnixNano()
	if claims.ExpiresAt <= nowNanos {
		err = codeError(CodeTokenExpired, "refresh token has expired")
		return RefreshResult{Code: CodeTokenExpired, TenantID: claims.TenantID, UserID: claims.UserID, FamilyID: claims.FamilyID, OldSequence: claims.Sequence}, err
	}

	overlapped := s.attempts.begin(claims.TokenID)
	defer s.attempts.end(claims.TokenID)

	lease, err := s.beginWrite(ctx, claims.TenantID)
	if err != nil {
		return RefreshResult{Code: CodeOf(err), TenantID: claims.TenantID, UserID: claims.UserID,
			FamilyID: claims.FamilyID, OldSequence: claims.Sequence,
			QueueNanos: lease.queueNanos, InFlight: lease.inFlight}, err
	}
	defer lease.release()
	releaseToken, _, err := s.tokenGate.acquire(ctx, claims.TokenID, time.Time{})
	if err != nil {
		return RefreshResult{Code: CodeOf(err), TenantID: claims.TenantID, UserID: claims.UserID,
			FamilyID: claims.FamilyID, OldSequence: claims.Sequence,
			QueueNanos: lease.queueNanos, InFlight: lease.inFlight}, err
	}
	defer releaseToken()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RefreshResult{Code: CodeInternal}, fmt.Errorf("begin refresh transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	read, err := s.readFamily(ctx, tx, claims.TenantID, claims.FamilyID, req.FamilyQuery)
	result := RefreshResult{
		TenantID: claims.TenantID, UserID: claims.UserID, FamilyID: claims.FamilyID,
		OldSequence: claims.Sequence, NewSequence: claims.Sequence,
		QueryPath: read.Path, Threshold: read.Threshold, IndexUsed: read.IndexUsed,
		QueueNanos: lease.queueNanos, InFlight: lease.inFlight,
	}
	if err != nil {
		result.Code = CodeOf(err)
		return result, err
	}
	family := read.Family
	if family.Status == familyStatusRevoked {
		result.Code = CodeTokenRevoked
		return result, codeError(CodeTokenRevoked, "token family has been revoked")
	}
	if family.UserID != claims.UserID {
		result.Code = CodeTokenInvalid
		return result, codeError(CodeTokenInvalid, "token user does not match family")
	}
	if claims.Sequence > family.CurrentSequence {
		result.Code = CodeTokenSequence
		return result, codeError(CodeTokenSequence, "token sequence is ahead of family sequence")
	}
	if claims.Sequence == math.MaxInt64 {
		result.Code = CodeTokenSequence
		return result, codeError(CodeTokenSequence, "token sequence cannot advance beyond int64")
	}

	record, err := loadTokenByID(ctx, tx, claims.TokenID)
	if err != nil {
		result.Code = CodeOf(err)
		return result, err
	}
	if err := validateTokenRecord(claims, req.RefreshToken, record); err != nil {
		result.Code = CodeOf(err)
		return result, err
	}

	if claims.Sequence < family.CurrentSequence {
		switch record.Status {
		case tokenStatusConsumed:
			if overlapped {
				result.Code = CodeTokenConsumed
				return result, codeError(CodeTokenConsumed, "concurrent refresh was already consumed")
			}
			version, count, err := s.revokeFamilyAndCommit(ctx, tx, claims.TenantID, claims.FamilyID, revokeReasonReuse, nowNanos, "replay_family_revoke")
			if err != nil {
				result.Code = CodeOf(err)
				return result, err
			}
			result.Code = CodeTokenReuse
			result.SnapshotVersion = version
			result.RevokedCount = count
			return result, codeError(CodeTokenReuse, "consumed refresh token replay revoked the family")
		case tokenStatusRevokedSingle, tokenStatusRevokedFamily:
			result.Code = CodeTokenRevoked
			return result, codeError(CodeTokenRevoked, "refresh token has been revoked")
		case tokenStatusActive:
			result.Code = CodeTokenSequence
			return result, codeError(CodeTokenSequence, "active token sequence is behind family sequence")
		default:
			result.Code = CodeTokenInvalid
			return result, codeError(CodeTokenInvalid, "unknown token status")
		}
	}

	switch record.Status {
	case tokenStatusActive:
		// Continue below.
	case tokenStatusConsumed:
		if overlapped {
			result.Code = CodeTokenConsumed
			return result, codeError(CodeTokenConsumed, "concurrent refresh was already consumed")
		}
		version, count, err := s.revokeFamilyAndCommit(ctx, tx, claims.TenantID, claims.FamilyID, revokeReasonReuse, nowNanos, "replay_family_revoke")
		if err != nil {
			result.Code = CodeOf(err)
			return result, err
		}
		result.Code = CodeTokenReuse
		result.SnapshotVersion = version
		result.RevokedCount = count
		return result, codeError(CodeTokenReuse, "consumed refresh token replay revoked the family")
	case tokenStatusRevokedSingle, tokenStatusRevokedFamily:
		result.Code = CodeTokenRevoked
		return result, codeError(CodeTokenRevoked, "refresh token has been revoked")
	default:
		result.Code = CodeTokenInvalid
		return result, codeError(CodeTokenInvalid, "unknown token status")
	}

	tenant, err := s.loadTenant(ctx, tx, claims.TenantID)
	if err != nil {
		result.Code = CodeOf(err)
		return result, err
	}
	charge, err := computeRefreshCharge(tenant)
	if err != nil {
		result.Code = CodeOf(err)
		return result, err
	}
	currentVersion, nextVersion, err := s.prepareSnapshotVersion(ctx, tx, claims.TenantID)
	if err != nil {
		result.Code = CodeOf(err)
		return result, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tokens SET status = ?, consumed_at = ?
		WHERE tenant_id = ? AND family_id = ? AND sequence = ? AND status = ?`,
		tokenStatusConsumed, nowNanos, claims.TenantID, claims.FamilyID, claims.Sequence, tokenStatusActive); err != nil {
		return result, fmt.Errorf("consume previous tokens: %w", err)
	}
	nextSequence := claims.Sequence + 1
	if _, err := tx.ExecContext(ctx, `UPDATE families SET current_sequence = ?, updated_at = ?
		WHERE tenant_id = ? AND family_id = ?`, nextSequence, nowNanos, claims.TenantID, claims.FamilyID); err != nil {
		return result, fmt.Errorf("advance family sequence: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tenants SET balance_fixed = ?, spent_fixed = ?,
		error_entry_count = ?, truncating_count = ?, remainder_units_total = ? WHERE tenant_id = ?`,
		charge.NewBalanceFixed, charge.NewSpentFixed, charge.NewEntryCount,
		charge.NewTruncatingCount, charge.NewRemainderTotal, claims.TenantID); err != nil {
		return result, fmt.Errorf("update quota: %w", err)
	}
	entryID := fmt.Sprintf("charge:%s:%s", claims.FamilyID, claims.TokenID)
	if err := insertCharge(ctx, tx, ChargeRecord{
		EntryID: entryID, TenantID: claims.TenantID, FamilyID: claims.FamilyID,
		RefreshTokenID: claims.TokenID, Sequence: claims.Sequence,
		UnitCostFixed: tenant.UnitCostFixed, UsageFixed: tenant.UsageFixed,
		ProductFixed: charge.ProductFixed, ChargedFixed: charge.ChargedFixed,
		RemainderUnits: charge.RemainderUnits, SnapshotVersion: nextVersion, CreatedAt: nowNanos,
	}); err != nil {
		return result, err
	}
	pair, err := s.mintPair(ctx, tx, claims.TenantID, claims.UserID, claims.FamilyID, nextSequence, nowNanos)
	if err != nil {
		return result, err
	}
	if _, err := s.writeSnapshot(ctx, tx, claims.TenantID, currentVersion, nextVersion, "refresh", nowNanos); err != nil {
		return result, err
	}
	if err := s.commitHook(ctx, CommitInfo{
		Operation: "refresh", TenantID: claims.TenantID, FamilyID: claims.FamilyID, Version: nextVersion,
	}); err != nil {
		result.Code = CodeOf(err)
		return result, err
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit refresh: %w", err)
	}
	bound, err := ErrorBoundFixedUpper(charge.NewRemainderTotal)
	if err != nil {
		return result, err
	}
	result.Code = CodeOK
	result.NewSequence = nextSequence
	result.Pair = &pair
	result.ChargedFixed = charge.ChargedFixed
	result.RemainderUnits = charge.RemainderUnits
	result.BalanceAfterFixed = charge.NewBalanceFixed
	result.SpentAfterFixed = charge.NewSpentFixed
	result.ErrorBoundFixed = bound
	result.SnapshotVersion = nextVersion
	return result, nil
}

func (s *Service) revokeFamilyAndCommit(ctx context.Context, tx *sql.Tx, tenantID, familyID, reason string, nowNanos int64, operation string) (int64, int64, error) {
	current, next, err := s.prepareSnapshotVersion(ctx, tx, tenantID)
	if err != nil {
		return 0, 0, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE families SET status = ?, revoke_reason = ?, updated_at = ?
		WHERE tenant_id = ? AND family_id = ? AND status = ?`,
		familyStatusRevoked, reason, nowNanos, tenantID, familyID, familyStatusActive)
	if err != nil {
		return 0, 0, fmt.Errorf("revoke family: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, 0, fmt.Errorf("revoke family rows affected: %w", err)
	}
	if n != 1 {
		return 0, 0, codeError(CodeAborted, "family was not active")
	}
	tokenRes, err := tx.ExecContext(ctx, `UPDATE tokens SET status = ?,
		revoked_at = CASE WHEN revoked_at = 0 THEN ? ELSE revoked_at END
		WHERE tenant_id = ? AND family_id = ?`, tokenStatusRevokedFamily, nowNanos, tenantID, familyID)
	if err != nil {
		return 0, 0, fmt.Errorf("revoke family tokens: %w", err)
	}
	count, err := tokenRes.RowsAffected()
	if err != nil {
		return 0, 0, fmt.Errorf("revoke family token count: %w", err)
	}
	if _, err := s.writeSnapshot(ctx, tx, tenantID, current, next, operation, nowNanos); err != nil {
		return 0, 0, err
	}
	if err := s.commitHook(ctx, CommitInfo{Operation: operation, TenantID: tenantID, FamilyID: familyID, Version: next}); err != nil {
		return 0, 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("commit family revoke: %w", err)
	}
	return next, count, nil
}

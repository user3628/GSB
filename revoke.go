package tokenforge

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type RevokeScope string

const (
	RevokeSingle RevokeScope = "single_token"
	RevokeFamily RevokeScope = "family"
)

type RevokeRequest struct {
	Token       string             `json:"token"`
	Scope       RevokeScope        `json:"scope"`
	FamilyQuery FamilyQueryOptions `json:"family_query"`
}

type RevokeResult struct {
	Code            Code        `json:"code"`
	TenantID        string      `json:"tenant_id"`
	UserID          string      `json:"user_id"`
	FamilyID        string      `json:"family_id"`
	TokenID         string      `json:"token_id"`
	Scope           RevokeScope `json:"scope"`
	RevokedCount    int64       `json:"revoked_count"`
	SnapshotVersion int64       `json:"snapshot_version"`
	QueryPath       PathMode    `json:"query_path,omitempty"`
	Threshold       int         `json:"threshold,omitempty"`
	IndexUsed       bool        `json:"index_used"`
	QueueNanos      int64       `json:"queue_nanos"`
	InFlight        int64       `json:"in_flight"`
}

func (s *Service) Revoke(ctx context.Context, req RevokeRequest) (RevokeResult, error) {
	claims, err := s.verifyToken(strings.TrimSpace(req.Token))
	if err != nil {
		return RevokeResult{Code: CodeOf(err)}, err
	}
	if req.Scope != RevokeSingle && req.Scope != RevokeFamily {
		return RevokeResult{Code: CodeInvalidArgument, TenantID: claims.TenantID}, codeError(CodeInvalidArgument, "unknown revoke scope")
	}
	lease, err := s.beginWrite(ctx, claims.TenantID)
	if err != nil {
		return RevokeResult{Code: CodeOf(err), TenantID: claims.TenantID, UserID: claims.UserID,
			FamilyID: claims.FamilyID, TokenID: claims.TokenID, Scope: req.Scope,
			QueueNanos: lease.queueNanos, InFlight: lease.inFlight}, err
	}
	defer lease.release()
	releaseToken, _, err := s.tokenGate.acquire(ctx, claims.TokenID, time.Time{})
	if err != nil {
		return RevokeResult{Code: CodeOf(err)}, err
	}
	defer releaseToken()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RevokeResult{Code: CodeInternal}, fmt.Errorf("begin revoke transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	read, err := s.readFamily(ctx, tx, claims.TenantID, claims.FamilyID, req.FamilyQuery)
	result := RevokeResult{
		TenantID: claims.TenantID, UserID: claims.UserID, FamilyID: claims.FamilyID,
		TokenID: claims.TokenID, Scope: req.Scope, QueryPath: read.Path,
		Threshold: read.Threshold, IndexUsed: read.IndexUsed,
		QueueNanos: lease.queueNanos, InFlight: lease.inFlight,
	}
	if err != nil {
		result.Code = CodeOf(err)
		return result, err
	}
	if read.Family.Status == familyStatusRevoked {
		result.Code = CodeTokenRevoked
		return result, codeError(CodeTokenRevoked, "token family has been revoked")
	}
	record, err := loadTokenByID(ctx, tx, claims.TokenID)
	if err != nil {
		result.Code = CodeOf(err)
		return result, err
	}
	if err := validateTokenRecord(claims, req.Token, record); err != nil {
		result.Code = CodeOf(err)
		return result, err
	}
	nowNanos := s.now().UnixNano()
	if req.Scope == RevokeFamily {
		version, count, err := s.revokeFamilyAndCommit(ctx, tx, claims.TenantID, claims.FamilyID, revokeReasonManual, nowNanos, "revoke_family")
		if err != nil {
			result.Code = CodeOf(err)
			return result, err
		}
		result.Code = CodeOK
		result.RevokedCount = count
		result.SnapshotVersion = version
		return result, nil
	}
	switch record.Status {
	case tokenStatusActive:
		// Continue.
	case tokenStatusConsumed:
		result.Code = CodeTokenConsumed
		return result, codeError(CodeTokenConsumed, "token has already been consumed")
	case tokenStatusRevokedSingle, tokenStatusRevokedFamily:
		result.Code = CodeTokenRevoked
		return result, codeError(CodeTokenRevoked, "token has already been revoked")
	default:
		result.Code = CodeTokenInvalid
		return result, codeError(CodeTokenInvalid, "unknown token status")
	}
	current, next, err := s.prepareSnapshotVersion(ctx, tx, claims.TenantID)
	if err != nil {
		result.Code = CodeOf(err)
		return result, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE tokens SET status = ?, revoked_at = ?
		WHERE token_id = ? AND status = ?`, tokenStatusRevokedSingle, nowNanos, claims.TokenID, tokenStatusActive)
	if err != nil {
		return result, fmt.Errorf("revoke single token: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		return result, codeError(CodeAborted, "single token was not active")
	}
	if _, err := s.writeSnapshot(ctx, tx, claims.TenantID, current, next, "revoke_single", nowNanos); err != nil {
		result.Code = CodeOf(err)
		return result, err
	}
	if err := s.commitHook(ctx, CommitInfo{Operation: "revoke_single", TenantID: claims.TenantID, FamilyID: claims.FamilyID, Version: next}); err != nil {
		result.Code = CodeOf(err)
		return result, err
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit single revoke: %w", err)
	}
	result.Code = CodeOK
	result.RevokedCount = 1
	result.SnapshotVersion = next
	return result, nil
}

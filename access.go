package tokenforge

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type TokenValidationResult struct {
	Code         Code         `json:"code"`
	Summary      TokenSummary `json:"summary"`
	TokenStatus  string       `json:"token_status"`
	FamilyStatus string       `json:"family_status"`
	TenantID     string       `json:"tenant_id"`
	UserID       string       `json:"user_id"`
	FamilyID     string       `json:"family_id"`
	Sequence     int64        `json:"sequence"`
}

// ValidateAccess validates signature, expiry, token revocation and family
// revocation for an access token. It never mutates state.
func (s *Service) ValidateAccess(ctx context.Context, raw string) (TokenValidationResult, error) {
	claims, err := s.verifyToken(strings.TrimSpace(raw))
	result := TokenValidationResult{TenantID: claims.TenantID, UserID: claims.UserID,
		FamilyID: claims.FamilyID, Sequence: claims.Sequence}
	if err != nil {
		result.Code = CodeOf(err)
		return result, err
	}
	if claims.Kind != TokenKindAccess {
		result.Code = CodeTokenInvalid
		return result, codeError(CodeTokenInvalid, "access validation requires an access token")
	}
	result.Summary = summarizeIssuedToken(IssuedToken{Value: raw, Claims: claims, Digest: tokenDigest(raw)})
	if claims.ExpiresAt <= s.now().UnixNano() {
		result.Code = CodeTokenExpired
		return result, codeError(CodeTokenExpired, "access token has expired")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		result.Code = CodeInternal
		return result, fmt.Errorf("begin access validation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	record, err := loadTokenByID(ctx, tx, claims.TokenID)
	if err != nil {
		result.Code = CodeOf(err)
		return result, err
	}
	if err := validateTokenRecord(claims, raw, record); err != nil {
		result.Code = CodeOf(err)
		return result, err
	}
	family, err := queryOneFamily(ctx, tx, claims.TenantID, claims.FamilyID)
	if err != nil {
		result.Code = CodeTokenInvalid
		return result, codeError(CodeTokenInvalid, "access token family not found")
	}
	result.TokenStatus = record.Status
	result.FamilyStatus = family.Status
	if family.Status == familyStatusRevoked {
		result.Code = CodeTokenRevoked
		return result, codeError(CodeTokenRevoked, "access token family has been revoked")
	}
	switch record.Status {
	case tokenStatusActive:
		result.Code = CodeOK
		return result, nil
	case tokenStatusConsumed:
		result.Code = CodeTokenConsumed
		return result, codeError(CodeTokenConsumed, "access token has been rotated")
	case tokenStatusRevokedSingle, tokenStatusRevokedFamily:
		result.Code = CodeTokenRevoked
		return result, codeError(CodeTokenRevoked, "access token has been revoked")
	default:
		result.Code = CodeTokenInvalid
		return result, codeError(CodeTokenInvalid, "unknown token status")
	}
}

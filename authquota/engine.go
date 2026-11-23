package authquota

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

type Engine struct {
	db     *sql.DB
	cfg    Config
	gate   *Gate
	issuer tokenIssuer

	faultMu          sync.Mutex
	snapshotJumpUsed bool
}

func Open(ctx context.Context, cfg Config) (*Engine, error) {
	cfg = normalizeConfig(cfg)
	db, err := openStore(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Engine{
		db:     db,
		cfg:    cfg,
		gate:   NewGate(cfg.MaxInFlight, cfg.MaxQueue, cfg.QueueTimeout),
		issuer: tokenIssuer{secret: cfg.TokenSecret},
	}, nil
}

func (e *Engine) Close() error { return e.db.Close() }

func (e *Engine) DB() *sql.DB { return e.db }

// Seed is idempotent for an existing tenant/client pair. It creates snapshot
// version 1, which is the root of the continuity chain.
func (e *Engine) Seed(ctx context.Context, seed TenantSeed) error {
	if seed.ID == "" || seed.ClientID == "" || seed.RedirectURI == "" {
		return bizError(CodeInvalidRequest, "tenant id, client id and redirect URI are required", nil)
	}
	if seed.Quota < 0 || seed.UnitCost < 0 {
		return bizError(CodeInvalidRequest, "quota and unit cost must be non-negative", nil)
	}
	if seed.RateNum == 0 && seed.RateDen == 0 {
		seed.RateNum, seed.RateDen = 1, 1
	}
	if seed.RateNum <= 0 || seed.RateDen <= 0 {
		return bizError(CodeInvalidRequest, "billing rate must be positive", nil)
	}
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := nowNano(e.cfg)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO tenants(id, quota_remaining, ledger_version, snapshot_version, unit_cost, rate_num, rate_den, created_at)
VALUES(?,?,?,?,?,?,?,?)
ON CONFLICT(id) DO NOTHING`, seed.ID, seed.Quota, 0, 1, seed.UnitCost, seed.RateNum, seed.RateDen, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO clients(id, tenant_id, redirect_uri) VALUES(?,?,?)
ON CONFLICT(id) DO NOTHING`, seed.ClientID, seed.ID, seed.RedirectURI); err != nil {
		return err
	}
	// The tenant may pre-exist while the snapshot root does not (for example,
	// after a deliberately truncated test database). Only create v1 when it is
	// safe to do so.
	var snapshotCount int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM tenant_snapshots WHERE tenant_id=?`, seed.ID).Scan(&snapshotCount); err != nil {
		return err
	}
	if snapshotCount == 0 {
		tenant, err := loadTenantTx(ctx, tx, seed.ID)
		if err != nil {
			return err
		}
		tenant.SnapshotVersion = 1
		state, err := buildSessionState(ctx, tx, tenant)
		if err != nil {
			return err
		}
		body, err := CanonicalJSON(state)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO tenant_snapshots(tenant_id, version, kind, state_json, state_hash, created_at)
VALUES(?,?,?,?,?,?)`, seed.ID, 1, "seed", body, hashBytes(body), now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (e *Engine) IssueAuthorizationCode(ctx context.Context, in IssueCodeInput) (IssueCodeResult, error) {
	if in.TenantID == "" || in.ClientID == "" || in.RedirectURI == "" || in.CodeChallenge == "" || in.Nonce == "" {
		return IssueCodeResult{}, bizError(CodeInvalidRequest, "tenant, client, redirect URI, PKCE challenge and nonce are required", nil)
	}
	var registeredRedirect string
	err := e.db.QueryRowContext(ctx, `
SELECT redirect_uri FROM clients WHERE id=? AND tenant_id=?`, in.ClientID, in.TenantID).Scan(&registeredRedirect)
	if err == sql.ErrNoRows {
		return IssueCodeResult{}, bizError(CodeInvalidGrant, "client is not registered for tenant", nil)
	}
	if err != nil {
		return IssueCodeResult{}, err
	}
	if registeredRedirect != in.RedirectURI {
		return IssueCodeResult{}, bizError(CodeInvalidGrant, "redirect URI does not exactly match registration", nil)
	}
	now := nowNano(e.cfg)
	expiresAt := now + int64(e.cfg.AuthCodeTTL)
	if e.cfg.Fault == FaultAuthCodeExpired {
		expiresAt = now - 1
	}
	code := e.issuer.authCode(in.TenantID, in.ClientID, in.RedirectURI, in.CodeChallenge, in.Nonce, in.RequestID, fmt.Sprintf("%d", now))
	_, err = e.db.ExecContext(ctx, `
INSERT INTO auth_codes(code_hash, tenant_id, client_id, redirect_uri, code_challenge, nonce, issued_at, expires_at)
VALUES(?,?,?,?,?,?,?,?)`, hashToken(code), in.TenantID, in.ClientID, in.RedirectURI, in.CodeChallenge, in.Nonce, now, expiresAt)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return IssueCodeResult{}, bizError(CodeInvalidRequest, "authorization-code nonce has already been used", err)
		}
		return IssueCodeResult{}, err
	}
	return IssueCodeResult{
		Code: code, TenantID: in.TenantID, ClientID: in.ClientID,
		RedirectURI: in.RedirectURI, Nonce: in.Nonce, ExpiresAt: expiresAt,
	}, nil
}

type authCodeRow struct {
	TenantID    string
	ClientID    string
	RedirectURI string
	Challenge   string
	Nonce       string
	ExpiresAt   int64
	ConsumedAt  int64
}

func loadAuthCodeTx(ctx context.Context, tx *sql.Tx, codeHash string) (authCodeRow, error) {
	var row authCodeRow
	var consumed sql.NullInt64
	err := tx.QueryRowContext(ctx, `
SELECT tenant_id, client_id, redirect_uri, code_challenge, nonce, expires_at, consumed_at
FROM auth_codes WHERE code_hash=?`, codeHash).Scan(
		&row.TenantID, &row.ClientID, &row.RedirectURI, &row.Challenge, &row.Nonce, &row.ExpiresAt, &consumed,
	)
	if err == sql.ErrNoRows {
		return authCodeRow{}, bizError(CodeInvalidGrant, "authorization code is unknown", nil)
	}
	if err != nil {
		return authCodeRow{}, err
	}
	if consumed.Valid {
		row.ConsumedAt = consumed.Int64
	}
	return row, nil
}

func (e *Engine) ExchangeAuthorizationCode(ctx context.Context, in ExchangeInput) (TokenResponse, error) {
	if in.Code == "" || in.ClientID == "" || in.RedirectURI == "" || in.CodeVerifier == "" {
		return TokenResponse{}, bizError(CodeInvalidRequest, "code, client, redirect URI and verifier are required", nil)
	}
	requestID := defaultRequestID(in.RequestID, in.IdempotencyKey, in.Code)
	codeHash := hashToken(in.Code)
	requestHash := requestFingerprint(codeHash, in.ClientID, in.RedirectURI, in.CodeVerifier)
	var tenantID string
	err := e.db.QueryRowContext(ctx, `SELECT tenant_id FROM auth_codes WHERE code_hash=?`, codeHash).Scan(&tenantID)
	if err == sql.ErrNoRows {
		err := bizError(CodeInvalidGrant, "authorization code is unknown", nil)
		_ = e.recordError(ctx, "", requestID, err.Code, err.Message)
		return TokenResponse{}, err
	}
	if err != nil {
		return TokenResponse{}, err
	}
	if err := e.gate.Acquire(ctx, tenantID); err != nil {
		_ = e.recordError(ctx, tenantID, requestID, ErrorCode(err), err.Error())
		return TokenResponse{}, err
	}
	defer e.gate.Release(tenantID)
	if err := e.requireHealthySnapshots(ctx, tenantID); err != nil {
		_ = e.recordError(ctx, tenantID, requestID, ErrorCode(err), err.Error())
		return TokenResponse{}, err
	}

	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return TokenResponse{}, err
	}
	defer tx.Rollback()

	code, err := loadAuthCodeTx(ctx, tx, codeHash)
	if err != nil {
		tx.Rollback()
		_ = e.recordError(ctx, tenantID, requestID, ErrorCode(err), err.Error())
		return TokenResponse{}, err
	}
	if code.TenantID != tenantID {
		tx.Rollback()
		err := bizError(CodeInternal, "authorization code tenant changed during exchange", nil)
		_ = e.recordError(ctx, tenantID, requestID, err.Code, err.Message)
		return TokenResponse{}, err
	}
	if rec, err := loadIdempotency(ctx, tx, code.TenantID, in.ClientID, "exchange", in.IdempotencyKey); err != nil {
		return TokenResponse{}, err
	} else if rec != nil {
		if rec.RequestHash != requestHash {
			tx.Rollback()
			return TokenResponse{}, bizError(CodeIdempotencyConflict, "idempotency key was used with a different exchange request", nil)
		}
		var response TokenResponse
		body, err := openIdempotencyPayload(e.cfg.TokenSecret, rec.ResponsePayload)
		if err != nil {
			return TokenResponse{}, bizError(CodeInternal, "decrypt exchange idempotency response", err)
		}
		if err := json.Unmarshal(body, &response); err != nil {
			return TokenResponse{}, err
		}
		tx.Rollback()
		return response, nil
	}

	now := nowNano(e.cfg)
	if code.ClientID != in.ClientID || code.RedirectURI != in.RedirectURI {
		tx.Rollback()
		err := bizError(CodeInvalidGrant, "client_id or redirect_uri does not exactly match the authorization code", nil)
		_ = e.recordError(ctx, code.TenantID, requestID, err.Code, err.Message)
		return TokenResponse{}, err
	}
	if code.ConsumedAt != 0 {
		tx.Rollback()
		err := bizError(CodeInvalidGrant, "authorization code has already been consumed", nil)
		_ = e.recordError(ctx, code.TenantID, requestID, err.Code, err.Message)
		return TokenResponse{}, err
	}
	if now >= code.ExpiresAt {
		tx.Rollback()
		err := bizError(CodeInvalidGrant, "authorization code has expired", nil)
		_ = e.recordError(ctx, code.TenantID, requestID, err.Code, err.Message)
		return TokenResponse{}, err
	}
	expectedChallenge := s256Verifier(in.CodeVerifier)
	if e.cfg.Fault == FaultPKCEMismatch || subtle.ConstantTimeCompare([]byte(expectedChallenge), []byte(code.Challenge)) != 1 {
		tx.Rollback()
		err := bizError(CodePKCEMismatch, "PKCE S256 verifier does not match code_challenge", nil)
		_ = e.recordError(ctx, code.TenantID, requestID, err.Code, err.Message)
		return TokenResponse{}, err
	}

	result, err := tx.ExecContext(ctx, `
UPDATE auth_codes SET consumed_at=? WHERE code_hash=? AND consumed_at IS NULL`, now, codeHash)
	if err != nil {
		return TokenResponse{}, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return TokenResponse{}, err
	}
	if affected != 1 {
		tx.Rollback()
		err := bizError(CodeInvalidGrant, "authorization code consumption lost a concurrency race", nil)
		_ = e.recordError(ctx, code.TenantID, requestID, err.Code, err.Message)
		return TokenResponse{}, err
	}

	tenant, err := loadTenantTx(ctx, tx, code.TenantID)
	if err != nil {
		return TokenResponse{}, err
	}
	familyID := e.issuer.familyID(code.TenantID, code.ClientID, codeHash, fmt.Sprintf("%d", now))
	accessRaw := e.issuer.accessToken(familyID, "1", codeHash, fmt.Sprintf("%d", now))
	refreshRaw := e.issuer.refreshToken(familyID, "1", codeHash, fmt.Sprintf("%d", now))
	accessHash := hashToken(accessRaw)
	refreshHash := hashToken(refreshRaw)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO token_families(id, tenant_id, client_id, created_at) VALUES(?,?,?,?)`,
		familyID, code.TenantID, code.ClientID, now); err != nil {
		return TokenResponse{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO refresh_tokens(token_hash, family_id, sequence, tenant_id, client_id, issued_at, expires_at)
VALUES(?,?,?,?,?,?,?)`, refreshHash, familyID, 1, code.TenantID, code.ClientID, now, now+int64(e.cfg.RefreshTokenTTL)); err != nil {
		return TokenResponse{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO access_tokens(token_hash, family_id, sequence, tenant_id, client_id, issued_at, expires_at)
VALUES(?,?,?,?,?,?,?)`, accessHash, familyID, 1, code.TenantID, code.ClientID, now, now+int64(e.cfg.AccessTokenTTL)); err != nil {
		return TokenResponse{}, err
	}
	snapshotVersion, err := appendSnapshot(ctx, e, tx, code.TenantID, "token_issued")
	if err != nil {
		return TokenResponse{}, err
	}
	response := TokenResponse{
		TenantID: code.TenantID, ClientID: code.ClientID,
		AccessToken: accessRaw, RefreshToken: refreshRaw, TokenType: "Bearer",
		ExpiresInSeconds: int64(e.cfg.AccessTokenTTL / time.Second),
		RefreshExpiresIn: int64(e.cfg.RefreshTokenTTL / time.Second),
		FamilyID:         familyID, LedgerVersion: tenant.LedgerVersion, SnapshotVersion: snapshotVersion,
	}
	if err := saveIdempotency(ctx, tx, e.cfg.TokenSecret, code.TenantID, code.ClientID, "exchange", in.IdempotencyKey, requestHash, response,
		response.LedgerVersion, response.SnapshotVersion, now); err != nil {
		return TokenResponse{}, err
	}
	if e.cfg.Hooks.BeforeCommit != nil {
		if err := e.cfg.Hooks.BeforeCommit(ctx, code.TenantID, requestID); err != nil {
			return TokenResponse{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return TokenResponse{}, err
	}
	if e.cfg.Hooks.CrashPoint != nil {
		e.cfg.Hooks.CrashPoint(ctx, "after_commit")
	}
	return response, nil
}

type refreshTokenRow struct {
	TokenHash       string
	FamilyID        string
	Sequence        int64
	TenantID        string
	ClientID        string
	ExpiresAt       int64
	RevokedAt       int64
	RevokeReason    string
	FamilyRevokedAt int64
}

func loadRefreshTokenTx(ctx context.Context, tx *sql.Tx, tokenHash string) (refreshTokenRow, error) {
	var row refreshTokenRow
	err := tx.QueryRowContext(ctx, `
SELECT r.token_hash, r.family_id, r.sequence, r.tenant_id, r.client_id, r.expires_at,
       r.revoked_at, r.revoke_reason, f.revoked_at
FROM refresh_tokens r JOIN token_families f ON f.id=r.family_id
WHERE r.token_hash=?`, tokenHash).Scan(
		&row.TokenHash, &row.FamilyID, &row.Sequence, &row.TenantID, &row.ClientID,
		&row.ExpiresAt, &row.RevokedAt, &row.RevokeReason, &row.FamilyRevokedAt,
	)
	if err == sql.ErrNoRows {
		return refreshTokenRow{}, bizError(CodeInvalidGrant, "refresh token is unknown", nil)
	}
	return row, err
}

func (e *Engine) findRefreshTenant(ctx context.Context, tokenHash string) (string, error) {
	var tenantID string
	err := e.db.QueryRowContext(ctx, `SELECT tenant_id FROM refresh_tokens WHERE token_hash=?`, tokenHash).Scan(&tenantID)
	if err == sql.ErrNoRows {
		return "", bizError(CodeInvalidGrant, "refresh token is unknown", nil)
	}
	return tenantID, err
}

func (e *Engine) Refresh(ctx context.Context, in RefreshInput) (TokenResponse, error) {
	if in.RefreshToken == "" || in.ClientID == "" {
		return TokenResponse{}, bizError(CodeInvalidRequest, "refresh token and client_id are required", nil)
	}
	tenantID, err := e.findRefreshTenant(ctx, hashToken(in.RefreshToken))
	if err != nil {
		requestID := defaultRequestID(in.RequestID, in.IdempotencyKey, in.RefreshToken)
		_ = e.recordError(ctx, "", requestID, ErrorCode(err), err.Error())
		return TokenResponse{}, err
	}
	requestID := defaultRequestID(in.RequestID, in.IdempotencyKey, in.RefreshToken)
	if err := e.gate.Acquire(ctx, tenantID); err != nil {
		_ = e.recordError(ctx, tenantID, requestID, ErrorCode(err), err.Error())
		return TokenResponse{}, err
	}
	defer e.gate.Release(tenantID)

	if err := e.requireHealthySnapshots(ctx, tenantID); err != nil {
		_ = e.recordError(ctx, tenantID, requestID, ErrorCode(err), err.Error())
		return TokenResponse{}, err
	}
	if e.cfg.Hooks.BeforeBilling != nil {
		if err := e.cfg.Hooks.BeforeBilling(ctx, tenantID, requestID); err != nil {
			_ = e.recordError(ctx, tenantID, requestID, CodeInternal, err.Error())
			return TokenResponse{}, bizError(CodeInternal, "billing hook failed", err)
		}
	}
	requestHash := requestFingerprint(in.RefreshToken, in.ClientID, fmt.Sprintf("%d", in.RequestCount))
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return TokenResponse{}, err
	}
	defer tx.Rollback()

	if rec, err := loadIdempotency(ctx, tx, tenantID, in.ClientID, "refresh", in.IdempotencyKey); err != nil {
		return TokenResponse{}, err
	} else if rec != nil {
		if rec.RequestHash != requestHash {
			tx.Rollback()
			return TokenResponse{}, bizError(CodeIdempotencyConflict, "idempotency key was used with a different refresh request", nil)
		}
		var response TokenResponse
		body, err := openIdempotencyPayload(e.cfg.TokenSecret, rec.ResponsePayload)
		if err != nil {
			return TokenResponse{}, bizError(CodeInternal, "decrypt refresh idempotency response", err)
		}
		if err := json.Unmarshal(body, &response); err != nil {
			return TokenResponse{}, err
		}
		tx.Rollback()
		return response, nil
	}

	oldToken, err := loadRefreshTokenTx(ctx, tx, hashToken(in.RefreshToken))
	if err != nil {
		tx.Rollback()
		_ = e.recordError(ctx, tenantID, requestID, ErrorCode(err), err.Error())
		return TokenResponse{}, err
	}
	if oldToken.ClientID != in.ClientID {
		tx.Rollback()
		err := bizError(CodeInvalidGrant, "refresh token does not belong to client_id", nil)
		_ = e.recordError(ctx, tenantID, requestID, err.Code, err.Message)
		return TokenResponse{}, err
	}
	tenant, err := loadTenantTx(ctx, tx, tenantID)
	if err != nil {
		return TokenResponse{}, err
	}

	now := nowNano(e.cfg)
	if oldToken.RevokedAt != 0 || oldToken.FamilyRevokedAt != 0 {
		if err := revokeFamilyTx(ctx, tx, tenantID, oldToken.FamilyID, now, "token_reuse_detected"); err != nil {
			return TokenResponse{}, err
		}
		snapshotVersion, err := appendSnapshot(ctx, e, tx, tenantID, "token_reuse_revocation")
		if err != nil {
			return TokenResponse{}, err
		}
		detail := fmt.Sprintf("replayed refresh token family=%s sequence=%d ledger_version_unchanged", oldToken.FamilyID, oldToken.Sequence)
		if err := insertErrorEventTx(ctx, tx, tenantID, requestID, CodeTokenReuse, detail,
			eventVersions{ledger: tenant.LedgerVersion, snapshot: snapshotVersion}, now); err != nil {
			return TokenResponse{}, err
		}
		if err := tx.Commit(); err != nil {
			return TokenResponse{}, err
		}
		return TokenResponse{}, bizError(CodeTokenReuse, "refresh token reuse detected; token family revoked", nil)
	}
	if now >= oldToken.ExpiresAt {
		tx.Rollback()
		err := bizError(CodeInvalidGrant, "refresh token has expired", nil)
		_ = e.recordError(ctx, tenantID, requestID, err.Code, err.Message)
		return TokenResponse{}, err
	}

	requests := in.RequestCount
	if requests == 0 {
		requests = 1
	}
	unitCost := tenant.UnitCost
	rateNum, rateDen := tenant.RateNum, tenant.RateDen
	switch e.cfg.Fault {
	case FaultPrecisionOverflow:
		requests = math.MaxInt64
		unitCost = Fixed(math.MaxInt64)
	case FaultQuotaExhausted:
		// Fault injection overrides only the effective in-memory quote. Normal
		// billing always uses the tenant's persisted integer pricing rule.
		requests = 1
		rateNum, rateDen = 1, 1
		if tenant.Quota < Fixed(math.MaxInt64) {
			unitCost = tenant.Quota + 1
		} else {
			unitCost = Fixed(math.MaxInt64)
		}
	}
	quote, err := ComputeCharge(requests, unitCost, rateNum, rateDen)
	if err != nil {
		tx.Rollback()
		_ = e.recordError(ctx, tenantID, requestID, ErrorCode(err), err.Error())
		return TokenResponse{}, err
	}
	if quote.Charged > tenant.Quota {
		snapshotVersion, err := appendSnapshot(ctx, e, tx, tenantID, "quota_exhausted")
		if err != nil {
			return TokenResponse{}, err
		}
		if err := insertErrorEventTx(ctx, tx, tenantID, requestID, CodeQuotaExhausted,
			fmt.Sprintf("quota=%d charged=%d", tenant.Quota, quote.Charged),
			eventVersions{ledger: tenant.LedgerVersion, snapshot: snapshotVersion}, now); err != nil {
			return TokenResponse{}, err
		}
		if err := tx.Commit(); err != nil {
			return TokenResponse{}, err
		}
		return TokenResponse{}, bizError(CodeQuotaExhausted, "tenant quota is insufficient for this refresh", nil)
	}

	newSequence := oldToken.Sequence + 1
	refreshRaw := e.issuer.refreshToken(oldToken.FamilyID, fmt.Sprintf("%d", newSequence), oldToken.TokenHash, fmt.Sprintf("%d", now))
	accessRaw := e.issuer.accessToken(oldToken.FamilyID, fmt.Sprintf("%d", newSequence), oldToken.TokenHash, fmt.Sprintf("%d", now))
	refreshHash := hashToken(refreshRaw)
	accessHash := hashToken(accessRaw)
	if _, err := tx.ExecContext(ctx, `
UPDATE refresh_tokens SET revoked_at=?, revoke_reason='rotated', replaced_by_hash=? WHERE token_hash=?`,
		now, refreshHash, oldToken.TokenHash); err != nil {
		return TokenResponse{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO refresh_tokens(token_hash, family_id, sequence, tenant_id, client_id, issued_at, expires_at)
VALUES(?,?,?,?,?,?,?)`, refreshHash, oldToken.FamilyID, newSequence, tenantID, in.ClientID, now, now+int64(e.cfg.RefreshTokenTTL)); err != nil {
		return TokenResponse{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO access_tokens(token_hash, family_id, sequence, tenant_id, client_id, issued_at, expires_at)
VALUES(?,?,?,?,?,?,?)`, accessHash, oldToken.FamilyID, newSequence, tenantID, in.ClientID, now, now+int64(e.cfg.AccessTokenTTL)); err != nil {
		return TokenResponse{}, err
	}
	remaining := tenant.Quota - quote.Charged
	newLedgerVersion := tenant.LedgerVersion + 1
	if _, err := tx.ExecContext(ctx, `
UPDATE tenants SET quota_remaining=?, ledger_version=? WHERE id=?`, remaining, newLedgerVersion, tenantID); err != nil {
		return TokenResponse{}, err
	}
	snapshotKind := "token_rotation"
	if remaining == 0 {
		snapshotKind = "quota_floor"
	}
	snapshotVersion, err := appendSnapshot(ctx, e, tx, tenantID, snapshotKind)
	if err != nil {
		return TokenResponse{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO billing_entries(tenant_id, ledger_version, request_id, idempotency_key, requests, unit_cost, rate_num, rate_den,
 charged, trunc_num, trunc_den, remaining_after, snapshot_version, created_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		tenantID, newLedgerVersion, requestID, in.IdempotencyKey, quote.Requests, int64(quote.UnitCost), quote.RateNum, quote.RateDen,
		int64(quote.Charged), quote.TruncNum, quote.TruncDen, int64(remaining), snapshotVersion, now); err != nil {
		return TokenResponse{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO truncation_entries(tenant_id, ledger_version, snapshot_version, charged_nano, trunc_num, trunc_den, created_at)
VALUES(?,?,?,?,?,?,?)`, tenantID, newLedgerVersion, snapshotVersion, int64(quote.Charged), quote.TruncNum, quote.TruncDen, now); err != nil {
		return TokenResponse{}, err
	}
	response := TokenResponse{
		TenantID: tenantID, ClientID: in.ClientID,
		AccessToken: accessRaw, RefreshToken: refreshRaw, TokenType: "Bearer",
		ExpiresInSeconds: int64(e.cfg.AccessTokenTTL / time.Second),
		RefreshExpiresIn: int64(e.cfg.RefreshTokenTTL / time.Second),
		FamilyID:         oldToken.FamilyID, LedgerVersion: newLedgerVersion, SnapshotVersion: snapshotVersion,
	}
	if err := saveIdempotency(ctx, tx, e.cfg.TokenSecret, tenantID, in.ClientID, "refresh", in.IdempotencyKey, requestHash, response,
		newLedgerVersion, snapshotVersion, now); err != nil {
		return TokenResponse{}, err
	}
	if e.cfg.Hooks.BeforeCommit != nil {
		if err := e.cfg.Hooks.BeforeCommit(ctx, tenantID, requestID); err != nil {
			tx.Rollback()
			_ = e.recordError(ctx, tenantID, requestID, CodeInternal, err.Error())
			return TokenResponse{}, bizError(CodeInternal, "commit hook failed", err)
		}
	}
	if e.cfg.Hooks.CrashPoint != nil {
		e.cfg.Hooks.CrashPoint(ctx, "mid_transaction")
	}
	if err := tx.Commit(); err != nil {
		_ = e.recordError(ctx, tenantID, requestID, CodeInternal, err.Error())
		return TokenResponse{}, bizError(CodeInternal, "commit refresh transaction", err)
	}
	if e.cfg.Hooks.CrashPoint != nil {
		e.cfg.Hooks.CrashPoint(ctx, "after_commit")
	}
	return response, nil
}

func revokeFamilyTx(ctx context.Context, tx *sql.Tx, tenantID, familyID string, now int64, reason string) error {
	if _, err := tx.ExecContext(ctx, `
UPDATE token_families SET revoked_at=CASE WHEN revoked_at=0 THEN ? ELSE revoked_at END,
 revoke_reason=CASE WHEN revoke_reason='' THEN ? ELSE revoke_reason END WHERE id=? AND tenant_id=?`,
		now, reason, familyID, tenantID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE refresh_tokens SET revoked_at=CASE WHEN revoked_at=0 THEN ? ELSE revoked_at END,
 revoke_reason=CASE WHEN revoke_reason='' THEN ? ELSE revoke_reason END WHERE family_id=? AND tenant_id=?`,
		now, reason, familyID, tenantID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
UPDATE access_tokens SET revoked_at=CASE WHEN revoked_at=0 THEN ? ELSE revoked_at END,
 revoke_reason=CASE WHEN revoke_reason='' THEN ? ELSE revoke_reason END WHERE family_id=? AND tenant_id=?`,
		now, reason, familyID, tenantID)
	return err
}

func (e *Engine) Revoke(ctx context.Context, in RevokeInput) (RevokeResult, error) {
	if in.TenantID == "" || in.Token == "" {
		return RevokeResult{}, bizError(CodeInvalidRequest, "tenant id and token are required", nil)
	}
	if err := e.gate.Acquire(ctx, in.TenantID); err != nil {
		_ = e.recordError(ctx, in.TenantID, defaultRequestID(in.RequestID, "", in.Token), ErrorCode(err), err.Error())
		return RevokeResult{}, err
	}
	defer e.gate.Release(in.TenantID)
	if err := e.requireHealthySnapshots(ctx, in.TenantID); err != nil {
		return RevokeResult{}, err
	}
	tokenHash := hashToken(in.Token)
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return RevokeResult{}, err
	}
	defer tx.Rollback()
	now := nowNano(e.cfg)
	var familyID string
	err = tx.QueryRowContext(ctx, `SELECT family_id FROM refresh_tokens WHERE token_hash=? AND tenant_id=?`, tokenHash, in.TenantID).Scan(&familyID)
	if err == nil {
		if err := revokeFamilyTx(ctx, tx, in.TenantID, familyID, now, "revoked_by_client"); err != nil {
			return RevokeResult{}, err
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		result, err := tx.ExecContext(ctx, `
UPDATE access_tokens SET revoked_at=?, revoke_reason='revoked_by_client' WHERE token_hash=? AND tenant_id=? AND revoked_at=0`,
			now, tokenHash, in.TenantID)
		if err != nil {
			return RevokeResult{}, err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return RevokeResult{}, err
		}
		if affected == 0 {
			tx.Rollback()
			return RevokeResult{}, bizError(CodeNotFound, "token not found or already revoked", nil)
		}
	} else {
		return RevokeResult{}, err
	}
	snapshotVersion, err := appendSnapshot(ctx, e, tx, in.TenantID, "token_revoked")
	if err != nil {
		return RevokeResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return RevokeResult{}, err
	}
	return RevokeResult{TenantID: in.TenantID, RevokedFamily: familyID, RevokedTokenHash: tokenHash, SnapshotVersion: snapshotVersion}, nil
}

func (e *Engine) Authenticate(ctx context.Context, accessToken string) (Principal, error) {
	if accessToken == "" {
		return Principal{}, bizError(CodeInvalidGrant, "bearer token is missing", nil)
	}
	tokenHash := hashToken(accessToken)
	var principal Principal
	var revokedAt, familyRevokedAt int64
	err := e.db.QueryRowContext(ctx, `
SELECT a.tenant_id, a.client_id, a.family_id, a.token_hash, a.expires_at, a.revoked_at, f.revoked_at
FROM access_tokens a JOIN token_families f ON f.id=a.family_id WHERE a.token_hash=?`, tokenHash).Scan(
		&principal.TenantID, &principal.ClientID, &principal.FamilyID, &principal.TokenHash,
		&principal.ExpiresAt, &revokedAt, &familyRevokedAt,
	)
	if err == sql.ErrNoRows {
		return Principal{}, bizError(CodeInvalidGrant, "access token is unknown", nil)
	}
	if err != nil {
		return Principal{}, err
	}
	if err := e.requireHealthySnapshots(ctx, principal.TenantID); err != nil {
		return Principal{}, err
	}
	if revokedAt != 0 || familyRevokedAt != 0 || nowNano(e.cfg) >= principal.ExpiresAt {
		return Principal{}, bizError(CodeInvalidGrant, "access token is revoked or expired", nil)
	}
	return principal, nil
}

func (e *Engine) Status(ctx context.Context, tenantID string) (SnapshotView, error) {
	return validateSnapshots(ctx, e.db, e.cfg, tenantID)
}

func (e *Engine) Ledger(ctx context.Context, tenantID string) (LedgerExport, error) {
	view, viewErr := validateSnapshots(ctx, e.db, e.cfg, tenantID)
	if viewErr != nil && e.cfg.RecoveryStrategy == RecoveryStrict {
		return LedgerExport{}, viewErr
	}
	if viewErr != nil && ErrorCode(viewErr) != CodeSnapshotVersion {
		return LedgerExport{}, viewErr
	}
	query := `
SELECT id, tenant_id, ledger_version, request_id, idempotency_key, requests, unit_cost, rate_num, rate_den,
       charged, trunc_num, trunc_den, remaining_after, snapshot_version, created_at
FROM billing_entries WHERE tenant_id=?`
	args := []any{tenantID}
	if viewErr != nil {
		query += ` AND snapshot_version<=?`
		args = append(args, view.Version)
	}
	query += ` ORDER BY ledger_version`
	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return LedgerExport{}, err
	}
	defer rows.Close()
	entries := make([]BillingEntry, 0)
	for rows.Next() {
		var entry BillingEntry
		if err := rows.Scan(&entry.ID, &entry.TenantID, &entry.LedgerVersion, &entry.RequestID, &entry.IdempotencyKey,
			&entry.Requests, &entry.UnitCostNano, &entry.RateNumerator, &entry.RateDenominator,
			&entry.ChargedNano, &entry.TruncationNum, &entry.TruncationDen, &entry.RemainingAfterNano,
			&entry.SnapshotVersion, &entry.CreatedAt); err != nil {
			return LedgerExport{}, err
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return LedgerExport{}, err
	}
	truncQuery := `SELECT charged_nano, trunc_num, trunc_den FROM truncation_entries WHERE tenant_id=?`
	truncArgs := []any{tenantID}
	if viewErr != nil {
		truncQuery += ` AND snapshot_version<=?`
		truncArgs = append(truncArgs, view.Version)
	}
	truncQuery += ` ORDER BY ledger_version`
	truncResult, err := e.db.QueryContext(ctx, truncQuery, truncArgs...)
	if err != nil {
		return LedgerExport{}, err
	}
	truncRows := make([]truncationRow, 0)
	for truncResult.Next() {
		var row truncationRow
		if err := truncResult.Scan(&row.charged, &row.truncNum, &row.truncDen); err != nil {
			truncResult.Close()
			return LedgerExport{}, err
		}
		truncRows = append(truncRows, row)
	}
	if err := truncResult.Close(); err != nil {
		return LedgerExport{}, err
	}
	if err := truncResult.Err(); err != nil {
		return LedgerExport{}, err
	}
	report, err := aggregateTruncation(truncRows)
	if err != nil {
		return LedgerExport{}, err
	}
	export := LedgerExport{
		SchemaVersion:   1,
		TenantID:        tenantID,
		ReadOnly:        viewErr != nil,
		LedgerVersion:   view.State.LedgerVersion,
		SnapshotVersion: view.Version,
		Entries:         entries,
		Truncation:      report,
		Session:         view.State,
	}
	return export, nil
}

type PlatformExport struct {
	SchemaVersion int            `json:"schema_version"`
	Recovery      string         `json:"recovery_strategy"`
	Tenants       []LedgerExport `json:"tenants"`
}

func (e *Engine) ExportAll(ctx context.Context) (PlatformExport, error) {
	rows, err := e.db.QueryContext(ctx, `SELECT id FROM tenants ORDER BY id`)
	if err != nil {
		return PlatformExport{}, err
	}
	var tenantIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return PlatformExport{}, err
		}
		tenantIDs = append(tenantIDs, id)
	}
	if err := rows.Close(); err != nil {
		return PlatformExport{}, err
	}
	if err := rows.Err(); err != nil {
		return PlatformExport{}, err
	}
	exports := make([]LedgerExport, 0, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		export, err := e.Ledger(ctx, tenantID)
		if err != nil {
			return PlatformExport{}, err
		}
		exports = append(exports, export)
	}
	return PlatformExport{SchemaVersion: 1, Recovery: string(e.cfg.RecoveryStrategy), Tenants: exports}, nil
}

func (e *Engine) Errors(ctx context.Context, tenantID string) ([]ErrorEvent, error) {
	rows, err := e.db.QueryContext(ctx, `
SELECT id, tenant_id, request_id, code, detail, ledger_version, snapshot_version, created_at
FROM error_events WHERE tenant_id=? ORDER BY id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]ErrorEvent, 0)
	for rows.Next() {
		var event ErrorEvent
		if err := rows.Scan(&event.ID, &event.TenantID, &event.RequestID, &event.Code, &event.Detail,
			&event.LedgerVersion, &event.SnapshotVersion, &event.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (e *Engine) recordError(ctx context.Context, tenantID, requestID, code, detail string) error {
	var ledgerVersion, snapshotVersion int64
	if tenantID != "" {
		err := e.db.QueryRowContext(ctx, `SELECT ledger_version, snapshot_version FROM tenants WHERE id=?`, tenantID).Scan(&ledgerVersion, &snapshotVersion)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	_, err := e.db.ExecContext(ctx, `
INSERT INTO error_events(tenant_id, request_id, code, detail, ledger_version, snapshot_version, created_at)
VALUES(?,?,?,?,?,?,?)`, tenantID, requestID, code, detail, ledgerVersion, snapshotVersion, nowNano(e.cfg))
	return err
}

type eventVersions struct {
	ledger   int64
	snapshot int64
}

func insertErrorEventTx(ctx context.Context, tx *sql.Tx, tenantID, requestID, code, detail string, versions eventVersions, now int64) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO error_events(tenant_id, request_id, code, detail, ledger_version, snapshot_version, created_at)
VALUES(?,?,?,?,?,?,?)`, tenantID, requestID, code, detail, versions.ledger, versions.snapshot, now)
	return err
}

func defaultRequestID(requestID, idempotencyKey, secretMaterial string) string {
	if requestID != "" {
		return requestID
	}
	if idempotencyKey != "" {
		return "req_" + idempotencyKey
	}
	return "req_" + requestFingerprint(secretMaterial)[:24]
}

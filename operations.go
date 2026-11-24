package tokenforge

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type IssueRequest struct {
	TenantID string `json:"tenant_id"`
	UserID   string `json:"user_id"`
}

type IssueResult struct {
	Code            Code      `json:"code"`
	TenantID        string    `json:"tenant_id"`
	UserID          string    `json:"user_id"`
	FamilyID        string    `json:"family_id"`
	Pair            TokenPair `json:"pair"`
	SnapshotVersion int64     `json:"snapshot_version"`
	QueueNanos      int64     `json:"queue_nanos"`
	InFlight        int64     `json:"in_flight"`
}

type writeLease struct {
	release    func()
	queueNanos int64
	inFlight   int64
}

func (s *Service) beginWrite(ctx context.Context, tenantID string) (writeLease, error) {
	start := time.Now()
	releaseAdmission, inFlight, err := s.admission.acquire(ctx)
	if err != nil {
		waited := time.Since(start)
		s.metrics.observeQueue(waited)
		return writeLease{queueNanos: waited.Nanoseconds(), inFlight: 0}, err
	}
	deadline := start.Add(s.cfg.AdmissionWait)
	releaseTenant, _, err := s.tenantGate.acquire(ctx, tenantID, deadline)
	if err != nil {
		releaseAdmission()
		waited := time.Since(start)
		s.metrics.observeQueue(waited)
		if CodeOf(err) == CodeQuotaContention {
			s.metrics.contended.Add(1)
		}
		return writeLease{queueNanos: waited.Nanoseconds(), inFlight: inFlight}, err
	}
	releaseWriter, _, err := s.sqliteGate.acquire(ctx, "sqlite-writer", deadline)
	if err != nil {
		releaseTenant()
		releaseAdmission()
		waited := time.Since(start)
		s.metrics.observeQueue(waited)
		if CodeOf(err) == CodeQuotaContention {
			s.metrics.contended.Add(1)
		}
		return writeLease{queueNanos: waited.Nanoseconds(), inFlight: inFlight}, err
	}
	queue := time.Since(start)
	s.metrics.observeQueue(queue)
	return writeLease{
		release: func() {
			releaseWriter()
			releaseTenant()
			releaseAdmission()
		},
		queueNanos: queue.Nanoseconds(),
		inFlight:   inFlight,
	}, nil
}

func (s *Service) commitHook(ctx context.Context, info CommitInfo) error {
	if s.cfg.BeforeCommit == nil {
		return nil
	}
	if err := s.cfg.BeforeCommit(ctx, info); err != nil {
		return domainError(CodeAborted, "before-commit hook aborted transaction", err)
	}
	return nil
}

// Issue creates a new family at sequence 1 and a signed access/refresh pair.
func (s *Service) Issue(ctx context.Context, req IssueRequest) (IssueResult, error) {
	req.TenantID = strings.TrimSpace(req.TenantID)
	req.UserID = strings.TrimSpace(req.UserID)
	if req.TenantID == "" {
		return IssueResult{Code: CodeInvalidArgument}, codeError(CodeInvalidArgument, "tenant_id is required")
	}
	lease, err := s.beginWrite(ctx, req.TenantID)
	if err != nil {
		return IssueResult{Code: CodeOf(err), QueueNanos: lease.queueNanos, InFlight: lease.inFlight}, err
	}
	defer lease.release()

	now := s.now().UnixNano()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return IssueResult{Code: CodeInternal}, fmt.Errorf("begin issue transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	tenant, err := s.loadTenant(ctx, tx, req.TenantID)
	if err != nil {
		return IssueResult{Code: CodeOf(err), QueueNanos: lease.queueNanos, InFlight: lease.inFlight}, err
	}
	if req.UserID == "" {
		req.UserID = tenant.UserID
	}
	current, next, err := s.prepareSnapshotVersion(ctx, tx, req.TenantID)
	if err != nil {
		return IssueResult{Code: CodeOf(err)}, err
	}
	familyID, err := s.cfg.IDs.NewFamilyID(req.TenantID, req.UserID)
	if err != nil || strings.TrimSpace(familyID) == "" {
		return IssueResult{Code: CodeInternal}, domainError(CodeInternal, "generate family id", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO families
		(tenant_id, family_id, user_id, current_sequence, status, revoke_reason, created_at, updated_at)
		VALUES (?, ?, ?, 1, ?, '', ?, ?)`, req.TenantID, familyID, req.UserID, familyStatusActive, now, now); err != nil {
		return IssueResult{Code: CodeInternal}, fmt.Errorf("insert family: %w", err)
	}
	pair, err := s.mintPair(ctx, tx, req.TenantID, req.UserID, familyID, 1, now)
	if err != nil {
		return IssueResult{Code: CodeOf(err)}, err
	}
	_, err = s.writeSnapshot(ctx, tx, req.TenantID, current, next, "issue", now)
	if err != nil {
		return IssueResult{Code: CodeOf(err)}, err
	}
	if err := s.commitHook(ctx, CommitInfo{Operation: "issue", TenantID: req.TenantID, FamilyID: familyID, Version: next}); err != nil {
		return IssueResult{Code: CodeOf(err)}, err
	}
	if err := tx.Commit(); err != nil {
		return IssueResult{Code: CodeInternal}, fmt.Errorf("commit issue: %w", err)
	}
	return IssueResult{
		Code: CodeOK, TenantID: req.TenantID, UserID: req.UserID, FamilyID: familyID,
		Pair: pair, SnapshotVersion: next, QueueNanos: lease.queueNanos, InFlight: lease.inFlight,
	}, nil
}

func (s *Service) mintPair(ctx context.Context, q familyQuerier, tenantID, userID, familyID string, sequence, now int64) (TokenPair, error) {
	access, err := s.mintToken(ctx, q, tenantID, userID, familyID, sequence, TokenKindAccess, now)
	if err != nil {
		return TokenPair{}, err
	}
	refresh, err := s.mintToken(ctx, q, tenantID, userID, familyID, sequence, TokenKindRefresh, now)
	if err != nil {
		return TokenPair{}, err
	}
	return TokenPair{FamilyID: familyID, Access: access, Refresh: refresh}, nil
}

func (s *Service) mintToken(ctx context.Context, q familyQuerier, tenantID, userID, familyID string, sequence int64, kind TokenKind, now int64) (IssuedToken, error) {
	tokenID, err := s.cfg.IDs.NewTokenID(tenantID, familyID, sequence, kind)
	if err != nil || strings.TrimSpace(tokenID) == "" {
		return IssuedToken{}, domainError(CodeInternal, "generate token id", err)
	}
	ttl := s.cfg.AccessTTL
	if kind == TokenKindRefresh {
		ttl = s.cfg.RefreshTTL
	}
	claims := Claims{
		Version: tokenFormatVersion, TokenID: tokenID,
		TenantID: tenantID, UserID: userID, FamilyID: familyID, Sequence: sequence,
		Kind: kind, IssuedAt: now, ExpiresAt: now + ttl.Nanoseconds(),
	}
	token, err := s.signClaims(claims)
	if err != nil {
		return IssuedToken{}, fmt.Errorf("sign token: %w", err)
	}
	if _, err := q.ExecContext(ctx, `INSERT INTO tokens
		(token_id, token_hash, tenant_id, user_id, family_id, sequence, kind,
		 issued_at, expires_at, status, consumed_at, revoked_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)`,
		claims.TokenID, token.Digest, tenantID, userID, familyID, sequence, string(kind),
		claims.IssuedAt, claims.ExpiresAt, tokenStatusActive); err != nil {
		return IssuedToken{}, fmt.Errorf("insert token: %w", err)
	}
	return token, nil
}

type tokenRecord struct {
	TokenID    string
	Digest     string
	TenantID   string
	UserID     string
	FamilyID   string
	Sequence   int64
	Kind       TokenKind
	IssuedAt   int64
	ExpiresAt  int64
	Status     string
	ConsumedAt int64
	RevokedAt  int64
}

func loadTokenByID(ctx context.Context, q queryRower, tokenID string) (tokenRecord, error) {
	var t tokenRecord
	var kind string
	err := q.QueryRowContext(ctx, `SELECT token_id, token_hash, tenant_id, user_id, family_id,
		sequence, kind, issued_at, expires_at, status, consumed_at, revoked_at
		FROM tokens WHERE token_id = ?`, tokenID).Scan(
		&t.TokenID, &t.Digest, &t.TenantID, &t.UserID, &t.FamilyID, &t.Sequence,
		&kind, &t.IssuedAt, &t.ExpiresAt, &t.Status, &t.ConsumedAt, &t.RevokedAt)
	if err == sql.ErrNoRows {
		return t, codeError(CodeTokenInvalid, "token is not registered")
	}
	if err != nil {
		return t, fmt.Errorf("load token: %w", err)
	}
	t.Kind = TokenKind(kind)
	return t, nil
}

func validateTokenRecord(claims Claims, raw string, record tokenRecord) error {
	if record.Digest != tokenDigest(raw) ||
		record.TenantID != claims.TenantID ||
		record.UserID != claims.UserID ||
		record.FamilyID != claims.FamilyID ||
		record.Sequence != claims.Sequence ||
		record.Kind != claims.Kind {
		return codeError(CodeTokenInvalid, "token record does not match signed claims")
	}
	return nil
}

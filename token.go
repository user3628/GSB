package tokenforge

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
)

const (
	tokenFormatVersion = 1
	tokenPrefix        = "tf1"
)

type TokenKind string

const (
	TokenKindAccess  TokenKind = "access"
	TokenKindRefresh TokenKind = "refresh"
)

// Claims is the signed token payload. Timestamps are Unix nanoseconds to keep
// the demo deterministic across Windows time zones.
type Claims struct {
	Version   int       `json:"v"`
	TokenID   string    `json:"jti"`
	TenantID  string    `json:"tid"`
	UserID    string    `json:"uid"`
	FamilyID  string    `json:"fid"`
	Sequence  int64     `json:"seq"`
	Kind      TokenKind `json:"kind"`
	IssuedAt  int64     `json:"iat"`
	ExpiresAt int64     `json:"exp"`
}

// IssuedToken is the host-facing representation of a signed token. The raw
// token is returned only at issuance/rotation time.
type IssuedToken struct {
	Value  string `json:"value"`
	Claims Claims `json:"claims"`
	Digest string `json:"digest"`
}

func (t IssuedToken) Summary() TokenSummary { return summarizeIssuedToken(t) }

type TokenPair struct {
	FamilyID string      `json:"family_id"`
	Access   IssuedToken `json:"access"`
	Refresh  IssuedToken `json:"refresh"`
}

type TokenSummary struct {
	TokenID   string    `json:"token_id"`
	TenantID  string    `json:"tenant_id"`
	UserID    string    `json:"user_id"`
	FamilyID  string    `json:"family_id"`
	Sequence  int64     `json:"sequence"`
	Kind      TokenKind `json:"kind"`
	IssuedAt  int64     `json:"issued_at_unix_nano"`
	ExpiresAt int64     `json:"expires_at_unix_nano"`
	Digest    string    `json:"digest"`
}

func summarizeIssuedToken(t IssuedToken) TokenSummary {
	return TokenSummary{
		TokenID:   t.Claims.TokenID,
		TenantID:  t.Claims.TenantID,
		UserID:    t.Claims.UserID,
		FamilyID:  t.Claims.FamilyID,
		Sequence:  t.Claims.Sequence,
		Kind:      t.Claims.Kind,
		IssuedAt:  t.Claims.IssuedAt,
		ExpiresAt: t.Claims.ExpiresAt,
		Digest:    t.Digest,
	}
}

// IDSource lets a host (and the deterministic sample runner) provide IDs. The
// default source uses crypto/rand and therefore never requires network access.
type IDSource interface {
	NewFamilyID(tenantID, userID string) (string, error)
	NewTokenID(tenantID, familyID string, sequence int64, kind TokenKind) (string, error)
}

type randomIDSource struct{}

func (randomIDSource) NewFamilyID(_, _ string) (string, error) {
	return randomHexID("fam_")
}

func (randomIDSource) NewTokenID(_, _ string, _ int64, _ TokenKind) (string, error) {
	return randomHexID("tok_")
}

func randomHexID(prefix string) (string, error) {
	var b [16]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(b[:]), nil
}

// CommitInfo is passed to Config.BeforeCommit. It is intentionally small so
// hosts can observe transaction boundaries and tests can inject failures.
type CommitInfo struct {
	Operation string
	TenantID  string
	FamilyID  string
	Version   int64
}

// Config configures a Service. Monetary fields are always int64 fixed point at
// Scale=1e9; durations are ordinary Go time durations.
type Config struct {
	DatabasePath string
	SigningKey   []byte

	AccessTTL  time.Duration
	RefreshTTL time.Duration

	DefaultFamilyScanThreshold int
	MaxInFlight                int
	AdmissionWait              time.Duration
	SQLiteBusyTimeout          time.Duration

	Clock func() time.Time
	IDs   IDSource

	// BeforeCommit runs inside the SQLite transaction, after domain rows and the
	// snapshot are written but before COMMIT. Returning an error rolls back the
	// whole operation. The demo uses it for deterministic contention markers;
	// crash-recovery tests use it to pause before an external process kill.
	BeforeCommit func(context.Context, CommitInfo) error
}

func (c Config) normalized() Config {
	if c.AccessTTL <= 0 {
		c.AccessTTL = 15 * time.Minute
	}
	if c.RefreshTTL <= 0 {
		c.RefreshTTL = 24 * time.Hour
	}
	if c.DefaultFamilyScanThreshold <= 0 {
		c.DefaultFamilyScanThreshold = 8
	}
	if c.MaxInFlight <= 0 {
		c.MaxInFlight = 8
	}
	if c.AdmissionWait < 0 {
		c.AdmissionWait = 0
	}
	if c.SQLiteBusyTimeout <= 0 {
		c.SQLiteBusyTimeout = 5 * time.Second
	}
	if c.Clock == nil {
		c.Clock = time.Now
	}
	if c.IDs == nil {
		c.IDs = randomIDSource{}
	}
	c.SigningKey = append([]byte(nil), c.SigningKey...)
	return c
}

func (s *Service) now() time.Time { return s.cfg.Clock().UTC() }

func (s *Service) signClaims(c Claims) (IssuedToken, error) {
	payload, err := json.Marshal(c)
	if err != nil {
		return IssuedToken{}, err
	}
	mac := hmac.New(sha256.New, s.signingKey)
	_, _ = mac.Write(payload)
	sig := mac.Sum(nil)
	raw := tokenPrefix + "." + base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig)
	return IssuedToken{Value: raw, Claims: c, Digest: tokenDigest(raw)}, nil
}

func (s *Service) verifyToken(raw string) (Claims, error) {
	var claims Claims
	parts := strings.Split(raw, ".")
	if len(parts) != 3 || parts[0] != tokenPrefix || parts[1] == "" || parts[2] == "" {
		return claims, codeError(CodeTokenInvalid, "malformed token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return claims, codeError(CodeTokenInvalid, "malformed token payload")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return claims, codeError(CodeTokenInvalid, "malformed token signature")
	}
	mac := hmac.New(sha256.New, s.signingKey)
	_, _ = mac.Write(payload)
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return claims, codeError(CodeTokenInvalid, "token signature mismatch")
	}
	dec := json.NewDecoder(strings.NewReader(string(payload)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&claims); err != nil {
		return claims, codeError(CodeTokenInvalid, "invalid token payload")
	}
	if err := ensureJSONEOF(dec); err != nil {
		return claims, codeError(CodeTokenInvalid, "invalid token payload suffix")
	}
	if err := validateClaims(claims); err != nil {
		return claims, err
	}
	return claims, nil
}

func ensureJSONEOF(dec *json.Decoder) error {
	var extra any
	err := dec.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("multiple JSON values")
	}
	return err
}

func validateClaims(c Claims) error {
	if c.Version != tokenFormatVersion || c.TokenID == "" || c.TenantID == "" || c.UserID == "" || c.FamilyID == "" {
		return codeError(CodeTokenInvalid, "token claims are incomplete")
	}
	if c.Sequence < 1 {
		return codeError(CodeTokenInvalid, "token sequence must be positive")
	}
	if c.Kind != TokenKindAccess && c.Kind != TokenKindRefresh {
		return codeError(CodeTokenInvalid, "unknown token kind")
	}
	if c.IssuedAt <= 0 || c.ExpiresAt <= c.IssuedAt {
		return codeError(CodeTokenInvalid, "invalid token timestamps")
	}
	return nil
}

func tokenDigest(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

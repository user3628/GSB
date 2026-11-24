package authlab

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
)

// TokenType distinguishes access and refresh tokens.
type TokenType string

const (
	AccessToken  TokenType = "access"
	RefreshToken TokenType = "refresh"
)

// Claims are signed into every token. TenantID is always derived from the
// signed payload before any tenant-scoped storage lookup.
type Claims struct {
	TokenID     string    `json:"jti"`
	Type        TokenType `json:"typ"`
	TenantID    string    `json:"tid"`
	UserID      string    `json:"uid"`
	SessionID   string    `json:"sid"`
	FamilyID    string    `json:"fid,omitempty"`
	Serial      int64     `json:"seq,omitempty"`
	Roles       []string  `json:"roles,omitempty"`
	Permissions []string  `json:"perms,omitempty"`
	IssuedAt    int64     `json:"iat"`
	ExpiresAt   int64     `json:"exp"`
}

// TokenCodec signs and verifies compact HMAC-SHA256 tokens. The codec performs
// no tenant lookup and does not consider a token valid until Engine validation
// succeeds against the tenant partition named in the signed claims.
type TokenCodec struct {
	key []byte
}

func NewTokenCodec(key []byte) *TokenCodec {
	return &TokenCodec{key: append([]byte(nil), key...)}
}

func (c *TokenCodec) Sign(claims Claims) (string, error) {
	claims.Roles = normalizeStrings(claims.Roles)
	claims.Permissions = normalizeStrings(claims.Permissions)
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", newError(CodeInternal, "sign_token", "marshal claims")
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := "v1." + encoded
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write([]byte(signingInput))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + signature, nil
}

// Parse verifies the signature and token fields before returning claims. Every
// malformed, expired, unsigned, or tenant-less token maps to E_TOKEN_INVALID.
func (c *TokenCodec) Parse(token string, now time.Time) (Claims, error) {
	var claims Claims
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != "v1" || parts[1] == "" || parts[2] == "" {
		return claims, newError(CodeTokenInvalid, "parse_token", "malformed token")
	}
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	want, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(mac.Sum(nil), want) {
		return claims, newError(CodeTokenInvalid, "parse_token", "signature mismatch")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return claims, newError(CodeTokenInvalid, "parse_token", "payload encoding")
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return claims, newError(CodeTokenInvalid, "parse_token", "payload json")
	}
	if claims.TokenID == "" || claims.TenantID == "" || claims.UserID == "" ||
		claims.SessionID == "" || claims.Type == "" || claims.ExpiresAt <= 0 {
		return Claims{}, newError(CodeTokenInvalid, "parse_token", "required claim missing")
	}
	if claims.Type != AccessToken && claims.Type != RefreshToken {
		return Claims{}, newError(CodeTokenInvalid, "parse_token", "token type")
	}
	if now.Unix() >= claims.ExpiresAt {
		return Claims{}, newError(CodeTokenInvalid, "parse_token", "expired")
	}
	claims.Roles = normalizeStrings(claims.Roles)
	claims.Permissions = normalizeStrings(claims.Permissions)
	return claims, nil
}

// TokenFingerprint is safe for deterministic reports: it does not reveal the
// bearer token itself.
func TokenFingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	return "sha256:" + base64.RawURLEncoding.EncodeToString(sum[:12])
}

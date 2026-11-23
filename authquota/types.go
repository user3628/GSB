package authquota

import "encoding/json"

const StateSchemaVersion = 1

type TenantSeed struct {
	ID          string `json:"id"`
	ClientID    string `json:"client_id"`
	RedirectURI string `json:"redirect_uri"`
	Quota       Fixed  `json:"quota_nano"`
	UnitCost    Fixed  `json:"unit_cost_nano"`
	RateNum     int64  `json:"rate_numerator"`
	RateDen     int64  `json:"rate_denominator"`
}

type IssueCodeInput struct {
	TenantID      string `json:"tenant_id"`
	ClientID      string `json:"client_id"`
	RedirectURI   string `json:"redirect_uri"`
	CodeChallenge string `json:"code_challenge"`
	Nonce         string `json:"nonce"`
	RequestID     string `json:"request_id"`
}

type IssueCodeResult struct {
	Code        string `json:"code"`
	TenantID    string `json:"tenant_id"`
	ClientID    string `json:"client_id"`
	RedirectURI string `json:"redirect_uri"`
	Nonce       string `json:"nonce"`
	ExpiresAt   int64  `json:"expires_at_unix_nano"`
}

type ExchangeInput struct {
	Code           string `json:"code"`
	ClientID       string `json:"client_id"`
	RedirectURI    string `json:"redirect_uri"`
	CodeVerifier   string `json:"code_verifier"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	RequestID      string `json:"request_id,omitempty"`
}

// TokenResponse is intentionally stable and JSON-canonical so repeated local
// runs can be compared byte-for-byte.
type TokenResponse struct {
	TenantID         string `json:"tenant_id"`
	ClientID         string `json:"client_id"`
	AccessToken      string `json:"access_token"`
	RefreshToken     string `json:"refresh_token"`
	TokenType        string `json:"token_type"`
	ExpiresInSeconds int64  `json:"expires_in_seconds"`
	RefreshExpiresIn int64  `json:"refresh_expires_in_seconds"`
	FamilyID         string `json:"family_id"`
	LedgerVersion    int64  `json:"ledger_version"`
	SnapshotVersion  int64  `json:"snapshot_version"`
}

type RefreshInput struct {
	RefreshToken   string `json:"refresh_token"`
	ClientID       string `json:"client_id"`
	IdempotencyKey string `json:"idempotency_key"`
	RequestCount   int64  `json:"request_count,omitempty"`
	RequestID      string `json:"request_id,omitempty"`
}

type RevokeInput struct {
	TenantID  string `json:"tenant_id"`
	Token     string `json:"token"`
	RequestID string `json:"request_id,omitempty"`
}

type RevokeResult struct {
	TenantID         string `json:"tenant_id"`
	RevokedFamily    string `json:"revoked_family_id,omitempty"`
	RevokedTokenHash string `json:"revoked_token_hash"`
	SnapshotVersion  int64  `json:"snapshot_version"`
}

type Principal struct {
	TenantID  string `json:"tenant_id"`
	ClientID  string `json:"client_id"`
	FamilyID  string `json:"family_id"`
	TokenHash string `json:"token_hash"`
	ExpiresAt int64  `json:"expires_at_unix_nano"`
}

type TokenState struct {
	TokenHash    string `json:"token_hash"`
	FamilyID     string `json:"family_id"`
	Sequence     int64  `json:"sequence"`
	TenantID     string `json:"tenant_id"`
	ClientID     string `json:"client_id"`
	IssuedAt     int64  `json:"issued_at_unix_nano"`
	ExpiresAt    int64  `json:"expires_at_unix_nano"`
	RevokedAt    int64  `json:"revoked_at_unix_nano"`
	RevokeReason string `json:"revoke_reason,omitempty"`
	ReplacedBy   string `json:"replaced_by_token_hash,omitempty"`
}

type FamilyState struct {
	FamilyID     string `json:"family_id"`
	TenantID     string `json:"tenant_id"`
	ClientID     string `json:"client_id"`
	CreatedAt    int64  `json:"created_at_unix_nano"`
	RevokedAt    int64  `json:"revoked_at_unix_nano"`
	RevokeReason string `json:"revoke_reason,omitempty"`
}

type BillingState struct {
	UnitCost Fixed `json:"unit_cost_nano"`
	RateNum  int64 `json:"rate_numerator"`
	RateDen  int64 `json:"rate_denominator"`
}

// SessionState is the canonical content of one tenant snapshot.
type SessionState struct {
	SchemaVersion  int           `json:"schema_version"`
	TenantID       string        `json:"tenant_id"`
	Version        int64         `json:"version"`
	LedgerVersion  int64         `json:"ledger_version"`
	QuotaRemaining Fixed         `json:"quota_remaining_nano"`
	Billing        BillingState  `json:"billing"`
	Families       []FamilyState `json:"families"`
	RefreshTokens  []TokenState  `json:"refresh_tokens"`
	AccessTokens   []TokenState  `json:"access_tokens"`
}

type SnapshotView struct {
	TenantID      string       `json:"tenant_id"`
	Version       int64        `json:"version"`
	LatestVersion int64        `json:"latest_version"`
	ReadOnly      bool         `json:"read_only"`
	Recovered     bool         `json:"recovered"`
	State         SessionState `json:"state"`
}

type BillingEntry struct {
	ID                 int64  `json:"id"`
	TenantID           string `json:"tenant_id"`
	LedgerVersion      int64  `json:"ledger_version"`
	RequestID          string `json:"request_id"`
	IdempotencyKey     string `json:"idempotency_key"`
	Requests           int64  `json:"requests"`
	UnitCostNano       Fixed  `json:"unit_cost_nano"`
	RateNumerator      int64  `json:"rate_numerator"`
	RateDenominator    int64  `json:"rate_denominator"`
	ChargedNano        Fixed  `json:"charged_nano"`
	TruncationNum      int64  `json:"truncation_numerator"`
	TruncationDen      int64  `json:"truncation_denominator"`
	RemainingAfterNano Fixed  `json:"remaining_after_nano"`
	SnapshotVersion    int64  `json:"snapshot_version"`
	CreatedAt          int64  `json:"created_at_unix_nano"`
}

type LedgerExport struct {
	SchemaVersion   int              `json:"schema_version"`
	TenantID        string           `json:"tenant_id"`
	ReadOnly        bool             `json:"read_only"`
	LedgerVersion   int64            `json:"ledger_version"`
	SnapshotVersion int64            `json:"snapshot_version"`
	Entries         []BillingEntry   `json:"entries"`
	Truncation      TruncationReport `json:"truncation"`
	Session         SessionState     `json:"session"`
}

type ErrorEvent struct {
	ID              int64  `json:"id"`
	TenantID        string `json:"tenant_id"`
	RequestID       string `json:"request_id"`
	Code            string `json:"code"`
	Detail          string `json:"detail"`
	LedgerVersion   int64  `json:"ledger_version"`
	SnapshotVersion int64  `json:"snapshot_version"`
	CreatedAt       int64  `json:"created_at_unix_nano"`
}

// CanonicalJSON marshals only exported struct fields in declaration order.
// Callers should pass structs with deterministically sorted slices.
func CanonicalJSON(value any) ([]byte, error) { return json.Marshal(value) }

func cloneSessionState(in SessionState) SessionState { return in }

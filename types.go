package mfa

import (
	"fmt"
	"sort"
	"strings"
)

const (
	DefaultMaxConcurrency    = 4
	DefaultMaxPendingPerUser = 8
	DefaultFailureThreshold  = 3
	DefaultTOTPStepSeconds   = 30
	DefaultTOTPWindow        = 1
	DefaultRecoveryCodes     = 5
)

type FactorType string

const (
	FactorTOTP     FactorType = "totp"
	FactorRecovery FactorType = "recovery"
	FactorWebAuthn FactorType = "webauthn"
)

func (t FactorType) Supported() bool {
	switch t {
	case FactorTOTP, FactorRecovery, FactorWebAuthn:
		return true
	default:
		return false
	}
}

type FactorStatus string

const (
	StatusPending  FactorStatus = "pending"
	StatusActive   FactorStatus = "active"
	StatusDegraded FactorStatus = "degraded"
	StatusRevoked  FactorStatus = "revoked"
	StatusExpired  FactorStatus = "expired"
)

func (s FactorStatus) Supported() bool {
	switch s {
	case StatusPending, StatusActive, StatusDegraded, StatusRevoked, StatusExpired:
		return true
	default:
		return false
	}
}

type Sensitivity string

const (
	SensitivityLow      Sensitivity = "low"
	SensitivityMedium   Sensitivity = "medium"
	SensitivityHigh     Sensitivity = "high"
	SensitivityCritical Sensitivity = "critical"
)

func (s Sensitivity) Strength() int {
	switch s {
	case SensitivityLow:
		return 1
	case SensitivityMedium:
		return 2
	case SensitivityHigh:
		return 3
	case SensitivityCritical:
		return 4
	default:
		return 0
	}
}

func (s Sensitivity) Supported() bool {
	return s.Strength() > 0
}

type Limits struct {
	MaxConcurrency    int `json:"max_concurrency"`
	MaxPendingPerUser int `json:"max_pending_per_user"`
	FailureThreshold  int `json:"failure_threshold"`
	TOTPStepSeconds   int `json:"totp_step_seconds"`
	TOTPWindow        int `json:"totp_window"`
}

func (l Limits) withDefaults() Limits {
	if l.MaxConcurrency == 0 {
		l.MaxConcurrency = DefaultMaxConcurrency
	}
	if l.MaxPendingPerUser == 0 {
		l.MaxPendingPerUser = DefaultMaxPendingPerUser
	}
	if l.FailureThreshold == 0 {
		l.FailureThreshold = DefaultFailureThreshold
	}
	if l.TOTPStepSeconds == 0 {
		l.TOTPStepSeconds = DefaultTOTPStepSeconds
	}
	if l.TOTPWindow == 0 {
		l.TOTPWindow = DefaultTOTPWindow
	}
	return l
}

func (l Limits) validate() error {
	if l.MaxConcurrency < 1 {
		return policyError("limits.max_concurrency", "must be at least 1")
	}
	if l.MaxPendingPerUser < 1 {
		return policyError("limits.max_pending_per_user", "must be at least 1")
	}
	if l.FailureThreshold < 1 {
		return policyError("limits.failure_threshold", "must be at least 1")
	}
	if l.TOTPStepSeconds < 1 {
		return policyError("limits.totp_step_seconds", "must be at least 1")
	}
	if l.TOTPWindow < 0 {
		return policyError("limits.totp_window", "must not be negative")
	}
	return nil
}

type TenantDeclaration struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type UserDeclaration struct {
	ID       string `json:"id"`
	TenantID string `json:"tenant_id"`
	Name     string `json:"name"`
}

type FactorDeclaration struct {
	ID                string       `json:"id"`
	TenantID          string       `json:"tenant_id"`
	UserID            string       `json:"user_id"`
	Type              FactorType   `json:"type"`
	Sensitivity       Sensitivity  `json:"sensitivity"`
	Status            FactorStatus `json:"status"`
	Rebindable        bool         `json:"rebindable"`
	LastActivated     bool         `json:"last_activated"`
	ExpiresAtUnix     int64        `json:"expires_at_unix"`
	SecretSeed        string       `json:"secret_seed,omitempty"`
	RecoveryCodeCount int          `json:"recovery_code_count,omitempty"`
}

type PolicyRequiredFactor struct {
	Type           FactorType  `json:"type"`
	MinSensitivity Sensitivity `json:"min_sensitivity"`
}

type PolicyDeclaration struct {
	TenantID        string                 `json:"tenant_id"`
	MinStrength     int                    `json:"min_strength"`
	RequiredFactors []PolicyRequiredFactor `json:"required_factors"`
}

type Config struct {
	ClockUnix int64               `json:"clock_unix"`
	Limits    Limits              `json:"limits"`
	Tenants   []TenantDeclaration `json:"tenants"`
	Users     []UserDeclaration   `json:"users"`
	Policies  []PolicyDeclaration `json:"policies"`
	Factors   []FactorDeclaration `json:"factors"`
}

type Tenant struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type User struct {
	ID       string `json:"id"`
	TenantID string `json:"tenant_id"`
	Name     string `json:"name"`
}

type RequiredFactor struct {
	Type           FactorType  `json:"type"`
	MinSensitivity Sensitivity `json:"min_sensitivity"`
	MinStrength    int         `json:"min_strength"`
}

type Policy struct {
	TenantID        string           `json:"tenant_id"`
	MinStrength     int              `json:"min_strength"`
	RequiredFactors []RequiredFactor `json:"required_factors"`
}

type Factor struct {
	ID                  string       `json:"id"`
	TenantID            string       `json:"tenant_id"`
	UserID              string       `json:"user_id"`
	Type                FactorType   `json:"type"`
	Sensitivity         Sensitivity  `json:"sensitivity"`
	Strength            int          `json:"strength"`
	Status              FactorStatus `json:"status"`
	Rebindable          bool         `json:"rebindable"`
	LastActivated       bool         `json:"last_activated"`
	ExpiresAtUnix       int64        `json:"expires_at_unix"`
	SecretSeed          string       `json:"-"`
	RecoveryCodeCount   int          `json:"recovery_code_count,omitempty"`
	SuccessCount        int          `json:"success_count"`
	FailureCount        int          `json:"failure_count"`
	ConsecutiveFailures int          `json:"consecutive_failures"`
	DegradedReason      string       `json:"degraded_reason,omitempty"`
}

type RecoveryCode struct {
	FactorID string `json:"factor_id"`
	Index    int    `json:"index"`
	CodeHash string `json:"code_hash"`
	Consumed bool   `json:"consumed"`
}

type NormalizedConfig struct {
	ClockUnix int64
	Limits    Limits
	Tenants   []Tenant
	Users     []User
	Policies  []Policy
	Factors   []Factor
	Codes     []RecoveryCode
}

func normalizeConfig(cfg Config) (*NormalizedConfig, error) {
	limits := cfg.Limits.withDefaults()
	if err := limits.validate(); err != nil {
		return nil, err
	}
	if cfg.ClockUnix <= 0 {
		return nil, policyError("clock_unix", "must be a positive deterministic Unix timestamp")
	}

	tenants := make([]Tenant, 0, len(cfg.Tenants))
	tenantIDs := map[string]struct{}{}
	for i, declared := range cfg.Tenants {
		id := strings.TrimSpace(declared.ID)
		if id == "" {
			return nil, policyError(fmt.Sprintf("tenants[%d].id", i), "is required")
		}
		if _, exists := tenantIDs[id]; exists {
			return nil, policyError(fmt.Sprintf("tenants[%d].id", i), "duplicate tenant identifier")
		}
		tenantIDs[id] = struct{}{}
		name := strings.TrimSpace(declared.Name)
		if name == "" {
			name = id
		}
		tenants = append(tenants, Tenant{ID: id, Name: name})
	}
	sort.Slice(tenants, func(i, j int) bool { return tenants[i].ID < tenants[j].ID })

	users := make([]User, 0, len(cfg.Users))
	userIDs := map[string]struct{}{}
	userTenant := map[string]string{}
	for i, declared := range cfg.Users {
		id := strings.TrimSpace(declared.ID)
		tenantID := strings.TrimSpace(declared.TenantID)
		if id == "" {
			return nil, policyError(fmt.Sprintf("users[%d].id", i), "is required")
		}
		if _, exists := userIDs[id]; exists {
			return nil, policyError(fmt.Sprintf("users[%d].id", i), "duplicate user identifier")
		}
		if _, exists := tenantIDs[tenantID]; !exists {
			return nil, policyError(fmt.Sprintf("users[%d].tenant_id", i), "references an unknown tenant")
		}
		userIDs[id] = struct{}{}
		userTenant[id] = tenantID
		name := strings.TrimSpace(declared.Name)
		if name == "" {
			name = id
		}
		users = append(users, User{ID: id, TenantID: tenantID, Name: name})
	}
	sort.Slice(users, func(i, j int) bool {
		if users[i].TenantID == users[j].TenantID {
			return users[i].ID < users[j].ID
		}
		return users[i].TenantID < users[j].TenantID
	})

	policies := make([]Policy, 0, len(cfg.Policies))
	policyTenant := map[string]struct{}{}
	for i, declared := range cfg.Policies {
		tenantID := strings.TrimSpace(declared.TenantID)
		if _, exists := tenantIDs[tenantID]; !exists {
			return nil, policyError(fmt.Sprintf("policies[%d].tenant_id", i), "references an unknown tenant")
		}
		if _, exists := policyTenant[tenantID]; exists {
			return nil, policyError(fmt.Sprintf("policies[%d].tenant_id", i), "duplicate policy for tenant")
		}
		policyTenant[tenantID] = struct{}{}
		if declared.MinStrength < 1 || declared.MinStrength > 4 {
			return nil, policyError(fmt.Sprintf("policies[%d].min_strength", i), "must be between 1 and 4")
		}
		if len(declared.RequiredFactors) == 0 {
			return nil, policyError(fmt.Sprintf("policies[%d].required_factors", i), "must not be empty")
		}
		seenTypes := map[FactorType]struct{}{}
		required := make([]RequiredFactor, 0, len(declared.RequiredFactors))
		for j, req := range declared.RequiredFactors {
			if !req.Type.Supported() {
				return nil, policyError(fmt.Sprintf("policies[%d].required_factors[%d].type", i, j), "unsupported factor type")
			}
			if _, exists := seenTypes[req.Type]; exists {
				return nil, policyError(fmt.Sprintf("policies[%d].required_factors[%d].type", i, j), "duplicate factor type")
			}
			seenTypes[req.Type] = struct{}{}
			if !req.MinSensitivity.Supported() {
				return nil, policyError(fmt.Sprintf("policies[%d].required_factors[%d].min_sensitivity", i, j), "unsupported sensitivity label")
			}
			required = append(required, RequiredFactor{
				Type:           req.Type,
				MinSensitivity: req.MinSensitivity,
				MinStrength:    req.MinSensitivity.Strength(),
			})
		}
		sort.Slice(required, func(a, b int) bool { return required[a].Type < required[b].Type })
		policies = append(policies, Policy{
			TenantID:        tenantID,
			MinStrength:     declared.MinStrength,
			RequiredFactors: required,
		})
	}
	sort.Slice(policies, func(i, j int) bool { return policies[i].TenantID < policies[j].TenantID })

	factors := make([]Factor, 0, len(cfg.Factors))
	codes := make([]RecoveryCode, 0)
	factorIDs := map[string]struct{}{}
	activeType := map[string]struct{}{}
	for i, declared := range cfg.Factors {
		id := strings.TrimSpace(declared.ID)
		tenantID := strings.TrimSpace(declared.TenantID)
		userID := strings.TrimSpace(declared.UserID)
		if id == "" {
			return nil, policyError(fmt.Sprintf("factors[%d].id", i), "is required")
		}
		if _, exists := factorIDs[id]; exists {
			return nil, policyError(fmt.Sprintf("factors[%d].id", i), "duplicate factor identifier")
		}
		factorIDs[id] = struct{}{}
		if gotTenant, exists := userTenant[userID]; !exists || gotTenant != tenantID {
			return nil, policyError(fmt.Sprintf("factors[%d].user_id", i), "does not resolve to a user in the declared tenant")
		}
		if _, exists := policyTenant[tenantID]; !exists {
			return nil, policyError(fmt.Sprintf("factors[%d].tenant_id", i), "tenant has no authentication policy")
		}
		if !declared.Type.Supported() {
			return nil, policyError(fmt.Sprintf("factors[%d].type", i), "unsupported factor type")
		}
		if !declared.Sensitivity.Supported() {
			return nil, policyError(fmt.Sprintf("factors[%d].sensitivity", i), "unsupported sensitivity label")
		}
		if !declared.Status.Supported() {
			return nil, policyError(fmt.Sprintf("factors[%d].status", i), "unsupported factor status")
		}
		if declared.ExpiresAtUnix < 0 {
			return nil, policyError(fmt.Sprintf("factors[%d].expires_at_unix", i), "must not be negative")
		}
		if declared.LastActivated && declared.Status != StatusActive {
			return nil, policyError(fmt.Sprintf("factors[%d].last_activated", i), "only active factors may carry the last activation marker")
		}
		if declared.Status == StatusActive && !declared.LastActivated {
			return nil, policyError(fmt.Sprintf("factors[%d].last_activated", i), "active factor must carry the last activation marker")
		}
		if declared.Status == StatusActive {
			key := tenantID + "\x00" + userID + "\x00" + string(declared.Type)
			if _, exists := activeType[key]; exists {
				return nil, policyError(fmt.Sprintf("factors[%d].status", i), "only one active factor per user and factor type is allowed")
			}
			activeType[key] = struct{}{}
		}
		recoveryCount := declared.RecoveryCodeCount
		if declared.Type == FactorRecovery {
			if recoveryCount == 0 {
				recoveryCount = DefaultRecoveryCodes
			}
			if recoveryCount < 1 || recoveryCount > 64 {
				return nil, policyError(fmt.Sprintf("factors[%d].recovery_code_count", i), "must be between 1 and 64")
			}
		} else if recoveryCount != 0 {
			return nil, policyError(fmt.Sprintf("factors[%d].recovery_code_count", i), "is only valid for recovery factors")
		}
		secretSeed := strings.TrimSpace(declared.SecretSeed)
		if secretSeed == "" {
			secretSeed = "deterministic-seed:" + id
		}
		factors = append(factors, Factor{
			ID:                id,
			TenantID:          tenantID,
			UserID:            userID,
			Type:              declared.Type,
			Sensitivity:       declared.Sensitivity,
			Strength:          declared.Sensitivity.Strength(),
			Status:            declared.Status,
			Rebindable:        declared.Rebindable,
			LastActivated:     declared.LastActivated,
			ExpiresAtUnix:     declared.ExpiresAtUnix,
			SecretSeed:        secretSeed,
			RecoveryCodeCount: recoveryCount,
		})
		if declared.Type == FactorRecovery {
			for index := 0; index < recoveryCount; index++ {
				codes = append(codes, RecoveryCode{
					FactorID: id,
					Index:    index,
					CodeHash: recoveryCodeHash(secretSeed, index),
				})
			}
		}
	}
	sort.Slice(factors, func(i, j int) bool {
		if factors[i].TenantID != factors[j].TenantID {
			return factors[i].TenantID < factors[j].TenantID
		}
		if factors[i].UserID != factors[j].UserID {
			return factors[i].UserID < factors[j].UserID
		}
		return factors[i].ID < factors[j].ID
	})
	sort.Slice(codes, func(i, j int) bool {
		if codes[i].FactorID == codes[j].FactorID {
			return codes[i].Index < codes[j].Index
		}
		return codes[i].FactorID < codes[j].FactorID
	})

	return &NormalizedConfig{
		ClockUnix: cfg.ClockUnix,
		Limits:    limits,
		Tenants:   tenants,
		Users:     users,
		Policies:  policies,
		Factors:   factors,
		Codes:     codes,
	}, nil
}

type ActivationRequest struct {
	TenantID  string `json:"tenant_id"`
	UserID    string `json:"user_id"`
	FactorID  string `json:"factor_id"`
	AttemptID string `json:"attempt_id"`
}

type ActivationResult struct {
	Code            Code   `json:"code"`
	TenantID        string `json:"tenant_id"`
	UserID          string `json:"user_id"`
	FactorID        string `json:"factor_id"`
	SnapshotVersion int64  `json:"snapshot_version"`
	Message         string `json:"message,omitempty"`
}

type AuthRequest struct {
	TenantID          string   `json:"tenant_id"`
	UserID            string   `json:"user_id"`
	SessionID         string   `json:"session_id"`
	AttemptID         string   `json:"attempt_id"`
	FactorHints       []string `json:"factor_hints,omitempty"`
	TOTPStepOffset    int      `json:"totp_step_offset"`
	RecoveryCodeIndex *int     `json:"recovery_code_index,omitempty"`
	AnswerMode        string   `json:"answer_mode,omitempty"`
	Answer            string   `json:"answer,omitempty"`
	HoldMillis        int      `json:"hold_millis,omitempty"`
}

type Challenge struct {
	FactorID    string     `json:"factor_id"`
	FactorType  FactorType `json:"factor_type"`
	Kind        string     `json:"kind"`
	Prompt      string     `json:"prompt"`
	TimeStep    int64      `json:"time_step,omitempty"`
	CodeIndex   *int       `json:"code_index,omitempty"`
	Nonce       string     `json:"nonce,omitempty"`
	ResponseSHA string     `json:"response_sha256"`
}

type AuthResult struct {
	Code            Code        `json:"code"`
	SessionID       string      `json:"session_id"`
	AttemptID       string      `json:"attempt_id"`
	TenantID        string      `json:"tenant_id"`
	UserID          string      `json:"user_id"`
	FactorIDs       []string    `json:"factor_ids"`
	Challenges      []Challenge `json:"challenges"`
	SnapshotVersion int64       `json:"snapshot_version"`
	Message         string      `json:"message,omitempty"`
}

type RebindRequest struct {
	TenantID          string      `json:"tenant_id"`
	UserID            string      `json:"user_id"`
	FactorType        FactorType  `json:"factor_type"`
	NewFactorID       string      `json:"new_factor_id"`
	AttemptID         string      `json:"attempt_id"`
	Sensitivity       Sensitivity `json:"sensitivity"`
	ExpiresAtUnix     int64       `json:"expires_at_unix"`
	SecretSeed        string      `json:"secret_seed,omitempty"`
	RecoveryCodeCount int         `json:"recovery_code_count,omitempty"`
	HoldMillis        int         `json:"hold_millis,omitempty"`
}

type RebindResult struct {
	Code            Code       `json:"code"`
	TenantID        string     `json:"tenant_id"`
	UserID          string     `json:"user_id"`
	FactorType      FactorType `json:"factor_type"`
	OldFactorID     string     `json:"old_factor_id"`
	NewFactorID     string     `json:"new_factor_id"`
	AttemptID       string     `json:"attempt_id"`
	SnapshotVersion int64      `json:"snapshot_version"`
	Message         string     `json:"message,omitempty"`
}

type RestoreRequest struct {
	TenantID  string `json:"tenant_id"`
	UserID    string `json:"user_id"`
	FactorID  string `json:"factor_id"`
	AttemptID string `json:"attempt_id"`
	Reason    string `json:"reason"`
}

type RestoreResult struct {
	Code            Code   `json:"code"`
	TenantID        string `json:"tenant_id"`
	UserID          string `json:"user_id"`
	FactorID        string `json:"factor_id"`
	SnapshotVersion int64  `json:"snapshot_version"`
	Message         string `json:"message,omitempty"`
}

type ExpireResult struct {
	Code            Code     `json:"code"`
	TenantID        string   `json:"tenant_id"`
	UserID          string   `json:"user_id"`
	FactorIDs       []string `json:"factor_ids"`
	SnapshotVersion int64    `json:"snapshot_version"`
	Message         string   `json:"message,omitempty"`
}

type UsageTrace struct {
	TraceID         string     `json:"trace_id"`
	TenantID        string     `json:"tenant_id"`
	UserID          string     `json:"user_id"`
	FactorID        string     `json:"factor_id"`
	AttemptID       string     `json:"attempt_id"`
	FactorType      FactorType `json:"factor_type"`
	Outcome         string     `json:"outcome"`
	Code            Code       `json:"code"`
	ChallengePrompt string     `json:"challenge_prompt"`
	ResponseSHA     string     `json:"response_sha256"`
	SnapshotVersion int64      `json:"snapshot_version"`
	Sequence        int64      `json:"sequence"`
}

type RiskTrace struct {
	TraceID             string `json:"trace_id"`
	TenantID            string `json:"tenant_id"`
	UserID              string `json:"user_id"`
	FactorID            string `json:"factor_id"`
	AttemptID           string `json:"attempt_id"`
	Kind                string `json:"kind"`
	Reason              string `json:"reason"`
	ConsecutiveFailures int    `json:"consecutive_failures"`
	SnapshotVersion     int64  `json:"snapshot_version"`
	Sequence            int64  `json:"sequence"`
}

type RebindTrace struct {
	TraceID         string     `json:"trace_id"`
	TenantID        string     `json:"tenant_id"`
	UserID          string     `json:"user_id"`
	FactorType      FactorType `json:"factor_type"`
	OldFactorID     string     `json:"old_factor_id"`
	NewFactorID     string     `json:"new_factor_id"`
	AttemptID       string     `json:"attempt_id"`
	Status          string     `json:"status"`
	Code            Code       `json:"code"`
	SnapshotVersion int64      `json:"snapshot_version"`
	Sequence        int64      `json:"sequence"`
}

type SnapshotInfo struct {
	Version       int64  `json:"version"`
	ParentVersion int64  `json:"parent_version"`
	ContentHash   string `json:"content_hash"`
	ChainHash     string `json:"chain_hash"`
	Reason        string `json:"reason"`
	AttemptID     string `json:"attempt_id,omitempty"`
	Valid         bool   `json:"valid"`
	Issue         string `json:"issue,omitempty"`
}

type SnapshotStatus struct {
	Entries         []SnapshotInfo `json:"entries"`
	HeadVersion     int64          `json:"head_version"`
	LatestValid     int64          `json:"latest_valid"`
	FallbackVersion int64          `json:"fallback_version"`
	FallbackUsed    bool           `json:"fallback_used"`
	Code            Code           `json:"code"`
	Reason          string         `json:"reason,omitempty"`
}

type FaultKind string

const (
	FaultSnapshotVersionGap           FaultKind = "snapshot_version_gap"
	FaultSnapshotWholesaleReplacement FaultKind = "snapshot_wholesale_replacement"
)

type FaultInjection struct {
	Kind FaultKind `json:"kind"`
}

type RecoveryResult struct {
	Code            Code           `json:"code"`
	SnapshotStatus  SnapshotStatus `json:"snapshot_status"`
	CommittedHead   int64          `json:"committed_head"`
	RestoredVersion int64          `json:"restored_version"`
	Message         string         `json:"message,omitempty"`
}

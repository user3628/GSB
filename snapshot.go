package authlab

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

const (
	SnapshotSchema = "authlab.snapshot.v1"
	GenesisDigest  = "GENESIS"

	SessionActive  = "active"
	SessionRevoked = "revoked"

	FamilyActive  = "active"
	FamilyRevoked = "revoked"

	RefreshActive  = "active"
	RefreshUsed    = "used"
	RefreshRevoked = "revoked"
)

// SessionRecord is the versioned, tenant-scoped session state.
type SessionRecord struct {
	SessionID      string `json:"session_id"`
	UserID         string `json:"user_id"`
	FamilyID       string `json:"family_id"`
	Status         string `json:"status"`
	CreatedAt      int64  `json:"created_at"`
	UpdatedAt      int64  `json:"updated_at"`
	RevokedAt      int64  `json:"revoked_at,omitempty"`
	RevokedReason  string `json:"revoked_reason,omitempty"`
	LastRotationAt int64  `json:"last_rotation_at,omitempty"`
}

// TokenFamily is the refresh-token lineage for one session. Only the current
// serial may be active; reuse detection revokes the entire family.
type TokenFamily struct {
	FamilyID      string `json:"family_id"`
	SessionID     string `json:"session_id"`
	UserID        string `json:"user_id"`
	CurrentSerial int64  `json:"current_serial"`
	Status        string `json:"status"`
	CreatedAt     int64  `json:"created_at"`
	RevokedAt     int64  `json:"revoked_at,omitempty"`
	RevokedReason string `json:"revoked_reason,omitempty"`
}

// RefreshTokenRecord stores no bearer secret, only the token id and lineage.
type RefreshTokenRecord struct {
	TokenID   string `json:"token_id"`
	FamilyID  string `json:"family_id"`
	SessionID string `json:"session_id"`
	UserID    string `json:"user_id"`
	Serial    int64  `json:"serial"`
	Status    string `json:"status"`
	ExpiresAt int64  `json:"expires_at"`
	UsedAt    int64  `json:"used_at,omitempty"`
	RevokedAt int64  `json:"revoked_at,omitempty"`
}

// TenantState is the complete session state for one tenant. It is represented as
// sorted slices so snapshot bytes are deterministic.
type TenantState struct {
	Sessions      []SessionRecord      `json:"sessions"`
	Families      []TokenFamily        `json:"families"`
	RefreshTokens []RefreshTokenRecord `json:"refresh_tokens"`
}

// Snapshot is a validated state version. Version 0 is the empty genesis state
// and has no persisted row.
type Snapshot struct {
	TenantID       string      `json:"tenant_id"`
	Version        int64       `json:"version"`
	PreviousDigest string      `json:"previous_digest"`
	Digest         string      `json:"digest"`
	State          TenantState `json:"state"`
}

type snapshotContent struct {
	Schema         string      `json:"schema"`
	TenantID       string      `json:"tenant_id"`
	Version        int64       `json:"version"`
	PreviousDigest string      `json:"previous_digest"`
	State          TenantState `json:"state"`
}

type SnapshotMeta struct {
	TenantID       string `json:"tenant_id"`
	Version        int64  `json:"version"`
	PreviousDigest string `json:"previous_digest"`
	Digest         string `json:"digest"`
}

type SnapshotAudit struct {
	TenantID     string  `json:"tenant_id"`
	HeadVersion  int64   `json:"head_version"`
	ValidVersion int64   `json:"valid_version"`
	Versions     []int64 `json:"versions"`
	IntegrityOK  bool    `json:"integrity_ok"`
	Problem      string  `json:"problem,omitempty"`
}

func emptySnapshot(tenantID string) Snapshot {
	return Snapshot{
		TenantID:       tenantID,
		Version:        0,
		PreviousDigest: "",
		Digest:         GenesisDigest,
		State:          TenantState{Sessions: []SessionRecord{}, Families: []TokenFamily{}, RefreshTokens: []RefreshTokenRecord{}},
	}
}

func (s TenantState) normalized() TenantState {
	out := TenantState{
		Sessions:      append([]SessionRecord(nil), s.Sessions...),
		Families:      append([]TokenFamily(nil), s.Families...),
		RefreshTokens: append([]RefreshTokenRecord(nil), s.RefreshTokens...),
	}
	if out.Sessions == nil {
		out.Sessions = []SessionRecord{}
	}
	if out.Families == nil {
		out.Families = []TokenFamily{}
	}
	if out.RefreshTokens == nil {
		out.RefreshTokens = []RefreshTokenRecord{}
	}
	sort.Slice(out.Sessions, func(i, j int) bool { return out.Sessions[i].SessionID < out.Sessions[j].SessionID })
	sort.Slice(out.Families, func(i, j int) bool { return out.Families[i].FamilyID < out.Families[j].FamilyID })
	sort.Slice(out.RefreshTokens, func(i, j int) bool { return out.RefreshTokens[i].TokenID < out.RefreshTokens[j].TokenID })
	return out
}

func marshalSnapshotContent(tenantID string, version int64, previous string, state TenantState) ([]byte, string, error) {
	content := snapshotContent{
		Schema:         SnapshotSchema,
		TenantID:       tenantID,
		Version:        version,
		PreviousDigest: previous,
		State:          state.normalized(),
	}
	payload, err := json.Marshal(content)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(payload)
	return payload, hex.EncodeToString(digest[:]), nil
}

func decodeSnapshotContent(payload []byte) (snapshotContent, error) {
	var content snapshotContent
	if err := json.Unmarshal(payload, &content); err != nil {
		return content, err
	}
	return content, nil
}

func (s *TenantState) session(id string) *SessionRecord {
	for i := range s.Sessions {
		if s.Sessions[i].SessionID == id {
			return &s.Sessions[i]
		}
	}
	return nil
}

func (s *TenantState) family(id string) *TokenFamily {
	for i := range s.Families {
		if s.Families[i].FamilyID == id {
			return &s.Families[i]
		}
	}
	return nil
}

func (s *TenantState) refresh(id string) *RefreshTokenRecord {
	for i := range s.RefreshTokens {
		if s.RefreshTokens[i].TokenID == id {
			return &s.RefreshTokens[i]
		}
	}
	return nil
}

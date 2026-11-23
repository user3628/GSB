package engine

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
)

// Credential binds device, tenant, capability set, score version and expiry
// with a local HMAC-SHA256 signature.
type Credential struct {
	DeviceID     string   `json:"device_id"`
	TenantID     string   `json:"tenant_id"`
	Capabilities []string `json:"capabilities"`
	ScoreVersion int64    `json:"score_version"`
	IssuedAt     int64    `json:"issued_at"`
	ExpiresAt    int64    `json:"expires_at"`
	Sig          string   `json:"sig"`
}

func canonicalCred(deviceID, tenantID string, caps []string, scoreVersion, issuedAt, expiresAt int64) string {
	sorted := make([]string, len(caps))
	copy(sorted, caps)
	sort.Strings(sorted)
	return strings.Join([]string{
		deviceID,
		tenantID,
		strings.Join(sorted, ","),
		strconv.FormatInt(scoreVersion, 10),
		strconv.FormatInt(issuedAt, 10),
		strconv.FormatInt(expiresAt, 10),
	}, "|")
}

func signCred(key []byte, payload string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// IssueCredential signs a credential for the device at the given logical
// time with the tenant policy TTL.
func IssueCredential(key []byte, deviceID, tenantID string, caps []string, scoreVersion, now, ttl int64) *Credential {
	c := &Credential{
		DeviceID:     deviceID,
		TenantID:     tenantID,
		Capabilities: append([]string(nil), caps...),
		ScoreVersion: scoreVersion,
		IssuedAt:     now,
		ExpiresAt:    now + ttl,
	}
	c.Sig = signCred(key, canonicalCred(c.DeviceID, c.TenantID, c.Capabilities, c.ScoreVersion, c.IssuedAt, c.ExpiresAt))
	return c
}

// VerifyCredential strictly checks signature, expiry, capability scope and
// score-version regression, in that order.
func VerifyCredential(c *Credential, key []byte, policy *TenantPolicy, currentScoreVersion, now int64) Code {
	expect := signCred(key, canonicalCred(c.DeviceID, c.TenantID, c.Capabilities, c.ScoreVersion, c.IssuedAt, c.ExpiresAt))
	if !hmac.Equal([]byte(expect), []byte(c.Sig)) {
		return CodeCredSignature
	}
	if now > c.ExpiresAt {
		return CodeCredExpired
	}
	for _, cap := range c.Capabilities {
		if !containsStr(policy.AllowedCaps, cap) {
			return CodeCapabilityDenied
		}
	}
	if currentScoreVersion < c.ScoreVersion {
		return CodeScoreRegressed
	}
	return CodeOK
}

// Digest is the short deterministic credential digest stored in snapshots.
func Digest(c *Credential) string {
	if c == nil || len(c.Sig) < 16 {
		return ""
	}
	return c.Sig[:16]
}

func containsStr(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

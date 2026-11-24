package authlab

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"
)

// Tenant is a deterministic local tenant fixture.
type Tenant struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// FixtureUser contains only credential material and authorization records. The
// sample package supplies the known demo passwords separately.
type FixtureUser struct {
	TenantID     string   `json:"tenant_id"`
	UserID       string   `json:"user_id"`
	Username     string   `json:"username"`
	PasswordHash string   `json:"password_hash"`
	PasswordSalt string   `json:"password_salt"`
	Iterations   int      `json:"iterations"`
	Roles        []string `json:"roles"`
	Permissions  []string `json:"permissions"`
}

// Fixture is installed into a fresh local store. All values are local and
// deterministic.
type Fixture struct {
	Tenants    []Tenant      `json:"tenants"`
	Users      []FixtureUser `json:"users"`
	SigningKey []byte        `json:"-"`
	IDSeed     []byte        `json:"-"`
	BaseTime   time.Time     `json:"base_time"`
}

// UserRecord is the tenant-scoped authorization view used during validation.
type UserRecord struct {
	TenantID     string   `json:"tenant_id"`
	UserID       string   `json:"user_id"`
	Username     string   `json:"username"`
	PasswordHash string   `json:"-"`
	PasswordSalt string   `json:"-"`
	Iterations   int      `json:"-"`
	Roles        []string `json:"roles"`
	Permissions  []string `json:"permissions"`
}

// ValidateFixture checks the minimum invariants needed by the engine.
func ValidateFixture(f Fixture) error {
	if len(f.SigningKey) < 32 {
		return newError(CodeInternal, "fixture", "signing key must be at least 32 bytes")
	}
	if len(f.IDSeed) < 16 {
		return newError(CodeInternal, "fixture", "id seed must be at least 16 bytes")
	}
	if f.BaseTime.IsZero() {
		return newError(CodeInternal, "fixture", "base time is required")
	}
	tenants := make(map[string]bool, len(f.Tenants))
	for _, tenant := range f.Tenants {
		if tenant.ID == "" || tenants[tenant.ID] {
			return newError(CodeInternal, "fixture", "tenant ids must be non-empty and unique")
		}
		tenants[tenant.ID] = true
	}
	seenUsername := make(map[string]bool)
	seenUserID := make(map[string]bool)
	for _, user := range f.Users {
		if !tenants[user.TenantID] || user.UserID == "" || user.Username == "" {
			return newError(CodeInternal, "fixture", "user references an unknown tenant or has empty identity")
		}
		key := user.TenantID + "\x00" + user.Username
		if seenUsername[key] || seenUserID[user.TenantID+"\x00"+user.UserID] {
			return newError(CodeInternal, "fixture", "tenant-scoped usernames and user ids must be unique")
		}
		seenUsername[key] = true
		seenUserID[user.TenantID+"\x00"+user.UserID] = true
		if user.Iterations < 1 || user.PasswordHash == "" || user.PasswordSalt == "" {
			return newError(CodeInternal, "fixture", "password hash material is incomplete")
		}
	}
	return nil
}

func normalizeStrings(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	out := append([]string(nil), in...)
	sort.Strings(out)
	unique := out[:0]
	for _, value := range out {
		if value == "" || (len(unique) > 0 && unique[len(unique)-1] == value) {
			continue
		}
		unique = append(unique, value)
	}
	return append([]string(nil), unique...)
}

func equalStringSets(a, b []string) bool {
	aa, bb := normalizeStrings(a), normalizeStrings(b)
	if len(aa) != len(bb) {
		return false
	}
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}

func hasString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func digestBytes(parts ...string) []byte {
	h := sha256.New()
	for _, part := range parts {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return h.Sum(nil)
}

func digestHex(parts ...string) string {
	return hex.EncodeToString(digestBytes(parts...))
}

func stableJoin(values []string) string {
	return strings.Join(normalizeStrings(values), "|")
}

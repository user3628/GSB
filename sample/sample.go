// Package sample provides deterministic local tenants, users, password hashes,
// and signing/id key material. It never contacts a network or identity provider.
package sample

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"example.com/authlab"
)

const (
	TenantA = "tenant-a"
	TenantB = "tenant-b"

	UserAlice = "user-alice"
	UserBob   = "user-bob"
	UserCarol = "user-carol"

	UsernameAlice = "alice"
	UsernameBob   = "bob"
	UsernameCarol = "carol"

	PasswordAlice = "alice-demo-password-v1"
	PasswordBob   = "bob-demo-password-v1"
	PasswordCarol = "carol-demo-password-v1"

	PasswordIterations = 20000
)

// BaseTime is shared by deterministic experiments. Tokens and snapshot reports
// use this value; the HTTP demo server uses the system clock instead.
var BaseTime = time.Date(2035, time.January, 1, 0, 0, 0, 0, time.UTC)

type Credential struct {
	TenantID string `json:"tenant_id"`
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	Password string `json:"password"`
}

var Credentials = []Credential{
	{TenantID: TenantA, UserID: UserAlice, Username: UsernameAlice, Password: PasswordAlice},
	{TenantID: TenantA, UserID: UserBob, Username: UsernameBob, Password: PasswordBob},
	{TenantID: TenantB, UserID: UserCarol, Username: UsernameCarol, Password: PasswordCarol},
}

func PasswordFor(tenantID, userID string) (string, bool) {
	for _, credential := range Credentials {
		if credential.TenantID == tenantID && credential.UserID == userID {
			return credential.Password, true
		}
	}
	return "", false
}

func SigningKey() []byte {
	sum := sha256.Sum256([]byte("authlab-local-demo-signing-key-v1"))
	return append([]byte(nil), sum[:]...)
}

func IDSeed() []byte {
	sum := sha256.Sum256([]byte("authlab-local-demo-id-seed-v1"))
	return append([]byte(nil), sum[:]...)
}

func passwordSalt(tenantID, userID string) string {
	sum := sha256.Sum256([]byte("authlab-local-demo-salt-v1\x00" + tenantID + "\x00" + userID))
	return hex.EncodeToString(sum[:])
}

// Fixture returns a fresh deterministic fixture with precomputed PBKDF2 hashes.
func Fixture() authlab.Fixture {
	fixture := authlab.Fixture{
		Tenants: []authlab.Tenant{
			{ID: TenantA, Name: "Alpha Local Tenant"},
			{ID: TenantB, Name: "Beta Local Tenant"},
		},
		SigningKey: SigningKey(),
		IDSeed:     IDSeed(),
		BaseTime:   BaseTime,
	}
	addUser := func(tenantID, userID, username, password string, roles, permissions []string) {
		salt := passwordSalt(tenantID, userID)
		saltBytes, _ := hex.DecodeString(salt)
		hash := authlab.HashPassword(password, saltBytes, PasswordIterations, sha256.Size)
		fixture.Users = append(fixture.Users, authlab.FixtureUser{
			TenantID: tenantID, UserID: userID, Username: username,
			PasswordHash: hex.EncodeToString(hash), PasswordSalt: salt,
			Iterations: PasswordIterations, Roles: roles, Permissions: permissions,
		})
	}
	addUser(TenantA, UserAlice, UsernameAlice, PasswordAlice,
		[]string{"member"}, []string{"doc:read", "doc:write", "profile:read"})
	addUser(TenantA, UserBob, UsernameBob, PasswordBob,
		[]string{"auditor"}, []string{"audit:read"})
	addUser(TenantB, UserCarol, UsernameCarol, PasswordCarol,
		[]string{"member"}, []string{"doc:read"})
	return fixture
}

type FixtureSummary struct {
	Tenants            []authlab.Tenant `json:"tenants"`
	Users              []UserSummary    `json:"users"`
	SigningKeySHA256   string           `json:"signing_key_sha256"`
	IDSeedSHA256       string           `json:"id_seed_sha256"`
	BaseTime           string           `json:"base_time"`
	PasswordIterations int              `json:"password_iterations"`
}

type UserSummary struct {
	TenantID     string   `json:"tenant_id"`
	UserID       string   `json:"user_id"`
	Username     string   `json:"username"`
	Roles        []string `json:"roles"`
	Permissions  []string `json:"permissions"`
	PasswordSalt string   `json:"password_salt"`
	PasswordHash string   `json:"password_hash"`
	Iterations   int      `json:"iterations"`
}

func Summary() FixtureSummary {
	fixture := Fixture()
	keyHash := sha256.Sum256(fixture.SigningKey)
	seedHash := sha256.Sum256(fixture.IDSeed)
	summary := FixtureSummary{
		Tenants:            fixture.Tenants,
		SigningKeySHA256:   hex.EncodeToString(keyHash[:]),
		IDSeedSHA256:       hex.EncodeToString(seedHash[:]),
		BaseTime:           fixture.BaseTime.Format(time.RFC3339),
		PasswordIterations: PasswordIterations,
	}
	for _, user := range fixture.Users {
		summary.Users = append(summary.Users, UserSummary{
			TenantID: user.TenantID, UserID: user.UserID, Username: user.Username,
			Roles: user.Roles, Permissions: user.Permissions, PasswordSalt: user.PasswordSalt,
			PasswordHash: user.PasswordHash, Iterations: user.Iterations,
		})
	}
	return summary
}

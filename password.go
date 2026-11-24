package authlab

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"strings"
)

// HashPassword derives a local password hash with PBKDF2-HMAC-SHA256. The
// implementation is dependency-free so the sample also builds on Windows
// without CGO or network access after dependencies are vendored/cached.
func HashPassword(password string, salt []byte, iterations, keyLen int) []byte {
	if iterations < 1 {
		iterations = 1
	}
	if keyLen < 1 {
		keyLen = sha256.Size
	}
	prf := hmac.New(sha256.New, []byte(password))
	out := make([]byte, 0, keyLen)
	for block := 1; len(out) < keyLen; block++ {
		prf.Reset()
		_, _ = prf.Write(salt)
		_, _ = prf.Write([]byte{byte(block >> 24), byte(block >> 16), byte(block >> 8), byte(block)})
		u := prf.Sum(nil)
		t := append([]byte(nil), u...)
		for i := 1; i < iterations; i++ {
			prf.Reset()
			_, _ = prf.Write(u)
			u = prf.Sum(nil)
			for j := range t {
				t[j] ^= u[j]
			}
		}
		out = append(out, t...)
	}
	return out[:keyLen]
}

// HashPasswordHex is a convenience wrapper for deterministic fixtures.
func HashPasswordHex(password, saltHex string, iterations int) (string, error) {
	salt, err := hex.DecodeString(saltHex)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(HashPassword(password, salt, iterations, sha256.Size)), nil
}

func verifyPassword(password string, user UserRecord) bool {
	salt, err := hex.DecodeString(user.PasswordSalt)
	if err != nil {
		return false
	}
	want, err := hex.DecodeString(user.PasswordHash)
	if err != nil {
		return false
	}
	got := HashPassword(password, salt, user.Iterations, len(want))
	return subtle.ConstantTimeCompare(got, want) == 1
}

func dummyPasswordWork(password string) {
	salt := sha256.Sum256([]byte("authlab-dummy-password-salt"))
	_ = HashPassword(password, salt[:], 20000, sha256.Size)
}

func canonicalPasswordSalt(tenantID, userID string) string {
	sum := sha256.Sum256([]byte("authlab-password-salt-v1\x00" + strings.TrimSpace(tenantID) + "\x00" + strings.TrimSpace(userID)))
	return hex.EncodeToString(sum[:])
}

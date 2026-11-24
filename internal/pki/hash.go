package pki

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"strings"
)

func SHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func SPKIHash(cert *x509.Certificate) string {
	return SHA256Hex(cert.RawSubjectPublicKeyInfo)
}

func CertHash(cert *x509.Certificate) string {
	return SHA256Hex(cert.Raw)
}

func CanonicalPin(cert *x509.Certificate) string {
	return "sha256:" + base64.StdEncoding.EncodeToString(mustHexDecode(SPKIHash(cert)))
}

func CanonicalRoot(cert *x509.Certificate) string {
	return "sha256:" + base64.StdEncoding.EncodeToString(mustHexDecode(CertHash(cert)))
}

func NormalizeHashValue(value string) (string, error) {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "sha256:")
	value = strings.TrimPrefix(value, "sha256/")
	value = strings.TrimSpace(value)

	if decoded, err := hex.DecodeString(strings.ToLower(value)); err == nil && len(decoded) == sha256.Size {
		return hex.EncodeToString(decoded), nil
	}
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if decoded, err := encoding.DecodeString(value); err == nil && len(decoded) == sha256.Size {
			return hex.EncodeToString(decoded), nil
		}
	}
	return "", errInvalidHash
}

var errInvalidHash = &hashError{}

type hashError struct{}

func (e *hashError) Error() string { return "invalid SHA-256 policy entry" }

func mustHexDecode(value string) []byte {
	decoded, _ := hex.DecodeString(value)
	return decoded
}

func hashEntryMatches(entry, computedHex string) bool {
	normalized, err := NormalizeHashValue(entry)
	if err != nil {
		return false
	}
	return normalized == computedHex
}

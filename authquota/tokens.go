package authquota

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
)

type tokenIssuer struct {
	secret []byte
}

func (t tokenIssuer) raw(kind string, parts ...string) string {
	mac := hmac.New(sha256.New, t.secret)
	mac.Write([]byte(kind))
	for _, part := range parts {
		mac.Write([]byte{0})
		mac.Write([]byte(part))
	}
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (t tokenIssuer) authCode(parts ...string) string {
	return "ac_" + t.raw("authorization-code", parts...)
}

func (t tokenIssuer) accessToken(parts ...string) string {
	return "at_" + t.raw("access-token", parts...)
}

func (t tokenIssuer) refreshToken(parts ...string) string {
	return "rt_" + t.raw("refresh-token", parts...)
}

func (t tokenIssuer) familyID(parts ...string) string {
	sum := sha256.Sum256([]byte("family:" + t.raw("family", parts...)))
	return "fam_" + hex.EncodeToString(sum[:12])
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func s256Verifier(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

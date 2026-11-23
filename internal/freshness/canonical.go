package freshness

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

const signatureDomain = "gwfresh.request.v1"

// CanonicalString is deliberately length-prefixed so field values cannot move
// across delimiter boundaries. The available wire fields are also restricted,
// but this keeps the digest rule deterministic for future field sets.
func CanonicalString(r Request) string {
	var b strings.Builder
	writePart := func(s string) {
		b.WriteString(strconv.Itoa(len(s)))
		b.WriteByte(':')
		b.WriteString(s)
		b.WriteByte('|')
	}
	b.WriteString(signatureDomain)
	b.WriteByte('|')
	writePart(strconv.FormatInt(r.TimestampMS, 10))
	writePart(r.RouteKey)
	writePart(r.ClientID)
	writePart(r.Nonce)
	writePart(strconv.FormatInt(r.Weight, 10))
	writePart(strconv.FormatInt(r.ToleranceMS, 10))
	writePart(strconv.FormatInt(r.WindowMS, 10))
	return b.String()
}

func Signature(key []byte, r Request) string {
	m := hmac.New(sha256.New, key)
	_, _ = m.Write([]byte(CanonicalString(r)))
	return hex.EncodeToString(m.Sum(nil))
}

// VerifySignature returns true only for a syntactically valid, constant-time
// match of the lower-case HMAC-SHA256 digest.
func VerifySignature(key []byte, r Request) bool {
	got, err := hex.DecodeString(strings.ToLower(strings.TrimSpace(r.Signature)))
	if err != nil || len(got) != sha256.Size {
		return false
	}
	want := hmac.New(sha256.New, key)
	_, _ = want.Write([]byte(CanonicalString(r)))
	return hmac.Equal(got, want.Sum(nil))
}

func ContentHash(b ValidatedBatch) string {
	h := sha256.New()
	part := func(s string) {
		_, _ = fmt.Fprintf(h, "%d:%s|", len(s), s)
	}
	part("batch-input.v1")
	part(b.BatchID)
	part(b.Mode)
	part(strconv.Itoa(len(b.Requests)))
	for _, r := range b.Requests {
		part(r.RequestID)
		part(strconv.FormatInt(r.TimestampMS, 10))
		part(r.RouteKey)
		part(r.ClientID)
		part(r.Nonce)
		part(strings.ToLower(r.Signature))
		part(strconv.FormatInt(r.Weight, 10))
		part(strconv.FormatInt(r.ToleranceMS, 10))
		part(strconv.FormatInt(r.WindowMS, 10))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func ShortHash(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		_, _ = fmt.Fprintf(h, "%d:%s|", len(p), p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

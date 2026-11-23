package mfa

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

func hmacBytes(seed, message string) []byte {
	mac := hmac.New(sha256.New, []byte(seed))
	_, _ = mac.Write([]byte(message))
	return mac.Sum(nil)
}

func sha256Hex(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func recoveryCodeValue(seed string, index int) string {
	sum := hmacBytes(seed, "recovery:"+strconv.Itoa(index))
	return strings.ToUpper(hex.EncodeToString(sum[:6]))
}

func recoveryCodeHash(seed string, index int) string {
	return sha256Hex([]byte(recoveryCodeValue(seed, index)))
}

func totpValue(seed string, step int64) string {
	sum := hmacBytes(seed, "totp:"+strconv.FormatInt(step, 10))
	offset := int(sum[len(sum)-1] & 0x0f)
	binary := (int(sum[offset])&0x7f)<<24 |
		int(sum[offset+1])<<16 |
		int(sum[offset+2])<<8 |
		int(sum[offset+3])
	return fmt.Sprintf("%06d", binary%1_000_000)
}

func webAuthnValue(seed, attemptID, factorID, nonce string) string {
	return hex.EncodeToString(hmacBytes(seed, "webauthn:"+attemptID+":"+factorID+":"+nonce))
}

func responseHash(response string) string {
	return sha256Hex([]byte(response))
}

func wrongResponse(expected string) string {
	return hex.EncodeToString(hmacBytes(expected, "deterministic-invalid-response"))
}

func snapshotChainHash(parentHash string, version int64, contentHash string) string {
	return sha256Hex([]byte(parentHash + "\x00" + strconv.FormatInt(version, 10) + "\x00" + contentHash))
}

func stableID(prefix string, sequence int64) string {
	return fmt.Sprintf("%s-%06d", prefix, sequence)
}

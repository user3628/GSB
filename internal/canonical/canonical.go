package canonical

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

const RequestVersion = "nonceguard-request-v1"
const ConfigVersion = "nonceguard-config-v1"

var FixedSeed = []byte("nonceguard.internal-proxy-gateway.seed.v1")

func FixedSigningKey() []byte {
	sum := sha256.Sum256(append([]byte("signing-key|"), FixedSeed...))
	return sum[:]
}

type FieldSource interface {
	CanonicalField(field string) (string, error)
}

func RequestValues(requestID string, timestampMs int64, routeKey, clientID, nonce string, weight int64) map[string]string {
	return map[string]string{
		"request_id":   requestID,
		"timestamp_ms": strconv.FormatInt(timestampMs, 10),
		"route_key":    routeKey,
		"client_id":    clientID,
		"nonce":        nonce,
		"weight":       strconv.FormatInt(weight, 10),
	}
}

func FieldValue(values map[string]string, field string) (string, error) {
	switch field {
	case "request_id":
		return values["request_id"], nil
	case "timestamp_ms":
		return values["timestamp_ms"], nil
	case "route_key":
		return values["route_key"], nil
	case "client_id":
		return values["client_id"], nil
	case "nonce":
		return values["nonce"], nil
	case "weight":
		return values["weight"], nil
	default:
		return "", fmt.Errorf("unknown signature field %q", field)
	}
}

func RequestString(source FieldSource, fields []string) (string, error) {
	var b strings.Builder
	b.WriteString(RequestVersion)
	b.WriteByte('\n')
	for _, field := range fields {
		value, err := source.CanonicalField(field)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "%s=%d:%s\n", field, len(value), value)
	}
	return b.String(), nil
}

func SignRequest(source FieldSource, fields []string) (string, error) {
	canonical, err := RequestString(source, fields)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, FixedSigningKey())
	mac.Write([]byte(canonical))
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func VerifyRequest(source FieldSource, providedDigest string, fields []string) (bool, error) {
	expected, err := SignRequest(source, fields)
	if err != nil {
		return false, err
	}
	return hmac.Equal([]byte(expected), []byte(strings.ToLower(providedDigest))), nil
}

type ConfigDigestInput struct {
	Version         int64    `json:"version"`
	GeneratedAtMs   int64    `json:"generated_at_ms"`
	MinOffsetMs     int64    `json:"min_offset_ms"`
	MaxOffsetMs     int64    `json:"max_offset_ms"`
	NonceWindowMs   int64    `json:"nonce_window_ms"`
	SignatureFields []string `json:"signature_fields"`
}

func ConfigString(cfg ConfigDigestInput) string {
	return fmt.Sprintf("%s\nversion=%d\ngenerated_at_ms=%d\nmin_offset_ms=%d\nmax_offset_ms=%d\nnonce_window_ms=%d\nsignature_fields=%s\n",
		ConfigVersion,
		cfg.Version,
		cfg.GeneratedAtMs,
		cfg.MinOffsetMs,
		cfg.MaxOffsetMs,
		cfg.NonceWindowMs,
		strings.Join(cfg.SignatureFields, ","),
	)
}

func ConfigDigest(cfg ConfigDigestInput) string {
	sum := sha256.Sum256([]byte(ConfigString(cfg)))
	return hex.EncodeToString(sum[:])
}

package normalize

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"supplyaudit/internal/model"
)

const (
	sha1Name       = "sha1"
	sha256Name     = "sha256"
	sha1MaxBytes   = 20
	sha256MaxBytes = 32
)

// ParseDigest accepts algorithm-tagged truncated prefixes as well as the three
// legacy encodings used by the sample manifests: SHA-1 hex, SHA-256 base64,
// and unambiguous hex/base64 prefixes. Tagged values can represent any prefix
// length from one byte through the algorithm's nominal width.
func ParseDigest(raw string) (model.DigestView, error) {
	view := model.DigestView{Raw: raw}
	value := strings.TrimSpace(raw)
	if value == "" {
		return view, digestError("digest is empty", raw)
	}

	algorithm := ""
	payload := value
	if colon := strings.IndexByte(value, ':'); colon >= 0 {
		algorithm = strings.ToLower(strings.TrimSpace(value[:colon]))
		payload = strings.TrimSpace(value[colon+1:])
		if algorithm != sha1Name && algorithm != sha256Name {
			return view, digestError(fmt.Sprintf("unsupported digest algorithm %q", algorithm), raw)
		}
	}
	if payload == "" {
		return view, digestError("digest payload is empty", raw)
	}

	bytes, encoding, err := decodeDigestPayload(payload)
	if err != nil {
		return view, digestError(err.Error(), raw)
	}
	if algorithm == "" {
		algorithm = inferAlgorithm(encoding, len(payload))
		if algorithm == "" {
			return view, digestError("unable to infer digest algorithm from payload width", raw)
		}
	}

	maxBytes, nominalBits := digestAlgorithmBounds(algorithm)
	if len(bytes) == 0 {
		return view, digestError("digest has zero bytes", raw)
	}
	if len(bytes) > maxBytes {
		return view, digestError(fmt.Sprintf("%s digest has %d bytes, maximum is %d", algorithm, len(bytes), maxBytes), raw)
	}

	view.Algorithm = algorithm
	view.Hex = hex.EncodeToString(bytes)
	view.Base64 = base64.StdEncoding.EncodeToString(bytes)
	view.ByteLength = len(bytes)
	view.NominalBits = nominalBits
	view.EffectiveBits = len(bytes) * 8
	if view.EffectiveBits > nominalBits {
		view.EffectiveBits = nominalBits
	}
	view.Truncated = len(bytes) < maxBytes
	return view, nil
}

func digestAlgorithmBounds(algorithm string) (int, int) {
	if algorithm == sha1Name {
		return sha1MaxBytes, 160
	}
	return sha256MaxBytes, 256
}

func decodeDigestPayload(payload string) ([]byte, string, error) {
	if len(payload)%2 == 0 && isHex(payload) {
		decoded, err := hex.DecodeString(payload)
		if err != nil {
			return nil, "", fmt.Errorf("invalid hexadecimal digest")
		}
		return decoded, "hex", nil
	}

	encodings := []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	}
	for _, encoding := range encodings {
		decoded, err := encoding.DecodeString(payload)
		if err == nil && len(decoded) > 0 {
			return decoded, "base64", nil
		}
	}
	return nil, "", fmt.Errorf("digest is neither valid hex nor valid base64")
}

func inferAlgorithm(encoding string, encodedLength int) string {
	switch encoding {
	case "hex":
		if encodedLength <= sha1MaxBytes*2 {
			return sha1Name
		}
		if encodedLength <= sha256MaxBytes*2 {
			return sha256Name
		}
	case "base64":
		// 28/44 characters are the padded full widths of SHA-1/SHA-256.
		if encodedLength <= 28 {
			return sha1Name
		}
		if encodedLength <= 44 {
			return sha256Name
		}
	}
	return ""
}

func isHex(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}
	return true
}

func digestError(message, raw string) error {
	return model.NewCodeError(model.CodeDigestInvalid, message+" (raw="+raw+")", 0, 0)
}

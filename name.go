package dnsdetect

import (
	"strconv"
	"strings"
)

// NormalizeName lowercases ASCII DNS labels, removes the final root dot, and
// builds a label-length-prefixed canonical key suitable for exact matching.
func NormalizeName(name string) (display string, key string, err error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", "", newBusinessError(CodeDNSName, "", -1, "empty DNS name", nil)
	}
	if trimmed == "." {
		return ".", "\x00", nil
	}
	if strings.HasSuffix(trimmed, ".") {
		trimmed = trimmed[:len(trimmed)-1]
	}
	if trimmed == "" || strings.HasSuffix(trimmed, ".") {
		return "", "", newBusinessError(CodeDNSName, "", -1, "invalid empty DNS label", nil)
	}
	labels := strings.Split(trimmed, ".")
	var normalized strings.Builder
	var wire strings.Builder
	wireLen := 1
	for i, label := range labels {
		if label == "" {
			return "", "", newBusinessError(CodeDNSName, "", -1, "invalid empty DNS label", nil)
		}
		if len(label) > 63 {
			return "", "", newBusinessError(CodeDNSName, "", -1, "DNS label exceeds 63 bytes", nil)
		}
		lower := strings.ToLower(label)
		for j := 0; j < len(lower); j++ {
			if lower[j] >= 0x80 {
				return "", "", newBusinessError(CodeDNSName, "", -1, "non-ASCII labels must be punycode encoded", nil)
			}
		}
		if i > 0 {
			normalized.WriteByte('.')
		}
		normalized.WriteString(lower)
		wire.WriteByte(byte(len(lower)))
		wire.WriteString(lower)
		wireLen += 1 + len(lower)
		if wireLen > 255 {
			return "", "", newBusinessError(CodeDNSName, "", -1, "DNS name exceeds 255 wire bytes", nil)
		}
	}
	wire.WriteByte(0)
	return normalized.String(), wire.String(), nil
}

// RelatedName reports whether child is equal to or inside the parent DNS
// subtree. Both inputs must already be normalized.
func RelatedName(parent, child string) bool {
	if parent == child || parent == "." {
		return true
	}
	return strings.HasSuffix(child, "."+parent)
}

// ParseTXID accepts decimal, 0x-prefixed hexadecimal, and bare hexadecimal
// strings, then returns the low 32-bit unsigned view.
func ParseTXID(value string) (uint32, error) {
	v := strings.TrimSpace(value)
	if v == "" {
		return 0, newBusinessError(CodeTXID, "", -1, "empty transaction id", nil)
	}
	base := 10
	if strings.HasPrefix(v, "0x") || strings.HasPrefix(v, "0X") {
		base = 16
		v = v[2:]
	} else if strings.HasPrefix(v, "hex:") || strings.HasPrefix(v, "HEX:") {
		base = 16
		v = v[4:]
	} else {
		hasHexLetter := false
		for _, r := range v {
			if (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F') {
				hasHexLetter = true
				break
			}
		}
		if hasHexLetter {
			base = 16
		}
	}
	if v == "" {
		return 0, newBusinessError(CodeTXID, "", -1, "empty transaction id", nil)
	}
	u, err := strconv.ParseUint(v, base, 32)
	if err != nil {
		return 0, newBusinessError(CodeTXID, "", -1, "transaction id is not a 32-bit integer", err)
	}
	return uint32(u), nil
}

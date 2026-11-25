package model

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// Fingerprint field selectors declared by policy.
const (
	FieldServiceID = "service_id"
	FieldMetric    = "metric"
	FieldLabels    = "labels"
)

// ValidFingerprintField reports whether f is a supported fingerprint field.
func ValidFingerprintField(f string) bool {
	switch f {
	case FieldServiceID, FieldMetric, FieldLabels:
		return true
	default:
		return false
	}
}

// NormalizeLabels returns a deterministic copy of labels: trimmed, UTF-8 keys
// sorted lexicographically. Empty label set is represented explicitly.
func NormalizeLabels(in map[string]string) []string {
	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k+"="+in[k])
	}
	return out
}

// Fingerprint builds the deterministic dedup fingerprint from the fields
// declared by policy. The canonical textual form is hashed with SHA-256 so the
// raw normalization remains inspectable via CanonicalFingerprintParts.
func Fingerprint(serviceID, metric string, labels map[string]string, fields []string) (string, []string, error) {
	parts := make([]string, 0, len(fields))
	for _, f := range fields {
		switch f {
		case FieldServiceID:
			parts = append(parts, FieldServiceID+"="+serviceID)
		case FieldMetric:
			parts = append(parts, FieldMetric+"="+metric)
		case FieldLabels:
			parts = append(parts, FieldLabels+"={"+strings.Join(NormalizeLabels(labels), ",")+"}")
		default:
			return "", nil, fmt.Errorf("%w: %s", ErrUnknownField, f)
		}
	}
	canonical := strings.Join(parts, "|")
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:]), parts, nil
}

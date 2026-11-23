package domain

import (
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"strings"
)

var quantityRE = regexp.MustCompile(`^([+]?)([0-9]+)(?:\.([0-9]+))?\s*([\p{L}]+)?$`)

type quantityKind int

const (
	quantityBytes quantityKind = iota
	quantityDuration
)

// ParseByteQuantity parses a non-negative byte quantity. Binary SI spellings
// (KiB/MiB/GiB/TiB and K/M/G/T) use powers of 1024; KB/MB/GB/TB use powers
// of 1000. Bytes are stored internally as int64 bytes.
func ParseByteQuantity(s string) (int64, error) {
	unitScale := func(unit string) (*big.Int, bool) {
		switch strings.ToUpper(unit) {
		case "", "B", "BYTE", "BYTES":
			return big.NewInt(1), true
		case "K", "KI", "KIB":
			return pow(1024, 1), true
		case "M", "MI", "MIB":
			return pow(1024, 2), true
		case "G", "GI", "GIB":
			return pow(1024, 3), true
		case "T", "TI", "TIB":
			return pow(1024, 4), true
		case "KB":
			return pow(1000, 1), true
		case "MB":
			return pow(1000, 2), true
		case "GB":
			return pow(1000, 3), true
		case "TB":
			return pow(1000, 4), true
		default:
			return nil, false
		}
	}
	return parseQuantity(s, quantityBytes, unitScale, "byte quantity")
}

// ParseDurationUS parses a duration quantity and returns whole microseconds.
// Fractional microseconds are rejected instead of rounded so the normalization
// contract never loses precision.
func ParseDurationUS(s string) (int64, error) {
	unitScale := func(unit string) (*big.Int, bool) {
		switch strings.ToLower(unit) {
		case "", "us", "µs", "μs":
			return big.NewInt(1), true
		case "ns":
			return big.NewInt(1), true
		case "ms":
			return big.NewInt(1000), true
		case "s", "sec", "secs":
			return big.NewInt(1_000_000), true
		default:
			return nil, false
		}
	}
	parsed, err := parseQuantity(s, quantityDuration, unitScale, "duration quantity")
	if err != nil {
		return 0, err
	}
	// Nanosecond input needs a second exact conversion to microseconds.
	unit := strings.ToLower(strings.TrimSpace(unitPart(s)))
	if unit == "ns" {
		if parsed%1000 != 0 {
			return 0, fmt.Errorf("duration %q is not a whole number of microseconds", s)
		}
		return parsed / 1000, nil
	}
	return parsed, nil
}

func ParseByteQuantityRaw(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, fmt.Errorf("value is required")
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		if n < 0 {
			return 0, fmt.Errorf("value must not be negative")
		}
		return n, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, fmt.Errorf("expected integer bytes or quantity string")
	}
	return ParseByteQuantity(s)
}

func ParseDurationUSRaw(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, fmt.Errorf("value is required")
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		if n < 0 {
			return 0, fmt.Errorf("value must not be negative")
		}
		return n, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, fmt.Errorf("expected integer microseconds or duration string")
	}
	return ParseDurationUS(s)
}

// ParseLatencyLimitRaw treats numeric values as milliseconds and strings as
// explicit duration quantities.
func ParseLatencyLimitRaw(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, fmt.Errorf("value is required")
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		if n < 0 {
			return 0, fmt.Errorf("value must not be negative")
		}
		if n > (1<<63-1)/1000 {
			return 0, fmt.Errorf("millisecond value overflows microseconds")
		}
		return n * 1000, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, fmt.Errorf("expected integer milliseconds or duration string")
	}
	return ParseDurationUS(s)
}

func ParsePositiveIntRaw(raw json.RawMessage, field string) (int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, fmt.Errorf("%s is required", field)
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		if n <= 0 {
			return 0, fmt.Errorf("%s must be positive", field)
		}
		return n, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, fmt.Errorf("%s must be an integer", field)
	}
	if strings.TrimSpace(s) != s || s == "" {
		return 0, fmt.Errorf("%s must be a canonical integer", field)
	}
	z, ok := new(big.Int).SetString(s, 10)
	if !ok || !z.IsInt64() || z.Sign() <= 0 {
		return 0, fmt.Errorf("%s must be a positive int64", field)
	}
	return z.Int64(), nil
}

func ParseNonNegativeIntRaw(raw json.RawMessage, field string) (int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, fmt.Errorf("%s is required", field)
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err != nil {
		return 0, fmt.Errorf("%s must be an integer", field)
	}
	if n < 0 {
		return 0, fmt.Errorf("%s must not be negative", field)
	}
	return n, nil
}

func parseQuantity(s string, kind quantityKind, unitScale func(string) (*big.Int, bool), label string) (int64, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return 0, fmt.Errorf("empty %s", label)
	}
	if strings.HasPrefix(trimmed, "-") {
		return 0, fmt.Errorf("negative %s is not allowed", label)
	}
	m := quantityRE.FindStringSubmatch(trimmed)
	if m == nil {
		return 0, fmt.Errorf("invalid %s %q", label, s)
	}
	integerPart, fractionPart, unit := m[2], m[3], strings.TrimSpace(m[4])
	if unit == "" {
		if kind == quantityDuration {
			return 0, fmt.Errorf("duration quantity %q requires a unit", s)
		}
	}
	scale, ok := unitScale(unit)
	if !ok {
		return 0, fmt.Errorf("unsupported unit %q in %s", unit, label)
	}
	digits := integerPart + fractionPart
	n, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return 0, fmt.Errorf("invalid %s %q", label, s)
	}
	n.Mul(n, scale)
	denom := pow(10, len(fractionPart))
	rem := new(big.Int)
	n.QuoRem(n, denom, rem)
	if rem.Sign() != 0 {
		return 0, fmt.Errorf("%s %q is fractional at the internal unit", label, s)
	}
	if !n.IsInt64() {
		return 0, fmt.Errorf("%s %q overflows int64", label, s)
	}
	return n.Int64(), nil
}

func unitPart(s string) string {
	m := quantityRE.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return ""
	}
	return m[4]
}

func pow(base int64, exp int) *big.Int {
	return new(big.Int).Exp(big.NewInt(base), big.NewInt(int64(exp)), nil)
}

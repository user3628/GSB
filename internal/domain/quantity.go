package domain

import (
	"errors"
	"strconv"
	"strings"
)

// ErrQuantity is wrapped into E_SPEC_INVALID by the validation layer.
var ErrQuantity = errors.New("quantity parse error")

var binaryScale = map[string]int64{
	"Ki": 1 << 10,
	"Mi": 1 << 20,
	"Gi": 1 << 30,
	"Ti": 1 << 40,
}

// ParseMemoryBytes parses values such as "512Mi", "40Gi", "1024Ki" or a raw
// byte count into an absolute integer number of bytes. Binary (1024) scale is
// used for the IEC suffixes. Negative values and overflow are rejected.
func ParseMemoryBytes(s string) (int64, error) {
	in := strings.TrimSpace(s)
	if in == "" {
		return 0, &parseErr{reason: "empty memory quantity"}
	}
	num, suffix := splitNumberSuffix(in)
	if num == "" {
		return 0, &parseErr{reason: "missing numeric part in " + in}
	}
	value, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return 0, &parseErr{reason: "invalid integer in " + in}
	}
	if value < 0 {
		return 0, &parseErr{reason: "negative memory quantity " + in}
	}
	if suffix == "" {
		return value, nil
	}
	scale, ok := binaryScale[suffix]
	if !ok {
		return 0, &parseErr{reason: "unknown memory suffix " + suffix}
	}
	result, ok := mul64(value, scale)
	if !ok {
		return 0, &parseErr{reason: "memory overflow for " + in}
	}
	return result, nil
}

// ParseComputeMilli parses a compute share into integer thousandths.
// Accepted forms: "250m" (250 thousandths), "1" or "1000" (whole units,
// multiplied by 1000), or a decimal fraction such as "0.25" (=250m).
func ParseComputeMilli(s string) (int64, error) {
	in := strings.TrimSpace(s)
	if in == "" {
		return 0, &parseErr{reason: "empty compute quantity"}
	}
	num, suffix := splitNumberSuffix(in)
	if num == "" {
		return 0, &parseErr{reason: "missing numeric part in " + in}
	}
	if suffix == "m" {
		v, err := strconv.ParseInt(num, 10, 64)
		if err != nil || v < 0 {
			return 0, &parseErr{reason: "invalid milli value " + in}
		}
		return v, nil
	}
	if suffix != "" {
		return 0, &parseErr{reason: "unknown compute suffix " + suffix}
	}
	whole, frac, isFloat := splitDecimal(num)
	wholeVal, err := strconv.ParseInt(whole, 10, 64)
	if err != nil || wholeVal < 0 {
		return 0, &parseErr{reason: "invalid compute integer " + in}
	}
	base, ok := mul64(wholeVal, 1000)
	if !ok {
		return 0, &parseErr{reason: "compute overflow for " + in}
	}
	if !isFloat {
		return base, nil
	}
	if len(frac) > 3 {
		return 0, &parseErr{reason: "compute fraction finer than 0.001 in " + in}
	}
	for len(frac) < 3 {
		frac += "0"
	}
	fracVal, err := strconv.ParseInt(frac, 10, 64)
	if err != nil || fracVal < 0 {
		return 0, &parseErr{reason: "invalid compute fraction " + in}
	}
	return base + fracVal, nil
}

type parseErr struct{ reason string }

func (e *parseErr) Error() string { return ErrQuantity.Error() + ": " + e.reason }
func (e *parseErr) Unwrap() error { return ErrQuantity }

func splitNumberSuffix(s string) (string, string) {
	i := len(s)
	for i > 0 && isLetter(s[i-1]) {
		i--
	}
	return s[:i], s[i:]
}

func splitDecimal(s string) (string, string, bool) {
	if i := strings.IndexByte(s, '.'); i >= 0 {
		return s[:i], s[i+1:], true
	}
	return s, "", false
}

func isLetter(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

// mul64 returns a*b and false when the multiplication overflows int64.
func mul64(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	r := a * b
	if r/b != a {
		return 0, false
	}
	return r, true
}

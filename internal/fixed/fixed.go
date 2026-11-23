package fixed

import (
	"errors"
	"math"
	"strings"

	"baselineworkbench/internal/codes"
)

const (
	Scale                 int64 = 1_000_000
	FiletimeTicksPerSec   int64 = 10_000_000
	FiletimeEpochSeconds  int64 = 11_644_473_600
	Filetime100nsUnitName       = "filetime_100ns"
)

var ErrSyntax = errors.New("invalid fixed-point decimal")

type MicroResult struct {
	Value          int64
	HasDiscard     bool
	ErrorUpper     int64
	FractionDigits string
}

// ParseDecimalToMicros converts a base-10 decimal string to 1e-6 fixed point
// by truncating toward zero. No floating-point operation is performed.
func ParseDecimalToMicros(s string) (MicroResult, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return MicroResult{}, ErrSyntax
	}
	negative := false
	switch s[0] {
	case '-':
		negative = true
		s = s[1:]
	case '+':
		s = s[1:]
	}
	if s == "" {
		return MicroResult{}, ErrSyntax
	}
	if strings.Count(s, ".") > 1 {
		return MicroResult{}, ErrSyntax
	}
	intPart, fracPart := s, ""
	if idx := strings.IndexByte(s, '.'); idx >= 0 {
		intPart, fracPart = s[:idx], s[idx+1:]
		if fracPart == "" {
			return MicroResult{}, ErrSyntax
		}
	}
	if intPart == "" && fracPart == "" {
		return MicroResult{}, ErrSyntax
	}
	for _, r := range intPart + fracPart {
		if r < '0' || r > '9' {
			return MicroResult{}, ErrSyntax
		}
	}
	if intPart == "" {
		intPart = "0"
	}
	var whole int64
	for i := 0; i < len(intPart); i++ {
		d := int64(intPart[i] - '0')
		if whole > (math.MaxInt64-d)/10 {
			return MicroResult{}, codes.New(codes.NumericOverflow, "integer part exceeds int64")
		}
		whole = whole*10 + d
	}
	wholeMicros, err := MulInt64(whole, Scale)
	if err != nil {
		return MicroResult{}, err
	}
	var fracMicros int64
	limit := len(fracPart)
	if limit > 6 {
		limit = 6
	}
	for i := 0; i < limit; i++ {
		fracMicros = fracMicros*10 + int64(fracPart[i]-'0')
	}
	for i := limit; i < 6; i++ {
		fracMicros *= 10
	}
	value, err := AddInt64(wholeMicros, fracMicros)
	if err != nil {
		return MicroResult{}, err
	}
	hasDiscard := false
	for i := 6; i < len(fracPart); i++ {
		if fracPart[i] != '0' {
			hasDiscard = true
			break
		}
	}
	errorUpper := int64(0)
	if hasDiscard {
		errorUpper = 1
	}
	if negative {
		value = -value
	}
	return MicroResult{
		Value:          value,
		HasDiscard:     hasDiscard,
		ErrorUpper:     errorUpper,
		FractionDigits: fracPart,
	}, nil
}

func ParseInt64Exact(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, ErrSyntax
	}
	negative := false
	switch s[0] {
	case '-':
		negative = true
		s = s[1:]
	case '+':
		s = s[1:]
	}
	if s == "" {
		return 0, ErrSyntax
	}
	var value uint64
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, ErrSyntax
		}
		d := uint64(s[i] - '0')
		if value > (math.MaxUint64-d)/10 {
			return 0, codes.New(codes.NumericOverflow, "integer literal exceeds uint64")
		}
		value = value*10 + d
	}
	if negative {
		if value > uint64(math.MaxInt64)+1 {
			return 0, codes.New(codes.NumericOverflow, "negative integer exceeds int64")
		}
		if value == uint64(math.MaxInt64)+1 {
			return math.MinInt64, nil
		}
		return -int64(value), nil
	}
	if value > math.MaxInt64 {
		return 0, codes.New(codes.NumericOverflow, "integer exceeds int64")
	}
	return int64(value), nil
}

func AddInt64(a, b int64) (int64, error) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, codes.New(codes.NumericOverflow, "int64 addition overflow")
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, codes.New(codes.NumericOverflow, "int64 addition underflow")
	}
	return a + b, nil
}

func SubInt64(a, b int64) (int64, error) {
	if b > 0 && a < math.MinInt64+b {
		return 0, codes.New(codes.NumericOverflow, "int64 subtraction underflow")
	}
	if b < 0 && a > math.MaxInt64+b {
		return 0, codes.New(codes.NumericOverflow, "int64 subtraction overflow")
	}
	return a - b, nil
}

func MulInt64(a, b int64) (int64, error) {
	if a == 0 || b == 0 {
		return 0, nil
	}
	if a == math.MinInt64 && b == -1 || b == math.MinInt64 && a == -1 {
		return 0, codes.New(codes.NumericOverflow, "int64 multiplication overflow")
	}
	p := a * b
	if p/b != a {
		return 0, codes.New(codes.NumericOverflow, "int64 multiplication overflow")
	}
	return p, nil
}

func MulDivInt64(a, b, denominator int64) (quotient, remainder int64, err error) {
	if denominator <= 0 {
		return 0, 0, codes.New(codes.PolicyMalformed, "non-positive denominator")
	}
	product, err := MulInt64(a, b)
	if err != nil {
		return 0, 0, err
	}
	return product / denominator, product % denominator, nil
}

// CeilMulDivNonNegative computes ceil(a*b/denominator) for a,b >= 0.
func CeilMulDivNonNegative(a, b, denominator int64) (int64, error) {
	if a < 0 || b < 0 || denominator <= 0 {
		return 0, codes.New(codes.PolicyMalformed, "ceil muldiv requires non-negative operands")
	}
	product, err := MulInt64(a, b)
	if err != nil {
		return 0, err
	}
	q, r := product/denominator, product%denominator
	if r != 0 {
		q, err = AddInt64(q, 1)
		if err != nil {
			return 0, err
		}
	}
	return q, nil
}

func AbsInt64(v int64) (int64, error) {
	if v == math.MinInt64 {
		return 0, codes.New(codes.NumericOverflow, "absolute value exceeds int64")
	}
	if v < 0 {
		return -v, nil
	}
	return v, nil
}

// FormatMicros renders a 1e-6 fixed-point integer without floating point.
func FormatMicros(v int64) string {
	negative := v < 0
	var magnitude uint64
	if negative {
		magnitude = uint64(-(v + 1)) + 1
	} else {
		magnitude = uint64(v)
	}
	whole := magnitude / uint64(Scale)
	frac := magnitude % uint64(Scale)
	var b strings.Builder
	if negative && magnitude != 0 {
		b.WriteByte('-')
	}
	b.WriteString(uintToString(whole))
	b.WriteByte('.')
	fs := uintToString(frac)
	for i := len(fs); i < 6; i++ {
		b.WriteByte('0')
	}
	b.WriteString(fs)
	return b.String()
}

func uintToString(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

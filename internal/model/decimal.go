package model

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// DecimalValue converts either a JSON number or a JSON string into a
// canonical decimal string. Rational arithmetic is used later; binary floats
// are intentionally never used for the precision contract.
func DecimalValue(raw json.RawMessage) (string, error) {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return "", fmt.Errorf("decimal value is required")
	}
	if s[0] == '"' {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return "", fmt.Errorf("invalid decimal string: %w", err)
		}
		s = strings.TrimSpace(text)
	}
	if s == "" {
		return "", fmt.Errorf("decimal value is empty")
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return "", fmt.Errorf("invalid decimal %q", s)
	}
	if r.Sign() <= 0 {
		return "", fmt.Errorf("decimal must be positive")
	}
	return canonicalRat(r), nil
}

func ParsePositiveRat(text string) (*big.Rat, error) {
	r, ok := new(big.Rat).SetString(strings.TrimSpace(text))
	if !ok || r.Sign() <= 0 {
		return nil, fmt.Errorf("invalid positive decimal %q", text)
	}
	return r, nil
}

func canonicalRat(r *big.Rat) string {
	if r.IsInt() {
		return r.Num().String()
	}
	// big.Rat is finite for all decimal inputs. Formatting with a fixed
	// denominator keeps the stored representation deterministic.
	text := r.FloatString(18)
	text = strings.TrimRight(text, "0")
	text = strings.TrimRight(text, ".")
	if text == "" || text == "-0" {
		return "0"
	}
	return text
}

func CanonicalDecimalOrOne(raw json.RawMessage) (string, error) {
	if len(strings.TrimSpace(string(raw))) == 0 || string(raw) == "null" {
		return "1", nil
	}
	return DecimalValue(raw)
}

func DecimalToInt64(raw json.RawMessage, field string) (int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, fmt.Errorf("%s is required", field)
	}
	s := strings.TrimSpace(string(raw))
	if len(s) > 0 && s[0] == '"' {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return 0, err
		}
		s = text
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", field, err)
	}
	return n, nil
}

// RoundRatToNanos rounds an exact rational to 1e-9 units using explicit
// round-to-nearest, ties-to-even. No float conversion is involved.
func RoundRatToNanos(r *big.Rat) int64 {
	if r == nil || r.Sign() == 0 {
		return 0
	}
	num := new(big.Int).Mul(r.Num(), big.NewInt(1_000_000_000))
	den := new(big.Int).Set(r.Denom())
	q, rem := new(big.Int), new(big.Int)
	q.QuoRem(num, den, rem)
	if rem.Sign() == 0 {
		return clampBigInt(q)
	}
	absRem := new(big.Int).Abs(rem)
	twice := new(big.Int).Lsh(absRem, 1)
	cmp := twice.Cmp(den.Abs(den))
	roundAway := cmp > 0
	if cmp == 0 {
		roundAway = q.Bit(0) == 1
	}
	if roundAway {
		if num.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	return clampBigInt(q)
}

func clampBigInt(n *big.Int) int64 {
	if n.IsInt64() {
		return n.Int64()
	}
	if n.Sign() < 0 {
		return -1 << 63
	}
	return 1<<63 - 1
}

func AbsInt64(n int64) int64 {
	if n == -1<<63 {
		return 1<<63 - 1
	}
	if n < 0 {
		return -n
	}
	return n
}

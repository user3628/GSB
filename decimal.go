package convergence

import (
	"encoding/json"
	"errors"
	"math/big"
	"strconv"
	"strings"
)

// Decimal is an exact signed decimal value. Numeric drift never uses float64 arithmetic.
type Decimal struct {
	coeff big.Int
	scale int
}

func NewDecimal(v int64) Decimal {
	return Decimal{coeff: *big.NewInt(v), scale: 0}.normalize()
}

func ParseDecimal(raw string) (Decimal, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return Decimal{}, errors.New("empty decimal")
	}

	neg := false
	if s[0] == '+' || s[0] == '-' {
		neg = s[0] == '-'
		s = s[1:]
	}

	exp := 0
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		var err error
		exp, err = parseIntStrict(s[i+1:])
		if err != nil {
			return Decimal{}, err
		}
		s = s[:i]
	}

	scale := 0
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		frac := s[dot+1:]
		if strings.ContainsRune(frac, '.') {
			return Decimal{}, errors.New("invalid decimal")
		}
		scale = len(frac)
		s = s[:dot] + frac
	}
	if s == "" {
		return Decimal{}, errors.New("invalid decimal")
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return Decimal{}, errors.New("invalid decimal digit")
		}
	}

	coeff, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return Decimal{}, errors.New("invalid decimal coefficient")
	}
	if neg {
		coeff.Neg(coeff)
	}

	scale -= exp
	for scale < 0 {
		coeff.Mul(coeff, big.NewInt(10))
		scale++
	}
	if scale > 12 {
		// Bound pathological inputs while retaining more precision than ordinary config fields need.
		pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale-12)), nil)
		coeff.Quo(coeff, pow)
		scale = 12
	}
	return Decimal{coeff: *coeff, scale: scale}.normalize(), nil
}

func parseIntStrict(s string) (int, error) {
	if s == "" {
		return 0, errors.New("invalid exponent")
	}
	neg := false
	if s[0] == '+' || s[0] == '-' {
		neg = s[0] == '-'
		s = s[1:]
	}
	if s == "" {
		return 0, errors.New("invalid exponent")
	}
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, errors.New("invalid exponent digit")
		}
		n = n*10 + int(r-'0')
		if n > 1_000_000 {
			return 0, errors.New("exponent overflow")
		}
	}
	if neg {
		n = -n
	}
	return n, nil
}

func (d Decimal) normalize() Decimal {
	ten := big.NewInt(10)
	zero := big.NewInt(0)
	for d.scale > 0 {
		mod := new(big.Int).Mod(&d.coeff, ten)
		if mod.Cmp(zero) != 0 {
			break
		}
		d.coeff.Quo(&d.coeff, ten)
		d.scale--
	}
	return d
}

func (d Decimal) String() string {
	sign := ""
	coeff := new(big.Int).Set(&d.coeff)
	if coeff.Sign() < 0 {
		sign = "-"
		coeff.Abs(coeff)
	}
	digits := coeff.String()
	if d.scale == 0 {
		return sign + digits
	}
	if len(digits) <= d.scale {
		digits = strings.Repeat("0", d.scale-len(digits)+1) + digits
	}
	dot := len(digits) - d.scale
	return sign + digits[:dot] + "." + digits[dot:]
}

func (d Decimal) MarshalJSON() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d *Decimal) UnmarshalJSON(data []byte) error {
	var raw any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	switch value := raw.(type) {
	case string:
		parsed, err := ParseDecimal(value)
		if err != nil {
			return err
		}
		*d = parsed
	case float64:
		parsed, err := ParseDecimal(json.Number(data).String())
		if err != nil {
			return err
		}
		*d = parsed
	default:
		return errors.New("decimal must be a JSON number or decimal string")
	}
	return nil
}

func (d Decimal) Rat() *big.Rat {
	denom := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d.scale)), nil)
	return new(big.Rat).SetFrac(new(big.Int).Set(&d.coeff), denom)
}

func decimalFromValue(v any) (Decimal, bool) {
	switch value := v.(type) {
	case Decimal:
		return value, true
	case json.Number:
		d, err := ParseDecimal(value.String())
		return d, err == nil
	case string:
		return Decimal{}, false
	case int:
		return NewDecimal(int64(value)), true
	case int8:
		return NewDecimal(int64(value)), true
	case int16:
		return NewDecimal(int64(value)), true
	case int32:
		return NewDecimal(int64(value)), true
	case int64:
		return NewDecimal(value), true
	case uint:
		return NewDecimal(int64(value)), true
	case uint8:
		return NewDecimal(int64(value)), true
	case uint16:
		return NewDecimal(int64(value)), true
	case uint32:
		return NewDecimal(int64(value)), true
	case uint64:
		if value > uint64(^uint64(0)>>1) {
			d, err := ParseDecimal(new(big.Int).SetUint64(value).String())
			return d, err == nil
		}
		return NewDecimal(int64(value)), true
	case float32:
		d, err := ParseDecimal(formatFloat(float64(value)))
		return d, err == nil
	case float64:
		d, err := ParseDecimal(formatFloat(value))
		return d, err == nil
	default:
		return Decimal{}, false
	}
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

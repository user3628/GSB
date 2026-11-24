package ledger

import (
	"fmt"
	"math/big"
	"strings"
)

const (
	RatioDecimalPlaces = 6
	RatioScale         = int64(1_000_000)
	RoundingMode       = "ROUND_HALF_UP"
)

// RoundRatioScaled rounds num/den to six decimal places using half-up rounding.
// The returned integer is the ratio multiplied by RatioScale.
func RoundRatioScaled(num, den *big.Int) int64 {
	if den == nil || den.Sign() == 0 {
		return 0
	}
	n := new(big.Int).Mul(num, big.NewInt(RatioScale))
	q, r := new(big.Int).QuoRem(n, den, new(big.Int))
	twiceR := new(big.Int).Lsh(r, 1)
	if twiceR.Cmp(den) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	return q.Int64()
}

func FormatScaledRatio(scaled int64) string {
	negative := scaled < 0
	if negative {
		scaled = -scaled
	}
	whole := scaled / RatioScale
	frac := scaled % RatioScale
	sign := ""
	if negative {
		sign = "-"
	}
	return fmt.Sprintf("%s%d.%06d", sign, whole, frac)
}

func FormatRatio(num, den int64) string {
	if den == 0 {
		return "0.000000"
	}
	n := big.NewInt(num)
	d := big.NewInt(den)
	if den < 0 {
		n.Neg(n)
		d.Neg(d)
	}
	return FormatScaledRatio(RoundRatioScaled(n, d))
}

func BigFormatRatio(num, den *big.Int) string {
	if den == nil || den.Sign() == 0 {
		return "0.000000"
	}
	n := new(big.Int).Set(num)
	d := new(big.Int).Set(den)
	if d.Sign() < 0 {
		n.Neg(n)
		d.Neg(d)
	}
	return FormatScaledRatio(RoundRatioScaled(n, d))
}

// ParseTolerance parses a decimal tolerance such as "0.01" into an exact rational.
func ParseTolerance(value string) (*big.Rat, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return big.NewRat(1, 100), nil
	}
	rat, ok := new(big.Rat).SetString(value)
	if !ok {
		return nil, NewErrorf(CodeLedgerInvalid, "invalid tolerance %q", value)
	}
	if rat.Sign() < 0 {
		return nil, NewError(CodeLedgerInvalid, "tolerance must be non-negative")
	}
	return rat, nil
}

func MustParseTolerance(value string) *big.Rat {
	rat, err := ParseTolerance(value)
	if err != nil {
		panic(err)
	}
	return rat
}

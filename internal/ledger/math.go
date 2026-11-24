package ledger

import (
	"math/big"
	"strconv"
	"strings"
)

func fixedRatio(num, den int64, decimals int) (string, error) {
	if den == 0 {
		return "", Biz(CodeLedgerInvalid, "ratio denominator must not be zero", nil)
	}
	n := new(big.Int).SetInt64(num)
	if den < 0 {
		n.Neg(n)
		den = -den
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	n.Mul(n, scale)
	d := new(big.Int).SetInt64(den)
	q, r := new(big.Int).QuoRem(n, d, new(big.Int))
	twiceR := new(big.Int).Lsh(r, 1)
	if twiceR.Cmp(d) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	neg := q.Sign() < 0
	if neg {
		q.Abs(q)
	}
	raw := q.String()
	if decimals == 0 {
		if neg {
			return "-" + raw, nil
		}
		return raw, nil
	}
	for len(raw) <= decimals {
		raw = "0" + raw
	}
	out := raw[:len(raw)-decimals] + "." + raw[len(raw)-decimals:]
	if neg {
		out = "-" + out
	}
	return out, nil
}

func fixedRatioPPM(num, den int64) (int64, error) {
	s, err := fixedRatio(num, den, 6)
	if err != nil {
		return 0, err
	}
	// The value is a fraction. Convert to integer PPM by removing the decimal point.
	s = strings.ReplaceAll(s, ".", "")
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, internalErrorf("ratio ppm conversion failed: %v", err)
	}
	return v, nil
}

func mulDivFloor(a, b, d int64) (int64, error) {
	if d == 0 {
		return 0, Biz(CodeLedgerInvalid, "division by zero", nil)
	}
	x := new(big.Int).SetInt64(a)
	x.Mul(x, big.NewInt(b))
	x.Quo(x, big.NewInt(d))
	if !x.IsInt64() {
		return 0, Biz(CodeLedgerInvalid, "integer overflow", map[string]any{"operation": "mul_div_floor"})
	}
	return x.Int64(), nil
}

func toleranceLimitMicros(capacityMicros, thresholdPPM, tolerancePPM int64) (int64, error) {
	if capacityMicros < 0 || thresholdPPM < 0 || tolerancePPM < 0 {
		return 0, Biz(CodeLedgerInvalid, "negative tolerance inputs", nil)
	}
	n := new(big.Int).SetInt64(capacityMicros)
	n.Mul(n, big.NewInt(thresholdPPM))
	n.Mul(n, big.NewInt(1_000_000+tolerancePPM))
	n.Quo(n, big.NewInt(1_000_000_000_000))
	if !n.IsInt64() {
		return 0, Biz(CodeLedgerInvalid, "integer overflow", map[string]any{"operation": "tolerance_limit"})
	}
	return n.Int64(), nil
}

func thresholdLimitMicros(capacityMicros, thresholdPPM int64) (int64, error) {
	return mulDivFloor(capacityMicros, thresholdPPM, 1_000_000)
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

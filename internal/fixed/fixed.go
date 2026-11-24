// Package fixed implements the scale=1e6 int64 decimal contract. Every
// operation checks its mathematical result, including intermediate products,
// against int64 bounds before converting back.
package fixed

import (
	"errors"
	"math/big"

	"columnar-executor/internal/model"
)

var (
	ErrNumericOverflow = errors.New("fixed point overflow")
	ErrDivideByZero    = errors.New("fixed point divide by zero")
)

var (
	minInt64 = big.NewInt(-1 << 63)
	maxInt64 = big.NewInt(1<<63 - 1)
	scaleBig = big.NewInt(model.FixedScale)
)

func inRange(v *big.Int) bool {
	return v.Cmp(minInt64) >= 0 && v.Cmp(maxInt64) <= 0
}

func Add(a, b int64) (int64, error) {
	z := new(big.Int).Add(big.NewInt(a), big.NewInt(b))
	if !inRange(z) {
		return 0, ErrNumericOverflow
	}
	return z.Int64(), nil
}

func Sub(a, b int64) (int64, error) {
	z := new(big.Int).Sub(big.NewInt(a), big.NewInt(b))
	if !inRange(z) {
		return 0, ErrNumericOverflow
	}
	return z.Int64(), nil
}

// Mul multiplies two fixed values and rounds the scaled quotient half away from
// zero. The unrounded product is retained in big.Int while overflow is checked.
func Mul(a, b int64) (int64, error) {
	product := new(big.Int).Mul(big.NewInt(a), big.NewInt(b))
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(product, scaleBig, r)
	if r.Sign() != 0 {
		twice := new(big.Int).Abs(r)
		twice.Lsh(twice, 1)
		if twice.Cmp(scaleBig) >= 0 {
			if product.Sign() >= 0 {
				q.Add(q, big.NewInt(1))
			} else {
				q.Sub(q, big.NewInt(1))
			}
		}
	}
	if !inRange(q) {
		return 0, ErrNumericOverflow
	}
	return q.Int64(), nil
}

// Div divides fixed value a by fixed value b and returns a fixed value.
func Div(a, b int64) (int64, error) {
	if b == 0 {
		return 0, ErrDivideByZero
	}
	numer := new(big.Int).Mul(big.NewInt(a), scaleBig)
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(numer, big.NewInt(b), r)
	if r.Sign() != 0 {
		twice := new(big.Int).Abs(r)
		twice.Lsh(twice, 1)
		den := new(big.Int).Abs(big.NewInt(b))
		if twice.Cmp(den) >= 0 {
			if (a >= 0 && b > 0) || (a < 0 && b < 0) {
				q.Add(q, big.NewInt(1))
			} else {
				q.Sub(q, big.NewInt(1))
			}
		}
	}
	if !inRange(q) {
		return 0, ErrNumericOverflow
	}
	return q.Int64(), nil
}

// DivInt divides a fixed value by a scalar integer, useful for predicates such
// as price / divisor > threshold.
func DivInt(a, divisor int64) (int64, error) {
	if divisor == 0 {
		return 0, ErrDivideByZero
	}
	q := new(big.Int).Quo(big.NewInt(a), big.NewInt(divisor))
	if !inRange(q) {
		return 0, ErrNumericOverflow
	}
	return q.Int64(), nil
}

// Average returns a rounded fixed quotient and the exact signed rounding error
// in units (sum - rounded*count). Rounding is half away from zero.
func Average(sum int64, count int64) (int64, int64, error) {
	if count <= 0 {
		return 0, 0, ErrDivideByZero
	}
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(big.NewInt(sum), big.NewInt(count), r)
	if r.Sign() != 0 {
		twice := new(big.Int).Abs(r)
		twice.Lsh(twice, 1)
		if twice.Cmp(big.NewInt(count)) >= 0 {
			if sum >= 0 {
				q.Add(q, big.NewInt(1))
			} else {
				q.Sub(q, big.NewInt(1))
			}
		}
	}
	if !inRange(q) {
		return 0, 0, ErrNumericOverflow
	}
	rounded := q.Int64()
	errValue := new(big.Int).Mul(q, big.NewInt(count))
	errValue.Sub(big.NewInt(sum), errValue)
	if !inRange(errValue) {
		return 0, 0, ErrNumericOverflow
	}
	return rounded, errValue.Int64(), nil
}

// EqualRelative compares two fixed values with a relative tolerance expressed
// in parts per million. It is used by consistency checks, never by the
// execution path's canonical aggregation.
func EqualRelative(a, b, tolerancePPM int64) bool {
	if tolerancePPM < 0 {
		tolerancePPM = 0
	}
	diff := new(big.Int).Sub(big.NewInt(a), big.NewInt(b))
	diff.Abs(diff)
	aa := new(big.Int).Abs(big.NewInt(a))
	bb := new(big.Int).Abs(big.NewInt(b))
	max := aa
	if bb.Cmp(max) > 0 {
		max = bb
	}
	allowed := new(big.Int).Mul(max, big.NewInt(tolerancePPM))
	allowed.Quo(allowed, big.NewInt(1_000_000))
	return diff.Cmp(allowed) <= 0
}

func AddCheckedInto(sum *int64, value int64) error {
	next, err := Add(*sum, value)
	if err != nil {
		return err
	}
	*sum = next
	return nil
}

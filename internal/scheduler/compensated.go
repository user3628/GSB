package scheduler

import (
	"fmt"
	"math/big"
)

// compensatedInt is an exact integer Neumaier accumulator. Values in this
// workbench are whole microseconds; the compensation term is normally zero,
// but the accumulator still prevents silent int64 overflow.
type compensatedInt struct {
	sum  big.Int
	comp big.Int
}

func (a *compensatedInt) Add(term int64) {
	t := big.NewInt(term)
	next := new(big.Int).Add(&a.sum, t)
	if new(big.Int).Abs(&a.sum).Cmp(new(big.Int).Abs(t)) >= 0 {
		c := new(big.Int).Sub(&a.sum, next)
		c.Add(c, t)
		a.comp.Add(&a.comp, c)
	} else {
		c := new(big.Int).Sub(t, next)
		c.Add(c, &a.sum)
		a.comp.Add(&a.comp, c)
	}
	a.sum.Set(next)
}

func (a *compensatedInt) Result() (sum, compensation int64, err error) {
	if !a.sum.IsInt64() {
		return 0, 0, fmt.Errorf("compensated sum overflows int64")
	}
	if !a.comp.IsInt64() {
		return 0, 0, fmt.Errorf("compensated term overflows int64")
	}
	return a.sum.Int64(), a.comp.Int64(), nil
}

func checkedMul(a, b int64) (int64, error) {
	if a == 0 || b == 0 {
		return 0, nil
	}
	z := new(big.Int).Mul(big.NewInt(a), big.NewInt(b))
	if !z.IsInt64() {
		return 0, fmt.Errorf("integer multiplication overflows int64")
	}
	return z.Int64(), nil
}

func checkedAdd(a, b int64) (int64, error) {
	z := new(big.Int).Add(big.NewInt(a), big.NewInt(b))
	if !z.IsInt64() {
		return 0, fmt.Errorf("integer addition overflows int64")
	}
	return z.Int64(), nil
}

func absInt64(v int64) int64 {
	if v < 0 {
		if v == -1<<63 {
			return 1<<63 - 1
		}
		return -v
	}
	return v
}

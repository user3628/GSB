// Package fixedpoint implements the integer-only scoring arithmetic of the
// workbench. All public scores live at scale Scale = 1e-6 (one micro unit).
// Truncation remainders are kept on a finer 1e-12 ledger scale so that every
// truncated multiplication is accounted for exactly. No float is used.
package fixedpoint

import (
	"privescworkbench/internal/domain"
)

const (
	// Scale is the fixed-point unit: stored value * 1e-6 = real value.
	Scale int64 = 1_000_000
	// LedgerScale is the finer scale at which truncation remainders live.
	LedgerScale int64 = 1_000_000_000_000
	// LedgerPerMicro converts one micro unit to ledger units.
	LedgerPerMicro int64 = LedgerScale / Scale
	// MaxMicro is the largest safe micro value; products of two micros
	// must stay within int64 after division by Scale.
	MaxMicro int64 = 1 << 62
)

// Mul multiplies two micro-scaled integers and truncates toward zero.
// It returns the micro result and the exact truncation remainder expressed
// in ledger units (always non-negative for non-negative operands).
func Mul(a, b int64) (int64, int64, error) {
	if err := checkMul(a, b); err != nil {
		return 0, 0, err
	}
	prod := a * b
	q := prod / Scale
	// remainder in micro*micro units; one micro*micro = 1/1e12 real,
	// which is exactly one ledger unit.
	r := prod % Scale
	if r < 0 {
		r = -r
	}
	return q, r, nil
}

// Add adds micro-scaled integers with overflow detection.
func Add(a, b int64) (int64, error) {
	if (b > 0 && a > MaxMicro-b) || (b < 0 && a < -MaxMicro-b) {
		return 0, domain.NewError(domain.CodeScoreOverflow, "fixed-point addition overflow")
	}
	return a + b, nil
}

// NanosToMicro converts a nanosecond offset to micro units and returns the
// dropped sub-micro remainder in ledger units (1ns = 1000 ledger units).
func NanosToMicro(nanos int64) (micro int64, remLedger int64, err error) {
	if nanos < 0 {
		return 0, 0, domain.NewError(domain.CodeSampleParse, "negative time offset")
	}
	if nanos > MaxMicro {
		return 0, 0, domain.NewError(domain.CodeScoreOverflow, "time offset out of range")
	}
	micro = nanos / 1000
	remNanos := nanos % 1000
	return micro, remNanos * 1000, nil
}

// BoundMicro folds a ledger-scale remainder sum into a micro-scaled error
// bound. The bound is rounded up so it always covers the true truncation.
func BoundMicro(remLedger int64) (int64, error) {
	if remLedger < 0 {
		return 0, domain.NewError(domain.CodeScoreOverflow, "negative ledger remainder")
	}
	q := remLedger / LedgerPerMicro
	r := remLedger % LedgerPerMicro
	if r > 0 {
		q++
	}
	if q > MaxMicro {
		return 0, domain.NewError(domain.CodeScoreOverflow, "error bound overflow")
	}
	return q, nil
}

func checkMul(a, b int64) error {
	if a == 0 || b == 0 {
		return nil
	}
	if a > MaxMicro || b > MaxMicro || a < -MaxMicro || b < -MaxMicro {
		return domain.NewError(domain.CodeScoreOverflow, "fixed-point multiplicand out of range")
	}
	if a == -1<<63 || b == -1<<63 {
		return domain.NewError(domain.CodeScoreOverflow, "fixed-point multiplication overflow")
	}
	if (a > 0 && b > 0 && a > (1<<63-1)/b) ||
		(a < 0 && b < 0 && -a > (1<<63-1)/(-b)) ||
		(a > 0 && b < 0 && b < -(1<<63-1)/a) ||
		(a < 0 && b > 0 && a < -(1<<63-1)/b) {
		return domain.NewError(domain.CodeScoreOverflow, "fixed-point multiplication overflow")
	}
	return nil
}

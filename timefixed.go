package dnsdetect

import (
	"math"
	"time"
)

const (
	TicksPerSecond     int64  = 10_000_000
	NanosecondsPerTick int64  = 100
	UnixEpochFILETIME  uint64 = 116_444_736_000_000_000
)

func tickSpanExceeds(minimum, maximum, limit int64) bool {
	if maximum < minimum || limit < 0 {
		return true
	}
	return uint64(maximum)-uint64(minimum) > uint64(limit)
}

func tickDistance(a, b int64) uint64 {
	if a >= b {
		return uint64(a) - uint64(b)
	}
	return uint64(b) - uint64(a)
}

func checkedMul(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	if a == -1 && b == math.MinInt64 || b == -1 && a == math.MinInt64 {
		return 0, false
	}
	p := a * b
	if p/b != a {
		return 0, false
	}
	return p, true
}

func checkedAdd(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, false
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, false
	}
	return a + b, true
}

// TicksFromUnixParts converts Unix seconds and nanoseconds to 100 ns ticks
// using integer arithmetic only. The returned truncation is in nanoseconds and
// is always in [0, 99].
func TicksFromUnixParts(sec int64, nsec int64) (ticks int64, truncationNS int64, err error) {
	if nsec < 0 || nsec >= int64(time.Second) {
		return 0, 0, newBusinessError(CodeTimeFormat, "", -1, "nanoseconds outside [0,1e9)", nil)
	}
	base, ok := checkedMul(sec, TicksPerSecond)
	if !ok {
		return 0, 0, newBusinessError(CodeTimeOverflow, "", -1, "Unix seconds overflow int64 100ns ticks", nil)
	}
	q := nsec / NanosecondsPerTick
	ticks, ok = checkedAdd(base, q)
	if !ok {
		return 0, 0, newBusinessError(CodeTimeOverflow, "", -1, "100ns tick addition overflow", nil)
	}
	return ticks, nsec % NanosecondsPerTick, nil
}

// TicksFromRFC3339 parses an RFC3339/RFC3339Nano timestamp into fixed Unix
// 100 ns ticks. Sub-tick precision is truncated and reported separately.
func TicksFromRFC3339(value string) (ticks int64, truncationNS int64, err error) {
	t, parseErr := time.Parse(time.RFC3339Nano, value)
	if parseErr != nil {
		return 0, 0, newBusinessError(CodeTimeFormat, "", -1, "invalid RFC3339 timestamp", parseErr)
	}
	return TicksFromUnixParts(t.Unix(), int64(t.Nanosecond()))
}

// TicksFromRelativeNS converts a nanosecond offset from an observation start
// already expressed in fixed ticks. Truncation toward zero is reported.
func TicksFromRelativeNS(observationStartTicks, relativeNS int64) (ticks int64, truncationNS int64, err error) {
	q := relativeNS / NanosecondsPerTick
	r := relativeNS % NanosecondsPerTick
	if r < 0 {
		r = -r
	}
	ticks, ok := checkedAdd(observationStartTicks, q)
	if !ok {
		return 0, 0, newBusinessError(CodeTimeOverflow, "", -1, "relative nanosecond offset overflows 100ns ticks", nil)
	}
	return ticks, r, nil
}

// TicksFromFILETIME converts Windows FILETIME (100 ns units since 1601) to
// fixed Unix 100 ns ticks. It is exact and has zero truncation.
func TicksFromFILETIME(filetime uint64) (ticks int64, truncationNS int64, err error) {
	maxFiletime := uint64(math.MaxInt64) + UnixEpochFILETIME
	if filetime > maxFiletime {
		return 0, 0, newBusinessError(CodeTimeOverflow, "", -1, "FILETIME exceeds int64 Unix tick range", nil)
	}
	delta := int64(filetime) - int64(UnixEpochFILETIME)
	return delta, 0, nil
}

// FILETIMEFromTicks is the inverse of TicksFromFILETIME for deterministic
// sample generation.
func FILETIMEFromTicks(ticks int64) (uint64, error) {
	if ticks > math.MaxInt64-int64(UnixEpochFILETIME) {
		return 0, newBusinessError(CodeTimeOverflow, "", -1, "tick value cannot be represented as FILETIME", nil)
	}
	value := int64(UnixEpochFILETIME) + ticks
	if value < 0 {
		return 0, newBusinessError(CodeTimeOverflow, "", -1, "tick value predates FILETIME epoch", nil)
	}
	return uint64(value), nil
}

// TimeErrorTicks converts a truncation error in nanoseconds to a conservative
// interval radius in 100 ns ticks. Any nonzero remainder can cross a tick
// boundary, hence the ceiling is one tick.
func TimeErrorTicks(truncationNS int64) int64 {
	if truncationNS <= 0 {
		return 0
	}
	return (truncationNS + NanosecondsPerTick - 1) / NanosecondsPerTick
}

func TicksToTime(ticks int64) time.Time {
	sec := floorDiv(ticks, TicksPerSecond)
	rem := ticks - sec*TicksPerSecond
	return time.Unix(sec, rem*NanosecondsPerTick).UTC()
}

// TicksToRFC3339 renders a fixed tick as RFC3339Nano. It is provided for
// diagnostics only; reports always expose integer ticks.
func TicksToRFC3339(ticks int64) string {
	return TicksToTime(ticks).Format(time.RFC3339Nano)
}

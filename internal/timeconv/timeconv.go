// Package timeconv converts the three source timestamp encodings to a single
// canonical form: UTC nanoseconds since the Unix epoch as int64.
//
// The FILETIME conversion is the only one that can overflow int64 or risk
// precision truncation; every failure path returns E_TIME_OVERFLOW while the
// caller keeps the original value and source offset.
package timeconv

import (
	"errors"
	"strconv"
	"time"

	"forensicworkbench/internal/codes"
)

// OverflowError carries enough context for the caller to preserve the original
// timestamp value together with its source offset.
type OverflowError struct {
	Code   string
	Raw    string
	Offset int64
	Reason string
}

func (e *OverflowError) Error() string {
	return e.Code + ": " + e.Reason + " raw=" + e.Raw + " offset=" + strconv.FormatInt(e.Offset, 10)
}

// NewOverflowError builds a tagged overflow error.
func NewOverflowError(raw string, offset int64, reason string) *OverflowError {
	return &OverflowError{Code: codes.ETimeOverflow, Raw: raw, Offset: offset, Reason: reason}
}

// IsOverflow reports whether err is an E_TIME_OVERFLOW failure.
func IsOverflow(err error) bool {
	var oe *OverflowError
	return errors.As(err, &oe)
}

const (
	// 100-ns ticks between 1601-01-01 and 1970-01-01 (UTC).
	filetimeUnixEpochTicks uint64 = 116444736000000000
	// One FILETIME tick is 100 ns.
	nsPerTick uint64 = 100
	// Maximum Unix nanosecond representable in int64 (year 2262-ish).
	maxUnixNano int64 = 1<<63 - 1
	// Minimum Unix nanosecond representable in int64.
	minUnixNano int64 = -1 << 63
)

// FILETIME bounds (inclusive) whose conversion stays inside int64 nanoseconds.
// Because ticks are 100 ns there is no sub-100ns precision to truncate.
var (
	maxUnixNanoU     = uint64(maxUnixNano)
	minUnixNanoU     = uint64(1) << 63
	filetimeMaxTicks = filetimeUnixEpochTicks + maxUnixNanoU/nsPerTick
	filetimeMinTicks = filetimeUnixEpochTicks - minUnixNanoU/nsPerTick
)

// FromFiletime converts a Windows FILETIME (1601 epoch, 100-ns ticks) to UTC
// Unix nanoseconds. It rejects values whose result cannot fit in int64.
func FromFiletime(ft uint64, raw string, offset int64) (int64, error) {
	if ft > filetimeMaxTicks {
		return 0, NewOverflowError(raw, offset, "filetime exceeds max int64 unix nanoseconds")
	}
	if ft < filetimeMinTicks {
		return 0, NewOverflowError(raw, offset, "filetime precedes min int64 unix nanoseconds")
	}
	if ft >= filetimeUnixEpochTicks {
		// Both halves fit int64 by construction of the bound above.
		return int64((ft - filetimeUnixEpochTicks) * nsPerTick), nil
	}
	return -int64((filetimeUnixEpochTicks - ft) * nsPerTick), nil
}

// FromEpochMillis converts epoch milliseconds to UTC Unix nanoseconds,
// guarding against int64 overflow during the *1e6 scaling.
func FromEpochMillis(ms int64, raw string, offset int64) (int64, error) {
	if ms > maxUnixNano/int64(time.Millisecond) || ms < minUnixNano/int64(time.Millisecond) {
		return 0, NewOverflowError(raw, offset, "epoch millisecond scaling overflows int64 nanoseconds")
	}
	return ms * int64(time.Millisecond), nil
}

// FromRFC3339 parses an RFC3339 (optionally fractional) timestamp and returns
// UTC Unix nanoseconds.
func FromRFC3339(s string, offset int64) (int64, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0, NewOverflowError(s, offset, "invalid RFC3339 timestamp: "+err.Error())
	}
	return t.UTC().UnixNano(), nil
}

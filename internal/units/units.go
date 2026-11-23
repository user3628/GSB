package units

import (
	"strings"

	"baselineworkbench/internal/codes"
	"baselineworkbench/internal/fixed"
)

const (
	FamilyDuration  = "duration"
	FamilyTimestamp = "timestamp"
	FamilyScore     = "score"
)

type Step struct {
	Stage            string `json:"stage"`
	From             string `json:"from"`
	To               string `json:"to"`
	Operation        string `json:"operation"`
	Factor           int64  `json:"factor,omitempty"`
	EpochOffset      int64  `json:"epoch_offset_seconds,omitempty"`
	Quotient         int64  `json:"quotient"`
	Remainder        int64  `json:"remainder,omitempty"`
	ErrorUpperScaled int64  `json:"error_upper_scaled,omitempty"`
}

type Converted struct {
	CanonicalUnit    string `json:"canonical_unit"`
	Family           string `json:"family"`
	Raw              string `json:"raw_value"`
	Normalized       int64  `json:"normalized"`
	NormalizedScaled int64  `json:"normalized_scaled"`
	ErrorUpperScaled int64  `json:"error_upper_scaled"`
	Steps            []Step `json:"conversion_chain"`
}

func Canonical(raw string) (canonical string, family string, factor int64, ok bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "s", "sec", "secs", "second", "seconds":
		return "second", FamilyDuration, 1, true
	case "m", "min", "mins", "minute", "minutes":
		return "minute", FamilyDuration, 60, true
	case "h", "hr", "hrs", "hour", "hours":
		return "hour", FamilyDuration, 3600, true
	case "d", "day", "days":
		return "day", FamilyDuration, 86400, true
	case "filetime", "filetime_100ns", "filetime100ns":
		return "filetime", FamilyTimestamp, 1, true
	case "score", "point", "points", "microscore":
		return "score", FamilyScore, 1, true
	default:
		return "", "", 0, false
	}
}

func Convert(rawUnit, rawValue string) (Converted, *codes.BusinessError) {
	canonical, family, factor, ok := Canonical(rawUnit)
	if !ok {
		return Converted{}, &codes.BusinessError{
			Code:    codes.UnitUnknown,
			Message: "unknown unit " + strings.TrimSpace(rawUnit),
		}
	}
	switch family {
	case FamilyDuration:
		return convertDuration(canonical, factor, rawValue)
	case FamilyTimestamp:
		return convertFiletime(rawValue)
	case FamilyScore:
		return convertScore(rawValue)
	default:
		return Converted{}, codes.New(codes.UnitUnknown, "unclassified unit %q", rawUnit)
	}
}

func convertDuration(canonical string, factor int64, raw string) (Converted, *codes.BusinessError) {
	value, err := fixed.ParseInt64Exact(raw)
	if err != nil {
		return Converted{}, asBusiness(err, "duration must be an exact integer")
	}
	if value < 0 {
		return Converted{}, codes.New(codes.PolicyMalformed, "duration must be non-negative")
	}
	seconds, err := fixed.MulInt64(value, factor)
	if err != nil {
		return Converted{}, asBusiness(err, "duration unit multiplication overflow")
	}
	scaled, err := fixed.MulInt64(seconds, fixed.Scale)
	if err != nil {
		return Converted{}, asBusiness(err, "duration scaling overflow")
	}
	return Converted{
		CanonicalUnit:    canonical,
		Family:           FamilyDuration,
		Raw:              raw,
		Normalized:       seconds,
		NormalizedScaled: scaled,
		Steps: []Step{
			{
				Stage:     "unit_normalize",
				From:      canonical,
				To:        "second",
				Operation: "multiply",
				Factor:    factor,
				Quotient:  seconds,
			},
			{
				Stage:     "scale_to_1e-6",
				From:      "second",
				To:        "second_x_1e6",
				Operation: "multiply",
				Factor:    fixed.Scale,
				Quotient:  scaled,
			},
		},
	}, nil
}

func convertFiletime(raw string) (Converted, *codes.BusinessError) {
	ticks, err := fixed.ParseInt64Exact(raw)
	if err != nil {
		return Converted{}, asBusiness(err, "FILETIME value exceeds int64")
	}
	if ticks < 0 {
		return Converted{}, codes.New(codes.PolicyMalformed, "FILETIME value must be non-negative")
	}
	secondsFrom1601 := ticks / fixed.FiletimeTicksPerSec
	remainderTicks := ticks % fixed.FiletimeTicksPerSec
	utcSeconds, err := fixed.SubInt64(secondsFrom1601, fixed.FiletimeEpochSeconds)
	if err != nil {
		return Converted{}, asBusiness(err, "FILETIME UTC epoch subtraction overflow")
	}
	scaled, err := fixed.MulInt64(utcSeconds, fixed.Scale)
	if err != nil {
		return Converted{}, asBusiness(err, "FILETIME UTC second scaling overflow")
	}
	errorUpper := int64(0)
	if remainderTicks != 0 {
		errorUpper = fixed.Scale
	}
	return Converted{
		CanonicalUnit:    "filetime",
		Family:           FamilyTimestamp,
		Raw:              raw,
		Normalized:       utcSeconds,
		NormalizedScaled: scaled,
		ErrorUpperScaled: errorUpper,
		Steps: []Step{
			{
				Stage:            "filetime_to_utc_seconds",
				From:             "filetime_100ns_since_1601",
				To:               "utc_second",
				Operation:        "divide_then_epoch_subtract",
				Factor:           fixed.FiletimeTicksPerSec,
				EpochOffset:      fixed.FiletimeEpochSeconds,
				Quotient:         utcSeconds,
				Remainder:        remainderTicks,
				ErrorUpperScaled: errorUpper,
			},
			{
				Stage:     "scale_to_1e-6",
				From:      "utc_second",
				To:        "utc_second_x_1e6",
				Operation: "multiply",
				Factor:    fixed.Scale,
				Quotient:  scaled,
			},
		},
	}, nil
}

func convertScore(raw string) (Converted, *codes.BusinessError) {
	result, err := fixed.ParseDecimalToMicros(raw)
	if err != nil {
		return Converted{}, asBusiness(err, "score is not a valid fixed-point decimal")
	}
	return Converted{
		CanonicalUnit:    "score",
		Family:           FamilyScore,
		Raw:              raw,
		Normalized:       result.Value,
		NormalizedScaled: result.Value,
		ErrorUpperScaled: result.ErrorUpper,
		Steps: []Step{
			{
				Stage:            "decimal_to_fixed_1e-6",
				From:             "decimal_score",
				To:               "score_x_1e-6",
				Operation:        "truncate_to_1e-6",
				Factor:           fixed.Scale,
				Quotient:         result.Value,
				ErrorUpperScaled: result.ErrorUpper,
			},
		},
	}, nil
}

func asBusiness(err error, fallback string) *codes.BusinessError {
	if err == nil {
		return nil
	}
	var business *codes.BusinessError
	if ok := errorsAs(err, &business); ok {
		return business
	}
	return codes.New(codes.PolicyMalformed, "%s: %v", fallback, err)
}

func errorsAs(err error, target **codes.BusinessError) bool {
	for err != nil {
		if e, ok := err.(*codes.BusinessError); ok {
			*target = e
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			break
		}
		err = u.Unwrap()
	}
	return false
}

package normalize

import "auditdrift/internal/model"

type microsConversion struct {
	ms          int64
	remainder   int64
	roundedUp   bool
	errorMicros int64
}

func convertMicros(us int64, rule model.RoundingRule) microsConversion {
	ms := us / 1000
	rem := us % 1000
	if rem < 0 {
		rem = -rem
	}
	out := microsConversion{ms: ms, remainder: rem}
	switch rule {
	case model.RoundNearest:
		if rem >= 500 {
			ms++
			out.roundedUp = true
		}
	case model.RoundCeiling:
		if rem > 0 {
			ms++
			out.roundedUp = true
		}
	case model.RoundTruncate:
		fallthrough
	default:
		rule = model.RoundTruncate
	}
	out.ms = ms
	out.errorMicros = ms*1000 - us
	return out
}

func newMicrosRounding(host, baseline, format string, us int64, rule model.RoundingRule, cumulative *int64) model.RoundingInfo {
	conv := convertMicros(us, rule)
	*cumulative += conv.errorMicros
	return model.RoundingInfo{
		Host:                  host,
		BaselineID:            baseline,
		SourceFormat:          format,
		Rule:                  rule,
		SourceTimestampMicros: us,
		NormalizedTimestampMS: conv.ms,
		PrecisionFrom:         "microsecond",
		PrecisionTo:           "millisecond",
		DiscardedDigits:       3,
		RemainderMicros:       conv.remainder,
		RoundedUp:             conv.roundedUp,
		RoundingErrorMicros:   conv.errorMicros,
		CumulativeErrorMicros: *cumulative,
	}
}

func newMillisRounding(host, baseline, format string, ms int64, cumulative *int64) model.RoundingInfo {
	*cumulative += 0
	return model.RoundingInfo{
		Host:                  host,
		BaselineID:            baseline,
		SourceFormat:          format,
		Rule:                  model.RoundTruncate,
		SourceTimestampMS:     ms,
		NormalizedTimestampMS: ms,
		PrecisionFrom:         "millisecond",
		PrecisionTo:           "millisecond",
		DiscardedDigits:       0,
		CumulativeErrorMicros: *cumulative,
	}
}

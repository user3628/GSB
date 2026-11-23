package normalize

import (
	"fmt"
	"math"
	"strings"

	"meshmigrate/internal/compat"
	"meshmigrate/internal/model"
)

const (
	CodeOverflow      = "E_NORMALIZE_OVERFLOW"
	CodeInvalidPolicy = "E_INVALID_POLICY"
	CodeInvalidTrunc  = "E_INVALID_TRUNCATION_RULE"
)

type Error struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	PolicyID string `json:"policyId,omitempty"`
	Field    string `json:"field,omitempty"`
	Offset   int64  `json:"offset"`
	Line     int    `json:"line,omitempty"`
}

func (e *Error) Error() string {
	if e.PolicyID == "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: policy %s field %s: %s", e.Code, e.PolicyID, e.Field, e.Message)
}

// Normalize converts source units into the internal bps/ms model and applies
// only defaults declared by the compatibility matrix.
func Normalize(parsed *model.ParsedSnapshot, defaults compat.Defaults, truncation string) (*model.Snapshot, *Error) {
	truncation = strings.ToLower(strings.TrimSpace(truncation))
	if truncation == "" {
		truncation = model.TruncationFloor
	}
	switch truncation {
	case model.TruncationFloor, model.TruncationCeil, model.TruncationNearest:
	default:
		return nil, &Error{Code: CodeInvalidTrunc, Message: "unknown truncation rule " + truncation, Field: "truncation"}
	}
	s := &model.Snapshot{
		SnapshotVersion: parsed.SnapshotVersion,
		SnapshotID:      parsed.SnapshotID,
		GeneratedAtMs:   parsed.GeneratedAtMs,
		SourceFormat:    parsed.SourceFormat,
		FormatVersion:   parsed.FormatVersion,
		SourceDigest:    parsed.SourceDigest,
		Policies:        make([]model.Policy, 0, len(parsed.Policies)),
	}
	var cumulativeError int64
	for _, rp := range parsed.Policies {
		if rp.PolicyID == "" {
			return nil, &Error{Code: CodeInvalidPolicy, Message: "policyId is required", Field: "policyId", Offset: rp.SourceOffset, Line: rp.SourceLine}
		}
		if rp.VirtualServiceID == "" {
			return nil, &Error{Code: CodeInvalidPolicy, Message: "virtualServiceId is required", PolicyID: rp.PolicyID, Field: "virtualServiceId", Offset: rp.SourceOffset, Line: rp.SourceLine}
		}
		if rp.Match.PathPrefix == "" {
			return nil, &Error{Code: CodeInvalidPolicy, Message: "match.pathPrefix is required", PolicyID: rp.PolicyID, Field: "match.pathPrefix", Offset: rp.SourceOffset, Line: rp.SourceLine}
		}
		p := model.Policy{
			PolicyID:               rp.PolicyID,
			VirtualServiceID:       rp.VirtualServiceID,
			PolicyRevision:         rp.PolicyRevision,
			LastWriteMs:            rp.LastWriteMs,
			Match:                  rp.Match,
			SourceOffset:           rp.SourceOffset,
			SourceLine:             rp.SourceLine,
			SourceOptionalPresence: rp.Presence,
		}
		if rp.PolicyRevision > math.MaxInt64 {
			return nil, overflowErr(rp, "policyRevision", fmt.Errorf("policy revision overflows int64"))
		}
		if p.Match.Headers == nil {
			p.Match.Headers = map[string]string{}
		}
		applied := make([]string, 0, 6)
		addDefault := func(field string, val any, reason string) {
			applied = append(applied, field)
			s.DefaultDetails = append(s.DefaultDetails, model.DefaultDetail{PolicyID: rp.PolicyID, Field: field, DefaultValue: val, Reason: reason})
		}
		if rp.PolicyRevision == 0 {
			p.PolicyRevision = defaults.PolicyRevision
			addDefault("policyRevision", defaults.PolicyRevision, "legacy record omitted policyRevision")
		}
		if !rp.Presence.RateLimit {
			p.RateLimitBps = defaults.RateLimitBps
			addDefault("rateLimitBps", defaults.RateLimitBps, "legacy record omitted rate limit; compatibility default applied")
		} else {
			v, err := rateToBps(rp.RateLimitValue, rp.RateLimitUnit)
			if err != nil {
				return nil, overflowErr(rp, "rateLimit", err)
			}
			p.RateLimitBps = v
		}
		if !rp.Presence.Timeout {
			p.TimeoutMs = defaults.TimeoutMs
			addDefault("timeoutMs", defaults.TimeoutMs, "legacy record omitted timeout; compatibility default applied")
		} else {
			v, err := durationToMs(rp.TimeoutValue, rp.TimeoutUnit, truncation)
			if err != nil {
				return nil, overflowErr(rp, "timeout", err)
			}
			p.TimeoutMs = v
		}
		if !rp.Presence.CircuitWindow {
			p.CircuitWindowMs = defaults.CircuitWindowMs
			addDefault("circuitWindowMs", defaults.CircuitWindowMs, "legacy record omitted circuit window; compatibility default applied")
		} else {
			v, err := durationToMs(rp.CircuitWindowVal, rp.CircuitWindowUnit, truncation)
			if err != nil {
				return nil, overflowErr(rp, "circuitWindow", err)
			}
			p.CircuitWindowMs = v
		}
		if !rp.Presence.RateWindow {
			p.RateWindowMs = defaults.RateWindowMs
			addDefault("rateWindowMs", defaults.RateWindowMs, "legacy record omitted rate window; compatibility default applied")
		} else {
			v, err := durationToMs(rp.RateWindowValue, rp.RateWindowUnit, truncation)
			if err != nil {
				return nil, overflowErr(rp, "rateWindow", err)
			}
			p.RateWindowMs = v
		}
		if !rp.Presence.WindowAlignment || rp.WindowAlignment == "" {
			p.WindowAlignment = defaults.WindowAlignment
			addDefault("windowAlignment", defaults.WindowAlignment, "legacy record omitted alignment; compatibility default applied")
		} else {
			p.WindowAlignment = rp.WindowAlignment
		}
		switch p.WindowAlignment {
		case model.AlignmentNone, model.AlignmentUp, model.AlignmentDown:
		default:
			return nil, &Error{Code: CodeInvalidPolicy, Message: "invalid windowAlignment " + p.WindowAlignment, PolicyID: rp.PolicyID, Field: "windowAlignment", Offset: rp.SourceOffset, Line: rp.SourceLine}
		}
		if p.RateWindowMs == 0 {
			return nil, &Error{Code: CodeInvalidPolicy, Message: "rateWindow must be positive", PolicyID: rp.PolicyID, Field: "rateWindow", Offset: rp.SourceOffset, Line: rp.SourceLine}
		}
		var sourceUS int64
		if rp.Presence.CapturedAt {
			switch rp.CapturedAtUnit {
			case "ms":
				if rp.CapturedAtValue > math.MaxInt64/1000 || rp.CapturedAtValue < math.MinInt64/1000 {
					return nil, overflowErr(rp, "capturedAt", fmt.Errorf("millisecond timestamp overflows microseconds"))
				}
				sourceUS = rp.CapturedAtValue * 1000
			case "us":
				sourceUS = rp.CapturedAtValue
			default:
				return nil, overflowErr(rp, "capturedAt", fmt.Errorf("unknown unit %q", rp.CapturedAtUnit))
			}
		} else {
			if parsed.GeneratedAtMs > math.MaxInt64/1000 || parsed.GeneratedAtMs < math.MinInt64/1000 {
				return nil, overflowErr(rp, "capturedAt", fmt.Errorf("snapshot generation time overflows microseconds"))
			}
			sourceUS = parsed.GeneratedAtMs * 1000
			addDefault("capturedAt", parsed.GeneratedAtMs, "legacy record omitted captured timestamp; snapshot generation time applied")
		}
		converted, remainder, roundBit, roundErr, err := convertTimestamp(sourceUS, truncation)
		if err != nil {
			return nil, overflowErr(rp, "capturedAt", err)
		}
		if (roundErr > 0 && cumulativeError > math.MaxInt64-roundErr) ||
			(roundErr < 0 && cumulativeError < math.MinInt64-roundErr) {
			return nil, overflowErr(rp, "capturedAt", fmt.Errorf("cumulative rounding error overflows int64"))
		}
		cumulativeError += roundErr
		p.CapturedAtMs = converted
		p.Rounding = model.Rounding{
			Rule:                 truncation,
			SourceTimestampUs:    sourceUS,
			RemainderUs:          remainder,
			RoundingBit:          roundBit,
			ErrorUs:              roundErr,
			CumulativeErrorUs:    cumulativeError,
			ConvertedTimestampMs: converted,
		}
		if p.CapturedAtMs < 0 {
			return nil, &Error{Code: CodeInvalidPolicy, Message: "captured timestamp must be non-negative", PolicyID: rp.PolicyID, Field: "capturedAt", Offset: rp.SourceOffset, Line: rp.SourceLine}
		}
		if p.RateWindowMs > math.MaxInt64 {
			return nil, overflowErr(rp, "rateWindow", fmt.Errorf("rate window overflows int64"))
		}
		start, end, err := alignWindow(p.CapturedAtMs, int64(p.RateWindowMs), p.WindowAlignment)
		if err != nil {
			return nil, &Error{Code: CodeInvalidPolicy, Message: err.Error(), PolicyID: rp.PolicyID, Field: "rateWindow", Offset: rp.SourceOffset, Line: rp.SourceLine}
		}
		p.RateWindowStartMs, p.RateWindowEndMs = start, end
		p.DefaultsApplied = applied
		s.Policies = append(s.Policies, p)
	}
	if len(s.Policies) == 0 {
		return nil, &Error{Code: CodeInvalidPolicy, Message: "snapshot must contain at least one policy"}
	}
	s.CumulativeRoundingError = cumulativeError
	if err := s.SetContentDigest(); err != nil {
		return nil, &Error{Code: CodeInvalidPolicy, Message: "compute content digest: " + err.Error()}
	}
	if err := s.Validate(); err != nil {
		return nil, &Error{Code: CodeInvalidPolicy, Message: err.Error()}
	}
	return s, nil
}

func rateToBps(v uint64, unit string) (uint64, error) {
	switch unit {
	case "bps":
		return v, nil
	case "kbps":
		if v > math.MaxUint64/1000 {
			return 0, fmt.Errorf("kbps value %d overflows bps uint64", v)
		}
		return v * 1000, nil
	default:
		return 0, fmt.Errorf("unknown rate unit %q", unit)
	}
}

func durationToMs(v uint64, unit, truncation string) (uint64, error) {
	switch unit {
	case "ms":
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("duration %d overflows int64 milliseconds", v)
		}
		return v, nil
	case "us":
		ms := v / 1000
		if v%1000 != 0 {
			switch truncation {
			case model.TruncationCeil:
				ms++
			case model.TruncationNearest:
				if v%1000 >= 500 {
					ms++
				}
			}
		}
		if ms > math.MaxInt64 {
			return 0, fmt.Errorf("duration %d microseconds overflows int64 milliseconds", v)
		}
		return ms, nil
	default:
		return 0, fmt.Errorf("unknown duration unit %q", unit)
	}
}

func convertTimestamp(us int64, rule string) (ms int64, remainder int64, bit int, errUs int64, err error) {
	if us < 0 {
		return 0, 0, 0, 0, fmt.Errorf("timestamp must be non-negative")
	}
	remainder = us % 1000
	bit = 0
	if remainder != 0 {
		bit = 1
	}
	switch rule {
	case model.TruncationFloor:
		ms = us / 1000
		errUs = ms*1000 - us
	case model.TruncationCeil:
		if remainder != 0 {
			if us > math.MaxInt64-999 {
				return 0, 0, 0, 0, fmt.Errorf("timestamp rounding overflows int64")
			}
			ms = (us + 999) / 1000
		} else {
			ms = us / 1000
		}
		errUs = ms*1000 - us
	case model.TruncationNearest:
		if us > math.MaxInt64-500 {
			return 0, 0, 0, 0, fmt.Errorf("timestamp rounding overflows int64")
		}
		ms = (us + 500) / 1000
		errUs = ms*1000 - us
	default:
		return 0, 0, 0, 0, fmt.Errorf("unknown truncation rule %q", rule)
	}
	return ms, remainder, bit, errUs, nil
}

func alignWindow(ts int64, window int64, rule string) (int64, int64, error) {
	if window <= 0 {
		return 0, 0, fmt.Errorf("window must be positive")
	}
	var start int64
	switch rule {
	case model.AlignmentNone:
		start = ts
	case model.AlignmentDown:
		start = (ts / window) * window
	case model.AlignmentUp:
		if ts%window == 0 {
			start = ts
		} else {
			if ts > math.MaxInt64-(window-1) {
				return 0, 0, fmt.Errorf("aligned window start overflows int64")
			}
			start = ((ts + window - 1) / window) * window
		}
	default:
		return 0, 0, fmt.Errorf("invalid alignment rule %q", rule)
	}
	if start > math.MaxInt64-window {
		return 0, 0, fmt.Errorf("aligned window end overflows int64")
	}
	return start, start + window, nil
}

func overflowErr(rp model.RawPolicy, field string, err error) *Error {
	code := CodeOverflow
	if strings.HasPrefix(err.Error(), "unknown ") || strings.Contains(err.Error(), "required") {
		code = CodeInvalidPolicy
	}
	return &Error{Code: code, Message: err.Error(), PolicyID: rp.PolicyID, Field: field, Offset: rp.SourceOffset, Line: rp.SourceLine}
}

package normalize

import (
	"fmt"
	"time"

	"mitmpipeline/internal/domain"
)

const (
	RoundTruncate = "truncate"
	RoundFloor    = "floor"
	RoundNearest  = "round"
	RoundCeil     = "ceil"
)

type Options struct {
	Rule string
}

func NormalizeRecordEvents(record *domain.Record, rule string) ([]domain.NormalizedEvent, []domain.RoundingInfo, error) {
	if rule == "" {
		rule = RoundTruncate
	}
	if rule == RoundFloor {
		rule = RoundTruncate
	}
	switch rule {
	case RoundTruncate, RoundNearest, RoundCeil:
	default:
		return nil, nil, domain.NewError(domain.CodeRecordCorrupt, "unsupported rounding rule").WithContext("rule", rule)
	}

	result := make([]domain.NormalizedEvent, 0, len(record.Events))
	roundings := make([]domain.RoundingInfo, 0, len(record.Events))
	var cumulative int64

	for i, event := range record.Events {
		rawNS, err := rawEventNS(record, event)
		if err != nil {
			return nil, nil, domain.WrapError(domain.CodeRecordCorrupt, err, "invalid event timestamp").
				WithContext("event_seq", event.Seq).
				WithContext("event_index", i)
		}
		rounded := RoundToMS(event.Seq, event.Type, rawNS, rule, cumulative)
		cumulative = rounded.CumulativeRoundingErrorNS

		normalized := domain.NormalizedEvent{
			Event:       event,
			RawUnixNS:   rawNS,
			TimestampMS: rounded.TimestampMS,
			Rounding:    rounded,
		}
		result = append(result, normalized)
		roundings = append(roundings, rounded)
	}
	return result, roundings, nil
}

func rawEventNS(record *domain.Record, event domain.Event) (int64, error) {
	if event.TimeRFC3339 != "" {
		parsed, err := time.Parse(time.RFC3339Nano, event.TimeRFC3339)
		if err != nil {
			return 0, err
		}
		return parsed.UnixNano(), nil
	}
	if record.ObservedStartMS <= 0 {
		return 0, fmt.Errorf("binary event requires observed_start_unix_ms")
	}
	return record.ObservedStartMS*int64(time.Millisecond/time.Nanosecond) + event.OffsetNS, nil
}

func RoundToMS(seq int, eventType string, rawNS int64, rule string, previousCumulative int64) domain.RoundingInfo {
	ms := rawNS / int64(time.Millisecond)
	remainder := rawNS % int64(time.Millisecond)
	if remainder < 0 {
		ms--
		remainder += int64(time.Millisecond)
	}

	roundingBit := 0
	if remainder >= int64(500*time.Microsecond) {
		roundingBit = 1
	}

	roundedUp := false
	outMS := ms
	switch rule {
	case RoundNearest:
		if roundingBit == 1 {
			outMS++
			roundedUp = true
		}
	case RoundCeil:
		if remainder > 0 {
			outMS++
			roundedUp = true
		}
	}

	normalizedNS := outMS * int64(time.Millisecond)
	delta := normalizedNS - rawNS
	cumulative := previousCumulative + delta

	return domain.RoundingInfo{
		EventSeq:                  seq,
		EventType:                 eventType,
		RawUnixNS:                 rawNS,
		TimestampMS:               outMS,
		RemainderNS:               remainder,
		RoundingBit:               roundingBit,
		RoundedUp:                 roundedUp,
		AppliedDeltaNS:            delta,
		CumulativeRoundingErrorNS: cumulative,
	}
}

func PolicyEffectiveAtMS(entry domain.PolicyTimelineEntry, fallbackIndex int, events []domain.NormalizedEvent) (int64, error) {
	if entry.EffectiveAtMS > 0 {
		return entry.EffectiveAtMS, nil
	}
	if entry.EffectiveTime != "" {
		t, err := time.Parse(time.RFC3339Nano, entry.EffectiveTime)
		if err != nil {
			return 0, err
		}
		return t.UnixMilli(), nil
	}
	if entry.EffectiveOffsetNS > 0 && len(events) > 0 {
		startMS := events[0].TimestampMS
		return startMS + entry.EffectiveOffsetNS/int64(time.Millisecond), nil
	}
	if fallbackIndex >= 0 && fallbackIndex < len(events) {
		return events[fallbackIndex].TimestampMS, nil
	}
	return 0, nil
}

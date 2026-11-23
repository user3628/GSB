package normalizer

import (
	"strconv"

	"forensicworkbench/internal/bincode"
	"forensicworkbench/internal/codes"
	"forensicworkbench/internal/timeconv"
)

// Result is the normalized outcome for one source file.
type Result struct {
	SourcePath    string
	Source        string
	FormatVersion string
	Events        []codes.Event
	Errors        []codes.ErrorRecord
	// Header-level failure (e.g. unknown version) prevents any event output.
	Fatal *codes.ErrorRecord
	// Abandoned counts structural malformed errors that an 'abort' strategy
	// chose to abandon; healthy events are always retained.
	Abandoned int
}

// Budget is invoked after every successfully normalized event; returning true
// means the session hit its event cap and must stop.
type Budget func(accepted int) bool

func timeEncOf(enc uint8) codes.TimeEncoding {
	switch enc {
	case bincode.EncRFC3339:
		return codes.TimeRFC3339
	case bincode.EncFiletime:
		return codes.TimeFiletime
	default:
		return codes.TimeEpochMS
	}
}

// normalizeTime converts a raw timestamp and records E_TIME_OVERFLOW failures
// without discarding the event. The conversion is a pure function, so repeated
// normalization yields byte-identical int64 results.
func normalizeTime(enc codes.TimeEncoding, raw string, offset int64) (*int64, *codes.ErrorRecord) {
	var (
		nano int64
		err  error
	)
	switch enc {
	case codes.TimeRFC3339:
		nano, err = timeconv.FromRFC3339(raw, offset)
	case codes.TimeFiletime:
		var ft uint64
		ft, perr := strconv.ParseUint(raw, 10, 64)
		if perr != nil {
			err = timeconv.NewOverflowError(raw, offset, "invalid filetime integer: "+perr.Error())
		} else {
			nano, err = timeconv.FromFiletime(ft, raw, offset)
		}
	default:
		var ms int64
		ms, perr := strconv.ParseInt(raw, 10, 64)
		if perr != nil {
			err = timeconv.NewOverflowError(raw, offset, "invalid epoch millis: "+perr.Error())
		} else {
			nano, err = timeconv.FromEpochMillis(ms, raw, offset)
		}
	}
	if err != nil {
		rec := &codes.ErrorRecord{
			Code: codes.ETimeOverflow, Message: err.Error(),
			Offset: offset, EndOffset: offset,
		}
		return nil, rec
	}
	return &nano, nil
}

// ParseBinary decodes a binary audit log and normalizes every record.
func ParseBinary(sourcePath string, data []byte, budget Budget) *Result {
	res := &Result{SourcePath: sourcePath, Source: codes.SourceBinary}
	pr, err := bincode.Parse(data)
	if err != nil {
		pe := err.(*bincode.ParseError)
		res.Fatal = &codes.ErrorRecord{
			SourcePath: sourcePath, Source: codes.SourceBinary,
			Code: pe.Code, Message: pe.Message, Offset: pe.Offset, EndOffset: pe.EndOffset,
		}
		return res
	}
	res.FormatVersion = bincode.FormatVersion(pr.Header.Version)
	for _, perr := range pr.Errors {
		res.Errors = append(res.Errors, codes.ErrorRecord{
			SourcePath: sourcePath, Source: codes.SourceBinary,
			Code: perr.Code, Message: perr.Message, Offset: perr.Offset, EndOffset: perr.EndOffset,
		})
	}
	for _, dr := range pr.Records {
		ev := codes.Event{
			Source:        codes.SourceBinary,
			FormatVersion: res.FormatVersion,
			SourcePath:    sourcePath,
			Offset:        dr.Offset,
			EndOffset:     dr.EndOffset,
			Seq:           dr.Seq,
			RawType:       strconv.Itoa(int(dr.Record.TypeCode)),
			Subject:       dr.Record.Subject,
			Object:        dr.Record.Object,
			SessionID:     dr.Record.SessionID,
			PID:           dr.Record.PID,
			PPID:          dr.Record.PPID,
			TimeEncoding:  timeEncOf(dr.Record.TimeEnc),
			RawTime:       dr.Record.RawTime,
			Payload:       append([]byte(nil), dr.Record.RawPayload...),
			Code:          codes.EOK,
		}
		if et, ok := bincode.NormalizeType(pr.Header.Version, dr.Record.TypeCode); ok {
			ev.Type = et
		} else {
			ev.Type = codes.EvUnknown
			ev.Code = codes.EUnknownEvent
			res.Errors = append(res.Errors, codes.ErrorRecord{
				SourcePath: sourcePath, Source: codes.SourceBinary, Code: codes.EUnknownEvent,
				Message: "unknown binary event type code " + ev.RawType,
				Offset:  dr.Offset, EndOffset: dr.EndOffset,
			})
		}
		if nano, terr := normalizeTime(ev.TimeEncoding, ev.RawTime, ev.Offset); terr != nil {
			terr.SourcePath = sourcePath
			terr.Source = codes.SourceBinary
			terr.EndOffset = ev.EndOffset
			res.Errors = append(res.Errors, *terr)
			ev.Code = codes.ETimeOverflow
		} else {
			ev.TimeUnixNano = nano
		}
		res.Events = append(res.Events, ev)
		if budget != nil && budget(len(res.Events)) {
			res.Errors = append(res.Errors, codes.ErrorRecord{
				SourcePath: sourcePath, Source: codes.SourceBinary, Code: codes.EEventBudget,
				Message: "per-session event budget exhausted",
				Offset:  ev.EndOffset, EndOffset: ev.EndOffset,
			})
			return res
		}
	}
	return res
}

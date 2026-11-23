package normalizer

import (
	"bytes"
	"encoding/json"
	"fmt"

	"forensicworkbench/internal/codes"
)

// jsonlEvent mirrors the external JSON-lines export. Fields use pointers /
// json.Number-ish types where a wrong primitive type must be detected rather
// than silently coerced.
type jsonlEvent struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
	Time      string `json:"time"` // RFC3339
	User      string `json:"user"`
	Target    string `json:"target"`
	SessionID string `json:"session_id"`
	PID       *int64 `json:"pid"`
	PPID      *int64 `json:"ppid"`
}

var jsonlTypeMap = map[string]codes.EventType{
	"PROCESS_CREATE":  codes.EvProcessCreate,
	"PROCESS_EXIT":    codes.EvProcessExit,
	"FILE_ACCESS":     codes.EvFileAccess,
	"REGISTRY_WRITE":  codes.EvRegistry,
	"NETWORK_CONNECT": codes.EvNetwork,
}

// ParseJSONL parses newline-delimited JSON events. A field with the wrong
// primitive type yields E_JSON_FIELD_TYPE and only that line is skipped.
func ParseJSONL(sourcePath string, data []byte, budget Budget) *Result {
	res := &Result{SourcePath: sourcePath, Source: codes.SourceJSONL, FormatVersion: codes.FmtJSONLV1}
	offset := int64(0)
	seq := 0
	for len(data) > 0 {
		// Include the newline in the record's byte range for accurate offsets.
		var line []byte
		var lineEnd int64
		if idx := bytes.IndexByte(data, '\n'); idx >= 0 {
			line = data[:idx]
			lineEnd = int64(idx) + 1
		} else {
			line = data
			lineEnd = int64(len(data))
		}
		trimmed := bytes.TrimSpace(line)
		start := offset + int64(len(line)-len(trimmed))
		if len(trimmed) > 0 {
			ev, skip, errRec := decodeJSONLine(sourcePath, trimmed, start, start+int64(len(trimmed)), seq)
			if errRec != nil {
				res.Errors = append(res.Errors, *errRec)
			}
			if !skip {
				res.Events = append(res.Events, ev)
				seq++
				if budget != nil && budget(len(res.Events)) {
					res.Errors = append(res.Errors, codes.ErrorRecord{
						SourcePath: sourcePath, Source: codes.SourceJSONL, Code: codes.EEventBudget,
						Message: "per-session event budget exhausted",
						Offset:  ev.EndOffset, EndOffset: ev.EndOffset,
					})
					return res
				}
			}
		}
		data = data[lineEnd:]
		offset += lineEnd
	}
	return res
}

func decodeJSONLine(sourcePath string, line []byte, start, end int64, seq int) (codes.Event, bool, *codes.ErrorRecord) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(line, &raw); err != nil {
		return codes.Event{}, true, &codes.ErrorRecord{
			SourcePath: sourcePath, Source: codes.SourceJSONL, Code: codes.EJSONFieldType,
			Message: "invalid JSON object: " + err.Error(), Offset: start, EndOffset: end,
		}
	}
	var je jsonlEvent
	if err := json.Unmarshal(line, &je); err != nil {
		return codes.Event{}, true, &codes.ErrorRecord{
			SourcePath: sourcePath, Source: codes.SourceJSONL, Code: codes.EJSONFieldType,
			Message: "field type mismatch: " + err.Error(), Offset: start, EndOffset: end,
		}
	}
	ev := codes.Event{
		Source:        codes.SourceJSONL,
		FormatVersion: codes.FmtJSONLV1,
		SourcePath:    sourcePath,
		Offset:        start,
		EndOffset:     end,
		Seq:           seq,
		RawType:       je.EventType,
		Subject:       je.User,
		Object:        je.Target,
		SessionID:     je.SessionID,
		TimeEncoding:  codes.TimeRFC3339,
		RawTime:       je.Time,
		Payload:       append([]byte(nil), line...),
		Code:          codes.EOK,
	}
	if je.PID != nil {
		ev.PID = *je.PID
	}
	if je.PPID != nil {
		ev.PPID = *je.PPID
	}
	if et, ok := jsonlTypeMap[je.EventType]; ok {
		ev.Type = et
	} else {
		ev.Type = codes.EvUnknown
		ev.Code = codes.EUnknownEvent
	}
	if nano, terr := normalizeTime(codes.TimeRFC3339, je.Time, start); terr != nil {
		terr.SourcePath = sourcePath
		terr.Source = codes.SourceJSONL
		terr.EndOffset = end
		ev.Code = codes.ETimeOverflow
		// keep event with nil time; surface the overflow error too
		return ev, false, terr
	} else {
		ev.TimeUnixNano = nano
	}
	if ev.Code == codes.EUnknownEvent {
		return ev, false, &codes.ErrorRecord{
			SourcePath: sourcePath, Source: codes.SourceJSONL, Code: codes.EUnknownEvent,
			Message: fmt.Sprintf("unknown jsonl event type %q", je.EventType), Offset: start, EndOffset: end,
		}
	}
	return ev, false, nil
}

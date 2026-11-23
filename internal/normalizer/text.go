package normalizer

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"forensicworkbench/internal/codes"
)

// Text block framing: magic "FWTX" + version byte, then records made of a
// big-endian uint32 payload length followed by that many bytes. Payloads are
// ';'-separated key=value pairs; values use Go-style quoted escaping (see
// strconv.Quote) so ';' inside values cannot break framing. Timestamps are
// epoch milliseconds.
const (
	textMagic  = "FWTX"
	textHdrLen = 5
)

var textTypeMap = map[string]codes.EventType{
	"PROCESS_CREATE":  codes.EvProcessCreate,
	"PROCESS_EXIT":    codes.EvProcessExit,
	"FILE_ACCESS":     codes.EvFileAccess,
	"REGISTRY_WRITE":  codes.EvRegistry,
	"NETWORK_CONNECT": codes.EvNetwork,
}

// ParseText decodes the legacy length-prefixed text blocks. A length prefix
// that exceeds the remaining bytes yields E_LENGTH_OVERRUN for that range and
// stops further decoding of this source (no later trustworthy boundary).
func ParseText(sourcePath string, data []byte, budget Budget) *Result {
	res := &Result{SourcePath: sourcePath, Source: codes.SourceText, FormatVersion: codes.FmtTextV1}
	if len(data) < textHdrLen || string(data[:4]) != textMagic {
		res.Fatal = &codes.ErrorRecord{
			SourcePath: sourcePath, Source: codes.SourceText, Code: codes.EBadHeader,
			Message: "bad text block magic", EndOffset: int64(len(data)),
		}
		return res
	}
	pos := textHdrLen
	seq := 0
	for pos < len(data) {
		if pos+4 > len(data) {
			res.Errors = append(res.Errors, codes.ErrorRecord{
				SourcePath: sourcePath, Source: codes.SourceText, Code: codes.ELengthOverrun,
				Message: "length prefix truncated", Offset: int64(pos), EndOffset: int64(len(data)),
			})
			break
		}
		plen := int(binary.BigEndian.Uint32(data[pos:]))
		start := pos
		end := pos + 4 + plen
		if end > len(data) {
			res.Errors = append(res.Errors, codes.ErrorRecord{
				SourcePath: sourcePath, Source: codes.SourceText, Code: codes.ELengthOverrun,
				Message: fmt.Sprintf("declared length %d exceeds remaining %d bytes", plen, len(data)-pos-4),
				Offset:  int64(start), EndOffset: int64(len(data)),
			})
			break
		}
		payload := data[pos+4 : end]
		ev, errRec := decodeTextBlock(sourcePath, payload, int64(start), int64(end), seq)
		if errRec != nil {
			res.Errors = append(res.Errors, *errRec)
		}
		res.Events = append(res.Events, ev)
		seq++
		if budget != nil && budget(len(res.Events)) {
			res.Errors = append(res.Errors, codes.ErrorRecord{
				SourcePath: sourcePath, Source: codes.SourceText, Code: codes.EEventBudget,
				Message: "per-session event budget exhausted",
				Offset:  ev.EndOffset, EndOffset: ev.EndOffset,
			})
			return res
		}
		pos = end
	}
	return res
}

func decodeTextBlock(sourcePath string, payload []byte, start, end int64, seq int) (codes.Event, *codes.ErrorRecord) {
	fields := map[string]string{}
	for _, pair := range strings.Split(string(payload), ";") {
		if pair == "" {
			continue
		}
		idx := strings.IndexByte(pair, '=')
		if idx < 0 {
			continue
		}
		key := pair[:idx]
		val, err := strconv.Unquote(pair[idx+1:])
		if err != nil {
			val = pair[idx+1:]
		}
		fields[key] = val
	}
	ev := codes.Event{
		Source:        codes.SourceText,
		FormatVersion: codes.FmtTextV1,
		SourcePath:    sourcePath,
		Offset:        start,
		EndOffset:     end,
		Seq:           seq,
		RawType:       fields["type"],
		Subject:       fields["user"],
		Object:        fields["target"],
		SessionID:     fields["session"],
		TimeEncoding:  codes.TimeEpochMS,
		RawTime:       fields["time"],
		Payload:       append([]byte(nil), payload...),
		Code:          codes.EOK,
	}
	if v, err := strconv.ParseInt(fields["pid"], 10, 64); err == nil {
		ev.PID = v
	}
	if v, err := strconv.ParseInt(fields["ppid"], 10, 64); err == nil {
		ev.PPID = v
	}
	if et, ok := textTypeMap[fields["type"]]; ok {
		ev.Type = et
	} else {
		ev.Type = codes.EvUnknown
		ev.Code = codes.EUnknownEvent
	}
	var errRec *codes.ErrorRecord
	if nano, terr := normalizeTime(codes.TimeEpochMS, fields["time"], start); terr != nil {
		terr.SourcePath = sourcePath
		terr.Source = codes.SourceText
		terr.EndOffset = end
		ev.Code = codes.ETimeOverflow
		errRec = terr
	} else {
		ev.TimeUnixNano = nano
	}
	if ev.Code == codes.EUnknownEvent {
		errRec = &codes.ErrorRecord{
			SourcePath: sourcePath, Source: codes.SourceText, Code: codes.EUnknownEvent,
			Message: "unknown text event type " + fields["type"], Offset: start, EndOffset: end,
		}
	}
	return ev, errRec
}

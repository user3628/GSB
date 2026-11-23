package normalizer

import (
	"strconv"
	"strings"
	"testing"

	"forensicworkbench/internal/bincode"
	"forensicworkbench/internal/codes"
)

func noBudget(int) bool { return false }

func TestParseBinaryUnknownEventPreserved(t *testing.T) {
	recs := []bincode.Record{
		{TypeCode: 99, TimeEnc: bincode.EncEpochMS, RawTime: "1704164645000",
			SessionID: "S", PID: 1, Subject: "u", Object: "o", RawPayload: []byte("raw")},
	}
	data, _ := bincode.Marshal(bincode.V2, 1, recs)
	res := ParseBinary("x.bin", data, noBudget)
	if len(res.Events) != 1 {
		t.Fatalf("unknown event must be retained, got %d", len(res.Events))
	}
	ev := res.Events[0]
	if ev.Type != codes.EvUnknown || ev.Code != codes.EUnknownEvent {
		t.Fatalf("expected unknown tagging, got %s/%s", ev.Type, ev.Code)
	}
	if string(ev.Payload) != "raw" {
		t.Fatalf("raw payload not retained: %q", ev.Payload)
	}
	if len(res.Errors) != 1 || res.Errors[0].Code != codes.EUnknownEvent {
		t.Fatalf("expected E_UNKNOWN_EVENT error, got %+v", res.Errors)
	}
}

func TestParseBinaryTimeOverflowKeepsEvent(t *testing.T) {
	recs := []bincode.Record{
		{TypeCode: bincode.TypeFileAccess, TimeEnc: bincode.EncFiletime,
			RawTime: "18446744073709551615", SessionID: "S", PID: 1, Object: "o"},
	}
	data, _ := bincode.Marshal(bincode.V2, 1, recs)
	res := ParseBinary("ft.bin", data, noBudget)
	if len(res.Events) != 1 {
		t.Fatal("event must survive time overflow")
	}
	if res.Events[0].TimeUnixNano != nil {
		t.Fatal("time must be nil on overflow")
	}
	if res.Errors[0].Code != codes.ETimeOverflow || res.Errors[0].Offset == 0 {
		t.Fatalf("overflow context missing: %+v", res.Errors)
	}
}

func TestParseJSONLFieldTypeAndContinuity(t *testing.T) {
	lines := []string{
		`{"event_id":"a","event_type":"FILE_ACCESS","time":"2024-01-02T03:04:05Z","user":"u","target":"t","session_id":"S","pid":1}`,
		`{"event_id":"b","event_type":"FILE_ACCESS","time":"2024-01-02T03:04:06Z","user":"u","target":"t","session_id":"S","pid":"oops"}`,
		`{"event_id":"c","event_type":"PROCESS_EXIT","time":"2024-01-02T03:04:07Z","user":"u","target":"t","session_id":"S","pid":1}`,
	}
	res := ParseJSONL("e.jsonl", []byte(strings.Join(lines, "\n")+"\n"), noBudget)
	if len(res.Events) != 2 {
		t.Fatalf("two healthy lines expected, got %d", len(res.Events))
	}
	if len(res.Errors) != 1 || res.Errors[0].Code != codes.EJSONFieldType {
		t.Fatalf("expected E_JSON_FIELD_TYPE, got %+v", res.Errors)
	}
	// Offsets must point at the damaged line only.
	if res.Errors[0].Offset <= res.Events[0].Offset {
		t.Fatal("error offset does not locate the bad line")
	}
}

func TestParseTextLengthOverrun(t *testing.T) {
	var b strings.Builder
	b.WriteString("FWTX")
	b.WriteByte(1)
	good := textBlockForTest(map[string]string{"type": "PROCESS_EXIT", "time": "1704164645000",
		"user": "u", "target": "t", "session": "S", "pid": "1", "ppid": "0"})
	b.Write([]byte(good))
	b.Write([]byte{0, 0, 0x27, 0x0F}) // 9999
	b.Write([]byte{1, 2, 3})
	res := ParseText("legacy.txt", []byte(b.String()), noBudget)
	if len(res.Events) != 1 {
		t.Fatalf("healthy block must survive, got %d events", len(res.Events))
	}
	if len(res.Errors) != 1 || res.Errors[0].Code != codes.ELengthOverrun {
		t.Fatalf("expected E_LENGTH_OVERRUN, got %+v", res.Errors)
	}
}

func TestEventBudgetStopsSession(t *testing.T) {
	var recs []bincode.Record
	for i := 0; i < 5; i++ {
		recs = append(recs, bincode.Record{
			TypeCode: bincode.TypeFileAccess, TimeEnc: bincode.EncEpochMS,
			RawTime:   strconv.FormatInt(1704164645000+int64(i)*1000, 10),
			SessionID: "S", PID: 1, Object: "o",
		})
	}
	data, _ := bincode.Marshal(bincode.V1, 1, recs)
	budget := func(accepted int) bool { return accepted >= 2 }
	res := ParseBinary("b.bin", data, budget)
	if len(res.Events) != 2 {
		t.Fatalf("budget should cap at 2, got %d", len(res.Events))
	}
	last := res.Errors[len(res.Errors)-1]
	if last.Code != codes.EEventBudget {
		t.Fatalf("expected E_EVENT_BUDGET, got %s", last.Code)
	}
}

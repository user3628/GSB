package parse

import (
	"testing"

	"flowtrace/internal/codes"
	"flowtrace/internal/model"
)

func sampleEvents() []model.Event {
	return []model.Event{
		{Seq: 1, Subject: "job-a", SrcField: "users.id_card", DstField: "dw.copy", Op: model.OpCrossTableCopy, LogicalTime: 100},
		{Seq: 2, Subject: "job-b", SrcField: "dw.copy", DstField: "ext.feed", Op: model.OpCrossServiceExport, LogicalTime: 110},
		{Seq: 3, Subject: "job-c", SrcField: "users.bank", DstField: "risk.f", Op: model.OpAggregateWrite, LogicalTime: 120},
	}
}

func TestBinaryRoundTripV1V2(t *testing.T) {
	for _, v := range []byte{Version1, Version2} {
		raw, err := EncodeBinary(v, sampleEvents())
		if err != nil {
			t.Fatalf("encode v%d: %v", v, err)
		}
		res := ParseBinary(raw)
		if len(res.Errors) != 0 {
			t.Fatalf("v%d unexpected errors: %+v", v, res.Errors)
		}
		if len(res.Events) != 3 {
			t.Fatalf("v%d got %d events", v, len(res.Events))
		}
		for i, ev := range res.Events {
			want := sampleEvents()[i]
			if ev != want {
				t.Fatalf("v%d event %d = %+v, want %+v", v, i, ev, want)
			}
		}
	}
}

func TestBinaryUnknownVersion(t *testing.T) {
	res := ParseBinary([]byte{'S', 'F', 'E', 'V', 9})
	if len(res.Errors) != 1 || res.Errors[0].Code != codes.EUnknownVersion {
		t.Fatalf("got %+v", res.Errors)
	}
	res = ParseBinary([]byte{'X', 'X', 'X', 'X', 1})
	if len(res.Errors) != 1 || res.Errors[0].Code != codes.EUnknownVersion {
		t.Fatalf("bad magic: got %+v", res.Errors)
	}
	if len(res.Events) != 0 {
		t.Fatal("error run must not produce events")
	}
}

func TestBinaryTruncated(t *testing.T) {
	raw, _ := EncodeBinary(Version1, sampleEvents())
	res := ParseBinary(raw[:len(raw)-3])
	if len(res.Errors) != 1 || res.Errors[0].Code != codes.ETruncated {
		t.Fatalf("got %+v", res.Errors)
	}
	if len(res.Events) != 2 {
		t.Fatalf("valid prefix must survive, got %d events", len(res.Events))
	}
}

func TestBinaryMissingField(t *testing.T) {
	evs := sampleEvents()
	evs[1].DstField = ""
	raw, _ := EncodeBinary(Version2, evs)
	res := ParseBinary(raw)
	if len(res.Errors) != 1 || res.Errors[0].Code != codes.EEventFieldMissing {
		t.Fatalf("got %+v", res.Errors)
	}
	if len(res.Events) != 2 {
		t.Fatalf("bad record must not pollute set, got %d", len(res.Events))
	}
}

func TestJSONL(t *testing.T) {
	data := []byte(`{"seq":1,"subject":"a","src":"users.id_card","dst":"dw.copy","op":"COPY_XTABLE","ts":100}
{"seq":2,"subject":"b","src":"dw.copy","dst":"ext.feed","op":"EXPORT_XSVC","ts":110}
{"seq":3,"subject":"c","src":"x","dst":"y","op":"NOPE","ts":120}
{"seq":4,"subject":"d","src":"x"}
not-json
`)
	res := ParseJSONL(data)
	if len(res.Events) != 2 {
		t.Fatalf("got %d events", len(res.Events))
	}
	if len(res.Errors) != 3 {
		t.Fatalf("got %d errors: %+v", len(res.Errors), res.Errors)
	}
	for _, e := range res.Errors {
		if e.Code != codes.EEventFieldMissing {
			t.Fatalf("unexpected code %s", e.Code)
		}
	}
	if res.Events[1].Op != model.OpCrossServiceExport {
		t.Fatalf("op mapping wrong: %v", res.Events[1].Op)
	}
}

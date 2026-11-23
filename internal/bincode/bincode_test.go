package bincode

import (
	"testing"

	"forensicworkbench/internal/codes"
)

func sampleRecs() []Record {
	return []Record{
		{TypeCode: TypeProcessCreate, TimeEnc: EncEpochMS, RawTime: "1704164645000",
			SessionID: "S", PID: 1, PPID: 0, Subject: "u", Object: "a.exe"},
		{TypeCode: TypeFileAccess, TimeEnc: EncRFC3339, RawTime: "2024-01-02T03:04:05Z",
			SessionID: "S", PID: 1, PPID: 0, Subject: "u", Object: "f.txt"},
	}
}

func TestRoundTripV1AndV2(t *testing.T) {
	for _, v := range []uint8{V1, V2} {
		data, err := Marshal(v, 123, sampleRecs())
		if err != nil {
			t.Fatal(err)
		}
		// Deterministic encoding.
		data2, _ := Marshal(v, 123, sampleRecs())
		if string(data) != string(data2) {
			t.Fatalf("v%d marshal not deterministic", v)
		}
		pr, err := Parse(data)
		if err != nil {
			t.Fatalf("v%d parse: %v", v, err)
		}
		if !pr.TailValid {
			t.Fatalf("v%d tail checksum invalid", v)
		}
		if pr.Header.Version != v || len(pr.Records) != 2 {
			t.Fatalf("v%d unexpected header/records: %+v n=%d", v, pr.Header, len(pr.Records))
		}
		// v2 retains PPID; v1 wire layout has no PPID field.
		if v == V2 && pr.Records[0].Record.PPID != 0 {
			t.Fatal("v2 PPID not decoded")
		}
		if pr.Records[0].Offset != int64(HeaderLen) {
			t.Fatalf("first record offset = %d want %d", pr.Records[0].Offset, HeaderLen)
		}
	}
}

func TestUnknownVersion(t *testing.T) {
	data, _ := Marshal(V1, 1, nil)
	bad := append([]byte(nil), data...)
	bad[4] = 9
	_, err := Parse(bad)
	if err == nil {
		t.Fatal("expected error for unknown version")
	}
	pe := err.(*ParseError)
	if pe.Code != codes.EUnknownBinaryVersion || pe.Offset != 4 || pe.EndOffset != 5 {
		t.Fatalf("wrong error/context: %+v", pe)
	}
}

func TestTruncatedRecord(t *testing.T) {
	data, _ := Marshal(V1, 1, sampleRecs())
	// Drop the tail and cut the last record short.
	body := append([]byte(nil), data[:len(data)-TailLen]...)
	body = body[:len(body)-3]
	pr, err := Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(pr.Errors) != 1 || pr.Errors[0].Code != codes.ETruncatedRecord {
		t.Fatalf("expected one truncation error, got %+v", pr.Errors)
	}
	// The first healthy record must survive the damage.
	if len(pr.Records) != 1 {
		t.Fatalf("expected 1 surviving record, got %d", len(pr.Records))
	}
}

func TestChecksumMismatch(t *testing.T) {
	data, _ := Marshal(V2, 1, sampleRecs())
	data[HeaderLen] ^= 0xFF // corrupt a body byte
	pr, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if pr.TailValid {
		t.Fatal("expected invalid tail after corruption")
	}
	found := false
	for _, e := range pr.Errors {
		if e.Code == codes.EChecksum {
			found = true
		}
	}
	if !found {
		t.Fatal("expected E_CHECKSUM after corruption")
	}
}

func TestNormalizeTypeSemantics(t *testing.T) {
	if et, ok := NormalizeType(V2, TypeModuleLoad); !ok || et != codes.EvModuleLoad {
		t.Fatal("v2 module load should be known")
	}
	if _, ok := NormalizeType(V1, TypeModuleLoad); ok {
		t.Fatal("module load must be unknown under v1")
	}
	if _, ok := NormalizeType(V2, 99); ok {
		t.Fatal("unknown code must be flagged")
	}
}

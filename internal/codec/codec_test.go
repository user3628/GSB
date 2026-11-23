package codec

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"strings"
	"testing"

	"meshmigrate/internal/model"
)

func codecFixture() model.ParsedSnapshot {
	return model.ParsedSnapshot{
		SnapshotVersion: 7,
		SnapshotID:      "codec-test",
		GeneratedAtMs:   1760000000000,
		FormatVersion:   2,
		Policies: []model.RawPolicy{
			{
				PolicyID: "p1", VirtualServiceID: "svc-a", PolicyRevision: 3, LastWriteMs: 10,
				Match:             model.RouteMatch{Method: "GET", PathPrefix: "/a", Headers: map[string]string{"x-k": "v"}},
				RateLimitValue:    120000,
				RateLimitUnit:     "bps",
				TimeoutValue:      1500,
				TimeoutUnit:       "ms",
				CircuitWindowVal:  30000,
				CircuitWindowUnit: "ms",
				RateWindowValue:   60000,
				RateWindowUnit:    "ms",
				CapturedAtValue:   1760000000123,
				CapturedAtUnit:    "ms",
				WindowAlignment:   model.AlignmentDown,
				Presence: model.Presence{
					RateLimit: true, Timeout: true, CircuitWindow: true,
					RateWindow: true, CapturedAt: true, WindowAlignment: true,
				},
			},
			{
				PolicyID: "p2", VirtualServiceID: "svc-b", PolicyRevision: 1, LastWriteMs: 20,
				Match:           model.RouteMatch{Method: "POST", PathPrefix: "/b"},
				TimeoutValue:    900,
				TimeoutUnit:     "ms",
				CapturedAtValue: 1760000000456,
				CapturedAtUnit:  "ms",
				Presence:        model.Presence{Timeout: true, CapturedAt: true},
			},
		},
	}
}

func TestBinaryRoundTripBothEndiannesses(t *testing.T) {
	doc := codecFixture()
	for _, big := range []bool{false, true} {
		encoded, err := EncodeBinary(doc, big)
		if err != nil {
			t.Fatalf("encode big=%v: %v", big, err)
		}
		decoded, de := Decode(encoded, model.FormatAuto)
		if de != nil {
			t.Fatalf("decode big=%v: %v", big, de)
		}
		if decoded.SourceFormat != model.FormatBinary || decoded.FormatVersion != doc.FormatVersion {
			t.Fatalf("unexpected header: %+v", decoded)
		}
		if decoded.SnapshotVersion != doc.SnapshotVersion || decoded.SnapshotID != doc.SnapshotID || decoded.PolicyCount != len(doc.Policies) {
			t.Fatalf("metadata mismatch: %+v", decoded)
		}
		if !reflect.DeepEqual(decoded.Policies[0].Match, doc.Policies[0].Match) {
			t.Fatalf("route mismatch: %+v", decoded.Policies[0].Match)
		}
		if decoded.Policies[0].RateLimitValue != 120000 || decoded.Policies[1].Presence.RateLimit {
			t.Fatalf("presence/value mismatch: %+v", decoded.Policies)
		}
		if !strings.HasPrefix(decoded.SourceDigest, "sha256:") {
			t.Fatalf("digest missing: %q", decoded.SourceDigest)
		}
	}
}

func TestBinaryCorruptionReturnsOffset(t *testing.T) {
	encoded, err := EncodeBinary(codecFixture(), false)
	if err != nil {
		t.Fatal(err)
	}
	recStart := BinaryHeaderLen + int(binary.LittleEndian.Uint32(encoded[28:32]))
	cases := []struct {
		name   string
		mutate func([]byte)
		offset int64
		frag   string
	}{
		{"header magic", func(b []byte) { b[0] ^= 0xff }, 0, "magic"},
		{"record magic", func(b []byte) { b[recStart] ^= 0x20 }, int64(recStart), "bad magic"},
		{"length prefix", func(b []byte) {
			for i := 0; i < 4; i++ {
				b[recStart+8+i] = 0xff
			}
		}, int64(recStart + 8), "length prefix"},
		{"crc32", func(b []byte) { b[recStart+14] ^= 0x01 }, int64(recStart + BinaryRecordHdrLen + int(binary.LittleEndian.Uint32(encoded[recStart+8:recStart+12]))), "CRC32 mismatch"},
		{"metadata digest", func(b []byte) { b[8] ^= 0x01 }, 32, "snapshot digest mismatch"},
		{"impossible record count", func(b []byte) {
			for i := 0; i < 4; i++ {
				b[24+i] = 0xff
			}
		}, 24, "record count"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := append([]byte(nil), encoded...)
			tc.mutate(b)
			out, de := DecodeBinary(b)
			if de == nil {
				t.Fatalf("expected corruption error, got %+v", out)
			}
			if de.Code != CodeFormatCorrupt {
				t.Fatalf("got code %s want %s", de.Code, CodeFormatCorrupt)
			}
			if de.Offset != tc.offset {
				t.Fatalf("got offset %d want %d (%s)", de.Offset, tc.offset, de.Message)
			}
			if !strings.Contains(de.Message, tc.frag) {
				t.Fatalf("message %q does not contain %q", de.Message, tc.frag)
			}
			if out != nil {
				t.Fatal("decoder returned partial output")
			}
		})
	}
}

func TestJSONLRoundTripAndWholeSnapshotDigest(t *testing.T) {
	doc := codecFixture()
	doc.SourceFormat = model.FormatJSONL
	doc.Policies[0].RateLimitValue = 120
	doc.Policies[0].RateLimitUnit = "kbps"
	encoded, err := EncodeJSONL(doc)
	if err != nil {
		t.Fatal(err)
	}
	decoded, de := DecodeJSONL(encoded)
	if de != nil {
		t.Fatalf("decode: %v", de)
	}
	if decoded.SourceFormat != model.FormatJSONL || decoded.FormatVersion != 2 || len(decoded.Policies) != 2 {
		t.Fatalf("unexpected decode: %+v", decoded)
	}
	if decoded.Policies[0].RateLimitValue != 120 || decoded.Policies[1].Presence.RateLimit {
		t.Fatalf("presence/value mismatch: %+v", decoded.Policies)
	}

	tampered := bytes.Replace(encoded, []byte(`"rateLimitKbps":120`), []byte(`"rateLimitKbps":121`), 1)
	if bytes.Equal(tampered, encoded) {
		t.Fatal("fixture did not contain rateLimitKbps")
	}
	_, de = DecodeJSONL(tampered)
	if de == nil || de.Code != CodeFormatCorrupt || de.Offset <= 0 || !strings.Contains(de.Message, "snapshot digest mismatch") {
		t.Fatalf("expected digest corruption, got %+v", de)
	}

	metadataTampered := bytes.Replace(encoded, []byte(`"snapshotVersion":7`), []byte(`"snapshotVersion":8`), 1)
	_, de = DecodeJSONL(metadataTampered)
	if de == nil || de.Code != CodeFormatCorrupt || !strings.Contains(de.Message, "snapshot digest mismatch") {
		t.Fatalf("expected metadata-bound digest corruption, got %+v", de)
	}

	markerTampered := bytes.Replace(encoded, []byte(`"optional":["capturedAtUs",`), []byte(`"optional":[`), 1)
	_, de = DecodeJSONL(markerTampered)
	if de == nil || de.Code != CodeFormatCorrupt || !strings.Contains(de.Message, "missing from optional markers") {
		t.Fatalf("expected optional marker corruption, got %+v", de)
	}
}

func TestUnknownRequiredFieldIsReported(t *testing.T) {
	doc := codecFixture()
	doc.SourceFormat = model.FormatJSONL
	doc.FormatVersion = 3
	doc.RequiredFeatures = []string{"geoAffinity"}
	doc.Policies = doc.Policies[:1]
	doc.Policies[0].ExtraFields = map[string]string{"geoAffinity": "cn-east"}
	encoded, err := EncodeJSONL(doc)
	if err != nil {
		t.Fatal(err)
	}
	decoded, de := DecodeJSONL(encoded)
	if de != nil {
		t.Fatal(de)
	}
	if !reflect.DeepEqual(decoded.RequiredFeatures, []string{"geoAffinity"}) || !reflect.DeepEqual(decoded.UnknownFields, []string{"geoAffinity"}) {
		t.Fatalf("unknown metadata not surfaced: required=%v unknown=%v", decoded.RequiredFeatures, decoded.UnknownFields)
	}
}

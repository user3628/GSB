package codec_test

import (
	"testing"

	"mitmpipeline/internal/codec"
	"mitmpipeline/internal/domain"
	"mitmpipeline/internal/normalize"
	"mitmpipeline/internal/sample"
)

func TestBinaryRoundTripAndNegotiation(t *testing.T) {
	fx := sample.NewFixture()
	for _, version := range []int{1, 2} {
		data, err := codec.EncodeBinary([]domain.Record{fx.Accepted}, version)
		if err != nil {
			t.Fatal(err)
		}
		records, decision, _, err := codec.DecodeAny(data)
		if err != nil {
			t.Fatalf("decode version %d: %v", version, err)
		}
		if len(records) != 1 {
			t.Fatalf("expected one record")
		}
		expectedAction := domain.ActionAccept
		if version == 1 {
			expectedAction = domain.ActionDegraded
			if records[0].ProtocolVersion != codec.DefaultProtocolVersion {
				t.Fatalf("expected default protocol, got %q", records[0].ProtocolVersion)
			}
		}
		if decision.Action != expectedAction {
			t.Fatalf("version %d action=%s want %s", version, decision.Action, expectedAction)
		}
		normalized, _, err := normalize.NormalizeRecordEvents(&records[0], normalize.RoundTruncate)
		if err != nil {
			t.Fatal(err)
		}
		if got := normalized[0].TimestampMS; got != fx.Accepted.ObservedStartMS+1 {
			t.Fatalf("version %d first event ms=%d want %d", version, got, fx.Accepted.ObservedStartMS+1)
		}
	}
}

func TestBinaryCRCCorruptionHasRecordIndex(t *testing.T) {
	fx := sample.NewFixture()
	data, err := codec.EncodeBinary([]domain.Record{fx.Accepted}, 2)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	_, _, _, err = codec.DecodeAny(data)
	if err == nil {
		t.Fatal("expected corrupt error")
	}
	de, ok := domain.AsError(err)
	if !ok || de.Code != domain.CodeRecordCorrupt {
		t.Fatalf("expected E_RECORD_CORRUPT, got %v", err)
	}
	if de.Context["record_index"] != 1 {
		t.Fatalf("expected record_index=1, got %#v", de.Context["record_index"])
	}
}

func TestJSONLUnsupportedRequiredField(t *testing.T) {
	fx := sample.NewFixture()
	_, _, _, err := codec.DecodeAny(fx.UnsupportedJSON)
	if err == nil {
		t.Fatal("expected unsupported error")
	}
	de, ok := domain.AsError(err)
	if !ok || de.Code != domain.CodeFormatUnsupported {
		t.Fatalf("expected E_FORMAT_UNSUPPORTED, got %v", err)
	}
}

func TestBinaryFutureVersionRejected(t *testing.T) {
	fx := sample.NewFixture()
	data, err := codec.EncodeBinary([]domain.Record{fx.Accepted}, 2)
	if err != nil {
		t.Fatal(err)
	}
	data[4] = 99
	_, _, _, err = codec.DecodeAny(data)
	if err == nil {
		t.Fatal("expected unsupported format")
	}
	de, ok := domain.AsError(err)
	if !ok || de.Code != domain.CodeFormatUnsupported {
		t.Fatalf("expected E_FORMAT_UNSUPPORTED, got %v", err)
	}
}

func TestJSONLRoundTrip(t *testing.T) {
	fx := sample.NewFixture()
	data, err := codec.EncodeJSONL([]domain.Record{fx.JSONAccepted})
	if err != nil {
		t.Fatal(err)
	}
	records, decision, _, err := codec.DecodeAny(data)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Action != domain.ActionAccept || len(records) != 1 {
		t.Fatalf("bad decision/records: %+v %d", decision, len(records))
	}
}

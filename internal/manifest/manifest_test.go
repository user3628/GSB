package manifest

import (
	"bytes"
	"encoding/binary"
	"testing"

	"depverify/internal/model"
)

func sampleComponents() []model.Component {
	return []model.Component{
		{ID: "acme/app", Version: []int{2, 4, 1}, DigestAlgo: "SHA256", Digest: []byte{1, 2, 3}, RegisteredDigest: []byte{1, 2, 3}},
		{ID: "acme/core", Version: []int{1, 7}, DigestAlgo: "SHA512", Digest: make([]byte, 16), RegisteredDigest: make([]byte, 16)},
	}
}

func TestRoundTrip(t *testing.T) {
	raw := Encode(sampleComponents())
	got, perr := Parse(bytes.NewReader(raw))
	if perr != nil {
		t.Fatalf("unexpected parse error: %v", perr)
	}
	if len(got) != 2 {
		t.Fatalf("got %d components, want 2", len(got))
	}
	if got[0].ID != "acme/app" || len(got[0].Version) != 3 || got[0].Version[2] != 1 {
		t.Fatalf("component normalization wrong: %+v", got[0])
	}
	if got[1].DigestAlgo != "SHA512" {
		t.Fatalf("algo mismatch: %s", got[1].DigestAlgo)
	}
}

func TestBadMagic(t *testing.T) {
	raw := Encode(sampleComponents())
	raw[0] = 'X'
	_, perr := Parse(bytes.NewReader(raw))
	if perr == nil || perr.Code != model.EBadMagic {
		t.Fatalf("want E_BAD_MAGIC, got %v", perr)
	}
}

func TestTruncated(t *testing.T) {
	raw := Encode(sampleComponents())
	_, perr := Parse(bytes.NewReader(raw[:len(raw)-4]))
	if perr == nil || perr.Code != model.ETruncated {
		t.Fatalf("want E_TRUNCATED, got %v", perr)
	}
	if perr.Resolved != 1 {
		t.Fatalf("want resolved=1, got %d", perr.Resolved)
	}
	if perr.Offset <= 0 {
		t.Fatalf("want positive last offset, got %d", perr.Offset)
	}
}

func TestLengthOverflow(t *testing.T) {
	raw := Encode(sampleComponents())
	evil := make([]byte, 0)
	evil = append(evil, raw[:len(Magic)]...)
	evil = append(evil, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x81)
	_, perr := Parse(bytes.NewReader(evil))
	if perr == nil || perr.Code != model.ELengthOverflow {
		t.Fatalf("want E_LENGTH_OVERFLOW, got %v", perr)
	}
}

func TestEmptyStream(t *testing.T) {
	got, perr := Parse(bytes.NewReader(nil))
	if perr == nil || perr.Code != model.ETruncated {
		t.Fatalf("want E_TRUNCATED on empty stream, got %v", perr)
	}
	if len(got) != 0 {
		t.Fatalf("want zero components, got %d", len(got))
	}
}

func TestHeaderOnlyIsValidEmptyManifest(t *testing.T) {
	got, perr := Parse(bytes.NewReader(append([]byte(nil), Magic...)))
	if perr != nil {
		t.Fatalf("header-only manifest should parse: %v", perr)
	}
	if len(got) != 0 {
		t.Fatalf("want zero components, got %d", len(got))
	}
}

func TestRecordBoundaryExceeded(t *testing.T) {
	raw := Encode(sampleComponents())
	// 在第一条记录负载尾部追加垃圾字节，破坏记录边界。
	headLen := len(Magic)
	l, n := decodeFirstLen(raw[headLen:])
	pos := headLen + n
	end := pos + int(l)
	tampered := append([]byte(nil), raw...)
	tampered = append(tampered[:end], append([]byte{0xff}, tampered[end:]...)...)
	_, perr := Parse(bytes.NewReader(tampered))
	if perr == nil {
		t.Fatal("want parse error on boundary violation")
	}
	if perr.Code != model.ELengthOverflow && perr.Code != model.ETruncated {
		t.Fatalf("want boundary error, got %s", perr.Code)
	}
}

func decodeFirstLen(buf []byte) (uint64, int) {
	return binary.Uvarint(buf)
}

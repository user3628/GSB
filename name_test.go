package dnsdetect

import (
	"encoding/hex"
	"testing"
)

func TestNameAndTXIDNormalization(t *testing.T) {
	display, key, err := NormalizeName("WWW.Example.COM.")
	if err != nil {
		t.Fatal(err)
	}
	if display != "www.example.com" {
		t.Fatalf("display = %q", display)
	}
	if got := hex.EncodeToString([]byte(key)); got != "03777777076578616d706c6503636f6d00" {
		t.Fatalf("label-length key = %s", got)
	}
	if _, rootKey, err := NormalizeName("."); err != nil || rootKey != "\x00" {
		t.Fatalf("root normalization = %q, %v", rootKey, err)
	}
	for _, test := range []struct {
		value string
		want  uint32
	}{
		{"4660", 4660},
		{"0x1234", 0x1234},
		{"hex:1234", 0x1234},
		{"deadbeef", 0xdeadbeef},
	} {
		got, err := ParseTXID(test.value)
		if err != nil || got != test.want {
			t.Fatalf("ParseTXID(%q) = %d, %v; want %d", test.value, got, err, test.want)
		}
	}
}

package dnsmsg

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"example.com/gsb/dnscache/internal/errs"
)

func TestAllRecordTypesRoundTripAndCompression(t *testing.T) {
	msg := &Message{Header: Header{ID: 0x1234, Flags: FlagQR | FlagAA | FlagRD | FlagRA}}
	msg.Questions = []Question{{Name: "www.example.test.", Type: TypeA, Class: 1}}
	msg.Answers = []RR{
		{Name: "www.example.test.", Type: TypeA, Class: 1, TTL: 60, Data: RData{Address: "192.0.2.10"}},
		{Name: "www.example.test.", Type: TypeAAAA, Class: 1, TTL: 61, Data: RData{Address: "2001:db8::10"}},
		{Name: "alias.example.test.", Type: TypeCNAME, Class: 1, TTL: 62, Data: RData{Target: "www.example.test."}},
		{Name: "example.test.", Type: TypeMX, Class: 1, TTL: 63, Data: RData{Preference: 10, Target: "mail.example.test."}},
		{Name: "txt.example.test.", Type: TypeTXT, Class: 1, TTL: 64, Data: RData{Texts: []string{"alpha", "beta"}}},
	}
	msg.Authorities = []RR{
		{Name: "example.test.", Type: TypeNS, Class: 1, TTL: 65, Data: RData{Target: "ns1.example.test."}},
		{Name: "example.test.", Type: TypeSOA, Class: 1, TTL: 30, Data: RData{SOA: &SOA{MName: "ns1.example.test.", RName: "hostmaster.example.test.", Serial: 7, Refresh: 3600, Retry: 600, Expire: 86400, Minimum: 5}}},
	}
	wire, err := msg.Pack()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(wire, []byte{0xc0, 0x0c}) {
		t.Fatalf("expected RFC1035 compression pointer in packet: %x", wire)
	}
	got, err := Parse(wire)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Answers, msg.Answers) {
		t.Fatalf("answers mismatch\n got: %#v\nwant: %#v", got.Answers, msg.Answers)
	}
	if !reflect.DeepEqual(got.Authorities, msg.Authorities) {
		t.Fatalf("authorities mismatch\n got: %#v\nwant: %#v", got.Authorities, msg.Authorities)
	}
	if got.Header.ID != msg.Header.ID {
		t.Fatalf("id=%d", got.Header.ID)
	}
}

func TestParseCompressedQuestion(t *testing.T) {
	// Two questions. The second name uses a backward pointer to the
	// "example.com." suffix of the first question.
	wire := []byte{
		0x12, 0x34, 0x01, 0x00, 0x00, 0x02, 0, 0, 0, 0, 0, 0,
		3, 'w', 'w', 'w', 7, 'e', 'x', 'a', 'm', 'p', 'l', 'e', 3, 'c', 'o', 'm', 0,
		0x00, 0x01, 0x00, 0x01,
		3, 'a', 'p', 'i', 0xc0, 0x10, 0x00, 0x1c, 0x00, 0x01,
	}
	msg, err := Parse(wire)
	if err != nil {
		t.Fatal(err)
	}
	if len(msg.Questions) != 2 {
		t.Fatalf("questions=%d", len(msg.Questions))
	}
	if msg.Questions[0].Name != "www.example.com." || msg.Questions[1].Name != "api.example.com." {
		t.Fatalf("names=%q,%q", msg.Questions[0].Name, msg.Questions[1].Name)
	}
	if msg.Questions[1].Type != TypeAAAA {
		t.Fatalf("second type=%s", msg.Questions[1].Type)
	}
}

func TestMalformedPackets(t *testing.T) {
	header := []byte{0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}
	overlong := append(append([]byte(nil), header...), 0)
	for i := 0; i < 5; i++ {
		overlong = append(overlong, 63)
		overlong = append(overlong, bytes.Repeat([]byte{'a'}, 63)...)
	}
	overlong = append(overlong, 0, 0, 1, 0, 1)
	cases := map[string][]byte{
		"short":              {1, 2, 3},
		"reserved-label":     append(append([]byte(nil), header...), 0x40, 0, 0, 1, 0, 1),
		"pointer-cycle":      append(append([]byte(nil), header...), 0xc0, 0x0c, 0, 1, 0, 1),
		"pointer-out-range":  append(append([]byte(nil), header...), 0xc0, 0xff, 0, 1, 0, 1),
		"truncated-label":    append(append([]byte(nil), header...), 5, 'a', 'b', 0, 0, 1, 0, 1),
		"expanded-name-255":  overlong,
		"trailing-bytes":     append(append([]byte(nil), header...), 0, 0, 1, 0, 1, 0xff),
		"truncated-question": append(append([]byte(nil), header...), 3, 'w', 'w', 'w', 0xc0, 0x0c),
	}
	for name, packet := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse(packet)
			if err == nil {
				t.Fatal("expected malformed error")
			}
			if got := errs.CodeOf(err); got != errs.EMessageMalformed {
				t.Fatalf("code=%s err=%v", got, err)
			}
		})
	}
}

func TestMalformedRDataLengths(t *testing.T) {
	// qd=0, an=1, name root, type A, class IN, ttl 1, rdlength 3.
	wire := []byte{
		0x12, 0x34, 0x81, 0x80, 0, 0, 0, 1, 0, 0, 0, 0,
		0, 0, 1, 0, 1, 0, 0, 0, 1, 0, 3, 1, 2, 3,
	}
	_, err := Parse(wire)
	if err == nil || errs.CodeOf(err) != errs.EMessageMalformed {
		t.Fatalf("want malformed, got %v", err)
	}
}

func TestTCPFraming(t *testing.T) {
	q := NewQuery(0xbeef, "www.example.test.", TypeA)
	var buf bytes.Buffer
	if err := WriteTCP(&buf, q); err != nil {
		t.Fatal(err)
	}
	packet, err := ReadTCP(&buf)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Parse(packet)
	if err != nil || got.Header.ID != 0xbeef {
		t.Fatalf("parse=%v msg=%#v", err, got)
	}

	_, err = ReadTCP(bytes.NewReader([]byte{0, 20, 1, 2, 3}))
	if err == nil || errs.CodeOf(err) != errs.ETCPFrameInvalid {
		t.Fatalf("short frame code=%s err=%v", errs.CodeOf(err), err)
	}
	_, err = ReadTCP(bytes.NewReader([]byte{0, 0}))
	if errs.CodeOf(err) != errs.ETCPFrameInvalid {
		t.Fatalf("zero frame code=%s err=%v", errs.CodeOf(err), err)
	}
}

func TestEncodeRejectsOversizeNameAndLabel(t *testing.T) {
	q := NewQuery(1, strings.Repeat("a", 64)+".test.", TypeA)
	_, err := q.Pack()
	if errs.CodeOf(err) != errs.EMessageMalformed {
		t.Fatalf("code=%s err=%v", errs.CodeOf(err), err)
	}
	var name strings.Builder
	for i := 0; i < 5; i++ {
		name.WriteString(strings.Repeat("b", 63))
		name.WriteByte('.')
	}
	q = NewQuery(1, name.String(), TypeA)
	_, err = q.Pack()
	if errs.CodeOf(err) != errs.EMessageMalformed {
		t.Fatalf("code=%s err=%v", errs.CodeOf(err), err)
	}
}

package parse

import (
	"strings"
	"testing"

	"fwadjudge/internal/codes"
)

const doc = `{
  "snapshot": "s1",
  "rules": [
    {"index": 1, "direction": "in", "protocol": "tcp", "src": "10.0.0.0/8", "dst": "0.0.0.0/0", "port_lo": 80, "port_hi": 80, "action": "allow", "priority": 100},
    {"index": 2, "direction": "in", "protocol": "tcp", "src": "bad-cidr", "dst": "0.0.0.0/0", "port_lo": 1, "port_hi": 2, "action": "deny", "priority": 90},
    {"index": 3, "direction": "in", "protocol": "tcp", "src": "10.0.0.0/8", "dst": "0.0.0.0/0", "port_lo": 90, "port_hi": 10, "action": "deny", "priority": 80},
    {"index": 4, "direction": "in", "protocol": "gre", "src": "10.0.0.0/8", "dst": "0.0.0.0/0", "port_lo": 0, "port_hi": 0, "action": "deny", "priority": 70},
    {"index": 5, "direction": "any", "protocol": "any", "src": "0.0.0.0/0", "dst": "0.0.0.0/0", "port_lo": 0, "port_hi": 65535, "action": "deny", "priority": 1}
  ]
}`

func TestParseJSONSkipsInvalidWithOffsets(t *testing.T) {
	res, err := ParseJSON([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rules) != 2 {
		t.Fatalf("valid rules %d, want 2", len(res.Rules))
	}
	if len(res.Issues) != 3 {
		t.Fatalf("issues %d, want 3", len(res.Issues))
	}
	for _, iss := range res.Issues {
		if iss.Code != codes.ERuleInvalid {
			t.Fatalf("code %s", iss.Code)
		}
		if iss.Offset < 0 {
			t.Fatalf("offset missing: %+v", iss)
		}
	}
	// Offsets point at the offending rule entries in the source.
	for _, iss := range res.Issues {
		frag := doc[iss.Offset:]
		if !strings.Contains(frag, `"index"`) {
			t.Fatalf("offset %d does not locate a rule entry", iss.Offset)
		}
	}
}

func TestParseBlocks(t *testing.T) {
	r1 := `{"index":1,"direction":"in","protocol":"tcp","src":"10.0.0.0/8","dst":"0.0.0.0/0","port_lo":22,"port_hi":22,"action":"allow","priority":100}`
	r2 := `{"index":2,"direction":"in","protocol":"sctp","src":"10.0.0.0/8","dst":"0.0.0.0/0","port_lo":9,"port_hi":9,"action":"deny","priority":90}`
	r3 := `{"index":3,"direction":"any","protocol":"any","src":"0.0.0.0/0","dst":"0.0.0.0/0","port_lo":0,"port_hi":65535,"action":"deny","priority":1}`
	blk := func(s string) string {
		return string([]byte(fmt8(len(s)))) + s
	}
	data := []byte(blk(r1) + blk(r2) + blk(r3))
	res := ParseBlocks(data, "blk")
	if len(res.Rules) != 2 {
		t.Fatalf("valid rules %d, want 2", len(res.Rules))
	}
	if len(res.Issues) != 1 || res.Issues[0].Code != codes.ERuleInvalid {
		t.Fatalf("issues %+v", res.Issues)
	}
	// The invalid block starts right after the first block.
	wantOff := int64(8 + len(r1))
	if res.Issues[0].Offset != wantOff {
		t.Fatalf("offset %d, want %d", res.Issues[0].Offset, wantOff)
	}
}

func fmt8(n int) string {
	s := "00000000" + itoa(n)
	return s[len(s)-8:]
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [12]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestDuplicateIndexRejected(t *testing.T) {
	dup := `{"snapshot":"d","rules":[
		{"index":1,"direction":"in","protocol":"tcp","src":"10.0.0.0/8","dst":"0.0.0.0/0","port_lo":1,"port_hi":2,"action":"allow","priority":10},
		{"index":1,"direction":"in","protocol":"udp","src":"10.0.0.0/8","dst":"0.0.0.0/0","port_lo":1,"port_hi":2,"action":"deny","priority":10}
	]}`
	res, err := ParseJSON([]byte(dup))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rules) != 1 || len(res.Issues) != 1 {
		t.Fatalf("rules %d issues %d", len(res.Rules), len(res.Issues))
	}
	if res.Issues[0].Code != codes.ERuleInvalid {
		t.Fatalf("code %s", res.Issues[0].Code)
	}
}

package requests

import (
	"strings"
	"testing"

	"vpnbench/internal/model"
)

func block(stream string, seq int64, source, destination string, packet int64) string {
	return FormatBlock(model.Request{
		StreamID:    stream,
		RequestSeq:  seq,
		Source:      source,
		Destination: destination,
		PacketBytes: packet,
	})
}

func TestParseLengthPrefixedBlocksPreservesArrivalOrder(t *testing.T) {
	input := block("flow-a", 2, "10.0.0.1", "10.0.0.2", 100) + block("flow-a", 1, "10.0.0.1", "10.0.0.3", 200)
	parsed, err := Parse([]byte(input))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(parsed) != 2 {
		t.Fatalf("len = %d", len(parsed))
	}
	if parsed[0].RequestSeq != 2 || parsed[0].ArrivalOrdinal != 1 || parsed[1].RequestSeq != 1 || parsed[1].ArrivalOrdinal != 2 {
		t.Fatalf("arrival order not preserved: %+v", parsed)
	}
	if parsed[0].InputOffset != 0 || parsed[1].InputOffset <= parsed[0].InputOffset {
		t.Fatalf("bad offsets: %+v", parsed)
	}
}

func TestParseAliasesAndCRLF(t *testing.T) {
	body := "flow_id=f1\r\nseq=1\r\nsource=10.0.0.1\r\ndestination=10.0.0.2\r\npacket_length=10\r\n"
	input := strings.TrimSpace(strings.Repeat("0", 0)) + itoa(len(body)) + "\r\n" + body
	parsed, err := Parse([]byte(input))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if parsed[0].StreamID != "f1" || parsed[0].PacketBytes != 10 {
		t.Fatalf("aliases failed: %+v", parsed[0])
	}
}

func TestParseRejectsInvalidBlocksWithOffset(t *testing.T) {
	valid := block("f", 1, "10.0.0.1", "10.0.0.2", 1)
	cases := map[string]string{
		"bad length":       "999\nstream_id=f\n",
		"missing field":    "0\n",
		"bad ip":           replacePrefix(block("f", 1, "10.0.0.1", "10.0.0.2", 1), "10.0.0.2", "999.1.1.1"),
		"bad seq":          replacePrefix(block("f", 1, "10.0.0.1", "10.0.0.2", 1), "request_seq=1", "request_seq=0"),
		"bad packet":       replacePrefix(block("f", 1, "10.0.0.1", "10.0.0.2", 1), "packet_bytes=1", "packet_bytes=-4"),
		"unknown field":    "29\nstream_id=f\nx=1\n",
		"truncated blocks": valid[:len(valid)-2],
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(input))
			if model.CodeOf(err) != model.CodeRequestInvalid {
				t.Fatalf("code = %s, want %s (err=%v)", model.CodeOf(err), model.CodeRequestInvalid, err)
			}
			coded, ok := err.(*model.CodedError)
			if !ok || coded.Offset < 0 {
				t.Fatalf("missing offset: %#v", err)
			}
		})
	}
}

func TestParseRejectsDuplicateField(t *testing.T) {
	body := "stream_id=f\nstream_id=g\nrequest_seq=1\nsrc=10.0.0.1\ndst=10.0.0.2\npacket_bytes=1\n"
	input := itoa(len(body)) + "\n" + body
	_, err := Parse([]byte(input))
	if model.CodeOf(err) != model.CodeRequestInvalid {
		t.Fatalf("code = %s", model.CodeOf(err))
	}
}

func replacePrefix(input, old, replacement string) string {
	bodyStart := strings.IndexByte(input, '\n') + 1
	body := strings.Replace(input[bodyStart:], old, replacement, 1)
	return itoa(len(body)) + "\n" + body
}

func itoa(value int) string {
	return strconvItoa(value)
}

func strconvItoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := make([]byte, 0, 12)
	for value > 0 {
		digits = append(digits, byte('0'+value%10))
		value /= 10
	}
	for left, right := 0, len(digits)-1; left < right; left, right = left+1, right-1 {
		digits[left], digits[right] = digits[right], digits[left]
	}
	return string(digits)
}

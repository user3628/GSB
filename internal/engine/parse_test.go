package engine

import (
	"strings"
	"testing"

	"h2diag/internal/model"
)

func TestParseRejectsInvalidFrameType(t *testing.T) {
	payload := validJSON(`"type":"PING"`)
	_, biz := ParseSample([]byte(payload))
	if biz == nil || biz.Code != model.CodeFrameInvalid {
		t.Fatalf("error = %#v, want %s", biz, model.CodeFrameInvalid)
	}
}

func TestParseRejectsStreamIDOverLimit(t *testing.T) {
	payload := validJSON(`"stream_id":2147483648`)
	_, biz := ParseSample([]byte(payload))
	if biz == nil || biz.Code != model.CodeFrameInvalid {
		t.Fatalf("error = %#v, want %s", biz, model.CodeFrameInvalid)
	}
}

func TestParseRejectsNonMonotonicTimestamp(t *testing.T) {
	payload := validJSON(`"timestamp":-1`, `"timestamp":20`)
	_, biz := ParseSample([]byte(payload))
	if biz == nil || biz.Code != model.CodeFrameInvalid {
		t.Fatalf("error = %#v, want %s", biz, model.CodeFrameInvalid)
	}
}

func TestParseRejectsNegativeWindowIncrement(t *testing.T) {
	payload := validJSON(`"type":"WINDOW_UPDATE","stream_id":0,"window_increment":-1`)
	_, biz := ParseSample([]byte(payload))
	if biz == nil || biz.Code != model.CodeFrameInvalid {
		t.Fatalf("error = %#v, want %s", biz, model.CodeFrameInvalid)
	}
}

func TestParseRejectsUnknownFlag(t *testing.T) {
	payload := validJSON(`"flags":["NOT_A_FLAG"]`)
	_, biz := ParseSample([]byte(payload))
	if biz == nil || biz.Code != model.CodeFrameInvalid {
		t.Fatalf("error = %#v, want %s", biz, model.CodeFrameInvalid)
	}
}

func TestCanonicalSampleTreatsMissingAndEmptyFlagsAlike(t *testing.T) {
	withEmpty := validJSON()
	withMissing := strings.Replace(withEmpty, `"flags": [],`, "", 1)
	emptySample, biz := ParseSample([]byte(withEmpty))
	if biz != nil {
		t.Fatal(biz)
	}
	missingSample, biz := ParseSample([]byte(withMissing))
	if biz != nil {
		t.Fatal(biz)
	}
	_, emptyHash, err := model.CanonicalSample(emptySample)
	if err != nil {
		t.Fatal(err)
	}
	_, missingHash, err := model.CanonicalSample(missingSample)
	if err != nil {
		t.Fatal(err)
	}
	if emptyHash != missingHash {
		t.Fatalf("canonical hashes differ: empty=%s missing=%s", emptyHash, missingHash)
	}
}

func validJSON(mutations ...string) string {
	frame := `{
      "sequence": 1,
      "timestamp": 0,
      "connection_id": "loopback",
      "stream_id": 0,
      "type": "SETTINGS",
      "flags": [],
      "payload_bytes": 0,
      "window_increment": 0
    }`
	for _, mutation := range mutations {
		key := strings.SplitN(mutation, ":", 2)[0]
		key = strings.Trim(key, `"`)
		switch key {
		case "type":
			frame = replaceJSONField(frame, "type", strings.Trim(strings.SplitN(mutation, ":", 2)[1], `"`))
		case "stream_id":
			frame = replaceJSONField(frame, "stream_id", strings.SplitN(mutation, ":", 2)[1])
		case "timestamp":
			frame = replaceJSONField(frame, "timestamp", strings.SplitN(mutation, ":", 2)[1])
		case "window_increment":
			frame = replaceJSONField(frame, "window_increment", strings.SplitN(mutation, ":", 2)[1])
		case "flags":
			frame = replaceJSONField(frame, "flags", strings.SplitN(mutation, ":", 2)[1])
		}
	}
	return `{
  "sample_id": "parse-test",
  "config": {
    "round_bytes": 1024,
    "scheduling_threshold": 8,
    "approximation": "exact",
    "weight_scale_ppm": 1000000,
    "rounding_mode": "truncate_then_largest_remainder",
    "max_scheduling_rounds": 1000,
    "sampling_fallback_below": 2
  },
  "connections": [{
    "id": "loopback",
    "initial_connection_window": 65535,
    "max_connection_window": 2147483647,
    "initial_stream_window": 65535,
    "max_stream_window": 2147483647,
    "priority_levels": 2
  }],
  "streams": [{"connection_id":"loopback","stream_id":1,"weight":1,"priority":0}],
  "frames": [
    ` + frame + `,
    {
      "sequence": 2,
      "timestamp": 10,
      "connection_id": "loopback",
      "stream_id": 1,
      "type": "HEADERS",
      "flags": ["END_HEADERS"],
      "payload_bytes": 0,
      "window_increment": 0
    }
  ]
}`
}

func replaceJSONField(document, field, value string) string {
	needle := `"` + field + `": 0`
	replacement := `"` + field + `": ` + value
	if strings.Contains(document, needle) {
		return strings.Replace(document, needle, replacement, 1)
	}
	needle = `"` + field + `": "SETTINGS"`
	replacement = `"` + field + `": "` + strings.Trim(value, `"`) + `"`
	if strings.Contains(document, needle) {
		return strings.Replace(document, needle, replacement, 1)
	}
	needle = `"` + field + `": []`
	if strings.Contains(document, needle) {
		return strings.Replace(document, needle, `"`+field+`": `+value, 1)
	}
	return document
}

package domain

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestQuantityParsingExactAndOverflow(t *testing.T) {
	byteCases := map[string]int64{
		"512Mi":  512 * 1024 * 1024,
		"2Gi":    2 * 1024 * 1024 * 1024,
		"1.5Mi":  1536 * 1024,
		"1000KB": 1_000_000,
		"7B":     7,
	}
	for input, want := range byteCases {
		got, err := ParseByteQuantity(input)
		if err != nil || got != want {
			t.Fatalf("ParseByteQuantity(%q)=%d,%v want %d", input, got, err, want)
		}
	}
	for _, invalid := range []string{"-1Mi", "9223372036854775808B", "1.1B", "1XB"} {
		if _, err := ParseByteQuantity(invalid); err == nil {
			t.Fatalf("ParseByteQuantity(%q) unexpectedly succeeded", invalid)
		}
	}
	usCases := map[string]int64{"1500us": 1500, "3ms": 3000, "1.5ms": 1500, "2s": 2_000_000, "333us": 333, "1µs": 1, "1μs": 1}
	for input, want := range usCases {
		got, err := ParseDurationUS(input)
		if err != nil || got != want {
			t.Fatalf("ParseDurationUS(%q)=%d,%v want %d", input, got, err, want)
		}
	}
	for _, invalid := range []string{"1ns", "-3ms", "3", "3fortnight"} {
		if _, err := ParseDurationUS(invalid); err == nil {
			t.Fatalf("ParseDurationUS(%q) unexpectedly succeeded", invalid)
		}
	}
}

func TestParseModelCatalogValidation(t *testing.T) {
	valid := []byte(`{"version":"v1","models":[
		{"model_id":"a","vram_per_sample":"512Mi","cost_per_sample":"1500us","max_batch_samples":4},
		{"model_id":"b","vram_per_sample":"2Gi","cost_per_sample":"3ms","max_batch_samples":2}
	]}`)
	catalog, err := ParseModelCatalog(valid)
	if err != nil {
		t.Fatal(err)
	}
	if catalog.ByID["a"].VRAMPerSample != 512*1024*1024 || catalog.ByID["b"].CostPerSample != 3000 {
		t.Fatalf("unexpected catalog: %+v", catalog.Models)
	}
	invalidCases := []string{
		`{"models":[{"model_id":"a","vram_per_sample":"-1Mi","cost_per_sample":"1us","max_batch_samples":1}]}`,
		`{"models":[{"model_id":"a","vram_per_sample":"1Mi","cost_per_sample":"1ns","max_batch_samples":1}]}`,
		`{"models":[{"model_id":"a","vram_per_sample":"1Mi","cost_per_sample":"1us","max_batch_samples":0}]}`,
		`{"models":[{"model_id":"a","vram_per_sample":"1Mi","cost_per_sample":"1us","max_batch_samples":1},{"model_id":"a","vram_per_sample":"1Mi","cost_per_sample":"1us","max_batch_samples":1}]}`,
		`{"models":[{"model_id":"bad id","vram_per_sample":"1Mi","cost_per_sample":"1us","max_batch_samples":1}]}`,
	}
	for _, input := range invalidCases {
		_, err := ParseModelCatalog([]byte(input))
		var biz *BizError
		if !errors.As(err, &biz) || biz.Code != CodeSpecInvalid {
			t.Fatalf("expected E_SPEC_INVALID for %s, got %v", input, err)
		}
	}
}

func testCatalog(t *testing.T) *ModelCatalog {
	t.Helper()
	catalog, err := ParseModelCatalog([]byte(`{"version":"v1","models":[
		{"model_id":"small","vram_per_sample":"512Mi","cost_per_sample":"1500us","max_batch_samples":256},
		{"model_id":"large","vram_per_sample":"2Gi","cost_per_sample":"3ms","max_batch_samples":128}
	]}`))
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func TestParseTraceNormalizesAndRejectsWholeBatch(t *testing.T) {
	catalog := testCatalog(t)
	valid := []byte(`{"trace_id":"t1","version":"v1","requests":[
		{"request_id":"r1","arrival_ms":3,"model_id":"small","samples":4,"max_batch_latency":"125ms","priority":2},
		{"request_id":"r2","arrival_ms":3,"model_id":"large","samples":2,"max_batch_latency_ms":250,"priority":2}
	]}`)
	trace, err := ParseTrace(valid, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if len(trace.Requests) != 2 || trace.Requests[0].LatencyLimitUS != 125000 || trace.Requests[1].LatencyLimitUS != 250000 {
		t.Fatalf("unexpected normalization: %+v", trace.Requests)
	}
	again, err := ParseTrace(valid, catalog)
	if err != nil || again.ContentHash != trace.ContentHash {
		t.Fatalf("trace hash is not deterministic: %v %s %s", err, trace.ContentHash, again.ContentHash)
	}

	invalid := [][]byte{
		[]byte(`{"trace_id":"t2","version":"v1","requests":[
			{"request_id":"r1","arrival_ms":5,"model_id":"small","samples":1,"max_batch_latency":"10ms","priority":1},
			{"request_id":"r2","arrival_ms":4,"model_id":"small","samples":1,"max_batch_latency":"10ms","priority":1}
		]}`),
		[]byte(`{"trace_id":"t3","version":"v1","requests":[
			{"request_id":"r1","arrival_ms":0,"model_id":"unknown","samples":1,"max_batch_latency":"10ms","priority":1}
		]}`),
		[]byte(`{"trace_id":"t4","version":"v1","requests":[
			{"request_id":"r1","arrival_ms":0,"model_id":"small","samples":1,"max_batch_latency":"10ms","priority":1},
			{"request_id":"r1","arrival_ms":1,"model_id":"small","samples":1,"max_batch_latency":"10ms","priority":1}
		]}`),
		[]byte(`{"trace_id":"t5","version":"v1","requests":[
			{"request_id":"r1","arrival_ms":0,"model_id":"small","samples":-1,"max_batch_latency":"10ms","priority":1}
		]}`),
	}
	for _, input := range invalid {
		var raw map[string]any
		_ = json.Unmarshal(input, &raw)
		_, err := ParseTrace(input, catalog)
		var biz *BizError
		if !errors.As(err, &biz) || biz.Code != CodeTraceInvalid {
			t.Fatalf("expected E_TRACE_INVALID for %s, got %v", input, err)
		}
	}
}

package freshness

import (
	"math"
	"strings"
	"testing"
)

const testKey = "local-unit-test-key-0123456789"

func signedRequest(id string, ts int64, client, nonce string, tolerance, window int64) RequestInput {
	r := Request{
		RequestID: id, TimestampMS: ts, RouteKey: "orders.create", ClientID: client,
		Nonce: nonce, Weight: 1, ToleranceMS: tolerance, WindowMS: window,
	}
	return RequestInput{
		RequestID: id, TimestampMS: ts, RouteKey: r.RouteKey, ClientID: client,
		Nonce: nonce, Signature: Signature([]byte(testKey), r), Weight: 1,
		ToleranceMS: tolerance, WindowMS: window,
	}
}

func testPolicy() ValidationPolicy {
	return ValidationPolicy{MaxToleranceMS: 5000, MaxWindowMS: 60000, MaxRequestsPerBatch: 100, AllowReferenceOverride: true}
}

func TestSignatureCanonicalAndDeterministic(t *testing.T) {
	r := Request{
		TimestampMS: 1800000000000, RouteKey: "orders.create", ClientID: "client-a",
		Nonce: "nonce-alpha-00000001", Weight: 2, ToleranceMS: 500, WindowMS: 5000,
	}
	first := Signature([]byte(testKey), r)
	second := Signature([]byte(testKey), r)
	if first != second || len(first) != 64 {
		t.Fatalf("signature is not deterministic: %q %q", first, second)
	}
	r.Nonce = "nonce-alpha-00000002"
	if Signature([]byte(testKey), r) == first {
		t.Fatal("signature did not bind nonce")
	}
	r.Nonce = "nonce-alpha-00000001"
	for _, input := range []RequestInput{signedRequest("req-1", 1000, "client-a", "nonce-alpha-00000001", 500, 5000)} {
		validated, err := ValidateBatch(BatchInput{BatchID: "batch-1", Requests: []RequestInput{input}}, testPolicy(), 1000)
		if err != nil {
			t.Fatal(err)
		}
		if !VerifySignature([]byte(testKey), validated.Requests[0]) {
			t.Fatal("valid signature was rejected")
		}
		validated.Requests[0].Weight++
		if VerifySignature([]byte(testKey), validated.Requests[0]) {
			t.Fatal("tampered field still verified")
		}
	}
}

func TestTimestampBoundaryIsInclusive(t *testing.T) {
	tests := []struct {
		name      string
		ts        int64
		tolerance int64
		offset    int64
		boundary  string
		code      string
	}{
		{"exact lower boundary", 500, 500, -500, BoundaryNone, CodeOK},
		{"exact upper boundary", 1500, 500, 500, BoundaryNone, CodeOK},
		{"below lower boundary", 499, 500, -501, BoundaryLower, CodeTimestampTooOld},
		{"above upper boundary", 1501, 500, 501, BoundaryUpper, CodeTimestampAhead},
		{"inside", 1200, 500, 200, BoundaryNone, CodeOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offset, boundary, code, lower, upper := JudgeTimestamp(tt.ts, 1000, tt.tolerance)
			if offset != tt.offset || boundary != tt.boundary || code != tt.code || lower != -tt.tolerance || upper != tt.tolerance {
				t.Fatalf("got offset=%d boundary=%s code=%s bounds=[%d,%d]", offset, boundary, code, lower, upper)
			}
		})
	}
}

func TestValidateBatchRejectsWholeBatch(t *testing.T) {
	valid := signedRequest("req-1", 1000, "client-a", "nonce-alpha-00000001", 500, 5000)
	validTwo := signedRequest("req-2", 1001, "client-b", "nonce-beta-000000001", 500, 5000)
	tests := []struct {
		name   string
		mutate func(*BatchInput)
		field  string
	}{
		{"short nonce", func(b *BatchInput) { b.Requests[0].Nonce = "short" }, "requests[0].nonce"},
		{"nonce charset", func(b *BatchInput) { b.Requests[0].Nonce = "nonce with spaces 1" }, "requests[0].nonce"},
		{"negative timestamp", func(b *BatchInput) { b.Requests[0].TimestampMS = -1 }, "requests[0].timestamp_ms"},
		{"zero weight", func(b *BatchInput) { b.Requests[0].Weight = 0 }, "requests[0].weight"},
		{"negative weight", func(b *BatchInput) { b.Requests[0].Weight = -1 }, "requests[0].weight"},
		{"tolerance over max", func(b *BatchInput) { b.Requests[0].ToleranceMS = 5001 }, "requests[0].tolerance_ms"},
		{"window over max", func(b *BatchInput) { b.Requests[0].WindowMS = 60001 }, "requests[0].window_ms"},
		{"duplicate request id", func(b *BatchInput) { b.Requests[1].RequestID = b.Requests[0].RequestID }, "requests[1].request_id"},
		{"empty requests", func(b *BatchInput) { b.Requests = nil }, "requests"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := BatchInput{BatchID: "batch-1", Requests: []RequestInput{valid, validTwo}}
			tt.mutate(&batch)
			_, err := ValidateBatch(batch, testPolicy(), 1000)
			if err == nil || err.Code != CodeRequestInvalid {
				t.Fatalf("expected E_REQUEST_INVALID, got %#v", err)
			}
			encoded := err.Details.(map[string]any)["issues"]
			issues := encoded.([]Issue)
			found := false
			for _, issue := range issues {
				if issue.Field == tt.field {
					found = true
				}
			}
			if !found {
				t.Fatalf("missing issue for %s: %#v", tt.field, issues)
			}
		})
	}
}

func TestValidateBatchWeightOverflow(t *testing.T) {
	first := signedRequest("req-1", 1000, "client-a", "nonce-alpha-00000001", 500, 5000)
	second := signedRequest("req-2", 1001, "client-b", "nonce-beta-000000001", 500, 5000)
	first.Weight = math.MaxInt64
	second.Weight = 1
	_, err := ValidateBatch(BatchInput{BatchID: "batch-1", Requests: []RequestInput{first, second}}, testPolicy(), 1000)
	if err == nil || err.Code != CodeRequestInvalid {
		t.Fatalf("expected overflow rejection, got %#v", err)
	}
}

func TestReferenceOverridePolicy(t *testing.T) {
	reference := int64(1234)
	req := signedRequest("req-1", 1234, "client-a", "nonce-alpha-00000001", 500, 5000)
	policy := testPolicy()
	policy.AllowReferenceOverride = false
	_, err := ValidateBatch(BatchInput{BatchID: "batch-1", GatewayReferenceTimeMS: &reference, Requests: []RequestInput{req}}, policy, 2000)
	if err == nil || !strings.Contains(err.Error(), CodeRequestInvalid) {
		t.Fatalf("expected disabled override rejection, got %#v", err)
	}
}

package engine

import (
	"testing"

	"vpnbench/internal/model"
	"vpnbench/internal/monotonic"
	"vpnbench/internal/policy"
)

const sessionPolicy = `[
  {"tunnel_id":"tun-a","destination_cidr":"10.0.0.0/8","priority":100,"bandwidth":1,"forwarding_cost":1},
  {"tunnel_id":"tun-b","destination_cidr":"10.0.0.0/8","priority":100,"bandwidth":1,"forwarding_cost":2}
]`

func testPolicySet(t *testing.T) model.PolicySet {
	t.Helper()
	set, err := policy.Parse([]byte(sessionPolicy))
	if err != nil {
		t.Fatal(err)
	}
	set.Version = 3
	return set
}

func testRequest(stream string, seq, arrival int64, destination string) model.Request {
	return model.Request{
		StreamID:       stream,
		RequestSeq:     seq,
		Source:         "10.1.0.1",
		Destination:    destination,
		PacketBytes:    100,
		InputOffset:    arrival * 100,
		ArrivalOrdinal: arrival,
	}
}

func TestRunReconcilesOutOfOrderRequests(t *testing.T) {
	outcome, err := Run(Config{
		SessionID:     "s1",
		Policy:        testPolicySet(t),
		Requests:      []model.Request{testRequest("flow", 2, 1, "10.1.0.1"), testRequest("flow", 1, 2, "10.1.0.2")},
		RequestBudget: 10,
		Clock:         monotonic.NewFixed(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Status != "FINALIZED" {
		t.Fatalf("status = %s", outcome.Status)
	}
	if outcome.Consistency.Total != 2 || outcome.Consistency.Consistent != 2 || len(outcome.Consistency.Mismatches) != 0 {
		t.Fatalf("consistency = %+v", outcome.Consistency)
	}
	if len(outcome.Corrections) != 2 || outcome.Corrections[0].Code != model.CorrectionOrder {
		t.Fatalf("corrections = %+v", outcome.Corrections)
	}
	if outcome.Requests[0].StreamID != "flow" || outcome.Requests[0].RequestSeq != 1 {
		t.Fatalf("results not sorted by stream/seq: %+v", outcome.Requests)
	}
	if outcome.InteractiveHash != outcome.SettlementHash {
		t.Fatalf("interactive and settlement hashes differ")
	}
}

func TestRunRequestBudgetPreservesProcessedResults(t *testing.T) {
	outcome, err := Run(Config{
		SessionID: "s1",
		Policy:    testPolicySet(t),
		Requests: []model.Request{
			testRequest("flow-a", 1, 1, "10.1.0.1"),
			testRequest("flow-b", 1, 2, "10.1.0.2"),
			testRequest("flow-c", 1, 3, "10.1.0.3"),
		},
		RequestBudget: 2,
		Clock:         monotonic.NewFixed(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Status != "TERMINATED_REQUEST_BUDGET" || outcome.RequestsProcessed != 2 {
		t.Fatalf("outcome = status %s processed %d", outcome.Status, outcome.RequestsProcessed)
	}
	found := false
	for _, occurrence := range outcome.BusinessCodes {
		if occurrence.Code == model.CodeRequestBudget && occurrence.StreamID == "flow-c" && occurrence.RequestSeq == 1 {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing budget location: %+v", outcome.BusinessCodes)
	}
	if len(outcome.LatencySamples) != 2 {
		t.Fatalf("latency samples = %d", len(outcome.LatencySamples))
	}
}

func TestRunDuplicateKeyReturnsLocatedMismatch(t *testing.T) {
	outcome, err := Run(Config{
		SessionID: "s1",
		Policy:    testPolicySet(t),
		Requests: []model.Request{
			testRequest("flow", 1, 1, "10.1.0.1"),
			testRequest("flow", 1, 2, "10.1.0.2"),
		},
		RequestBudget: 10,
		Clock:         monotonic.NewFixed(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Status != "FAILED_RESULT_MISMATCH" {
		t.Fatalf("status = %s", outcome.Status)
	}
	if len(outcome.BusinessCodes) != 1 || outcome.BusinessCodes[0].Code != model.CodeResultMismatch {
		t.Fatalf("codes = %+v", outcome.BusinessCodes)
	}
	if outcome.BusinessCodes[0].StreamID != "flow" || outcome.BusinessCodes[0].RequestSeq != 1 {
		t.Fatalf("location = %+v", outcome.BusinessCodes[0])
	}
	if len(outcome.Consistency.Mismatches) != 1 || outcome.Consistency.Mismatches[0].Code != model.CodeResultMismatch {
		t.Fatalf("consistency mismatches = %+v", outcome.Consistency.Mismatches)
	}
}

func TestRunLatencyUsesFixedMonotonicMarker(t *testing.T) {
	outcome, err := Run(Config{
		SessionID:     "s1",
		Policy:        testPolicySet(t),
		Requests:      []model.Request{testRequest("flow", 1, 1, "10.1.0.1"), testRequest("flow", 2, 2, "203.0.113.9")},
		RequestBudget: 10,
		Clock:         monotonic.NewFixed(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.LatencyMarker != "T_MONO_FIXED_250US_V1" || outcome.LatencyMode != "monotonic-fixed-v1" {
		t.Fatalf("latency marker = %s %s", outcome.LatencyMode, outcome.LatencyMarker)
	}
	for _, sample := range outcome.LatencySamples {
		if sample.DurationNS != 250_000 {
			t.Fatalf("sample = %+v", sample)
		}
	}
}

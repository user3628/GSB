package decision

import (
	"testing"

	"vpnbench/internal/model"
	"vpnbench/internal/policy"
)

const testPolicy = `{
  "policies": [
    {"tunnel_id":"tun-alpha","destination_cidr":"10.0.0.0/8","priority":100,"bandwidth":7,"forwarding_cost":2},
    {"tunnel_id":"tun-beta","destination_cidr":"10.0.0.0/8","priority":100,"bandwidth":11,"forwarding_cost":3},
    {"tunnel_id":"tun-gamma","destination_cidr":"10.0.0.0/8","priority":50,"bandwidth":13,"forwarding_cost":1},
    {"tunnel_id":"tun-delta","destination_cidr":"192.168.0.0/16","priority":100,"bandwidth":17,"forwarding_cost":5}
  ]
}`

func newEngine(t *testing.T, body string) *Engine {
	t.Helper()
	set, err := policy.Parse([]byte(body))
	if err != nil {
		t.Fatalf("parse policy: %v", err)
	}
	engine, err := New(set)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	return engine
}

func request(seq int64) model.Request {
	return model.Request{
		StreamID:       "flow-a",
		RequestSeq:     seq,
		Source:         "10.1.0.1",
		Destination:    "10.2.0.1",
		PacketBytes:    1200,
		InputOffset:    7,
		ArrivalOrdinal: seq,
	}
}

func TestDecideSelectsTopPriorityAndAllocatesFixedPointWeights(t *testing.T) {
	engine := newEngine(t, testPolicy)
	outcome := engine.Decide("s1", request(1), 0)
	if outcome.Error != nil {
		t.Fatalf("unexpected error: %v", outcome.Error)
	}
	if outcome.Decision.PrimaryTunnelID != "tun-beta" {
		t.Fatalf("primary = %s, want tun-beta", outcome.Decision.PrimaryTunnelID)
	}
	if len(outcome.Decision.Allocations) != 2 {
		t.Fatalf("allocations = %+v", outcome.Decision.Allocations)
	}
	if outcome.Decision.Allocations[0].TunnelID != "tun-beta" || outcome.Decision.Allocations[0].WeightMicros != 611111 {
		t.Fatalf("first allocation = %+v", outcome.Decision.Allocations[0])
	}
	if outcome.Decision.Allocations[1].TunnelID != "tun-alpha" || outcome.Decision.Allocations[1].WeightMicros != 388889 {
		t.Fatalf("second allocation = %+v", outcome.Decision.Allocations[1])
	}
	var sum int64
	for _, allocation := range outcome.Decision.Allocations {
		sum += allocation.WeightMicros
	}
	if sum != WeightScale {
		t.Fatalf("weight sum = %d", sum)
	}
	if len(outcome.Ledger) != 2 {
		t.Fatalf("ledger = %+v", outcome.Ledger)
	}
	if outcome.Ledger[0].CompensationMicros+outcome.Ledger[1].CompensationMicros != 1 {
		t.Fatalf("expected one micro-unit compensation: %+v", outcome.Ledger)
	}
	for _, allocation := range outcome.Decision.Allocations {
		if allocation.TunnelID == "tun-gamma" {
			t.Fatal("lower-priority fallback received active weight")
		}
	}
}

func TestDecideNoTunnelAndOverflow(t *testing.T) {
	engine := newEngine(t, testPolicy)
	noMatch := request(1)
	noMatch.Destination = "203.0.113.9"
	outcome := engine.Decide("s1", noMatch, 0)
	if outcome.Error == nil || outcome.Error.Code != model.CodeNoTunnel {
		t.Fatalf("code = %+v", outcome.Error)
	}
	if outcome.Error.StreamID != "flow-a" || outcome.Error.RequestSeq != 1 {
		t.Fatalf("missing mismatch location: %+v", outcome.Error)
	}

	overflow := newEngine(t, `[{"tunnel_id":"big","destination_cidr":"10.0.0.0/8","priority":1,"bandwidth":10000000000000,"forwarding_cost":1}]`)
	outcome = overflow.Decide("s1", request(2), 0)
	if outcome.Error == nil || outcome.Error.Code != model.CodeFixedOverflow {
		t.Fatalf("overflow code = %+v", outcome.Error)
	}
	if len(outcome.Ledger) != 0 {
		t.Fatalf("overflow wrote ledger entries: %+v", outcome.Ledger)
	}
}

func TestDecideLedgerAggregationIsBounded(t *testing.T) {
	engine := newEngine(t, testPolicy)
	var ledger []model.LedgerEntry
	for seq := int64(1); seq <= 3; seq++ {
		outcome := engine.Decide("s1", request(seq), int64(len(ledger)))
		if outcome.Error != nil {
			t.Fatal(outcome.Error)
		}
		ledger = append(ledger, outcome.Ledger...)
	}
	aggregates := AggregateLedger(ledger)
	if len(aggregates) != 1 || aggregates[0].StreamID != "flow-a" {
		t.Fatalf("aggregates = %+v", aggregates)
	}
	if aggregates[0].AllocationCount != 6 || aggregates[0].TruncationCount == 0 {
		t.Fatalf("aggregate counts = %+v", aggregates[0])
	}
	if aggregates[0].ErrorUpperBoundMicros != int64(aggregates[0].TruncationCount) {
		t.Fatalf("error bound = %+v", aggregates[0])
	}
}

func TestEqualWeightRemainderIsDistributedDeterministically(t *testing.T) {
	engine := newEngine(t, `[
	  {"tunnel_id":"b","destination_cidr":"10.0.0.0/8","priority":1,"bandwidth":1,"forwarding_cost":0},
	  {"tunnel_id":"a","destination_cidr":"10.0.0.0/8","priority":1,"bandwidth":1,"forwarding_cost":0},
	  {"tunnel_id":"c","destination_cidr":"10.0.0.0/8","priority":1,"bandwidth":1,"forwarding_cost":0}
	]`)
	first := engine.Decide("s1", request(1), 0).Decision
	second := engine.Decide("s1", request(1), 0).Decision
	if Fingerprint(first) != Fingerprint(second) {
		t.Fatal("decision not deterministic")
	}
	if first.Allocations[0].TunnelID != "a" || first.Allocations[0].WeightMicros != 333334 {
		t.Fatalf("tie break or remainder failed: %+v", first.Allocations)
	}
	var sum int64
	for _, allocation := range first.Allocations {
		sum += allocation.WeightMicros
	}
	if sum != WeightScale {
		t.Fatalf("sum = %d", sum)
	}
}

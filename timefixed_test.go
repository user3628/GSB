package dnsdetect

import (
	"context"
	"math"
	"testing"
)

func TestFixedTicksConversions(t *testing.T) {
	ticks, truncation, err := TicksFromRFC3339("2024-01-01T00:00:00.123456789Z")
	if err != nil {
		t.Fatal(err)
	}
	base, _, _ := TicksFromRFC3339("2024-01-01T00:00:00Z")
	if ticks-base != 1_234_567 || truncation != 89 {
		t.Fatalf("RFC3339 conversion = %d (+%d ns), want %d (+89 ns)", ticks-base, truncation, 1_234_567)
	}
	if got := TimeErrorTicks(truncation); got != 1 {
		t.Fatalf("truncation bound = %d, want 1", got)
	}

	positive, truncation, err := TicksFromRelativeNS(1000, 150)
	if err != nil || positive != 1001 || truncation != 50 {
		t.Fatalf("relative +150ns = (%d,%d,%v), want (1001,50,nil)", positive, truncation, err)
	}
	negative, truncation, err := TicksFromRelativeNS(1000, -150)
	if err != nil || negative != 999 || truncation != 50 {
		t.Fatalf("relative -150ns = (%d,%d,%v), want (999,50,nil)", negative, truncation, err)
	}
	if _, _, err := TicksFromRelativeNS(math.MaxInt64, 100); CodeOf(err) != CodeTimeOverflow {
		t.Fatalf("relative overflow code = %s", CodeOf(err))
	}

	epoch, truncation, err := TicksFromFILETIME(UnixEpochFILETIME)
	if err != nil || epoch != 0 || truncation != 0 {
		t.Fatalf("FILETIME Unix epoch = (%d,%d,%v)", epoch, truncation, err)
	}
	if _, _, err := TicksFromFILETIME(math.MaxUint64); CodeOf(err) != CodeTimeOverflow {
		t.Fatalf("FILETIME overflow code = %s", CodeOf(err))
	}
	roundTripTicks := int64(1_704_067_200_000_000_0)
	filetime, err := FILETIMEFromTicks(roundTripTicks)
	if err != nil {
		t.Fatal(err)
	}
	back, _, err := TicksFromFILETIME(filetime)
	if err != nil || back != roundTripTicks {
		t.Fatalf("FILETIME round trip = %d, want %d (%v)", back, roundTripTicks, err)
	}
}

func TestAssociationBoundaryUncertainty(t *testing.T) {
	config := DefaultConfig()
	config.AssociationWindowTicks = 200
	config.WindowSpanTicks = 100_000
	config.WindowAnchorTicks = 0
	events := []Event{
		{SourcePath: "q1", SourceKind: SourceJSONL, Offset: 1, TXID: 1, Question: "uncertain.example.", Role: RoleQuery, TimeTicks: 10_000},
		{SourcePath: "r1", SourceKind: SourceJSONL, Offset: 2, TXID: 1, Question: "uncertain.example.", Role: RoleResponse, TimeTicks: 10_201, TimeErrorTicks: 1},
		{SourcePath: "q2", SourceKind: SourceJSONL, Offset: 3, TXID: 2, Question: "definite.example.", Role: RoleQuery, TimeTicks: 10_000},
		{SourcePath: "r2", SourceKind: SourceJSONL, Offset: 4, TXID: 2, Question: "definite.example.", Role: RoleResponse, TimeTicks: 10_199, TimeErrorTicks: 1},
		{SourcePath: "q3", SourceKind: SourceJSONL, Offset: 5, TXID: 3, Question: "outside.example.", Role: RoleQuery, TimeTicks: 10_000},
		{SourcePath: "r3", SourceKind: SourceJSONL, Offset: 6, TXID: 3, Question: "outside.example.", Role: RoleResponse, TimeTicks: 10_202},
	}
	result, err := Adjudicate(events, config)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Uncertain) != 1 || result.Uncertain[0].TXID != 1 {
		t.Fatalf("uncertain associations = %+v, want txid 1", result.Uncertain)
	}
	if len(result.Findings) != 1 || result.Findings[0].Code != CodeUnsolicitedResponse || result.Findings[0].TXID != 3 {
		t.Fatalf("findings = %+v, want one unsolicited txid 3", result.Findings)
	}
}

func TestOverflowDoesNotPolluteLedger(t *testing.T) {
	dataset := testDataset(t)
	var sample *MalformedSample
	for index := range dataset.Malformed {
		if dataset.Malformed[index].ExpectedCode == CodeTimeOverflow {
			sample = &dataset.Malformed[index]
			break
		}
	}
	if sample == nil {
		t.Fatal("missing FILETIME overflow sample")
	}
	session, err := LoadSource(context.Background(), SourceSpec{Path: sample.Path, Kind: sample.Kind}, LoadOptions{})
	requireCode(t, err, CodeTimeOverflow)
	if session == nil {
		t.Fatal("expected partial session")
	}
	if len(session.Ledger.Entries) != 0 {
		t.Fatalf("overflow event polluted ledger: %+v", session.Ledger.Entries)
	}
}

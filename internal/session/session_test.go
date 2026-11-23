package session

import (
	"testing"
	"time"

	"injectctl/internal/metrics"
	"injectctl/internal/model"
)

func sample(id string, seq int, raw string) model.Sample {
	return model.Sample{
		ID: id, Seq: seq, Surface: "sql", Landing: model.LandingStringLiteral,
		Encodings: []string{model.EncodingSingleQuoteEscape}, Raw: raw,
	}
}

func TestInteractiveSettlementConsistency(t *testing.T) {
	rec := metrics.NewRecorder()
	sess := New("b1", "ctx-1", 0, rec)
	samples := []model.Sample{
		sample("s1", 1, "O'Reilly"),
		sample("s2", 2, "x' OR '1'='1"),
	}
	for _, sm := range samples {
		if _, err := sess.AdmitInteractive(sm); err != nil {
			t.Fatal(err)
		}
	}
	final, corrections, err := sess.Close(samples)
	if err != nil {
		t.Fatal(err)
	}
	if len(corrections) != 0 {
		t.Fatalf("deterministic codec must produce no corrections: %+v", corrections)
	}
	for _, sm := range samples {
		prelim := sess.interactive[sm.ID]
		settled := final[sm.ID]
		if !equalAdjudication(prelim, settled) {
			t.Fatalf("interactive/settlement mismatch for %s: %+v vs %+v", sm.ID, prelim, settled)
		}
		if settled.ReasonVersion != 1 {
			t.Fatalf("reason version must stay 1 without corrections, got %d", settled.ReasonVersion)
		}
	}
}

func TestLateArrivalCorrectionIncrementsVersion(t *testing.T) {
	rec := metrics.NewRecorder()
	sess := New("b2", "ctx-1", 0, rec)

	v1, _ := sess.AdmitInteractive(sample("s1", 1, "abc"))
	if v1.ReasonVersion != 1 {
		t.Fatalf("first version = %d", v1.ReasonVersion)
	}
	// Late-arriving duplicate with a different payload.
	v2, _ := sess.AdmitInteractive(sample("s1", 3, "abc'--"))
	if v2.ReasonVersion != 2 {
		t.Fatalf("correction must bump version to 2, got %d", v2.ReasonVersion)
	}
	corrections := sess.Corrections()
	if len(corrections) != 1 || corrections[0].SampleID != "s1" ||
		corrections[0].ReasonVersion != 2 || corrections[0].SourceSeq != 3 {
		t.Fatalf("correction record wrong: %+v", corrections)
	}
	if sess.Version() != 2 {
		t.Fatalf("session version = %d", sess.Version())
	}
}

func TestDeterministicClockAggregation(t *testing.T) {
	base := time.Unix(0, 0)
	ticks := int64(0)
	clock := func() time.Time {
		// Monotonic readings are synthetic but differences are real durations.
		ticks += 10
		return base.Add(time.Duration(ticks) * time.Nanosecond)
	}
	rec := metrics.NewDeterministicRecorder(clock)
	for i := 0; i < 20; i++ {
		rec.Measure(i+1, model.LandingPathSegment, model.CodeOK, func() {})
	}
	stats := rec.Aggregate()
	if len(stats) != 1 || stats[0].Count != 20 {
		t.Fatalf("aggregation wrong: %+v", stats)
	}
	if stats[0].Average != metrics.FixedMarker || stats[0].P95 != metrics.FixedMarker ||
		stats[0].Maximum != metrics.FixedMarker {
		t.Fatalf("exported time fields must be fixed markers: %+v", stats)
	}
	// Each Measure spans two clock calls => 20ns monotonic difference.
	if stats[0].AvgNanos != 10 || stats[0].MaxNanos != 10 || stats[0].P95Nanos != 10 {
		t.Fatalf("monotonic math wrong: %+v", stats)
	}
}

func TestP95NearestRank(t *testing.T) {
	base := time.Unix(0, 0)
	ticks := int64(0)
	clock := func() time.Time {
		ticks++
		return base.Add(time.Duration(ticks*ticks) * time.Nanosecond)
	}
	rec := metrics.NewDeterministicRecorder(clock)
	for i := 0; i < 10; i++ {
		rec.Measure(i+1, model.LandingStringLiteral, model.CodeCtxEscape, func() {})
	}
	stats := rec.Aggregate()
	if len(stats) != 1 || stats[0].Code != model.CodeCtxEscape {
		t.Fatalf("grouping by code wrong: %+v", stats)
	}
	if stats[0].P95Nanos <= 0 || stats[0].MaxNanos < stats[0].P95Nanos {
		t.Fatalf("p95/max ordering wrong: %+v", stats)
	}
}

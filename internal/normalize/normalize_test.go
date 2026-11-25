package normalize_test

import (
	"bytes"
	"reflect"
	"testing"

	"auditdrift/internal/model"
	"auditdrift/internal/normalize"
	"auditdrift/internal/sample"
)

func TestJSONLAndLegacyNormalizeToIdenticalObservations(t *testing.T) {
	set := sample.FixedSet()
	jsonResult, err := normalize.ParseJSONL(bytes.NewReader(set.JSONL))
	if err != nil {
		t.Fatalf("parse jsonl: %v", err)
	}
	legacyResult, err := normalize.ParseLegacyBlock(bytes.NewReader(set.LegacyBlock), model.RoundTruncate)
	if err != nil {
		t.Fatalf("parse legacy: %v", err)
	}
	if len(jsonResult.Observations) != len(legacyResult.Observations) {
		t.Fatalf("length mismatch json=%d legacy=%d", len(jsonResult.Observations), len(legacyResult.Observations))
	}
	for i := range jsonResult.Observations {
		if !reflect.DeepEqual(jsonResult.Observations[i], legacyResult.Observations[i]) {
			t.Fatalf("observation %d mismatch\njson=%#v\nlegacy=%#v", i, jsonResult.Observations[i], legacyResult.Observations[i])
		}
	}
	if len(legacyResult.Rounding) != len(set.Observations) {
		t.Fatalf("rounding records=%d want %d", len(legacyResult.Rounding), len(set.Observations))
	}
	var sum int64
	for i, rounding := range legacyResult.Rounding {
		if rounding.DiscardedDigits != 3 {
			t.Fatalf("rounding %d discarded digits=%d", i, rounding.DiscardedDigits)
		}
		sum += rounding.RoundingErrorMicros
		if rounding.CumulativeErrorMicros != sum {
			t.Fatalf("cumulative error at %d =%d want %d", i, rounding.CumulativeErrorMicros, sum)
		}
	}
	if legacyResult.Rounding[len(legacyResult.Rounding)-1].CumulativeErrorMicros != sum {
		t.Fatal("final cumulative rounding error does not match sum")
	}
}

func TestRoundingRules(t *testing.T) {
	block, err := normalize.EncodeLegacyBlock([]normalize.LegacyRecord{{
		Observation: model.Observation{Host: "h", BaselineID: "b", Actual: model.Value{Kind: model.KindNumber, Number: 1}, SampledAtMS: 1000},
		TimestampMicros: 1_000_765,
	}})
	if err != nil {
		t.Fatal(err)
	}
	truncated, err := normalize.ParseLegacyBlock(bytes.NewReader(block), model.RoundTruncate)
	if err != nil {
		t.Fatal(err)
	}
	if got := truncated.Observations[0].SampledAtMS; got != 1000 {
		t.Fatalf("truncate ms=%d want 1000", got)
	}
	if truncated.Rounding[0].RoundingErrorMicros != -765 {
		t.Fatalf("truncate error=%d", truncated.Rounding[0].RoundingErrorMicros)
	}

	nearest, err := normalize.ParseLegacyBlock(bytes.NewReader(block), model.RoundNearest)
	if err != nil {
		t.Fatal(err)
	}
	if got := nearest.Observations[0].SampledAtMS; got != 1001 {
		t.Fatalf("nearest ms=%d want 1001", got)
	}
	if !nearest.Rounding[0].RoundedUp || nearest.Rounding[0].RoundingErrorMicros != 235 {
		t.Fatalf("nearest rounding info=%+v", nearest.Rounding[0])
	}
}

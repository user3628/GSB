package numeric_test

import (
	"encoding/json"
	"testing"

	"mpckpt/internal/model"
	"mpckpt/internal/numeric"
	"mpckpt/internal/sample"
)

func TestHalfQuantizationIsDeterministic(t *testing.T) {
	cases := []struct {
		input float32
		want  float32
	}{
		{1, 1},
		{-2.5, -2.5},
		{0.1, 0.099975586},
		{1.00048828125, 1},
		{1.0009765625, 1.0009765625},
	}
	for _, testCase := range cases {
		first := numeric.QuantizeHalf(testCase.input)
		second := numeric.QuantizeHalf(testCase.input)
		if first != second {
			t.Fatalf("quantization not deterministic for %v: %v != %v", testCase.input, first, second)
		}
		if first != testCase.want {
			t.Fatalf("QuantizeHalf(%v)=%v, want %v", testCase.input, first, testCase.want)
		}
	}
}

func TestPrecisionReportDeterministicAndWithinPolicy(t *testing.T) {
	strategy := sample.StrategyV1()
	first, berr := numeric.Evaluate(sample.Build().Shards, strategy, numeric.DefaultElementsPerShard)
	if berr != nil {
		t.Fatalf("evaluate: %v", berr)
	}
	second, berr := numeric.Evaluate(sample.Build().Shards, strategy, numeric.DefaultElementsPerShard)
	if berr != nil {
		t.Fatalf("evaluate again: %v", berr)
	}
	if !first.WithinPolicy || first.MaxAbsErrorFixed <= 0 || first.CumulativeRoundingError <= 0 {
		t.Fatalf("unexpected report: %+v", first)
	}
	firstJSON, _ := json.Marshal(first)
	secondJSON, _ := json.Marshal(second)
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("repeated report differed\nfirst=%s\nsecond=%s", firstJSON, secondJSON)
	}
}

func TestPrecisionDriftReturnsBusinessError(t *testing.T) {
	strategy := sample.StrategyV1()
	strategy.MaxAbsErrorFixed = 1
	report, berr := numeric.Evaluate(sample.Build().Shards, strategy, numeric.DefaultElementsPerShard)
	if berr == nil || berr.Code != model.CodePrecisionDrift {
		t.Fatalf("expected E_PRECISION_DRIFT, got %v", berr)
	}
	if report.WithinPolicy {
		t.Fatalf("report incorrectly marked within policy")
	}
	if berr.Context["shard_id"] == "" {
		t.Fatalf("drift error must identify shard: %v", berr.Context)
	}
}

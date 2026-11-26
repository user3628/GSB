package agg

import (
	"math"
	"testing"

	"logagg/internal/errs"
)

func TestSegmentPartialSumsAndDeterministicMerge(t *testing.T) {
	first := NewSegmentSum("s", "shard", 1, 0, 3, 0, 3, "def")
	if err := first.AddRows([]int64{1, 1, 2}, []int64{10, 20, 30}); err != nil {
		t.Fatal(err)
	}
	second := NewSegmentSum("s", "shard", 1, 0, 3, 3, 6, "def")
	if err := second.AddRows([]int64{1, 3, 2}, []int64{40, 5, 60}); err != nil {
		t.Fatal(err)
	}
	totalA := NewTotalSum()
	if err := totalA.MergeSegment(first); err != nil {
		t.Fatal(err)
	}
	if err := totalA.MergeSegment(second); err != nil {
		t.Fatal(err)
	}
	totalB := NewTotalSum()
	if err := totalB.MergeSegment(second.Clone()); err != nil {
		t.Fatal(err)
	}
	if err := totalB.MergeSegment(first.Clone()); err != nil {
		t.Fatal(err)
	}
	if totalA.Groups[1].Sum != 70 || totalA.Groups[1].Count != 3 || totalA.Groups[1].Min != 10 || totalA.Groups[1].Max != 40 {
		t.Fatalf("bad group1 acc: %+v", totalA.Groups[1])
	}
	if totalA.Groups[2].Sum != 90 || totalA.Groups[3].Min != 5 {
		t.Fatalf("bad group2/3: %+v %+v", totalA.Groups[2], totalA.Groups[3])
	}
	for key, acc := range totalA.Groups {
		other := totalB.Groups[key]
		if other == nil || *acc != *other {
			t.Fatalf("non-deterministic merge at %d: %+v vs %+v", key, acc, other)
		}
	}
}

func TestInt64OverflowNamesWindowAndGroup(t *testing.T) {
	acc := &Accumulator{Has: true, Sum: math.MaxInt64 - 1}
	err := acc.Add(2, "shard:v1:b0:r0-10", 42)
	if errs.Code(err) != errs.ENumericOverflow {
		t.Fatalf("expected overflow, got %v", err)
	}
	coded, ok := errs.As(err)
	if !ok || coded.Window == "" || coded.Group != "42" {
		t.Fatalf("overflow missing window/group: %+v", coded)
	}
}

func TestPrecisionRoundingTrace(t *testing.T) {
	total := NewTotalSum()
	total.Groups[7] = &Accumulator{Has: true, Count: 3, Sum: 123456, Min: 1000, Max: 90000}
	result, err := Finalize(total, Precision{InputScale: 4, ResultScale: 2, Mode: RoundHalfUp})
	if err != nil {
		t.Fatal(err)
	}
	group := result.Groups[0]
	if group.SumRounded != "12.35" || group.SumExact != "12.3456" {
		t.Fatalf("rounding mismatch: exact=%s rounded=%s", group.SumExact, group.SumRounded)
	}
	if len(result.RoundingTrace) != 1 || result.RoundingTrace[0].ResidualRaw != -44 {
		t.Fatalf("bad trace: %+v", result.RoundingTrace)
	}
	if result.CumulativeRoundingError != "-0.0044" {
		t.Fatalf("cumulative error=%s", result.CumulativeRoundingError)
	}
}

func TestNaiveFloatDifferenceWithinQuantifiedBound(t *testing.T) {
	values := []int64{12345, -23456, 34567, 45678, -56789, 67890, 78901, -89012, 90123, 123456}
	var exact int64
	for _, value := range values {
		next, ok := addInt64(exact, value)
		if !ok {
			t.Fatal("unexpected overflow")
		}
		exact = next
	}
	naive := NaiveFloatSum(values, 4)
	exactDecimal := float64(exact) / 10000
	accumulationBound := FloatAccumulationBound(values, 4)
	observed := math.Abs(naive - exactDecimal)
	if observed > accumulationBound {
		t.Fatalf("float accumulation difference %.12g exceeds bound %.12g", observed, accumulationBound)
	}

	total := NewTotalSum()
	total.Groups[1] = &Accumulator{Has: true, Count: int64(len(values)), Sum: exact}
	result, err := Finalize(total, Precision{InputScale: 4, ResultScale: 2, Mode: RoundHalfUp})
	if err != nil {
		t.Fatal(err)
	}
	ourRounded, err := parseFloatScaled(result.Groups[0].SumRounded, 2)
	if err != nil {
		t.Fatal(err)
	}
	naiveRounded := math.Round(naive*100) / 100
	difference := math.Abs(ourRounded - naiveRounded)
	totalBound := accumulationBound + 0.005 + 1e-12
	t.Logf("rows=%d exact=%s naive=%.10f observed_pre_round=%.3g accumulation_bound=%.3g output_diff=%.6f total_bound=%.6f",
		len(values), Decimal(exact, 4), naive, observed, accumulationBound, difference, totalBound)
	if difference > totalBound {
		t.Fatalf("rounded difference %.6f exceeds bound %.6f", difference, totalBound)
	}
}

func parseFloatScaled(text string, scale int) (float64, error) {
	var whole, frac int64
	negative := text[0] == '-'
	parts := splitDot(text)
	if _, err := parseInt64(parts[0], &whole); err != nil {
		return 0, err
	}
	if len(parts) == 2 {
		for len(parts[1]) < scale {
			parts[1] += "0"
		}
		if _, err := parseInt64(parts[1][:scale], &frac); err != nil {
			return 0, err
		}
	}
	value := float64(whole) + float64(frac)/math.Pow10(scale)
	if negative {
		value = -value
	}
	return value, nil
}

func splitDot(text string) []string {
	for i := 0; i < len(text); i++ {
		if text[i] == '.' {
			return []string{text[:i], text[i+1:]}
		}
	}
	return []string{text}
}

func parseInt64(text string, out *int64) (int, error) {
	value := int64(0)
	for _, r := range text {
		if r == '-' {
			continue
		}
		value = value*10 + int64(r-'0')
	}
	if text != "" && text[0] == '-' {
		value = -value
	}
	*out = value
	return len(text), nil
}

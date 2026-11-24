package mixed_test

import (
	"testing"

	"ampcheckpoint/internal/domain"
	"ampcheckpoint/internal/mixed"
)

func TestHalfRoundTripKnownValues(t *testing.T) {
	for _, value := range []float32{0, 1, -1, 0.5, 65504, 0.00006103515625} {
		bits := mixed.Float32ToHalfBits(value)
		got := mixed.HalfBitsToFloat32(bits)
		if value == 65504 && got != 65504 {
			t.Fatalf("value=%v bits=%#x got=%v", value, bits, got)
		}
		if value != 65504 && value != 0 && got == 0 {
			t.Fatalf("value=%v collapsed to zero (%#x)", value, bits)
		}
	}
}

func TestQuantizeDeclaresFixedPointAndProducesBoundedError(t *testing.T) {
	master := []int64{1_000_000, -2_000_000, 333_333, 0, 125_000}
	q, err := mixed.Quantize("s00", 3, 128*domain.FixedScale, master)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Bits) != len(master) || len(q.RestoredFixed) != len(master) {
		t.Fatalf("length mismatch: %d %d %d", len(master), len(q.Bits), len(q.RestoredFixed))
	}
	if q.MaxAbsDeviation <= 0 {
		t.Fatalf("expected measurable FP16 rounding, got max_abs=%d cumulative=%d", q.MaxAbsDeviation, q.CumulativeAbsDev)
	}
	if q.CumulativeAbsDev < q.MaxAbsDeviation {
		t.Fatalf("cumulative %d < max %d", q.CumulativeAbsDev, q.MaxAbsDeviation)
	}
	if q.Digest == q.RestoredDigest || len(q.Digest) != 64 || len(q.RestoredDigest) != 64 {
		t.Fatalf("digest contract not populated: %#v", q)
	}
	for i, restored := range q.RestoredFixed {
		deviation := restored - master[i]
		if deviation < 0 {
			deviation = -deviation
		}
		if deviation > q.MaxAbsDeviation {
			t.Fatalf("element %d deviation %d > max %d", i, deviation, q.MaxAbsDeviation)
		}
	}
}

func TestQuantizeRejectsFP16Overflow(t *testing.T) {
	_, err := mixed.Quantize("s00", 1, 4096*domain.FixedScale, []int64{100 * domain.FixedScale})
	if err == nil {
		t.Fatal("expected FP16 overflow")
	}
}

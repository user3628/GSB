package normalize_test

import (
	"testing"

	"mitmpipeline/internal/normalize"
)

func TestRoundToMSAndCumulativeError(t *testing.T) {
	first := normalize.RoundToMS(1, "e1", 1_250_499, normalize.RoundNearest, 0)
	if first.TimestampMS != 1 || first.RoundingBit != 0 || first.AppliedDeltaNS != -250_499 {
		t.Fatalf("unexpected first rounding: %+v", first)
	}
	second := normalize.RoundToMS(2, "e2", 2_650_000, normalize.RoundNearest, first.CumulativeRoundingErrorNS)
	if second.TimestampMS != 3 || second.RoundingBit != 1 || !second.RoundedUp {
		t.Fatalf("unexpected second rounding: %+v", second)
	}
	if second.CumulativeRoundingErrorNS != first.AppliedDeltaNS+second.AppliedDeltaNS {
		t.Fatalf("cumulative error not accumulated: %+v %+v", first, second)
	}

	ceil := normalize.RoundToMS(3, "e3", 5_000_001, normalize.RoundCeil, 0)
	if ceil.TimestampMS != 6 || !ceil.RoundedUp {
		t.Fatalf("ceil should advance one millisecond: %+v", ceil)
	}
}

func TestIdentityNormalization(t *testing.T) {
	if got := normalize.ConnectionID(" abc-001 "); got != "ABC-001" {
		t.Fatalf("connection id: %q", got)
	}
	if got := normalize.SNI("Good.Internal."); got != "good.internal" {
		t.Fatalf("sni: %q", got)
	}
	if got := normalize.Serial("ab:cd:01"); got != "ABCD01" {
		t.Fatalf("serial: %q", got)
	}
}

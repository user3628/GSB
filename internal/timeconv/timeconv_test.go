package timeconv

import (
	"testing"

	"forensicworkbench/internal/codes"
)

func TestFiletimeKnownAnchor(t *testing.T) {
	// FILETIME for 2024-01-02T03:04:05Z equals 133486382450000000 ticks.
	const ft = uint64(133486382450000000)
	got, err := FromFiletime(ft, "133486382450000000", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Repeated conversion must be byte-identical (pure function).
	got2, _ := FromFiletime(ft, "133486382450000000", 0)
	if got != got2 {
		t.Fatalf("non-deterministic conversion: %d vs %d", got, got2)
	}
	// Cross-check against RFC3339 of the same instant.
	want, err := FromRFC3339("2024-01-02T03:04:05Z", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("filetime %d != rfc3339 %d", got, want)
	}
}

func TestFiletimeEpochDifference(t *testing.T) {
	// Exactly the Unix epoch (1970-01-01T00:00:00Z) must map to zero.
	got, err := FromFiletime(filetimeUnixEpochTicks, "116444736000000000", 0)
	if err != nil || got != 0 {
		t.Fatalf("epoch mapping got %d err %v", got, err)
	}
}

func TestFiletimeNoSubTickPrecisionLoss(t *testing.T) {
	// One FILETIME tick is exactly 100 ns, so every representable tick maps to
	// an integer number of nanoseconds: there is never sub-tick truncation.
	// Assert this invariant on an arbitrary non-aligned tick value.
	ft := filetimeUnixEpochTicks + 1234567 // 1234567 ticks after the Unix epoch
	got, err := FromFiletime(ft, "x", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != int64(1234567)*100 {
		t.Fatalf("expected exact %d ns, got %d", int64(1234567)*100, got)
	}
	if got%100 != 0 {
		t.Fatal("conversion introduced sub-100ns precision that FILETIME cannot express")
	}
}

func TestFiletimeOverflow(t *testing.T) {
	cases := []uint64{
		1<<64 - 1,            // max uint64
		filetimeMaxTicks + 1, // just past the positive bound
		0,                    // far before representable negative range
		filetimeMinTicks - 1, // just before the negative bound
	}
	for _, ft := range cases {
		_, err := FromFiletime(ft, "raw", 42)
		if err == nil {
			t.Fatalf("expected overflow for %d", ft)
		}
		if !IsOverflow(err) {
			t.Fatalf("expected OverflowError, got %T %v", err, err)
		}
		if oe := err.(*OverflowError); oe.Code != codes.ETimeOverflow || oe.Offset != 42 || oe.Raw != "raw" {
			t.Fatalf("overflow context not preserved: %+v", oe)
		}
	}
}

func TestEpochMillisOverflow(t *testing.T) {
	// 1<<63 milliseconds cannot scale to nanoseconds inside int64.
	big := int64(1) << 62
	if _, err := FromEpochMillis(big, "raw", 7); err == nil || !IsOverflow(err) {
		t.Fatalf("expected epoch-ms overflow, got %v", err)
	}
	// A normal value converts and stays deterministic.
	got, err := FromEpochMillis(1704164645000, "x", 0)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := FromEpochMillis(1704164645000, "x", 0)
	if got != again {
		t.Fatal("epoch-ms conversion not stable")
	}
}

func TestRFC3339RejectsGarbage(t *testing.T) {
	if _, err := FromRFC3339("not-a-timestamp", 3); err == nil || !IsOverflow(err) {
		t.Fatalf("expected tagged error, got %v", err)
	}
}

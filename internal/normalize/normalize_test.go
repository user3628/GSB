package normalize

import (
	"math"
	"testing"

	"meshmigrate/internal/compat"
	"meshmigrate/internal/model"
)

func testDefaults() compat.Defaults {
	return compat.Defaults{
		RateLimitBps: 1000000, TimeoutMs: 1500, CircuitWindowMs: 30000,
		RateWindowMs: 60000, WindowAlignment: model.AlignmentUp, PolicyRevision: 1,
	}
}

func fullRaw(policyID string) model.RawPolicy {
	return model.RawPolicy{
		PolicyID: policyID, VirtualServiceID: "svc", PolicyRevision: 2, LastWriteMs: 10,
		Match:          model.RouteMatch{Method: "GET", PathPrefix: "/x", Headers: map[string]string{"h": "v"}},
		RateLimitValue: 42, RateLimitUnit: "kbps",
		TimeoutValue: 1500000, TimeoutUnit: "us",
		CircuitWindowVal: 30000000, CircuitWindowUnit: "us",
		RateWindowValue: 60000000, RateWindowUnit: "us",
		CapturedAtValue: 1760000000123456, CapturedAtUnit: "us",
		WindowAlignment: model.AlignmentDown,
		Presence: model.Presence{
			RateLimit: true, Timeout: true, CircuitWindow: true,
			RateWindow: true, CapturedAt: true, WindowAlignment: true,
		},
	}
}

func TestNormalizeUnitsRoundingAndAlignment(t *testing.T) {
	parsed := &model.ParsedSnapshot{
		SnapshotVersion: 1, SnapshotID: "n", GeneratedAtMs: 1760000000000,
		SourceFormat: model.FormatJSONL, FormatVersion: 2, SourceDigest: "sha256:x",
		Policies: []model.RawPolicy{fullRaw("p1")},
	}
	got, ne := Normalize(parsed, testDefaults(), model.TruncationFloor)
	if ne != nil {
		t.Fatal(ne)
	}
	p := got.Policies[0]
	if p.RateLimitBps != 42000 || p.TimeoutMs != 1500 || p.CircuitWindowMs != 30000 || p.RateWindowMs != 60000 {
		t.Fatalf("unit normalization: %+v", p)
	}
	if p.Rounding.RemainderUs != 456 || p.Rounding.RoundingBit != 1 || p.Rounding.ErrorUs != -456 || p.Rounding.CumulativeErrorUs != -456 {
		t.Fatalf("floor rounding audit: %+v", p.Rounding)
	}
	if p.Rounding.ConvertedTimestampMs != 1760000000123 {
		t.Fatalf("floor timestamp: %d", p.Rounding.ConvertedTimestampMs)
	}
	if p.RateWindowStartMs%int64(p.RateWindowMs) != 0 || p.RateWindowEndMs-p.RateWindowStartMs != int64(p.RateWindowMs) {
		t.Fatalf("window not aligned: start=%d end=%d window=%d", p.RateWindowStartMs, p.RateWindowEndMs, p.RateWindowMs)
	}

	ceil, _ := Normalize(parsed, testDefaults(), model.TruncationCeil)
	if ceil.Policies[0].Rounding.ConvertedTimestampMs != 1760000000124 || ceil.Policies[0].Rounding.ErrorUs != 544 {
		t.Fatalf("ceil rounding: %+v", ceil.Policies[0].Rounding)
	}
	nearest, _ := Normalize(parsed, testDefaults(), model.TruncationNearest)
	if nearest.Policies[0].Rounding.ConvertedTimestampMs != 1760000000123 || nearest.Policies[0].Rounding.ErrorUs != -456 {
		t.Fatalf("nearest rounding: %+v", nearest.Policies[0].Rounding)
	}
}

func TestNormalizeDefaultsAndOverflow(t *testing.T) {
	legacy := &model.ParsedSnapshot{
		SnapshotVersion: 1, SnapshotID: "legacy", GeneratedAtMs: 1760000000000,
		SourceFormat: model.FormatBinary, FormatVersion: 1, SourceDigest: "sha256:x",
		Policies: []model.RawPolicy{{
			PolicyID: "legacy", VirtualServiceID: "svc",
			Match:           model.RouteMatch{Method: "GET", PathPrefix: "/legacy"},
			TimeoutValue:    1000,
			TimeoutUnit:     "ms",
			CapturedAtValue: 1760000000001,
			CapturedAtUnit:  "ms",
			Presence:        model.Presence{Timeout: true, CapturedAt: true},
		}},
	}
	got, ne := Normalize(legacy, testDefaults(), model.TruncationFloor)
	if ne != nil {
		t.Fatal(ne)
	}
	p := got.Policies[0]
	if p.RateLimitBps != 1000000 || p.CircuitWindowMs != 30000 || p.RateWindowMs != 60000 || p.WindowAlignment != model.AlignmentUp || p.PolicyRevision != 1 {
		t.Fatalf("defaults not applied: %+v", p)
	}
	if len(got.DefaultDetails) < 4 || len(p.DefaultsApplied) < 4 {
		t.Fatalf("default detail missing: snapshot=%d policy=%v", len(got.DefaultDetails), p.DefaultsApplied)
	}

	overflow := &model.ParsedSnapshot{
		SnapshotVersion: 1, SnapshotID: "overflow", SourceFormat: model.FormatJSONL, FormatVersion: 2,
		Policies: []model.RawPolicy{{
			PolicyID: "overflow", VirtualServiceID: "svc", Match: model.RouteMatch{PathPrefix: "/x"},
			RateLimitValue: math.MaxUint64, RateLimitUnit: "kbps", Presence: model.Presence{RateLimit: true},
		}},
	}
	_, ne = Normalize(overflow, testDefaults(), model.TruncationFloor)
	if ne == nil || ne.Code != CodeOverflow {
		t.Fatalf("expected overflow, got %+v", ne)
	}
}

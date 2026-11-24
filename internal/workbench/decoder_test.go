package workbench

import (
	"reflect"
	"testing"
)

func TestDecodeBusinessCodes(t *testing.T) {
	cfg := DefaultConfig()
	cases := []struct {
		kind string
		code string
	}{
		{SampleValid, CodeOK},
		{SampleTruncated, CodeTruncated},
		{SampleLengthWrap, CodeLengthWrap},
		{SampleLengthOverflow, CodeLengthOverflow},
		{SampleDepth, CodeDepthExceeded},
		{SampleNestedBudget, CodeNestedBudget},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			spec := TaskSpec{TaskID: "sample-" + tc.kind, SampleKind: tc.kind, DeclaredBudget: 1 << 20}
			sample := BuildSample(spec, cfg)
			got := DecodeContainer(sample, DecodeLimits{MaxDepth: cfg.MaxDepth, NestedBudget: cfg.NestedBudget})
			if got.Code != tc.code {
				t.Fatalf("code = %s, want %s; failure=%+v issues=%+v", got.Code, tc.code, got.Failure, got.Issues)
			}
			if tc.kind == SampleTruncated {
				if got.SafePrefix != cfg.RecordCount || got.Failure == nil || got.Failure.RecordIndex != got.SafePrefix {
					t.Fatalf("safe prefix/failure = %d/%v, want %d/non-nil", got.SafePrefix, got.Failure, cfg.RecordCount)
				}
			}
			if tc.kind == SampleDepth || tc.kind == SampleNestedBudget {
				if !hasAction(got.Trajectory, "subtree_abort") || !hasAction(got.Trajectory, "parent_recover") {
					t.Fatalf("trajectory lacks subtree recovery: %+v", got.Trajectory)
				}
				if len(got.Records) < 2 {
					t.Fatalf("recoverable sample did not continue to sibling record: %d records", len(got.Records))
				}
			}
		})
	}
}

func TestDecodeMetricsAreDeterministic(t *testing.T) {
	cfg := DefaultConfig()
	spec := TaskSpec{TaskID: "metric", SampleKind: SampleValid}
	sample := BuildSample(spec, cfg)
	limits := DecodeLimits{MaxDepth: cfg.MaxDepth, NestedBudget: cfg.NestedBudget}
	first := DecodeContainer(sample, limits)
	for i := 0; i < 20; i++ {
		next := DecodeContainer(sample, limits)
		if !reflect.DeepEqual(first, next) {
			t.Fatalf("decode %d differs from first decode", i)
		}
	}
	if first.PeakBytes <= 0 || first.Allocations <= 0 {
		t.Fatalf("metrics were not recorded: peak=%d allocations=%d", first.PeakBytes, first.Allocations)
	}
	for _, record := range first.Records {
		if record.PeakBytes <= 0 || record.Allocations <= 0 || record.Digest == "" {
			t.Fatalf("record metrics/summary incomplete: %+v", record)
		}
	}
}

func TestNestedLimitsAreConfigurable(t *testing.T) {
	cfg := DefaultConfig()
	spec := TaskSpec{TaskID: "limit", SampleKind: SampleValid}
	sample := BuildSample(spec, cfg)
	depthLimited := DecodeContainer(sample, DecodeLimits{MaxDepth: 1, NestedBudget: cfg.NestedBudget})
	if depthLimited.Code != CodeDepthExceeded {
		t.Fatalf("depth-limited code = %s, want %s", depthLimited.Code, CodeDepthExceeded)
	}
	budgetLimited := DecodeContainer(sample, DecodeLimits{MaxDepth: cfg.MaxDepth, NestedBudget: 1})
	if budgetLimited.Code != CodeNestedBudget {
		t.Fatalf("budget-limited code = %s, want %s", budgetLimited.Code, CodeNestedBudget)
	}
	if !hasAction(depthLimited.Trajectory, "parent_recover") || !hasAction(budgetLimited.Trajectory, "parent_recover") {
		t.Fatal("configured limits did not record parent recovery")
	}
}

func hasAction(events []TraceEvent, action string) bool {
	for _, event := range events {
		if event.Action == action {
			return true
		}
	}
	return false
}

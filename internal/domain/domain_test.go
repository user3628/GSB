package domain

import (
	"encoding/json"
	"testing"
)

func TestParseFixedPoint(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		present bool
		scaled  int64
		invalid bool
	}{
		{name: "integer string", raw: `"12"`, present: true, scaled: 12_000_000},
		{name: "decimal string", raw: `"-1.250001"`, present: true, scaled: -1_250_001},
		{name: "JSON number", raw: `2.5`, present: true, scaled: 2_500_000},
		{name: "null", raw: `null`, present: false},
		{name: "too many decimals", raw: `"1.0000001"`, invalid: true},
		{name: "exponent", raw: `1e3`, invalid: true},
		{name: "empty", raw: `""`, invalid: true},
		{name: "overflow", raw: `"9223372036854.775808"`, invalid: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			measure, err := ParseFixedPoint(json.RawMessage(test.raw))
			if test.invalid {
				if err == nil || err.Code != CodeSnapshotInvalid {
					t.Fatalf("expected E_SNAPSHOT_INVALID, got measure=%+v err=%v", measure, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if measure.Present != test.present || (test.present && measure.Scaled != test.scaled) {
				t.Fatalf("got %+v, want present=%v scaled=%d", measure, test.present, test.scaled)
			}
		})
	}
}

func TestPlanWindowTypesAndBoundaries(t *testing.T) {
	snapshot := mustSnapshot(t, `{
	  "snapshot_id":"plan-test",
	  "tenants":[{"id":"t1","priority":1}],
	  "window_specs":[
	    {"id":"tumble","type":"tumbling","width_ms":1000},
	    {"id":"slide","type":"sliding","width_ms":1000,"slide_ms":400},
	    {"id":"session","type":"session","width_ms":100000,"session_gap_ms":1000}
	  ],
	  "events":[
	    {"event_id":"a","tenant_id":"t1","group_key":"g","timestamp_ms":1000,"seq":1,"measure":"1"},
	    {"event_id":"b","tenant_id":"t1","group_key":"g","timestamp_ms":1001,"seq":2,"measure":"2"},
	    {"event_id":"c","tenant_id":"t1","group_key":"g","timestamp_ms":999,"seq":3,"measure":"3"}
	  ]
	}`)
	windows, err := PlanWindows(snapshot)
	if err != nil {
		t.Fatalf("plan windows: %v", err)
	}
	bySpec := map[string][]Window{}
	for _, window := range windows {
		bySpec[window.SpecID] = append(bySpec[window.SpecID], window)
	}
	if len(bySpec["tumble"]) != 2 {
		t.Fatalf("tumbling windows = %d, want 2", len(bySpec["tumble"]))
	}
	if bySpec["tumble"][0].StartMS != 0 || bySpec["tumble"][1].StartMS != 1000 {
		t.Fatalf("tumbling boundaries wrong: %+v", bySpec["tumble"])
	}

	eventAt1000Starts := make([]int64, 0)
	for _, window := range bySpec["slide"] {
		if window.StartMS <= 1000 && 1000 < window.EndMS {
			eventAt1000Starts = append(eventAt1000Starts, window.StartMS)
		}
	}
	if len(eventAt1000Starts) != 2 || eventAt1000Starts[0] != 400 || eventAt1000Starts[1] != 800 {
		t.Fatalf("event at ts=1000 should be in sliding starts [400,800], got %v", eventAt1000Starts)
	}

	aggregateWindow := bySpec["tumble"][1]
	aggregate, aggregateErr := AggregateWindow(aggregateWindow, snapshot.Events)
	if aggregateErr != nil {
		t.Fatalf("aggregate: %v", aggregateErr)
	}
	if aggregate.Count != 2 {
		t.Fatalf("left-closed boundary count = %d, want 2 (999 is excluded, 1000 and 1001 included)", aggregate.Count)
	}
}

func TestCrossYearEpochAlignment(t *testing.T) {
	snapshot := mustSnapshot(t, `{
	  "snapshot_id":"epoch-test",
	  "tenants":[{"id":"t","priority":1}],
	  "window_specs":[{"id":"day","type":"tumbling","width_ms":86400000}],
	  "events":[
	    {"event_id":"before","tenant_id":"t","group_key":"g","timestamp_ms":1704067199999,"seq":1,"measure":"1"},
	    {"event_id":"at","tenant_id":"t","group_key":"g","timestamp_ms":1704067200000,"seq":2,"measure":"2"}
	  ]
	}`)
	windows, err := PlanWindows(snapshot)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(windows) != 2 {
		t.Fatalf("windows = %d, want 2", len(windows))
	}
	if windows[0].StartMS != 1703980800000 || windows[1].StartMS != 1704067200000 {
		t.Fatalf("cross-year starts = %v, %v", windows[0].StartMS, windows[1].StartMS)
	}
}

func TestSessionGapBoundary(t *testing.T) {
	snapshot := mustSnapshot(t, `{
	  "snapshot_id":"session-test",
	  "tenants":[{"id":"t","priority":1}],
	  "window_specs":[{"id":"s","type":"session","width_ms":1000000,"session_gap_ms":1000}],
	  "events":[
	    {"event_id":"1","tenant_id":"t","group_key":"g","timestamp_ms":0,"seq":1,"measure":"1"},
	    {"event_id":"2","tenant_id":"t","group_key":"g","timestamp_ms":500,"seq":2,"measure":"1"},
	    {"event_id":"3","tenant_id":"t","group_key":"g","timestamp_ms":1500,"seq":3,"measure":"1"},
	    {"event_id":"4","tenant_id":"t","group_key":"g","timestamp_ms":2000,"seq":4,"measure":"1"}
	  ]
	}`)
	windows, err := PlanWindows(snapshot)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(windows) != 2 {
		t.Fatalf("sessions = %d, want 2: %+v", len(windows), windows)
	}
	if windows[0].StartMS != 0 || windows[0].EndMS != 1500 {
		t.Fatalf("first session = [%d,%d), want [0,1500)", windows[0].StartMS, windows[0].EndMS)
	}
	if windows[1].StartMS != 1500 || windows[1].EndMS != 3000 {
		t.Fatalf("second session = [%d,%d), want [1500,3000)", windows[1].StartMS, windows[1].EndMS)
	}
}

func TestSlidingWindowIncludesPreEpochStart(t *testing.T) {
	snapshot := mustSnapshot(t, `{
	  "snapshot_id":"pre-epoch",
	  "tenants":[{"id":"t","priority":1}],
	  "window_specs":[{"id":"slide","type":"sliding","width_ms":1000,"slide_ms":400}],
	  "events":[{"event_id":"zero","tenant_id":"t","group_key":"g","timestamp_ms":0,"seq":1,"measure":"1"}]
	}`)
	windows, err := PlanWindows(snapshot)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	starts := make([]int64, 0, len(windows))
	for _, window := range windows {
		starts = append(starts, window.StartMS)
	}
	want := []int64{-800, -400, 0}
	if len(starts) != len(want) {
		t.Fatalf("starts=%v, want %v", starts, want)
	}
	for index := range want {
		if starts[index] != want[index] {
			t.Fatalf("starts=%v, want %v", starts, want)
		}
	}
}

func TestSessionUsesGapAndOnlyExplicitMaxWidth(t *testing.T) {
	snapshot := mustSnapshot(t, `{
	  "snapshot_id":"session-width",
	  "tenants":[{"id":"t","priority":1}],
	  "window_specs":[
	    {"id":"gap-only","type":"session","width_ms":10,"session_gap_ms":1000},
	    {"id":"max-width","type":"session","width_ms":10,"session_gap_ms":1000,"max_session_ms":100}
	  ],
	  "events":[
	    {"event_id":"1","tenant_id":"t","group_key":"g","timestamp_ms":0,"seq":1,"measure":"1"},
	    {"event_id":"2","tenant_id":"t","group_key":"g","timestamp_ms":50,"seq":2,"measure":"1"},
	    {"event_id":"3","tenant_id":"t","group_key":"g","timestamp_ms":150,"seq":3,"measure":"1"}
	  ]
	}`)
	windows, err := PlanWindows(snapshot)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	counts := map[string]int{}
	for _, window := range windows {
		counts[window.SpecID]++
	}
	if counts["gap-only"] != 1 {
		t.Fatalf("gap-only sessions=%d, want 1 (width_ms is not an implicit max)", counts["gap-only"])
	}
	if counts["max-width"] != 2 {
		t.Fatalf("max-width sessions=%d, want 2", counts["max-width"])
	}
}

func TestAggregateDedupMissingAndOverflow(t *testing.T) {
	window := Window{
		SnapshotID: "s", WindowID: "w", TenantID: "t", SpecID: "x", GroupKey: "g",
		StartMS: 0, EndMS: 100, Boundary: BoundaryLeftClosedRightOpen, WindowType: WindowTumbling,
	}
	events := []Event{
		{EventID: "a", TenantID: "t", GroupKey: "g", TimestampMS: 10, Seq: 1, Measure: Measure{Present: true, Scaled: 1_000_000}},
		{EventID: "b", TenantID: "t", GroupKey: "g", TimestampMS: 20, Seq: 1, Measure: Measure{Present: true, Scaled: 2_000_000}},
		{EventID: "missing", TenantID: "t", GroupKey: "g", TimestampMS: 30, Seq: 2},
		{EventID: "outside", TenantID: "t", GroupKey: "g", TimestampMS: 100, Seq: 3, Measure: Measure{Present: true, Scaled: 9_000_000}},
	}
	aggregate, err := AggregateWindow(window, events)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if aggregate.Count != 2 || aggregate.SampleSize != 1 {
		t.Fatalf("count/sample = %d/%d, want 2/1", aggregate.Count, aggregate.SampleSize)
	}
	if aggregate.SumScaled != 2_000_000 || *aggregate.MinScaled != 2_000_000 || *aggregate.MaxScaled != 2_000_000 {
		t.Fatalf("dedup winner wrong: %+v", aggregate)
	}

	overflowEvents := []Event{
		{EventID: "max", TenantID: "t", GroupKey: "g", TimestampMS: 1, Seq: 1, Measure: Measure{Present: true, Scaled: 9223372036854775807}},
		{EventID: "one", TenantID: "t", GroupKey: "g", TimestampMS: 2, Seq: 2, Measure: Measure{Present: true, Scaled: 1}},
	}
	_, overflowErr := AggregateWindow(window, overflowEvents)
	if overflowErr == nil || overflowErr.Code != CodeNumericOverflow {
		t.Fatalf("expected overflow, got %v", overflowErr)
	}
	if overflowErr.Context["tenant_id"] != "t" || overflowErr.Context["window_id"] != "w" || overflowErr.Context["group_key"] != "g" {
		t.Fatalf("overflow context incomplete: %+v", overflowErr.Context)
	}
}

func TestSnapshotValidation(t *testing.T) {
	cases := []string{
		`{"snapshot_id":"s","tenants":[{"id":"t","priority":1},{"id":"t","priority":2}],"window_specs":[{"id":"w","type":"tumbling","width_ms":1}]}`,
		`{"snapshot_id":"s","tenants":[{"id":"t","priority":1}],"window_specs":[{"id":"w","type":"tumbling","width_ms":0}]}`,
		`{"snapshot_id":"s","tenants":[{"id":"t","priority":1}],"window_specs":[{"id":"w","type":"tumbling","width_ms":1}],"events":[{"tenant_id":"t","group_key":"g","timestamp_ms":-1,"seq":1,"measure":"1"}]}`,
		`{"snapshot_id":"s","tenants":[{"id":"t","priority":1}],"window_specs":[{"id":"w","type":"tumbling","width_ms":1}],"events":[{"tenant_id":"t","group_key":"g","timestamp_ms":1,"seq":1,"measure":"1.1234567"}]}`,
	}
	for index, raw := range cases {
		_, err := ParseSnapshot([]byte(raw))
		if err == nil || err.Code != CodeSnapshotInvalid {
			t.Fatalf("case %d: expected E_SNAPSHOT_INVALID, got %v", index, err)
		}
	}
}

func mustSnapshot(t *testing.T, raw string) *Snapshot {
	t.Helper()
	snapshot, err := ParseSnapshot([]byte(raw))
	if err != nil {
		t.Fatalf("parse snapshot: %v", err)
	}
	return snapshot
}

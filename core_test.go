package abeval

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
)

func TestQuantileNearestRankIsDeterministic(t *testing.T) {
	values := []int64{9, 1, 5, 3, 7, 2, 8, 4, 6, 10}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	got95, ok := QuantileNearestRank(sortedLatencies(values), 0.95)
	if !ok || got95 != 10 {
		t.Fatalf("p95 = %d, %v; want 10, true", got95, ok)
	}
	got50, ok := QuantileNearestRank(sortedLatencies(values), 0.5)
	if !ok || got50 != 5 {
		t.Fatalf("p50 = %d, %v; want 5, true", got50, ok)
	}
}

func sortedLatencies(values []int64) []int64 {
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted
}

func TestGeneratedScenarioDecisions(t *testing.T) {
	cases := []struct {
		scenario string
		want     string
		codePart string
	}{
		{ScenarioRollout, DecisionRollout, ReasonRolloutMet},
		{ScenarioInsufficient, DecisionObserve, ReasonInsufficientSample},
		{ScenarioRollback, DecisionRollback, "ROLLBACK_"},
		{ScenarioRecovery, DecisionRollout, ReasonRolloutMet},
		{"rollback_ctr", DecisionRollback, ReasonRollbackCTR},
		{"rollback_latency", DecisionRollback, ReasonRollbackLatency},
	}

	for _, tc := range cases {
		t.Run(tc.scenario, func(t *testing.T) {
			dataset := DefaultDataset(tc.scenario)
			report, err := EvaluateDataset(dataset)
			if err != nil {
				t.Fatal(err)
			}
			if report.Decision != tc.want {
				for _, stage := range report.Stages {
					t.Logf("stage %d: %s/%s ctr=%+v latency=%+v", stage.Metrics.StageIndex,
						stage.Verdict.Decision, stage.Verdict.ReasonCode,
						stage.Metrics.CTR, stage.Metrics.Latency)
				}
				t.Fatalf("decision = %s (%s), want %s", report.Decision, report.DecisionCode, tc.want)
			}
			if !strings.Contains(report.DecisionCode, tc.codePart) {
				t.Fatalf("decision code = %s, want substring %s", report.DecisionCode, tc.codePart)
			}

			first, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			secondReport, err := EvaluateDataset(dataset)
			if err != nil {
				t.Fatal(err)
			}
			second, err := json.Marshal(secondReport)
			if err != nil {
				t.Fatal(err)
			}
			if string(first) != string(second) {
				t.Fatal("canonical report is not byte deterministic")
			}
		})
	}
}

func TestInvalidEventStructuredErrors(t *testing.T) {
	base := DefaultDataset(ScenarioRollout)

	tests := []struct {
		name   string
		mutate func(*Dataset)
		code   string
		event  string
	}{
		{
			name:   "timestamp",
			mutate: func(ds *Dataset) { ds.Events[10].TimestampMS = ds.Events[0].TimestampMS },
			code:   CodeInvalidEvent, event: dsEventID(base, 10),
		},
		{
			name:   "group",
			mutate: func(ds *Dataset) { ds.Events[11].Group = "unknown" },
			code:   CodeInvalidEvent, event: dsEventID(base, 11),
		},
		{
			name:   "clicked",
			mutate: func(ds *Dataset) { ds.Events[12].Clicked = 2 },
			code:   CodeInvalidEvent, event: dsEventID(base, 12),
		},
		{
			name:   "negative latency",
			mutate: func(ds *Dataset) { ds.Events[13].LatencyUS = -1 },
			code:   CodeInvalidEvent, event: dsEventID(base, 13),
		},
		{
			name:   "coverage",
			mutate: func(ds *Dataset) { ds.Events[14].TimestampMS = ds.Config.Stages[len(ds.Config.Stages)-1].EndMS + 1 },
			code:   CodeInvalidEvent, event: dsEventID(base, 14),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dataset := DefaultDataset(ScenarioRollout)
			tc.mutate(&dataset)
			err := ValidateDataset(&dataset)
			if ErrorCode(err) != tc.code {
				t.Fatalf("error = %v, want code %s", err, tc.code)
			}
			businessError, _ := AsError(err)
			if businessError.EventID != tc.event {
				t.Fatalf("event_id = %q, want %q", businessError.EventID, tc.event)
			}
			if businessError.Stage == "" {
				t.Fatal("diagnostic stage is empty")
			}
			if tc.name == "coverage" && businessError.Details["sub_code"] != CodeStageCoverage {
				t.Fatalf("coverage sub_code = %v, want %s", businessError.Details["sub_code"], CodeStageCoverage)
			}
		})
	}
}

func dsEventID(dataset Dataset, index int) string {
	return dataset.Events[index].EventID
}

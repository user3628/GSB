package samplegen

import (
	"encoding/json"

	"featureplatform/internal/configx"
)

func SentimentV2Fragment() configx.Fragment {
	return configx.Fragment{
		FragmentID:  "inc-sentiment-task-v2",
		BaseVersion: 1,
		NewVersion:  2,
		Reason:      "advance short_text_sentiment to mapping v2 for deterministic cross-task version demo",
		Changes: []configx.Change{
			{Op: "replace", Path: "/task_versions/short_text_sentiment", Value: json.RawMessage(`2`)},
			{Op: "replace", Path: "/batch_budget", Value: json.RawMessage(`15`)},
		},
	}
}

func CoverageThresholdFragment() configx.Fragment {
	return configx.Fragment{
		FragmentID:  "inc-coverage-threshold-065",
		BaseVersion: 2,
		NewVersion:  3,
		Reason:      "raise global coverage threshold",
		Changes: []configx.Change{
			{Op: "replace", Path: "/coverage_threshold", Value: json.RawMessage(`0.65`)},
		},
	}
}

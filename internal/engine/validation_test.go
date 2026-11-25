package engine

import (
	"testing"

	"alarmengine/internal/errcode"
	"alarmengine/internal/model"
)

func TestValidateServices(t *testing.T) {
	base := []model.Service{
		{ID: "a", Zone: "z1", WeightPerMille: 100, Upstreams: nil, Sensitivity: model.SensHigh, SilenceWindow: 1000},
		{ID: "b", Zone: "z1", WeightPerMille: 200, Upstreams: []string{"a"}, Sensitivity: model.SensLow, SilenceWindow: 2000},
	}
	if err := model.ValidateServices(base); err != nil {
		t.Fatalf("valid set rejected: %v", err)
	}

	cases := []struct {
		name string
		mod  func([]model.Service) []model.Service
	}{
		{"duplicate id", func(s []model.Service) []model.Service { s[1].ID = "a"; return s }},
		{"weight zero", func(s []model.Service) []model.Service { s[0].WeightPerMille = 0; return s }},
		{"weight over 1000", func(s []model.Service) []model.Service { s[0].WeightPerMille = 1001; return s }},
		{"unknown upstream", func(s []model.Service) []model.Service { s[0].Upstreams = []string{"ghost"}; return s }},
		{"bad sensitivity", func(s []model.Service) []model.Service { s[0].Sensitivity = 9; return s }},
		{"negative silence", func(s []model.Service) []model.Service { s[0].SilenceWindow = -1; return s }},
		{"self dependency", func(s []model.Service) []model.Service { s[0].Upstreams = []string{"a"}; return s }},
		{"cycle", func(s []model.Service) []model.Service { s[0].Upstreams = []string{"b"}; return s }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cp := append([]model.Service(nil), base...)
			cp = tc.mod(cp)
			err := model.ValidateServices(cp)
			if errcode.CodeOf(err) != errcode.ESampleInvalid {
				t.Fatalf("want E_SAMPLE_INVALID, got %v", err)
			}
		})
	}
}

func TestValidateObservations(t *testing.T) {
	known := map[string]bool{"a": true}
	good := []model.Observation{obs(1, "a", "cpu", 90, nil)}
	if err := model.ValidateObservations(good, known); err != nil {
		t.Fatalf("valid obs rejected: %v", err)
	}
	bad := []model.Observation{
		{TimestampMS: -1, ServiceID: "a", Metric: "cpu", Value: 1, Labels: map[string]string{}},
		{TimestampMS: 1, ServiceID: "unknown", Metric: "cpu", Value: 1, Labels: map[string]string{}},
		{TimestampMS: 1, ServiceID: "a", Metric: "", Value: 1, Labels: map[string]string{}},
	}
	for i, b := range bad {
		if err := model.ValidateObservations([]model.Observation{b}, known); errcode.CodeOf(err) != errcode.ESampleInvalid {
			t.Fatalf("case %d want E_SAMPLE_INVALID, got %v", i, err)
		}
	}
}

func TestInvalidBatchPersistsNothing(t *testing.T) {
	eng, data := bootstrappedEngine(t)
	bad := append([]model.Observation(nil), data.Observations...)
	bad[0].ServiceID = "ghost"
	_, err := eng.IngestObservations(IngestRequest{
		RequestID: "req-bad", BaseVersion: 1, Observations: bad,
	})
	if errcode.CodeOf(err) != errcode.ESampleInvalid {
		t.Fatalf("want E_SAMPLE_INVALID, got %v", err)
	}
	if got := eng.Version(); got != 1 {
		t.Fatalf("version changed after failed batch: %d", got)
	}
	alerts, _ := eng.QueryAlerts(2)
	if len(alerts) != 0 {
		t.Fatalf("derived rows created despite rejected batch: %d", len(alerts))
	}
}

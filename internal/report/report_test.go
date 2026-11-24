package report

import (
	"encoding/json"
	"testing"

	"privescworkbench/internal/detector"
	"privescworkbench/internal/domain"
	"privescworkbench/internal/sample"
)

func TestReportStableSortAndNormalization(t *testing.T) {
	g := sample.Generate(1, sample.ScenarioBaseline)
	b, err := sample.ParseBundle(g.Bundle.Version, g.ManifestBytes, g.StreamBytes)
	if err != nil {
		t.Fatal(err)
	}
	r1, err := detector.Run(b, nil, detector.Options{SessionID: "a", ThresholdMicro: 56_000_000})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := detector.Run(b, nil, detector.Options{SessionID: "b", ThresholdMicro: 56_000_000})
	if err != nil {
		t.Fatal(err)
	}
	rep1 := FromRound(r1.Round, 7, true)
	rep2 := FromRound(r2.Round, 99, true)
	j1, err := rep1.JSON()
	if err != nil {
		t.Fatal(err)
	}
	j2, err := rep2.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(j1) != string(j2) {
		t.Fatal("normalized reports must be byte-identical across runs")
	}
	var doc Report
	if err := json.Unmarshal(j1, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.SampledAt != domain.FixedSampledAt || doc.Version != 0 || doc.ElapsedMicros != 0 {
		t.Fatal("normalization markers wrong")
	}
	for i := 1; i < len(doc.Rows); i++ {
		if doc.Rows[i-1].PID > doc.Rows[i].PID {
			t.Fatal("rows must be sorted by pid then code")
		}
	}
}

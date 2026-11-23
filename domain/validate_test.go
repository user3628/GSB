package domain

import (
	"math"
	"testing"
)

func definition() ReleaseDefinition {
	return ReleaseDefinition{ReleaseID: "r1", QuantileMethod: "linear", ConfidenceLevelPPM: 9500, Stages: []StageDefinition{{
		Number: 1, TargetTrafficPerMille: 100, MinObservationSamples: 2, LatencyQuantilePerMille: 950,
		Gate: GateThresholds{MaxAbsSuccessDropPPM: 60000, MaxCIHalfWidthPPM: 80000},
	}}}
}

func TestValidateDefinitionRejectsBadStageOrder(t *testing.T) {
	def := definition()
	def.Stages = append(def.Stages, StageDefinition{Number: 1, TargetTrafficPerMille: 300, LatencyQuantilePerMille: 950})
	if err := ValidateDefinition(def); err == nil {
		t.Fatal("expected duplicate stage number to fail")
	}
}

func TestNormalizeBatchRejectsWholeBatch(t *testing.T) {
	def := definition()
	success := true
	latency := int64(10)
	in := BatchInput{ReleaseID: "r1", BatchID: "b1", Stage: 1, Observations: []ObservationInput{
		{TimestampMS: 2, InstanceID: "i1", Group: GroupCanary, Success: &success, LatencyUS: &latency},
		{TimestampMS: 1, InstanceID: "i2", Group: GroupStable, Success: &success, LatencyUS: &latency},
	}}
	if _, err := NormalizeBatch(def, in); err == nil {
		t.Fatal("expected non-monotonic timestamps to reject the whole batch")
	}
}

func TestNormalizeBatchRejectsMissingGroup(t *testing.T) {
	def := definition()
	success := true
	latency := int64(10)
	in := BatchInput{ReleaseID: "r1", BatchID: "b1", Stage: 1, Observations: []ObservationInput{
		{TimestampMS: 1, InstanceID: "i1", Group: "", Success: &success, LatencyUS: &latency},
	}}
	if _, err := NormalizeBatch(def, in); err == nil {
		t.Fatal("expected missing group marker to fail")
	}
}

func TestNormalizeBatchRejectsUnknownStage(t *testing.T) {
	def := definition()
	success := true
	latency := int64(10)
	in := BatchInput{ReleaseID: "r1", BatchID: "b1", Stage: 99, Observations: []ObservationInput{
		{TimestampMS: 1, InstanceID: "i1", Group: GroupCanary, Success: &success, LatencyUS: &latency},
	}}
	if _, err := NormalizeBatch(def, in); err == nil {
		t.Fatal("expected unknown stage to fail")
	}
}

func TestNormalizeBatchRejectsLatencyOverflow(t *testing.T) {
	def := definition()
	success := true
	first := int64(math.MaxInt64)
	second := int64(1)
	in := BatchInput{ReleaseID: "r1", BatchID: "b1", Stage: 1, Observations: []ObservationInput{
		{TimestampMS: 1, InstanceID: "i1", Group: GroupCanary, Success: &success, LatencyUS: &first},
		{TimestampMS: 2, InstanceID: "i2", Group: GroupCanary, Success: &success, LatencyUS: &second},
	}}
	if _, err := NormalizeBatch(def, in); err == nil {
		t.Fatal("expected latency overflow rejection")
	}
}

func TestNormalizeBatchInferStageFromObservations(t *testing.T) {
	def := definition()
	success := true
	latency := int64(10)
	in := BatchInput{ReleaseID: "r1", BatchID: "b1", Observations: []ObservationInput{
		{TimestampMS: 1, InstanceID: "i1", Group: GroupCanary, Success: &success, LatencyUS: &latency, Stage: 1},
		{TimestampMS: 2, InstanceID: "i2", Group: GroupStable, Success: &success, LatencyUS: &latency, Stage: 1},
	}}
	batch, err := NormalizeBatch(def, in)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Stage != 1 || batch.Observations[0].Stage != 1 {
		t.Fatalf("stage was not inferred: %+v", batch)
	}
}

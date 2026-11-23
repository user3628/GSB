package domain

import (
	"errors"
	"math"
	"strings"

	"releasegate/numeric"
)

func (d ReleaseDefinition) Stage(number int) (StageDefinition, bool) {
	for _, s := range d.Stages {
		if s.Number == number {
			return s, true
		}
	}
	return StageDefinition{}, false
}

func (d ReleaseDefinition) LastStage() StageDefinition {
	if len(d.Stages) == 0 {
		return StageDefinition{}
	}
	return d.Stages[len(d.Stages)-1]
}

func ValidateDefinition(def ReleaseDefinition) error {
	if strings.TrimSpace(def.ReleaseID) == "" {
		return NewError(CodeReleaseInvalid, "release_id is required")
	}
	if def.QuantileMethod != numeric.QuantileNearestRank && def.QuantileMethod != numeric.QuantileLinear {
		return WithContext(NewError(CodeReleaseInvalid, "unsupported quantile_method"), "quantile_method", def.QuantileMethod)
	}
	if def.ConfidenceLevelPPM < 5000 || def.ConfidenceLevelPPM >= 10000 {
		return WithContext(NewError(CodeReleaseInvalid, "confidence_level_ppm must be in [5000,9999]"), "confidence_level_ppm", def.ConfidenceLevelPPM)
	}
	if len(def.Stages) == 0 {
		return NewError(CodeReleaseInvalid, "at least one stage is required")
	}
	last := 0
	for i, s := range def.Stages {
		if s.Number <= 0 || s.Number <= last {
			return WithContext(NewError(CodeReleaseInvalid, "stage numbers must be strictly increasing positive integers"), "index", i)
		}
		last = s.Number
		if s.TargetTrafficPerMille < 1 || s.TargetTrafficPerMille > 1000 {
			return WithContext(NewError(CodeReleaseInvalid, "target_traffic_per_mille must be in [1,1000]"), "stage", s.Number)
		}
		if s.MinObservationSamples < 0 {
			return WithContext(NewError(CodeReleaseInvalid, "min_observation_samples must be non-negative"), "stage", s.Number)
		}
		if s.LatencyQuantilePerMille < 1 || s.LatencyQuantilePerMille > 1000 {
			return WithContext(NewError(CodeReleaseInvalid, "latency_quantile_per_mille must be in [1,1000]"), "stage", s.Number)
		}
		g := s.Gate
		if g.MaxAbsSuccessDropPPM < 0 || g.MaxRelativeSuccessDropPPM < 0 || g.MaxLatencyDeltaUS < 0 || g.MaxCIHalfWidthPPM < 0 || g.RelativeTolerancePPM < 0 {
			return WithContext(NewError(CodeReleaseInvalid, "gate thresholds and tolerance must be non-negative"), "stage", s.Number)
		}
	}
	return nil
}

func NormalizeBatch(def ReleaseDefinition, in BatchInput) (Batch, error) {
	if strings.TrimSpace(in.ReleaseID) == "" {
		return Batch{}, NewError(CodeReleaseInvalid, "release_id is required")
	}
	if in.ReleaseID != def.ReleaseID {
		return Batch{}, WithContext(NewError(CodeReleaseInvalid, "batch release_id does not match release definition"), "expected", def.ReleaseID)
	}
	if strings.TrimSpace(in.BatchID) == "" {
		return Batch{}, NewError(CodeReleaseInvalid, "batch_id is required")
	}
	effectiveStage := in.Stage
	if effectiveStage <= 0 {
		for _, raw := range in.Observations {
			if raw.Stage > 0 {
				effectiveStage = raw.Stage
				break
			}
		}
	}
	if effectiveStage <= 0 {
		return Batch{}, NewError(CodeReleaseInvalid, "stage is required on the batch or every observation")
	}
	stage, ok := def.Stage(effectiveStage)
	if !ok {
		return Batch{}, WithContext(NewError(CodeReleaseInvalid, "batch references an unknown stage"), "stage", effectiveStage)
	}
	if len(in.Observations) == 0 {
		return Batch{}, NewError(CodeReleaseInvalid, "observations must not be empty")
	}

	out := Batch{ReleaseID: in.ReleaseID, BatchID: in.BatchID, Stage: effectiveStage, Observations: make([]Observation, 0, len(in.Observations))}
	var lastTimestamp int64
	var canaryLatencySum, stableLatencySum int64
	for i, raw := range in.Observations {
		if !raw.TimestampPresent && raw.TimestampMS == 0 {
			return batchInvalid(i, "timestamp_ms is required")
		}
		if raw.TimestampMS < 0 {
			return batchInvalid(i, "timestamp_ms must be non-negative")
		}
		if i > 0 && raw.TimestampMS < lastTimestamp {
			return batchInvalid(i, "timestamps must be monotonic non-decreasing within a batch")
		}
		lastTimestamp = raw.TimestampMS
		if strings.TrimSpace(raw.InstanceID) == "" {
			return batchInvalid(i, "instance_id is required")
		}
		if raw.Group != GroupCanary && raw.Group != GroupStable {
			return batchInvalid(i, "group must be exactly canary or stable")
		}
		if !raw.SuccessPresent && raw.Success == nil {
			return batchInvalid(i, "success is required")
		}
		if raw.Success == nil {
			return batchInvalid(i, "success is required")
		}
		if !raw.LatencyPresent && raw.LatencyUS == nil {
			return batchInvalid(i, "latency_us is required")
		}
		if raw.LatencyUS == nil || *raw.LatencyUS < 0 {
			return batchInvalid(i, "latency_us must be a non-negative integer")
		}
		if raw.Stage != 0 && raw.Stage != effectiveStage {
			return batchInvalid(i, "observation stage does not match batch stage")
		}
		if raw.Group == GroupCanary {
			if canaryLatencySum > math.MaxInt64-*raw.LatencyUS {
				return batchInvalid(i, "canary latency sum overflows int64")
			}
			canaryLatencySum += *raw.LatencyUS
		} else {
			if stableLatencySum > math.MaxInt64-*raw.LatencyUS {
				return batchInvalid(i, "stable latency sum overflows int64")
			}
			stableLatencySum += *raw.LatencyUS
		}
		out.Observations = append(out.Observations, Observation{
			TimestampMS: raw.TimestampMS,
			InstanceID:  raw.InstanceID,
			Group:       raw.Group,
			Success:     *raw.Success,
			LatencyUS:   *raw.LatencyUS,
			Stage:       effectiveStage,
			BatchID:     in.BatchID,
			Seq:         i,
		})
	}
	_ = stage
	return out, nil
}

func batchInvalid(index int, message string) (Batch, error) {
	return Batch{}, WithContext(NewError(CodeReleaseInvalid, message), "observation_index", index)
}

func ClassifyInsufficient(reason string) *BusinessError {
	return WithContext(NewError(CodeInsufficientSamples, "insufficient samples for requested statistic"), "reason", reason)
}

func CheckedLatencySum(values []int64) (int64, error) {
	var sum int64
	for _, v := range values {
		if v < 0 {
			return 0, errors.New("negative latency")
		}
		if sum > math.MaxInt64-v {
			return 0, errors.New("latency sum overflow")
		}
		sum += v
	}
	return sum, nil
}

package abeval

import (
	"fmt"
	"math"
	"sort"
)

const defaultZScore = 1.959963984540054

func NormalizeConfig(input Config) Config {
	cfg := input
	if cfg.ControlGroup == "" {
		cfg.ControlGroup = "control"
	}
	if cfg.TreatmentGroup == "" {
		cfg.TreatmentGroup = "treatment"
	}
	if len(cfg.Groups) == 0 {
		cfg.Groups = []string{cfg.ControlGroup, cfg.TreatmentGroup}
	}
	if len(cfg.Traffic) == 0 && len(cfg.Groups) == 2 {
		cfg.Traffic = []GroupWeight{
			{Group: cfg.Groups[0], Weight: 0.5},
			{Group: cfg.Groups[1], Weight: 0.5},
		}
	}
	if cfg.MinExposuresPerGroup == 0 {
		cfg.MinExposuresPerGroup = 200
	}
	if cfg.CTR.ZScore == 0 {
		cfg.CTR.ZScore = defaultZScore
	}
	if cfg.CTR.MinLiftAbs == 0 {
		cfg.CTR.MinLiftAbs = 0.02
	}
	if cfg.CTR.RollbackDropAbs == 0 {
		cfg.CTR.RollbackDropAbs = 0.02
	}
	if cfg.Latency.Quantile == 0 {
		cfg.Latency.Quantile = 0.95
	}
	if cfg.Latency.MaxQuantileUS == 0 {
		cfg.Latency.MaxQuantileUS = 300_000
	}
	if cfg.Latency.MaxRegressionUS == 0 {
		cfg.Latency.MaxRegressionUS = 100_000
	}

	stages := append([]StageConfig(nil), cfg.Stages...)
	sort.SliceStable(stages, func(i, j int) bool {
		if stages[i].Index != stages[j].Index {
			return stages[i].Index < stages[j].Index
		}
		return stages[i].StartMS < stages[j].StartMS
	})
	for i := range stages {
		if stages[i].Index == 0 {
			stages[i].Index = i + 1
		}
		if stages[i].MinExposuresPerGroup == 0 {
			stages[i].MinExposuresPerGroup = cfg.MinExposuresPerGroup
		}
	}
	cfg.Stages = stages
	return cfg
}

func ValidateConfig(cfg Config) error {
	if cfg.ExperimentID == "" {
		return NewError(CodeInvalidConfig, "experiment_id is required").
			WithStage("validate_config").WithField("experiment_id")
	}
	if cfg.ControlGroup == "" || cfg.TreatmentGroup == "" {
		return NewError(CodeInvalidConfig, "control_group and treatment_group are required").
			WithStage("validate_config").WithField("groups")
	}
	if cfg.ControlGroup == cfg.TreatmentGroup {
		return NewError(CodeInvalidConfig, "control_group and treatment_group must differ").
			WithStage("validate_config").WithField("control_group")
	}

	groupSet := make(map[string]struct{}, len(cfg.Groups))
	for _, group := range cfg.Groups {
		if group == "" {
			return NewError(CodeInvalidConfig, "group name must not be empty").
				WithStage("validate_config").WithField("groups")
		}
		if _, exists := groupSet[group]; exists {
			return NewError(CodeInvalidConfig, "duplicate configured group").
				WithStage("validate_config").WithField("groups").WithDetail("group", group)
		}
		groupSet[group] = struct{}{}
	}
	for _, group := range []string{cfg.ControlGroup, cfg.TreatmentGroup} {
		if _, ok := groupSet[group]; !ok {
			return NewError(CodeInvalidConfig, "decision group is not in groups").
				WithStage("validate_config").WithField("groups").WithDetail("group", group)
		}
	}

	if len(cfg.Traffic) != 0 {
		weightByGroup := make(map[string]float64, len(cfg.Traffic))
		var total float64
		for _, item := range cfg.Traffic {
			if item.Group == "" {
				return NewError(CodeInvalidConfig, "traffic group must not be empty").
					WithStage("validate_config").WithField("traffic")
			}
			if _, ok := groupSet[item.Group]; !ok {
				return NewError(CodeInvalidConfig, "traffic group is not configured").
					WithStage("validate_config").WithField("traffic").WithDetail("group", item.Group)
			}
			if item.Weight < 0 || item.Weight > 1 || math.IsNaN(item.Weight) {
				return NewError(CodeInvalidConfig, "traffic weight must be in [0,1]").
					WithStage("validate_config").WithField("traffic").
					WithDetail("group", item.Group).WithDetail("weight", item.Weight)
			}
			if _, exists := weightByGroup[item.Group]; exists {
				return NewError(CodeInvalidConfig, "duplicate traffic weight").
					WithStage("validate_config").WithField("traffic").WithDetail("group", item.Group)
			}
			weightByGroup[item.Group] = item.Weight
			total += item.Weight
		}
		if len(cfg.Traffic) != len(cfg.Groups) {
			return NewError(CodeInvalidConfig, "traffic weights must cover every group").
				WithStage("validate_config").WithField("traffic").
				WithDetail("traffic_groups", len(cfg.Traffic)).
				WithDetail("configured_groups", len(cfg.Groups))
		}
		if math.Abs(total-1.0) > 1e-9 {
			return NewError(CodeInvalidConfig, "traffic weights must sum to 1").
				WithStage("validate_config").WithField("traffic").WithDetail("sum", total)
		}
	}

	if len(cfg.Stages) == 0 {
		return NewError(CodeInvalidConfig, "at least one stage is required").
			WithStage("validate_config").WithField("stages")
	}
	for i, stage := range cfg.Stages {
		if stage.Index != i+1 {
			return NewError(CodeInvalidConfig, "stage indexes must be contiguous starting at 1").
				WithStage("validate_config").WithField(fmt.Sprintf("stages[%d].index", i)).
				WithDetail("expected", i+1).WithDetail("actual", stage.Index)
		}
		if stage.StartMS >= stage.EndMS {
			return NewError(CodeInvalidConfig, "stage start_ms must be before end_ms").
				WithStage("validate_config").WithField("stages").
				WithDetail("stage_index", stage.Index)
		}
		if stage.MinExposuresPerGroup < 0 {
			return NewError(CodeInvalidConfig, "min_exposures_per_group must not be negative").
				WithStage("validate_config").WithField("stages").
				WithDetail("stage_index", stage.Index)
		}
		if i > 0 && cfg.Stages[i-1].EndMS != stage.StartMS {
			return NewError(CodeInvalidConfig, "stage ranges must be contiguous").
				WithStage("validate_config").WithField("stages").
				WithDetail("previous_end_ms", cfg.Stages[i-1].EndMS).
				WithDetail("stage_start_ms", stage.StartMS)
		}
	}

	if cfg.CTR.ZScore <= 0 || math.IsNaN(cfg.CTR.ZScore) {
		return NewError(CodeInvalidConfig, "ctr.z_score must be positive").
			WithStage("validate_config").WithField("ctr.z_score")
	}
	if cfg.CTR.MinLiftAbs < 0 || cfg.CTR.RollbackDropAbs < 0 {
		return NewError(CodeInvalidConfig, "CTR thresholds must not be negative").
			WithStage("validate_config").WithField("ctr")
	}
	if cfg.Latency.Quantile <= 0 || cfg.Latency.Quantile > 1 {
		return NewError(CodeInvalidConfig, "latency.quantile must be in (0,1]").
			WithStage("validate_config").WithField("latency.quantile")
	}
	if cfg.Latency.MaxQuantileUS < 0 || cfg.Latency.MaxRegressionUS < 0 {
		return NewError(CodeInvalidConfig, "latency thresholds must not be negative").
			WithStage("validate_config").WithField("latency")
	}
	return nil
}

func StageIndexAt(timestampMS int64, cfg Config) (int, error) {
	for _, stage := range cfg.Stages {
		if timestampMS >= stage.StartMS && timestampMS < stage.EndMS {
			return stage.Index, nil
		}
	}
	first := cfg.Stages[0]
	last := cfg.Stages[len(cfg.Stages)-1]
	return 0, NewError(CodeStageCoverage, "event timestamp is outside configured stage coverage").
		WithStage("assign_stage").
		WithField("timestamp_ms").
		WithDetail("timestamp_ms", timestampMS).
		WithDetail("coverage_start_ms", first.StartMS).
		WithDetail("coverage_end_ms", last.EndMS)
}

func ValidateEvents(events []Event, cfg Config) error {
	allowedGroups := make(map[string]struct{}, len(cfg.Groups))
	for _, group := range cfg.Groups {
		allowedGroups[group] = struct{}{}
	}
	seenEventIDs := make(map[string]int, len(events))
	var previousTimestamp int64
	havePrevious := false

	for index, event := range events {
		eventError := func(code, message, field string) *Error {
			return NewError(code, message).
				WithStage("validate_events").
				WithEventID(event.EventID).
				WithField(field).
				WithDetail("event_index", index)
		}

		if event.EventID == "" {
			return eventError(CodeInvalidEvent, "event_id is required", "event_id")
		}
		if priorIndex, exists := seenEventIDs[event.EventID]; exists {
			return eventError(CodeInvalidEvent, "duplicate event_id", "event_id").
				WithDetail("prior_event_index", priorIndex)
		}
		seenEventIDs[event.EventID] = index

		if havePrevious && event.TimestampMS < previousTimestamp {
			return eventError(CodeInvalidEvent, "timestamp_ms is not nondecreasing", "timestamp_ms").
				WithDetail("previous_timestamp_ms", previousTimestamp)
		}
		previousTimestamp = event.TimestampMS
		havePrevious = true

		if _, ok := allowedGroups[event.Group]; !ok {
			return eventError(CodeInvalidEvent, "group is not in configuration set", "group").
				WithDetail("configured_groups", cfg.Groups)
		}
		if event.Clicked != 0 && event.Clicked != 1 {
			return eventError(CodeInvalidEvent, "clicked must be 0 or 1", "clicked").
				WithDetail("actual", event.Clicked)
		}
		if event.LatencyUS < 0 {
			return eventError(CodeInvalidEvent, "latency_us must not be negative", "latency_us").
				WithDetail("actual", event.LatencyUS)
		}
		if _, err := StageIndexAt(event.TimestampMS, cfg); err != nil {
			firstStage := cfg.Stages[0]
			lastStage := cfg.Stages[len(cfg.Stages)-1]
			return NewError(CodeInvalidEvent, "event timestamp is outside configured stage coverage").
				WithStage("assign_stage").
				WithEventID(event.EventID).
				WithField("timestamp_ms").
				WithDetail("sub_code", CodeStageCoverage).
				WithDetail("event_index", index).
				WithDetail("timestamp_ms", event.TimestampMS).
				WithDetail("coverage_start_ms", firstStage.StartMS).
				WithDetail("coverage_end_ms", lastStage.EndMS)
		}
	}
	return nil
}

func ValidateDataset(dataset *Dataset) error {
	dataset.Config = NormalizeConfig(dataset.Config)
	if err := ValidateConfig(dataset.Config); err != nil {
		return err
	}
	return ValidateEvents(dataset.Events, dataset.Config)
}

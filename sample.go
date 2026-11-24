package abeval

import (
	"fmt"
	"math/rand"
)

const DefaultSeed int64 = 20261125

type GenerateOptions struct {
	Scenario string
	Seed     int64
	Version  int64
	BatchID  string
}

func DefaultDataset(scenario string) Dataset {
	dataset, err := GenerateDataset(GenerateOptions{Scenario: scenario})
	if err != nil {
		panic(err)
	}
	return dataset
}

func GenerateDataset(options GenerateOptions) (Dataset, error) {
	scenario := options.Scenario
	if scenario == "" {
		scenario = ScenarioRollout
	}
	seed := options.Seed
	if seed == 0 {
		seed = DefaultSeed
	}
	version := options.Version
	if version == 0 {
		version = 1
	}

	eventsPerStage := 800
	controlCTR := 0.11
	treatmentCTR := 0.19
	controlLatencyMeanUS := int64(55_000)
	treatmentLatencyMeanUS := int64(60_000)
	stopAfterStage := 0
	resumeRound := 0

	switch scenario {
	case ScenarioRollout:
	case ScenarioRecovery:
		stopAfterStage = 1
		resumeRound = 1
	case ScenarioInsufficient:
		eventsPerStage = 100
	case ScenarioRollback:
		treatmentCTR = 0.04
		treatmentLatencyMeanUS = 170_000
	case "rollback_ctr":
		treatmentCTR = 0.035
	case "rollback_latency":
		treatmentLatencyMeanUS = 180_000
		treatmentCTR = 0.18
	default:
		return Dataset{}, NewError(CodeInvalidConfig, "unknown built-in scenario").
			WithStage("generate_dataset").
			WithField("scenario").
			WithDetail("scenario", scenario)
	}

	const stageCount = 3
	const stepMS = 1_000
	const startMS = int64(1_735_689_600_000)
	totalEvents := eventsPerStage * stageCount

	stageMinimums := []int{160, 320, 480}
	stages := make([]StageConfig, 0, stageCount)
	for stage := 1; stage <= stageCount; stage++ {
		stages = append(stages, StageConfig{
			Index:                stage,
			Name:                 fmt.Sprintf("stage-%d", stage),
			StartMS:              startMS + int64((stage-1)*eventsPerStage)*stepMS,
			EndMS:                startMS + int64(stage*eventsPerStage)*stepMS,
			MinExposuresPerGroup: stageMinimums[stage-1],
		})
	}

	batchID := options.BatchID
	if batchID == "" {
		batchID = "batch-" + scenario
	}
	config := Config{
		ExperimentID:   "exp-abeval-" + scenario,
		ControlGroup:   "control",
		TreatmentGroup: "treatment",
		Groups:         []string{"control", "treatment"},
		Traffic: []GroupWeight{
			{Group: "control", Weight: 0.5},
			{Group: "treatment", Weight: 0.5},
		},
		Stages:               stages,
		MinExposuresPerGroup: 160,
		CTR: CTRThreshold{
			MinLiftAbs:      0.02,
			RollbackDropAbs: 0.02,
			ZScore:          defaultZScore,
		},
		Latency: LatencyThreshold{
			Quantile:        0.95,
			MaxQuantileUS:   300_000,
			MaxRegressionUS: 100_000,
		},
		StopAfterStage: stopAfterStage,
		ResumeRound:    resumeRound,
	}

	random := rand.New(rand.NewSource(seed))
	events := make([]Event, 0, totalEvents)
	for index := 0; index < totalEvents; index++ {
		group := config.TreatmentGroup
		ctr := treatmentCTR
		latencyMean := treatmentLatencyMeanUS
		if index%2 == 0 {
			group = config.ControlGroup
			ctr = controlCTR
			latencyMean = controlLatencyMeanUS
		}

		click := 0
		if random.Float64() < ctr {
			click = 1
		}
		latency := int64(20_000 + random.ExpFloat64()*float64(latencyMean))
		if latency < 0 {
			latency = 0
		}

		events = append(events, Event{
			EventID:     fmt.Sprintf("evt-%06d", index+1),
			TimestampMS: startMS + int64(index)*stepMS,
			UserID:      fmt.Sprintf("user-%04d", random.Intn(240)+1),
			Group:       group,
			ItemID:      fmt.Sprintf("item-%04d", random.Intn(120)+1),
			Clicked:     click,
			LatencyUS:   latency,
		})
	}

	dataset := Dataset{
		Version: version,
		BatchID: batchID,
		Config:  config,
		Events:  events,
	}
	if err := ValidateDataset(&dataset); err != nil {
		return Dataset{}, fmt.Errorf("generated dataset failed validation: %w", err)
	}
	return dataset, nil
}

package demo

import "regworkbench/internal/config"

func FastConfig(datasetKey string) config.Config {
	cfg := config.Default(datasetKey)
	cfg.KFolds = 3
	cfg.Lambdas = []float64{0, 0.001, 0.01}
	cfg.L1Ratios = []float64{0, 0.5, 1}
	cfg.MaxIterations = 90
	cfg.Tolerance = 1e-7
	cfg.LatencyBudgetMS = 250
	return cfg
}

func VerificationOldConfig(datasetKey string) config.Config {
	cfg := FastConfig(datasetKey)
	cfg.Lambdas = []float64{0, 0.001}
	cfg.L1Ratios = []float64{0, 1}
	cfg.MaxIterations = 55
	return cfg
}

func VerificationNewConfig(datasetKey string) config.Config {
	cfg := FastConfig(datasetKey)
	cfg.Lambdas = []float64{0, 0.001, 0.01}
	cfg.L1Ratios = []float64{0, 0.5, 1}
	cfg.MaxIterations = 55
	return cfg
}

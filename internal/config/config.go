package config

import (
	"sort"

	"regworkbench/internal/canonical"
	"regworkbench/internal/workerr"
)

type Config struct {
	DatasetKey                 string            `json:"dataset_key"`
	KFolds                     int               `json:"k_folds"`
	Seed                       int64             `json:"seed"`
	DefaultCategoricalEncoding string            `json:"default_categorical_encoding"`
	Encoding                   map[string]string `json:"encoding,omitempty"`
	ImputeNumeric              string            `json:"impute_numeric"`
	ImputeCategorical          string            `json:"impute_categorical"`
	Metric                     string            `json:"metric"`
	Lambdas                    []float64         `json:"lambdas"`
	L1Ratios                   []float64         `json:"l1_ratios"`
	MaxIterations              int               `json:"max_iterations"`
	Tolerance                  float64           `json:"tolerance"`
	PathPoints                 int               `json:"path_points,omitempty"`
	LatencyBudgetMS            int64             `json:"latency_budget_ms"`
	TargetSmoothing            float64           `json:"target_smoothing"`
	Extra                      map[string]string `json:"extra,omitempty"`
}

func Default(datasetKey string) Config {
	return Config{
		DatasetKey:                 datasetKey,
		KFolds:                     3,
		Seed:                       20261125,
		DefaultCategoricalEncoding: "one_hot",
		Encoding:                   map[string]string{},
		ImputeNumeric:              "mean",
		ImputeCategorical:          "mode",
		Metric:                     "log_loss",
		Lambdas:                    []float64{0, 0.0003, 0.001, 0.003, 0.01},
		L1Ratios:                   []float64{0, 0.5, 1},
		MaxIterations:              160,
		Tolerance:                  1e-7,
		LatencyBudgetMS:            250,
		TargetSmoothing:            5,
	}
}

func (c Config) Normalize() (Config, error) {
	out := c
	if out.KFolds == 0 {
		out.KFolds = 3
	}
	if out.Seed == 0 {
		out.Seed = 20261125
	}
	if out.DefaultCategoricalEncoding == "" {
		out.DefaultCategoricalEncoding = "one_hot"
	}
	if out.Encoding == nil {
		out.Encoding = map[string]string{}
	}
	if out.ImputeNumeric == "" {
		out.ImputeNumeric = "mean"
	}
	if out.ImputeCategorical == "" {
		out.ImputeCategorical = "mode"
	}
	if out.Metric == "" {
		out.Metric = "log_loss"
	}
	if len(out.Lambdas) == 0 {
		out.Lambdas = []float64{0, 0.0003, 0.001, 0.003, 0.01}
	}
	if len(out.L1Ratios) == 0 {
		out.L1Ratios = []float64{0, 0.5, 1}
	}
	if out.MaxIterations <= 0 {
		out.MaxIterations = 160
	}
	if out.Tolerance <= 0 {
		out.Tolerance = 1e-7
	}
	if out.LatencyBudgetMS == 0 {
		out.LatencyBudgetMS = 250
	}
	if out.TargetSmoothing == 0 {
		out.TargetSmoothing = 5
	}

	out.Lambdas = append([]float64(nil), out.Lambdas...)
	sort.Float64s(out.Lambdas)
	out.L1Ratios = append([]float64(nil), out.L1Ratios...)
	sort.Float64s(out.L1Ratios)
	for _, v := range out.Lambdas {
		if v < 0 {
			return out, workerr.New(workerr.EConfig, "lambda values must be non-negative")
		}
	}
	for _, v := range out.L1Ratios {
		if v < 0 || v > 1 {
			return out, workerr.New(workerr.EConfig, "l1_ratio values must be in [0,1]")
		}
	}
	switch out.Metric {
	case "log_loss", "accuracy", "auc":
	default:
		return out, workerr.New(workerr.EConfig, "metric must be log_loss, accuracy, or auc")
	}
	switch out.ImputeNumeric {
	case "mean", "median", "zero":
	default:
		return out, workerr.New(workerr.EConfig, "numeric imputation must be mean, median, or zero")
	}
	switch out.ImputeCategorical {
	case "mode", "missing":
	default:
		return out, workerr.New(workerr.EConfig, "categorical imputation must be mode or missing")
	}
	for col, enc := range out.Encoding {
		switch enc {
		case "one_hot", "target":
		default:
			return out, workerr.New(workerr.EConfig, "encoding for "+col+" must be one_hot or target")
		}
	}
	return out, nil
}

func (c Config) Version() (string, error) {
	n, err := c.Normalize()
	if err != nil {
		return "", err
	}
	sum, _, err := canonical.SHA256JSON(n)
	if err != nil {
		return "", err
	}
	return sum[:16], nil
}

func (c Config) EncodingFor(column string) string {
	if v, ok := c.Encoding[column]; ok && v != "" {
		return v
	}
	return c.DefaultCategoricalEncoding
}

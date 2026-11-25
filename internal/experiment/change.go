package experiment

import (
	"reflect"

	"regworkbench/internal/config"
)

func AnalyzeChange(oldCfg, newCfg config.Config, foldCount int) (ChangePlan, config.Config, error) {
	oldN, err := oldCfg.Normalize()
	if err != nil {
		return ChangePlan{}, oldCfg, err
	}
	newN, err := newCfg.Normalize()
	if err != nil {
		return ChangePlan{}, newCfg, err
	}
	oldV, err := oldN.Version()
	if err != nil {
		return ChangePlan{}, newN, err
	}
	newV, err := newN.Version()
	if err != nil {
		return ChangePlan{}, newN, err
	}
	plan := ChangePlan{ConfigVersionOld: oldV, ConfigVersionNew: newV}
	if oldV == newV {
		plan.EarliestStage = "none"
		plan.Reason = "identical_config_version"
		for i := 0; i < foldCount; i++ {
			plan.ReusableFolds = append(plan.ReusableFolds, i)
		}
		return plan, newN, nil
	}

	if oldN.DatasetKey != newN.DatasetKey || oldN.ImputeNumeric != newN.ImputeNumeric ||
		oldN.ImputeCategorical != newN.ImputeCategorical ||
		oldN.DefaultCategoricalEncoding != newN.DefaultCategoricalEncoding ||
		!reflect.DeepEqual(oldN.Encoding, newN.Encoding) {
		plan.EarliestStage = "preprocess"
		plan.ChangedFields = append(plan.ChangedFields, "column_preprocessing")
		plan.Reason = "encoding_or_imputation_changes_feature_matrix"
		plan.AffectedFolds = allFolds(foldCount)
		return plan, newN, nil
	}

	if oldN.KFolds != newN.KFolds || oldN.Seed != newN.Seed {
		plan.EarliestStage = "split"
		plan.ChangedFields = append(plan.ChangedFields, "fold_layout")
		plan.Reason = "fold_count_or_seed_changes_split_indices"
		plan.AffectedFolds = allFolds(maxInt(oldN.KFolds, newN.KFolds))
		return plan, newN, nil
	}

	if !reflect.DeepEqual(oldN.Lambdas, newN.Lambdas) || !reflect.DeepEqual(oldN.L1Ratios, newN.L1Ratios) ||
		oldN.MaxIterations != newN.MaxIterations || oldN.Tolerance != newN.Tolerance ||
		oldN.TargetSmoothing != newN.TargetSmoothing {
		plan.EarliestStage = "fold_model"
		plan.ChangedFields = append(plan.ChangedFields, "regularization_or_solver")
		plan.Reason = "regularization_grid_or_solver_changes_fold_fits_but_preprocess_and_split_are_reused"
		for i := 0; i < oldN.KFolds; i++ {
			plan.AffectedFolds = append(plan.AffectedFolds, i)
		}
		plan.AffectedFolds = []int{}
		return plan, newN, nil
	}

	if oldN.Metric != newN.Metric {
		plan.EarliestStage = "selection"
		plan.ChangedFields = append(plan.ChangedFields, "selection_metric")
		plan.Reason = "metric_change_reuses_all_fold_products"
		for i := 0; i < oldN.KFolds; i++ {
			plan.ReusableFolds = append(plan.ReusableFolds, i)
		}
		return plan, newN, nil
	}

	plan.EarliestStage = "query_settings"
	plan.ChangedFields = append(plan.ChangedFields, "latency_budget_or_metadata")
	plan.Reason = "training_products_are_reused"
	for i := 0; i < oldN.KFolds; i++ {
		plan.ReusableFolds = append(plan.ReusableFolds, i)
	}
	return plan, newN, nil
}

func allFolds(n int) []int {
	out := make([]int, maxInt(n, 0))
	for i := range out {
		out[i] = i
	}
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

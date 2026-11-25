package verify

import (
	"context"
	"os"
	"path/filepath"

	"regworkbench/internal/canonical"
	"regworkbench/internal/config"
	"regworkbench/internal/dataset"
	"regworkbench/internal/demo"
	"regworkbench/internal/experiment"
	"regworkbench/internal/workerr"
)

type Check struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

type DatasetReport struct {
	Key               string  `json:"key"`
	OK                bool    `json:"ok"`
	Checks            []Check `json:"checks"`
	CoefficientSHA256 string  `json:"coefficient_sha256,omitempty"`
	SummarySHA256     string  `json:"summary_sha256,omitempty"`
}

type Report struct {
	Version  string          `json:"version"`
	OK       bool            `json:"ok"`
	Out      string          `json:"out"`
	Datasets []DatasetReport `json:"datasets"`
}

func Run(out string) (*Report, error) {
	samplesDir := filepath.Join(out, "samples")
	if _, err := dataset.GenerateDirectory(samplesDir, dataset.DefaultSeed); err != nil {
		return nil, err
	}
	report := &Report{Version: "regworkbench.verification/v1", OK: true, Out: out}
	for _, key := range []string{"small", "medium"} {
		dr := verifyDataset(out, samplesDir, key)
		report.Datasets = append(report.Datasets, dr)
		if !dr.OK {
			report.OK = false
		}
	}
	b, err := canonical.JSON(report)
	if err != nil {
		return report, err
	}
	if err := canonical.AtomicWriteFile(filepath.Join(out, "verification.json"), append(b, '\n'), 0o644); err != nil {
		return report, err
	}
	return report, nil
}

func verifyDataset(out, samplesDir, key string) DatasetReport {
	dr := DatasetReport{Key: key, OK: true}
	add := func(name string, ok bool, detail string) {
		dr.Checks = append(dr.Checks, Check{Name: name, OK: ok, Detail: detail})
		if !ok {
			dr.OK = false
		}
	}
	manifestPath := filepath.Join(samplesDir, dataset.ManifestName)
	cfgOld := demo.VerificationOldConfig(key)
	cfgNew := demo.VerificationNewConfig(key)

	aDir := filepath.Join(out, key, "from-zero-a")
	bDir := filepath.Join(out, key, "from-zero-b")
	fullDir := filepath.Join(out, key, "incremental-full")
	incDir := filepath.Join(out, key, "incremental-reused")
	recoveryDir := filepath.Join(out, key, "recovery")
	liveDir := filepath.Join(out, key, "live-query-stop")
	for _, dir := range []string{aDir, bDir, fullDir, incDir, recoveryDir, liveDir} {
		if err := os.RemoveAll(dir); err != nil {
			dr.OK = false
			dr.Checks = append(dr.Checks, Check{Name: "reset_output", OK: false, Detail: err.Error()})
			return dr
		}
	}

	sessA, repA, err := runFresh(manifestPath, key, aDir, cfgOld, experiment.Hooks{})
	if err != nil {
		add("from_zero_train", false, err.Error())
		return dr
	}
	add("from_zero_train", repA != nil && repA.State == "completed", stateDetail(repA, err))
	defer sessA.Close()

	sessB, repB, err := runFresh(manifestPath, key, bDir, cfgOld, experiment.Hooks{})
	if err != nil {
		add("byte_identity_second_run", false, err.Error())
		return dr
	}
	add("byte_identity_second_run", repB != nil && repB.State == "completed", stateDetail(repB, err))
	defer sessB.Close()
	if err := experiment.CompareRunDirectories(aDir, bDir); err != nil {
		add("from_zero_byte_identity", false, err.Error())
	} else {
		add("from_zero_byte_identity", true, "coefficients.json and validation_summary.json are byte-identical")
	}
	if sha, err := canonical.SHA256File(filepath.Join(aDir, "exports", "coefficients.json")); err == nil {
		dr.CoefficientSHA256 = sha
	}
	if sha, err := canonical.SHA256File(filepath.Join(aDir, "exports", "validation_summary.json")); err == nil {
		dr.SummarySHA256 = sha
	}

	fold0 := 0
	budget := int64(2000)
	q1, err := sessA.Query(experiment.QueryRequest{Kind: "metrics", Fold: &fold0, BudgetMS: &budget})
	add("query_fold_metrics", err == nil && q1 != nil, errDetail(err))
	q2, err := sessA.Query(experiment.QueryRequest{Kind: "metrics", Fold: &fold0, BudgetMS: &budget})
	cacheOK := err == nil && q2 != nil && q2.CacheHit && string(q1.Result) == string(q2.Result)
	add("cache_hit_byte_identity", cacheOK, cacheDetail(q2))
	if q2 != nil {
		sessA.Cache.DeleteID(q2.CacheKey)
		q3, err3 := sessA.Query(experiment.QueryRequest{Kind: "metrics", Fold: &fold0, BudgetMS: &budget})
		recomputeOK := err3 == nil && q3 != nil && !q3.CacheHit && string(q1.Result) == string(q3.Result)
		add("cache_recompute_byte_identity", recomputeOK, errDetail(err3))
	}

	zeroBudget := int64(0)
	_, budgetErr := sessA.Query(experiment.QueryRequest{Kind: "selection", QueryID: "verify-budget-" + key, BudgetMS: &zeroBudget})
	we, isWork := workerr.As(budgetErr)
	budgetOK := isWork && we.Code == workerr.ELatencyBudget && we.QueryID == "verify-budget-"+key
	add("latency_budget_structured_error", budgetOK, errDetail(budgetErr))

	var liveSess *experiment.Session
	liveTrajectoryOK := false
	liveBudgetOK := false
	hooks := experiment.Hooks{Round: func(fold int, lambda, ratio float64, iteration int) {
		if fold != 0 || iteration != 3 || lambda != 0 || ratio != 0 || liveSess == nil {
			return
		}
		f := 0
		b := int64(2000)
		q, qerr := liveSess.Query(experiment.QueryRequest{Kind: "trajectory", Fold: &f, Lambda: &lambda, L1Ratio: &ratio, BudgetMS: &b})
		liveTrajectoryOK = qerr == nil && q != nil && len(q.Result) > 0
		z := int64(0)
		_, berr := liveSess.Query(experiment.QueryRequest{Kind: "trajectory", Fold: &f, Lambda: &lambda, L1Ratio: &ratio, BudgetMS: &z, QueryID: "live-budget-" + key})
		if w, ok := workerr.As(berr); ok && w.Code == workerr.ELatencyBudget {
			liveBudgetOK = w.QueryID != ""
		}
		_ = liveSess.Store.RequestStop()
	}}
	liveSess, err = createFresh(manifestPath, key, liveDir, cfgOld, hooks)
	if err != nil {
		add("in_training_query", false, err.Error())
	} else {
		liveReport, runErr := liveSess.Run(context.Background())
		add("in_training_query", liveTrajectoryOK && liveBudgetOK, "trajectory and E_LATENCY_BUDGET observed from round hook")
		add("graceful_stop_after_round", runErr == nil && liveReport != nil && liveReport.State == "stopped", stateDetail(liveReport, runErr))
		_ = liveSess.Close()
	}

	var recoverySess *experiment.Session
	recoveryHooks := experiment.Hooks{AfterCheckpoint: func(stage string, fold int, seq int64) {
		if stage == "fold" && fold == 0 && recoverySess != nil {
			_ = recoverySess.Store.RequestStop()
		}
	}}
	recoverySess, err = createFresh(manifestPath, key, recoveryDir, cfgOld, recoveryHooks)
	if err != nil {
		add("checkpoint_stop", false, err.Error())
	} else {
		stopReport, stopErr := recoverySess.Run(context.Background())
		stopOK := stopErr == nil && stopReport != nil && stopReport.State == "stopped"
		add("stop_after_first_fold_checkpoint", stopOK, stateDetail(stopReport, stopErr))
		_ = recoverySess.Close()

		resumed, openErr := experiment.OpenSession(recoveryDir, experiment.Hooks{})
		if openErr != nil {
			add("resume_from_disk", false, openErr.Error())
		} else {
			_ = resumed.Store.ClearStop()
			resumeReport, resumeErr := resumed.Run(context.Background())
			add("resume_from_disk", resumeErr == nil && resumeReport != nil && resumeReport.State == "completed", stateDetail(resumeReport, resumeErr))
			_ = resumed.Close()
			if err := experiment.CompareRunDirectories(recoveryDir, aDir); err != nil {
				add("recovery_byte_identity", false, err.Error())
			} else {
				add("recovery_byte_identity", true, "resumed coefficients and summary match uninterrupted run")
			}
		}
	}

	incSess, incReport, plan, err := experiment.RunIncremental(context.Background(), sessA, cfgNew, incDir, experiment.Hooks{})
	if err != nil {
		add("incremental_recompute", false, err.Error())
	} else {
		reuseOK := incReport != nil && incReport.ReusedFoldPoints == len(cfgOld.Lambdas)*len(cfgOld.L1Ratios)*cfgOld.KFolds
		add("incremental_reuses_unaffected_points", reuseOK, plan.EarliestStage+": "+plan.Reason)
		_ = incSess.Close()
	}
	sessFull, repFull, err := runFresh(manifestPath, key, fullDir, cfgNew, experiment.Hooks{})
	if err != nil {
		add("incremental_full_rebuild", false, err.Error())
	} else {
		add("incremental_full_rebuild", repFull != nil && repFull.State == "completed", stateDetail(repFull, err))
		_ = sessFull.Close()
	}
	if err == nil {
		if cmpErr := experiment.CompareRunDirectories(incDir, fullDir); cmpErr != nil {
			add("incremental_byte_identity", false, cmpErr.Error())
		} else {
			add("incremental_byte_identity", true, "incremental and from-scratch products are byte-identical")
		}
	}
	return dr
}

func runFresh(manifestPath, key, dir string, cfg config.Config, hooks experiment.Hooks) (*experiment.Session, *experiment.RunReport, error) {
	ds, err := dataset.LoadDataset(manifestPath, key)
	if err != nil {
		return nil, nil, err
	}
	sess, err := experiment.CreateSession(dir, cfg, ds, hooks)
	if err != nil {
		return nil, nil, err
	}
	rep, err := sess.Run(context.Background())
	if err != nil {
		_ = sess.Close()
		return nil, rep, err
	}
	return sess, rep, nil
}

func createFresh(manifestPath, key, dir string, cfg config.Config, hooks experiment.Hooks) (*experiment.Session, error) {
	ds, err := dataset.LoadDataset(manifestPath, key)
	if err != nil {
		return nil, err
	}
	return experiment.CreateSession(dir, cfg, ds, hooks)
}

func errDetail(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func stateDetail(rep *experiment.RunReport, err error) string {
	if err != nil {
		return err.Error()
	}
	if rep == nil {
		return "nil report"
	}
	return rep.State
}

func cacheDetail(q *experiment.QueryResponse) string {
	if q == nil {
		return "nil response"
	}
	return "cache_hit=" + boolString(q.CacheHit)
}

func boolString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

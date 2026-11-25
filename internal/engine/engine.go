package engine

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"strings"

	"localsearch/multirecall/internal/budget"
	"localsearch/multirecall/internal/model"
	"localsearch/multirecall/internal/pool"
	"localsearch/multirecall/internal/recall"
	"localsearch/multirecall/internal/sample"
	"localsearch/multirecall/internal/store"
	"localsearch/multirecall/internal/worker"
)

type Request struct {
	DataDir                     string
	DBPath                      string
	Config                      *model.Config
	Workers                     int
	Concurrency                 int
	Mode                        string
	SkipIncrementalVerification bool
	// VerifyIncremental is retained for source compatibility. Verification is
	// enabled by default when any checkpoint is reused.
	VerifyIncremental bool
	Stop              <-chan struct{}
	OnCheckpoint      func(queryID, stage string)
	Executor          pool.Executor
}

type Engine struct{}

type queryWork struct {
	query        model.Query
	judgments    []model.Judgment
	decision     model.BudgetDecision
	queryDigest  string
	recallFP     string
	recallBase   int
	fusionBase   int
	evalBase     int
	recall       *model.RecallBundle
	recallDigest string
	fusion       *model.FusionResult
	fusionDigest string
	evaluation   *model.EvaluationResult
	evalDigest   string
	startStage   string
	needsTask    bool
	complete     bool
	reused       int
}

type runContext struct {
	data               sample.Dataset
	config             model.Config
	configFingerprint  string
	recallConfigDigest string
	fusionConfigDigest string
	evalConfigDigest   string
	runID              string
	plan               budget.Plan
	jobs               []*queryWork
	judgmentsByQuery   map[string][]model.Judgment
}

func (e *Engine) Run(ctx context.Context, request Request) (*model.RunOutput, error) {
	if strings.TrimSpace(request.DataDir) == "" || strings.TrimSpace(request.DBPath) == "" {
		return nil, model.NewError(model.CodeInvalidConfig, "data directory and database path are required", "")
	}
	data, err := sample.Load(request.DataDir)
	if err != nil {
		return nil, err
	}
	config := data.Config
	if request.Config != nil {
		config = request.Config.Normalize()
	}
	if request.Concurrency > 0 {
		config.Execution.MaxConcurrentQueries = request.Concurrency
	}
	config = config.Normalize()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if len(data.Items) == 0 {
		queryID := ""
		if len(data.Queries) > 0 {
			queryID = data.Queries[0].QueryID
		}
		return nil, model.NewError(model.CodeEmptyInput, "item library must not be empty", queryID)
	}
	if len(data.Queries) == 0 {
		return nil, model.NewError(model.CodeEmptyInput, "query set must not be empty", "")
	}
	for _, query := range data.Queries {
		if recall.IsEmptyQuery(query.Text) {
			return nil, model.NewError(model.CodeEmptyInput, "query text must not be empty", query.QueryID)
		}
	}
	for _, item := range data.Items {
		if len(item.Vector) != config.VectorDimension {
			return nil, model.NewError(model.CodeDataLoad, "item vector dimension does not match config: "+item.ItemID, data.Queries[0].QueryID)
		}
	}
	configFingerprint, err := model.ConfigContentFingerprint(config)
	if err != nil {
		return nil, err
	}
	recallConfigDigest, err := model.RecallConfigDigest(config)
	if err != nil {
		return nil, err
	}
	fusionConfigDigest, err := model.FusionConfigDigest(config)
	if err != nil {
		return nil, err
	}
	evalConfigDigest, err := model.EvaluationConfigDigest(config)
	if err != nil {
		return nil, err
	}
	runID := model.HashParts("run-v1", configFingerprint, data.DataFingerprint)[:24]
	judgmentsByQuery := make(map[string][]model.Judgment)
	for _, judgment := range data.Judgments {
		judgmentsByQuery[judgment.QueryID] = append(judgmentsByQuery[judgment.QueryID], judgment)
	}
	plan := budget.Build(config, len(data.Items), data.Queries)
	rc := &runContext{
		data:               data,
		config:             config,
		configFingerprint:  configFingerprint,
		recallConfigDigest: recallConfigDigest,
		fusionConfigDigest: fusionConfigDigest,
		evalConfigDigest:   evalConfigDigest,
		runID:              runID,
		plan:               plan,
		judgmentsByQuery:   judgmentsByQuery,
	}

	st, err := store.Open(request.DBPath)
	if err != nil {
		return nil, err
	}
	defer st.Close()
	if err := st.UpsertRun(store.RunRecord{
		RunID: runID, ConfigVersion: config.Version, ConfigFingerprint: configFingerprint,
		Seed: config.Seed, DataFingerprint: data.DataFingerprint, Status: "running",
	}); err != nil {
		return nil, err
	}
	fail := func(runErr error) (*model.RunOutput, error) {
		_ = st.UpdateRunStatus(runID, "failed", false)
		return nil, runErr
	}

	rc.jobs, err = rc.prepareAll(ctx, st)
	if err != nil {
		return fail(err)
	}
	reused := 0
	needsExecution := false
	for _, work := range rc.jobs {
		reused += work.reused
		if work.needsTask {
			needsExecution = true
		}
	}

	executor, cleanup, err := rc.makeExecutor(request, needsExecution)
	if err != nil {
		return fail(err)
	}
	defer cleanup()
	defer executor.Close()

	results := make(map[string]model.QueryResult, len(rc.jobs))
	stopped := false
	if checkStop(request.Stop) {
		stopped = true
	}
	rounds := rc.rounds()
	roundNumbers := make([]int, 0, len(rounds))
	for round := range rounds {
		roundNumbers = append(roundNumbers, round)
	}
	sort.Ints(roundNumbers)
	for _, roundNumber := range roundNumbers {
		if stopped {
			break
		}
		if checkStop(request.Stop) {
			stopped = true
			break
		}
		roundWork := rounds[roundNumber]
		tasks := make([]worker.Task, 0)
		taskWork := make([]*queryWork, 0)
		for _, work := range roundWork {
			if !work.decision.Processed || work.complete || !work.needsTask {
				continue
			}
			shard := len(taskWork) % effectiveWorkers(request, config)
			tasks = append(tasks, worker.Task{
				Shard:        shard,
				Query:        work.query,
				Judgments:    work.judgments,
				Plan:         work.decision,
				StartStage:   work.startStage,
				CachedRecall: work.recall,
				CachedFusion: work.fusion,
			})
			taskWork = append(taskWork, work)
		}
		if len(tasks) > 0 {
			workerResults, runErr := executor.RunRound(ctx, tasks)
			var firstWorkerFailure *model.WorkerFailure
			for _, result := range workerResults {
				if result.Error == nil {
					continue
				}
				_ = st.SaveFailure(context.Background(), store.FailureRecord{
					RunID: runID, Shard: result.Shard, QueryID: result.QueryID,
					Code: result.Error.Code, Message: result.Error.Message,
				})
				if firstWorkerFailure == nil {
					firstWorkerFailure = &model.WorkerFailure{
						Code: result.Error.Code, Message: result.Error.Message,
						Shard: result.Shard, QueryID: result.QueryID,
					}
				}
			}
			for i, result := range workerResults {
				if i >= len(taskWork) {
					break
				}
				work := taskWork[i]
				if result.Error != nil {
					continue
				}
				if result.QueryID != work.query.QueryID {
					return fail(model.NewErrorDetails(model.CodeWorkerFailure, "worker result query_id mismatch", work.query.QueryID,
						map[string]any{"result_query_id": result.QueryID}))
				}
				if err := rc.acceptWorkerResult(ctx, st, request, work, result); err != nil {
					return fail(err)
				}
			}
			if runErr != nil && !hasStructuredResultError(workerResults) {
				return fail(model.NewError(model.CodeWorkerFailure, "worker round failed: "+runErr.Error(), ""))
			}
			if firstWorkerFailure != nil {
				return fail(firstWorkerFailure)
			}
		}
		for _, work := range roundWork {
			if work.decision.Processed && !work.complete {
				if err := rc.assemble(work); err != nil {
					return fail(err)
				}
			}
			if work.decision.Processed && work.complete {
				if _, exists := results[work.query.QueryID]; !exists {
					results[work.query.QueryID] = model.QueryResult{
						QueryID: work.query.QueryID, Recall: *work.recall,
						Fusion: *work.fusion, Evaluation: *work.evaluation,
					}
				}
			}
		}
	}

	if !stopped && checkStop(request.Stop) {
		stopped = true
	}
	unprocessed := append([]model.UnprocessedQuery(nil), plan.Unprocessed...)
	for _, work := range rc.jobs {
		if work.decision.Processed && !work.complete {
			unprocessed = append(unprocessed, model.UnprocessedQuery{QueryID: work.query.QueryID, Reason: "graceful_stop_after_current_query_unit"})
			stopped = true
		}
	}
	unprocessed = dedupeUnprocessed(unprocessed)
	queryResults := make([]model.QueryResult, 0, len(results))
	for _, result := range results {
		queryResults = append(queryResults, result)
	}
	sort.Slice(queryResults, func(i, j int) bool { return queryResults[i].QueryID < queryResults[j].QueryID })
	summary := buildSummary(rc, queryResults, unprocessed, stopped)
	output := &model.RunOutput{
		RunID: runID, ConfigVersion: config.Version, ConfigFingerprint: configFingerprint,
		Seed: config.Seed, Results: queryResults, BudgetDecisions: plan.Decisions,
		Unprocessed: unprocessed, Summary: summary, Stopped: stopped, ReusedCheckpoints: reused,
	}
	if reused > 0 && !stopped && !request.SkipIncrementalVerification {
		if verifyErr := rc.verifyIncremental(output); verifyErr != nil {
			_ = st.UpdateRunStatus(runID, "incremental_mismatch", false)
			return nil, verifyErr
		}
	}
	summaryJSON, err := json.Marshal(summary)
	if err != nil {
		return fail(err)
	}
	status := "completed"
	if stopped {
		status = "interrupted"
	}
	if err := st.SaveSummary(runID, summary.SummaryFingerprint, string(summaryJSON), status, stopped); err != nil {
		return fail(err)
	}
	return output, nil
}

func (rc *runContext) makeExecutor(request Request, needsExecution bool) (pool.Executor, func(), error) {
	if request.Executor != nil {
		return request.Executor, func() {}, nil
	}
	snapshot := worker.Snapshot{Items: rc.data.Items, Config: rc.config}
	mode := strings.ToLower(strings.TrimSpace(request.Mode))
	if !needsExecution || mode == "inline" {
		return &pool.Inline{Snapshot: snapshot}, func() {}, nil
	}
	tmp, err := os.CreateTemp("", "multirecall-snapshot-*.json")
	if err != nil {
		return nil, func() {}, model.NewError(model.CodeInternal, "create worker snapshot: "+err.Error(), "")
	}
	cleanup := func() { _ = os.Remove(tmp.Name()) }
	encoder := json.NewEncoder(tmp)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(snapshot); err != nil {
		_ = tmp.Close()
		cleanup()
		return nil, func() {}, model.NewError(model.CodeInternal, "encode worker snapshot: "+err.Error(), "")
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return nil, func() {}, model.NewError(model.CodeInternal, "close worker snapshot: "+err.Error(), "")
	}
	executable, err := os.Executable()
	if err != nil {
		cleanup()
		return nil, func() {}, model.NewError(model.CodeInternal, "resolve executable: "+err.Error(), "")
	}
	executor, err := pool.NewProcess(executable, tmp.Name(), effectiveWorkers(request, rc.config))
	if err != nil {
		cleanup()
		return nil, func() {}, model.NewError(model.CodeWorkerFailure, "start worker pool: "+err.Error(), "")
	}
	return executor, cleanup, nil
}

func effectiveWorkers(request Request, config model.Config) int {
	workers := request.Workers
	if workers <= 0 {
		workers = config.Execution.WorkerProcesses
	}
	if workers < 1 {
		workers = 1
	}
	if workers > config.Execution.MaxConcurrentQueries {
		workers = config.Execution.MaxConcurrentQueries
	}
	return workers
}

func (rc *runContext) rounds() map[int][]*queryWork {
	byRound := make(map[int][]*queryWork)
	for _, work := range rc.jobs {
		byRound[work.decision.Round] = append(byRound[work.decision.Round], work)
	}
	return byRound
}

func checkStop(stop <-chan struct{}) bool {
	if stop == nil {
		return false
	}
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

func hasStructuredResultError(results []worker.Result) bool {
	for _, result := range results {
		if result.Error != nil {
			return true
		}
	}
	return false
}

func dedupeUnprocessed(values []model.UnprocessedQuery) []model.UnprocessedQuery {
	seen := make(map[string]struct{}, len(values))
	out := make([]model.UnprocessedQuery, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value.QueryID]; ok {
			continue
		}
		seen[value.QueryID] = struct{}{}
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].QueryID < out[j].QueryID })
	return out
}

func (rc *runContext) verifyIncremental(output *model.RunOutput) error {
	runner := worker.NewRunner(worker.Snapshot{Items: rc.data.Items, Config: rc.config})
	results := make([]model.QueryResult, 0, len(rc.jobs))
	for _, work := range rc.jobs {
		if !work.decision.Processed {
			continue
		}
		task := worker.Task{
			Shard: 0, Query: work.query, Judgments: work.judgments,
			Plan: work.decision, StartStage: model.StageRecall,
		}
		result := runner.Execute(task)
		if result.Error != nil {
			return model.NewError(model.CodeIncrementalMismatch, "fresh verification failed: "+result.Error.Message, work.query.QueryID)
		}
		if result.Recall == nil || result.Fusion == nil || result.Evaluation == nil {
			return model.NewError(model.CodeIncrementalMismatch, "fresh verification returned incomplete stages", work.query.QueryID)
		}
		results = append(results, model.QueryResult{
			QueryID: work.query.QueryID, Recall: *result.Recall,
			Fusion: *result.Fusion, Evaluation: *result.Evaluation,
		})
	}
	firstDifferent := model.FirstDifferentQuery(output.Results, results)
	if firstDifferent != "" {
		return model.NewErrorDetails(model.CodeIncrementalMismatch,
			"incremental query result differs from a from-scratch run", firstDifferent,
			map[string]any{"mismatched_section": "query_results"})
	}
	freshSummary := buildSummary(rc, results, rc.plan.Unprocessed, false)
	gotData, err := model.CanonicalJSON(output.Summary)
	if err != nil {
		return err
	}
	wantData, err := model.CanonicalJSON(freshSummary)
	if err != nil {
		return err
	}
	if string(gotData) != string(wantData) {
		firstQueryID := ""
		if len(output.Results) > 0 {
			firstQueryID = output.Results[0].QueryID
		} else if len(output.Unprocessed) > 0 {
			firstQueryID = output.Unprocessed[0].QueryID
		}
		return model.NewErrorDetails(model.CodeIncrementalMismatch,
			"incremental summary differs from a from-scratch run", firstQueryID, map[string]any{"mismatched_section": "summary"})
	}
	return nil
}

func (rc *runContext) prepareAll(ctx context.Context, st *store.Store) ([]*queryWork, error) {
	jobs := make([]*queryWork, 0, len(rc.data.Queries))
	for _, query := range rc.data.Queries {
		decision, ok := rc.plan.ByQuery[query.QueryID]
		if !ok {
			return nil, model.NewError(model.CodeInternal, "budget decision is missing", query.QueryID)
		}
		queryDigest, err := model.QueryDigest(query)
		if err != nil {
			return nil, err
		}
		work := &queryWork{
			query: query, decision: decision, queryDigest: queryDigest,
			judgments: append([]model.Judgment(nil), rc.judgmentsByQuery[query.QueryID]...),
		}
		work.recallFP = model.RecallInputFingerprint(rc.data.ItemsDigest, queryDigest, rc.recallConfigDigest)
		jobs = append(jobs, work)
		if !decision.Processed {
			continue
		}
		if work.recallBase, err = st.LatestRevision(ctx, query.QueryID, model.StageRecall); err != nil {
			return nil, err
		}
		if work.fusionBase, err = st.LatestRevision(ctx, query.QueryID, model.StageFusion); err != nil {
			return nil, err
		}
		if work.evalBase, err = st.LatestRevision(ctx, query.QueryID, model.StageEvaluation); err != nil {
			return nil, err
		}
		if err := rc.prepareCache(ctx, st, work); err != nil {
			return nil, err
		}
	}
	return jobs, nil
}

func (rc *runContext) prepareCache(ctx context.Context, st *store.Store, work *queryWork) error {
	cp, ok, err := st.LoadReusable(ctx, work.query.QueryID, model.StageRecall, work.recallFP)
	if err != nil {
		return err
	}
	if ok {
		var bundle model.RecallBundle
		if err := json.Unmarshal(cp.Payload, &bundle); err != nil {
			return tamperedError(work.query.QueryID, model.StageRecall, "decode cached recall: "+err.Error())
		}
		if bundle.QueryID != work.query.QueryID {
			return tamperedError(work.query.QueryID, model.StageRecall, "cached recall query_id mismatch")
		}
		work.recall = &bundle
		work.recallDigest = cp.ArtifactDigest
		work.reused++
	}
	if work.recall != nil {
		fusionFP := model.FusionInputFingerprint(work.recallDigest, rc.fusionConfigDigest)
		fcp, found, err := st.LoadReusable(ctx, work.query.QueryID, model.StageFusion, fusionFP)
		if err != nil {
			return err
		}
		if found {
			var fusion model.FusionResult
			if err := json.Unmarshal(fcp.Payload, &fusion); err != nil {
				return tamperedError(work.query.QueryID, model.StageFusion, "decode cached fusion: "+err.Error())
			}
			if fusion.QueryID != work.query.QueryID || !recall.VerifyFusion(*work.recall, rc.config.Fusion, fusion) {
				return model.NewErrorDetails(model.CodeIncrementalMismatch,
					"cached fusion is not self-consistent with recall details", work.query.QueryID,
					map[string]any{"mismatched_section": "fusion", "stage": model.StageFusion})
			}
			work.fusion = &fusion
			work.fusionDigest = fcp.ArtifactDigest
			work.reused++
		}
	}
	if work.fusion != nil {
		evalFP := model.EvaluationInputFingerprint(work.fusionDigest, rc.data.JudgmentsDigest, rc.evalConfigDigest)
		ecp, found, err := st.LoadReusable(ctx, work.query.QueryID, model.StageEvaluation, evalFP)
		if err != nil {
			return err
		}
		if found {
			var evaluation model.EvaluationResult
			if err := json.Unmarshal(ecp.Payload, &evaluation); err != nil {
				return tamperedError(work.query.QueryID, model.StageEvaluation, "decode cached evaluation: "+err.Error())
			}
			expected := recall.Evaluate(*work.recall, *work.fusion, work.judgments, rc.config.Evaluation)
			if evaluation.QueryID != work.query.QueryID || !sameJSON(evaluation, expected) {
				return model.NewErrorDetails(model.CodeIncrementalMismatch,
					"cached evaluation is not reproducible", work.query.QueryID,
					map[string]any{"mismatched_section": "evaluation", "stage": model.StageEvaluation})
			}
			work.evaluation = &evaluation
			work.evalDigest = ecp.ArtifactDigest
			work.reused++
		}
	}
	switch {
	case work.evaluation != nil:
		work.complete = true
		work.startStage = ""
	case work.fusion != nil:
		work.startStage = model.StageEvaluation
		work.needsTask = true
	case work.recall != nil:
		work.startStage = model.StageFusion
		work.needsTask = true
	default:
		work.startStage = model.StageRecall
		work.needsTask = true
	}
	return nil
}

func (rc *runContext) acceptWorkerResult(ctx context.Context, st *store.Store, request Request, work *queryWork, result worker.Result) error {
	if result.Recall != nil {
		if result.Recall.QueryID != work.query.QueryID {
			return model.NewError(model.CodeWorkerFailure, "recall query_id mismatch", work.query.QueryID)
		}
		digest, payload, err := model.DigestWire(*result.Recall)
		if err != nil {
			return err
		}
		recallFP := work.recallFP
		if _, err := st.Publish(ctx, store.PublishRequest{
			RunID: rc.runID, QueryID: work.query.QueryID, Stage: model.StageRecall,
			Sequence: model.StageSequence(model.StageRecall), Revision: work.recallBase + 1, InputFingerprint: recallFP,
			ConfigVersion: rc.config.Version, ArtifactDigest: digest, Payload: payload,
		}); err != nil {
			return err
		}
		work.recall = result.Recall
		work.recallDigest = digest
		rc.checkpoint(request, work.query.QueryID, model.StageRecall)
	}
	if result.Fusion != nil {
		if result.Fusion.QueryID != work.query.QueryID || work.recall == nil {
			return model.NewError(model.CodeWorkerFailure, "fusion result missing recall or has wrong query_id", work.query.QueryID)
		}
		if !recall.VerifyFusion(*work.recall, rc.config.Fusion, *result.Fusion) {
			return model.NewError(model.CodeInternal, "fusion result is not self-consistent", work.query.QueryID)
		}
		digest, payload, err := model.DigestWire(*result.Fusion)
		if err != nil {
			return err
		}
		fusionFP := model.FusionInputFingerprint(work.recallDigest, rc.fusionConfigDigest)
		if _, err := st.Publish(ctx, store.PublishRequest{
			RunID: rc.runID, QueryID: work.query.QueryID, Stage: model.StageFusion,
			Sequence: model.StageSequence(model.StageFusion), Revision: work.fusionBase + 1, InputFingerprint: fusionFP,
			ConfigVersion: rc.config.Version, ArtifactDigest: digest,
			PrerequisiteDigest: work.recallDigest, Payload: payload,
		}); err != nil {
			return err
		}
		work.fusion = result.Fusion
		work.fusionDigest = digest
		rc.checkpoint(request, work.query.QueryID, model.StageFusion)
	}
	if result.Evaluation != nil {
		if result.Evaluation.QueryID != work.query.QueryID || work.recall == nil || work.fusion == nil {
			return model.NewError(model.CodeWorkerFailure, "evaluation result prerequisites are missing", work.query.QueryID)
		}
		expected := recall.Evaluate(*work.recall, *work.fusion, work.judgments, rc.config.Evaluation)
		if !sameJSON(*result.Evaluation, expected) {
			return model.NewError(model.CodeInternal, "evaluation result is not reproducible", work.query.QueryID)
		}
		digest, payload, err := model.DigestWire(*result.Evaluation)
		if err != nil {
			return err
		}
		evalFP := model.EvaluationInputFingerprint(work.fusionDigest, rc.data.JudgmentsDigest, rc.evalConfigDigest)
		if _, err := st.Publish(ctx, store.PublishRequest{
			RunID: rc.runID, QueryID: work.query.QueryID, Stage: model.StageEvaluation,
			Sequence: model.StageSequence(model.StageEvaluation), Revision: work.evalBase + 1, InputFingerprint: evalFP,
			ConfigVersion: rc.config.Version, ArtifactDigest: digest,
			PrerequisiteDigest: work.fusionDigest, Payload: payload,
		}); err != nil {
			return err
		}
		work.evaluation = result.Evaluation
		work.evalDigest = digest
		rc.checkpoint(request, work.query.QueryID, model.StageEvaluation)
	}
	return nil
}

func (rc *runContext) assemble(work *queryWork) error {
	if work.recall == nil || work.fusion == nil || work.evaluation == nil {
		return model.NewError(model.CodeMissingPrerequisite, "query result stages are incomplete", work.query.QueryID)
	}
	if !recall.VerifyFusion(*work.recall, rc.config.Fusion, *work.fusion) {
		return model.NewError(model.CodeIncrementalMismatch, "fusion detail is not self-consistent", work.query.QueryID)
	}
	work.complete = true
	work.needsTask = false
	return nil
}

func (rc *runContext) checkpoint(request Request, queryID, stage string) {
	if request.OnCheckpoint != nil {
		request.OnCheckpoint(queryID, stage)
	}
}

func tamperedError(queryID, stage, message string) *model.AppError {
	return model.NewErrorDetails(model.CodeStaleCheckpoint, message, queryID,
		map[string]any{"classification": "tampered", "stage": stage})
}

func sameJSON(left, right any) bool {
	leftData, err := model.CanonicalJSON(left)
	if err != nil {
		return false
	}
	rightData, err := model.CanonicalJSON(right)
	if err != nil {
		return false
	}
	return string(leftData) == string(rightData)
}

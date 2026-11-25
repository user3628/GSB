package worker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime/debug"

	"localsearch/multirecall/internal/model"
	"localsearch/multirecall/internal/recall"
)

const (
	EnvWorker   = "MULTIRECALL_WORKER"
	EnvSnapshot = "MULTIRECALL_SNAPSHOT"
)

type Snapshot struct {
	Items  []model.Item `json:"items"`
	Config model.Config `json:"config"`
}

type Task struct {
	Shard             int                  `json:"shard"`
	Query             model.Query          `json:"query"`
	Judgments         []model.Judgment     `json:"judgments"`
	Plan              model.BudgetDecision `json:"plan"`
	StartStage        string               `json:"start_stage"`
	CachedRecall      *model.RecallBundle  `json:"cached_recall,omitempty"`
	CachedFusion      *model.FusionResult  `json:"cached_fusion,omitempty"`
	StageFingerprints StageFingerprints    `json:"stage_fingerprints"`
}

type StageFingerprints struct {
	Recall     string `json:"recall"`
	Fusion     string `json:"fusion"`
	Evaluation string `json:"evaluation"`
}

type Result struct {
	Shard      int                     `json:"shard"`
	QueryID    string                  `json:"query_id"`
	Recall     *model.RecallBundle     `json:"recall,omitempty"`
	Fusion     *model.FusionResult     `json:"fusion,omitempty"`
	Evaluation *model.EvaluationResult `json:"evaluation,omitempty"`
	Error      *model.WorkerFailure    `json:"error,omitempty"`
}

type Runner struct {
	snapshot Snapshot
	index    *recall.Index
	config   model.Config
}

func NewRunner(snapshot Snapshot) *Runner {
	return &Runner{snapshot: snapshot, index: recall.NewIndex(snapshot.Items), config: snapshot.Config.Normalize()}
}

func (r *Runner) Execute(task Task) Result {
	return executeSafely(r.index, r.config, task)
}

func IsWorkerProcess() bool { return os.Getenv(EnvWorker) == "1" }

func Main() int {
	snapshotPath := os.Getenv(EnvSnapshot)
	if snapshotPath == "" {
		fmt.Fprintln(os.Stderr, "worker snapshot path is missing")
		return 2
	}
	data, err := os.ReadFile(snapshotPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "read worker snapshot:", err)
		return 2
	}
	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		fmt.Fprintln(os.Stderr, "parse worker snapshot:", err)
		return 2
	}
	if err := Run(os.Stdin, os.Stdout, snapshot); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func Run(input io.Reader, output io.Writer, snapshot Snapshot) error {
	runner := NewRunner(snapshot)
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), 64*1024*1024)
	writer := bufio.NewWriterSize(output, 64*1024)
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var task Task
		if err := json.Unmarshal(line, &task); err != nil {
			result := Result{Error: &model.WorkerFailure{Code: model.CodeWorkerFailure, Message: "decode task: " + err.Error(), Shard: -1, QueryID: ""}}
			if err := encoder.Encode(result); err != nil {
				return err
			}
			if err := writer.Flush(); err != nil {
				return err
			}
			continue
		}
		result := runner.Execute(task)
		if err := encoder.Encode(result); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return writer.Flush()
}

func ProcessTask(snapshot Snapshot, task Task) Result {
	return NewRunner(snapshot).Execute(task)
}

func executeSafely(index *recall.Index, config model.Config, task Task) (result Result) {
	result = Result{Shard: task.Shard, QueryID: task.Query.QueryID}
	defer func() {
		if recovered := recover(); recovered != nil {
			result = Result{
				Shard:   task.Shard,
				QueryID: task.Query.QueryID,
				Error: &model.WorkerFailure{
					Code:    model.CodeWorkerFailure,
					Message: fmt.Sprintf("worker panic: %v\n%s", recovered, debug.Stack()),
					Shard:   task.Shard,
					QueryID: task.Query.QueryID,
				},
			}
		}
	}()
	plans := make(map[string]recall.RoutePlan, len(task.Plan.Routes))
	for _, route := range task.Plan.Routes {
		plans[route.Route] = recall.RoutePlan{Enabled: route.Enabled, Depth: route.AppliedDepth}
	}
	var bundle model.RecallBundle
	var fusion model.FusionResult
	var evaluation model.EvaluationResult
	switch task.StartStage {
	case model.StageRecall:
		var err error
		bundle, err = index.RecallAll(task.Query, plans)
		if err != nil {
			return failureResult(task, err)
		}
		fusion, err = recall.Fuse(bundle, config.Fusion)
		if err != nil {
			return failureResult(task, err)
		}
		result.Recall = &bundle
		result.Fusion = &fusion
	case model.StageFusion:
		if task.CachedRecall == nil {
			return errorResult(task, model.CodeMissingPrerequisite, "cached recall artifact is required")
		}
		bundle = *task.CachedRecall
		var err error
		fusion, err = recall.Fuse(bundle, config.Fusion)
		if err != nil {
			return failureResult(task, err)
		}
		result.Fusion = &fusion
	case model.StageEvaluation:
		if task.CachedFusion == nil {
			return errorResult(task, model.CodeMissingPrerequisite, "cached fusion artifact is required")
		}
		fusion = *task.CachedFusion
	default:
		return errorResult(task, model.CodeIllegalStageTransition, "unknown start stage: "+task.StartStage)
	}
	evaluation = recall.Evaluate(bundleOrCached(task, bundle), fusion, task.Judgments, config.Evaluation)
	result.Evaluation = &evaluation
	return result
}

func bundleOrCached(task Task, current model.RecallBundle) model.RecallBundle {
	if current.QueryID != "" {
		return current
	}
	if task.CachedRecall != nil {
		return *task.CachedRecall
	}
	return model.RecallBundle{QueryID: task.Query.QueryID}
}

func failureResult(task Task, err error) Result {
	appErr := model.AsAppError(err)
	result := errorResult(task, appErr.Code, appErr.Message)
	return result
}

func errorResult(task Task, code, message string) Result {
	return Result{
		Shard:   task.Shard,
		QueryID: task.Query.QueryID,
		Error: &model.WorkerFailure{
			Code:    code,
			Message: message,
			Shard:   task.Shard,
			QueryID: task.Query.QueryID,
		},
	}
}

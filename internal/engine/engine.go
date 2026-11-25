package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"ropechunk/internal/model"
	"ropechunk/internal/rope"
	"ropechunk/internal/store"
)

type Engine struct {
	store   *store.Store
	stopped atomic.Bool
}

type RotationPayload struct {
	Q      model.RotatedBlock `json:"q"`
	K      model.RotatedBlock `json:"k"`
	Digest string             `json:"digest"`
}

type ScoredRowState struct {
	QueryIndex int                `json:"query_index"`
	State      model.SoftmaxState `json:"state"`
}

type ScorePayload struct {
	States   []ScoredRowState     `json:"states,omitempty"`
	Rows     []model.AttentionRow `json:"rows,omitempty"`
	Digest   string               `json:"digest,omitempty"`
	Complete bool                 `json:"complete"`
}

type taskKey struct {
	batch, head, rowBlock int
}

type rowTask struct {
	key         taskKey
	shard       model.Shard
	stage       string
	rowStart    int
	rowEnd      int
	estimatedUS int64
	existing    *store.CheckpointRecord
}

func New(st *store.Store) *Engine { return &Engine{store: st} }

func (e *Engine) Store() *store.Store { return e.store }

func (e *Engine) Stop()           { e.stopped.Store(true) }
func (e *Engine) ResetStop()      { e.stopped.Store(false) }
func (e *Engine) IsStopped() bool { return e.stopped.Load() }

func (e *Engine) Import(ctx context.Context, bundle model.Bundle) (ImportReport, error) {
	transition, err := e.store.SaveBundle(ctx, bundle)
	if err != nil {
		return ImportReport{}, err
	}
	invs, err := e.InvalidateMismatched(ctx, fmt.Sprintf("config transition: %s", transition.Kind))
	if err != nil {
		return ImportReport{Transition: transition}, err
	}
	return ImportReport{Transition: transition, Invalidations: invs}, nil
}

func (e *Engine) Compute(ctx context.Context, opts ComputeOptions) (ComputeReport, error) {
	cfg, err := e.store.CurrentConfig(ctx)
	if err != nil {
		return ComputeReport{}, err
	}
	if err := cfg.Validate(); err != nil {
		return ComputeReport{}, &model.AppError{Code: model.CodeInvalidConfig, Message: err.Error()}
	}
	stage := opts.Stage
	if stage == "" {
		stage = "all"
	}
	if stage != "all" && stage != model.StageRotation && stage != model.StageScore {
		return ComputeReport{}, &model.AppError{Code: model.CodeInvalidConfig, Message: "stage must be rotation, score, or all"}
	}
	runID := opts.RunID
	if runID == "" {
		runID = newRunID()
	}
	started := time.Now().UTC()
	report := ComputeReport{RunID: runID, ConfigVersion: cfg.SnapshotVersion, ConfigDigest: model.ConfigDigest(cfg), Stage: stage, StartedAt: started.Format(time.RFC3339Nano)}
	_ = e.store.StartRun(ctx, model.RunRecord{ID: runID, ConfigVersion: cfg.SnapshotVersion, Stage: stage, Status: "running", StartedAt: report.StartedAt, Note: opts.Note})
	invs, err := e.InvalidateMismatched(ctx, "content or operator version changed")
	if err != nil {
		return report, err
	}
	report.Invalidations = invs
	shards, err := e.selectShards(ctx, opts.ShardIDs)
	if err != nil {
		return report, err
	}
	stages := []string{model.StageRotation, model.StageScore}
	if stage != "all" {
		stages = []string{stage}
	}
	stopAll := false
	for _, shard := range shards {
		exec := ShardExecution{ShardID: shard.ID, Shape: shard.Shape, Fingerprints: model.ComputeFingerprints(shard, cfg)}
		if stage == model.StageScore {
			if err := e.assertRotationReady(ctx, shard, cfg); err != nil {
				app, _ := model.AsAppError(err)
				exec.Stages = append(exec.Stages, StageExecution{Stage: model.StageScore, Fingerprint: exec.Fingerprints.Score, Error: app, Errors: []*model.AppError{app}})
				report.Errors = append(report.Errors, app)
				report.Shards = append(report.Shards, exec)
				continue
			}
		}
		for _, currentStage := range stages {
			if stopAll {
				break
			}
			stageExec := e.runStage(ctx, shard, cfg, currentStage, runID, opts)
			exec.Stages = append(exec.Stages, stageExec)
			report.Processed += stageExec.Processed
			report.Reused += stageExec.Reused
			report.Unprocessed += stageExec.Unprocessed
			if stageExec.Error != nil {
				report.Errors = append(report.Errors, stageExec.Error)
			}
			report.Errors = append(report.Errors, stageExec.Errors...)
			if e.IsStopped() || errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				report.Stopped = true
				stopAll = true
			}
			if stage == "all" && currentStage == model.StageRotation && stageExec.Unprocessed > 0 {
				stopAll = true
			}
		}
		report.Shards = append(report.Shards, exec)
	}
	report.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	status := "complete"
	if report.Stopped {
		status = "stopped"
	}
	if len(report.Errors) > 0 {
		status = "error"
	}
	_ = e.store.FinishRun(ctx, runID, status, opts.Note)
	if len(report.Errors) > 0 {
		return report, report.Errors[0]
	}
	return report, nil
}

func (e *Engine) runStage(ctx context.Context, shard model.Shard, cfg model.Config, stage, runID string, opts ComputeOptions) StageExecution {
	fp := model.ComputeFingerprints(shard, cfg).ForStage(stage)
	exec := StageExecution{Stage: stage, Fingerprint: fp}
	plan, err := PlanMemory(shard, cfg, stage)
	exec.Plan = plan
	if err != nil {
		app, _ := model.AsAppError(err)
		exec.Error = app
		exec.Errors = append(exec.Errors, app)
		return exec
	}
	_ = e.store.SaveMemoryPlan(ctx, runID, plan)
	checkpoints, err := e.store.ListCheckpoints(ctx, shard.ID, stage)
	if err != nil {
		app := wrapIO(err)
		exec.Error = app
		exec.Errors = append(exec.Errors, app)
		return exec
	}
	checkpointMap := make(map[taskKey]*store.CheckpointRecord, len(checkpoints))
	for i := range checkpoints {
		rec := &checkpoints[i]
		checkpointMap[taskKey{rec.Batch, rec.Head, rec.RowBlock}] = rec
	}
	tasks := e.buildTasks(shard, cfg, stage, checkpointMap)

	passes := []int{-1}
	if plan.Mode == "batch-split" {
		passes = nil
		for batch := 0; batch < shard.Shape.Batch(); batch++ {
			passes = append(passes, batch)
		}
	}
	final := make(map[taskKey]TaskOutcome)
	lastSeen := make(map[taskKey]TaskOutcome)
	for passIndex, batchPass := range passes {
		var passTasks []rowTask
		for _, task := range tasks {
			if batchPass >= 0 && task.key.batch != batchPass {
				continue
			}
			if _, done := final[task.key]; done {
				continue
			}
			passTasks = append(passTasks, task)
		}
		maxRounds := len(passTasks) + 2
		for round := 1; round <= maxRounds; round++ {
			if len(passTasks) == 0 {
				break
			}
			roundReport, processed, remainingTasks := e.runRound(ctx, shard, cfg, stage, fp, passIndex, round, passTasks, checkpointMap, runID, opts)
			exec.Rounds = append(exec.Rounds, roundReport)
			for _, outcome := range roundReport.Outcomes {
				if outcome.Status == "processed" || outcome.Status == "reused" || outcome.Status == "error" || outcome.Status == "stopped" {
					final[taskKey{outcome.Batch, outcome.Head, outcome.RowBlock}] = outcome
				} else {
					lastSeen[taskKey{outcome.Batch, outcome.Head, outcome.RowBlock}] = outcome
				}
			}
			passTasks = remainingTasks
			if processed == 0 || e.IsStopped() || errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				break
			}
		}
		for _, task := range passTasks {
			outcome, ok := lastSeen[task.key]
			if !ok {
				outcome = TaskOutcome{ShardID: shard.ID, Stage: stage, Batch: task.key.batch, Head: task.key.head, RowBlock: task.key.rowBlock, RowStart: task.rowStart, RowEnd: task.rowEnd, Status: "unprocessed", Reason: "round ended without progress"}
			}
			if e.IsStopped() {
				outcome.Status = "stopped"
				outcome.Reason = "graceful stop requested"
			}
			final[task.key] = outcome
		}
		if e.IsStopped() {
			break
		}
	}
	for _, task := range tasks {
		outcome, ok := final[task.key]
		if !ok {
			outcome = TaskOutcome{ShardID: shard.ID, Stage: stage, Batch: task.key.batch, Head: task.key.head, RowBlock: task.key.rowBlock, RowStart: task.rowStart, RowEnd: task.rowEnd, Status: "unprocessed", Reason: "not admitted"}
		}
		exec.Tasks = append(exec.Tasks, outcome)
		switch outcome.Status {
		case "processed":
			exec.Processed++
		case "reused":
			exec.Reused++
		default:
			exec.Unprocessed++
		}
		if outcome.Error != nil {
			exec.Errors = append(exec.Errors, outcome.Error)
			if exec.Error == nil {
				exec.Error = outcome.Error
			}
		}
	}
	return exec
}

func (e *Engine) buildTasks(shard model.Shard, cfg model.Config, stage string, existing map[taskKey]*store.CheckpointRecord) []rowTask {
	var tasks []rowTask
	for b := 0; b < shard.Shape.Batch(); b++ {
		for h := 0; h < shard.Shape.Heads(); h++ {
			for rb, rowStart := 0, 0; rowStart < shard.Shape.Seq(); rb++ {
				rowEnd := min(rowStart+cfg.RowBlockSize, shard.Shape.Seq())
				key := taskKey{b, h, rb}
				covered := rowEnd
				if stage == model.StageScore && !cfg.CausalMask {
					covered = shard.Shape.Seq()
				}
				tasks = append(tasks, rowTask{key: key, shard: shard, stage: stage, rowStart: rowStart, rowEnd: rowEnd,
					estimatedUS: EstimateTaskUS(stage, rowEnd-rowStart, covered, shard.Shape.HeadDim()), existing: existing[key]})
				rowStart = rowEnd
			}
		}
	}
	return tasks
}

func (e *Engine) runRound(ctx context.Context, shard model.Shard, cfg model.Config, stage, fingerprint string, batchStep, round int, tasks []rowTask, existing map[taskKey]*store.CheckpointRecord, runID string, opts ComputeOptions) (RoundReport, int, []rowTask) {
	report := RoundReport{Round: round, Stage: stage, ShardID: shard.ID, BatchStep: batchStep, Workers: cfg.MaxConcurrentBlock, WorkerQuota: cfg.WorkerBlockQuota,
		Capacity: cfg.MaxConcurrentBlock * cfg.WorkerBlockQuota, BudgetUS: cfg.RoundTimeBudgetMS * 1000, RemainingUS: cfg.RoundTimeBudgetMS * 1000}
	var remaining []rowTask
	var admitted []rowTask
	for _, task := range tasks {
		if rec := task.existing; rec != nil && taskReusable(task, rec, fingerprint, shard) {
			outcome := TaskOutcome{ShardID: shard.ID, Stage: stage, Batch: task.key.batch, Head: task.key.head, RowBlock: task.key.rowBlock, RowStart: task.rowStart, RowEnd: task.rowEnd, Status: "reused", Reason: "compatible committed checkpoint", RemainingUS: report.RemainingUS, Sequence: rec.Sequence}
			report.Outcomes = append(report.Outcomes, outcome)
			report.Reused++
			continue
		}
		if e.stopRequested(ctx) {
			outcome := TaskOutcome{ShardID: shard.ID, Stage: stage, Batch: task.key.batch, Head: task.key.head, RowBlock: task.key.rowBlock, RowStart: task.rowStart, RowEnd: task.rowEnd, Status: "stopped", Reason: "graceful stop requested before admission", EstimatedUS: task.estimatedUS, RemainingUS: report.RemainingUS}
			report.Outcomes = append(report.Outcomes, outcome)
			report.Unprocessed++
			continue
		}
		if len(admitted) >= report.Capacity {
			outcome := TaskOutcome{ShardID: shard.ID, Stage: stage, Batch: task.key.batch, Head: task.key.head, RowBlock: task.key.rowBlock, RowStart: task.rowStart, RowEnd: task.rowEnd, Status: "unprocessed", Reason: fmt.Sprintf("worker quota exhausted this round (capacity %d)", report.Capacity), EstimatedUS: task.estimatedUS, RemainingUS: report.RemainingUS}
			report.Outcomes = append(report.Outcomes, outcome)
			report.Unprocessed++
			remaining = append(remaining, task)
			continue
		}
		if task.estimatedUS > report.RemainingUS {
			outcome := TaskOutcome{ShardID: shard.ID, Stage: stage, Batch: task.key.batch, Head: task.key.head, RowBlock: task.key.rowBlock, RowStart: task.rowStart, RowEnd: task.rowEnd, Status: "unprocessed", Reason: fmt.Sprintf("estimated %dus exceeds remaining round budget %dus", task.estimatedUS, report.RemainingUS), EstimatedUS: task.estimatedUS, RemainingUS: report.RemainingUS}
			report.Outcomes = append(report.Outcomes, outcome)
			report.Unprocessed++
			remaining = append(remaining, task)
			continue
		}
		report.RemainingUS -= task.estimatedUS
		report.UsedUS += task.estimatedUS
		admitted = append(admitted, task)
	}
	if len(admitted) > 0 {
		workerCount := min(cfg.MaxConcurrentBlock, len(admitted))
		queues := make([][]rowTask, workerCount)
		for i, task := range admitted {
			worker := i % workerCount
			if len(queues[worker]) >= cfg.WorkerBlockQuota {
				remaining = append(remaining, task)
				continue
			}
			queues[worker] = append(queues[worker], task)
		}
		results := make(chan TaskOutcome, len(admitted))
		var wg sync.WaitGroup
		for _, queue := range queues {
			if len(queue) == 0 {
				continue
			}
			wg.Add(1)
			go func(workerTasks []rowTask) {
				defer wg.Done()
				for _, task := range workerTasks {
					if e.stopRequested(ctx) {
						results <- TaskOutcome{ShardID: shard.ID, Stage: stage, Batch: task.key.batch, Head: task.key.head, RowBlock: task.key.rowBlock, RowStart: task.rowStart, RowEnd: task.rowEnd, Status: "stopped", Reason: "stop requested after completing current block", EstimatedUS: task.estimatedUS, RemainingUS: report.RemainingUS}
						continue
					}
					sequence, err := e.processTask(ctx, task, shard, cfg, stage, fingerprint, runID, opts.HookAfterPublish)
					outcome := TaskOutcome{ShardID: shard.ID, Stage: stage, Batch: task.key.batch, Head: task.key.head, RowBlock: task.key.rowBlock, RowStart: task.rowStart, RowEnd: task.rowEnd, EstimatedUS: task.estimatedUS, RemainingUS: report.RemainingUS, Sequence: sequence}
					if err != nil {
						app, ok := model.AsAppError(err)
						if !ok {
							app = &model.AppError{Code: model.CodeIO, Message: err.Error(), ShardID: shard.ID, Stage: stage, Batch: task.key.batch, Head: task.key.head, RowBlock: task.key.rowBlock}
						}
						outcome.Status = "error"
						outcome.Error = app
						outcome.Reason = app.Error()
					} else {
						outcome.Status = "processed"
					}
					results <- outcome
				}
			}(queue)
		}
		wg.Wait()
		close(results)
		for outcome := range results {
			report.Outcomes = append(report.Outcomes, outcome)
			if outcome.Status == "processed" {
				report.Processed++
			} else if outcome.Status == "stopped" {
				report.Unprocessed++
			}
		}
	}
	report.Outcomes = sortOutcomes(report.Outcomes)
	events := make([]model.BudgetEvent, 0, len(report.Outcomes))
	for _, outcome := range report.Outcomes {
		events = append(events, model.BudgetEvent{RunID: runID, ShardID: shard.ID, Stage: stage, Batch: outcome.Batch, Head: outcome.Head, RowBlock: outcome.RowBlock,
			Status: outcome.Status, Reason: outcome.Reason, EstimatedUS: outcome.EstimatedUS, RemainingUS: outcome.RemainingUS})
	}
	_ = e.store.RecordBudgetEvents(ctx, events)
	return report, report.Processed, remaining
}

func sortOutcomes(out []TaskOutcome) []TaskOutcome {
	for i := 1; i < len(out); i++ {
		for j := i; j > 0; j-- {
			a, b := out[j-1], out[j]
			if a.Batch < b.Batch || (a.Batch == b.Batch && (a.Head < b.Head || (a.Head == b.Head && a.RowBlock <= b.RowBlock))) {
				break
			}
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

func taskReusable(task rowTask, rec *store.CheckpointRecord, fingerprint string, shard model.Shard) bool {
	return rec.Complete && rec.VersionFingerprint == fingerprint && rec.InputFingerprint == model.InputDigest(shard) && rec.RowStart == task.rowStart && rec.RowEnd == task.rowEnd
}

func (e *Engine) processTask(ctx context.Context, task rowTask, shard model.Shard, cfg model.Config, stage, fingerprint, runID string, hook func(string, string, int, int, int, int64)) (int64, error) {
	if stage == model.StageRotation {
		return e.processRotation(ctx, task, shard, cfg, fingerprint, runID, hook)
	}
	return e.processScore(ctx, task, shard, cfg, fingerprint, runID, hook)
}

func (e *Engine) processRotation(ctx context.Context, task rowTask, shard model.Shard, cfg model.Config, fingerprint, runID string, hook func(string, string, int, int, int, int64)) (int64, error) {
	qBlock, err := rope.RotateBlock(shard.Q, cfg, task.key.batch, task.key.head, task.rowStart, task.rowEnd, rope.RotateOptions{OutputScale: shard.Q.Scale})
	if err != nil {
		return 0, err
	}
	qBlock.ShardID = shard.ID
	kBlock, err := rope.RotateBlock(shard.K, cfg, task.key.batch, task.key.head, task.rowStart, task.rowEnd, rope.RotateOptions{OutputScale: shard.K.Scale})
	if err != nil {
		return 0, err
	}
	kBlock.ShardID = shard.ID
	payload := RotationPayload{Q: qBlock, K: kBlock, Digest: combineRotationDigests(qBlock.Digest, kBlock.Digest)}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	expected := int64(1)
	if task.existing != nil {
		expected = task.existing.Sequence + 1
	}
	req := model.PublishRequest{
		Checkpoint: model.Checkpoint{ShardID: shard.ID, Stage: model.StageRotation, Batch: task.key.batch, Head: task.key.head, RowBlock: task.key.rowBlock,
			RowStart: task.rowStart, RowEnd: task.rowEnd, CoveredKeyStart: task.rowStart, CoveredKeyEnd: task.rowEnd,
			RunningMax: 0, ExpSum: 1, LSE: 0, PartialDigest: payload.Digest,
			InputFingerprint: model.InputDigest(shard), VersionFingerprint: fingerprint, Complete: true,
			ConfigVersion: cfg.SnapshotVersion, RunID: runID},
		AssignedShardID: shard.ID, AssignedRowBlock: task.key.rowBlock, ExpectedSequence: expected, ResultPayload: payloadJSON,
	}
	seq, err := e.store.PublishCheckpoint(ctx, req)
	if err == nil && hook != nil {
		hook(model.StageRotation, shard.ID, task.key.batch, task.key.head, task.key.rowBlock, seq)
	}
	return seq, err
}

func (e *Engine) processScore(ctx context.Context, task rowTask, shard model.Shard, cfg model.Config, fingerprint, runID string, hook func(string, string, int, int, int, int64)) (int64, error) {
	q, k, err := e.loadRotatedQK(ctx, shard, cfg, task)
	if err != nil {
		return 0, err
	}
	headDim := shard.Shape.HeadDim()
	seqLen := shard.Shape.Seq()
	states := make([]ScoredRowState, task.rowEnd-task.rowStart)
	for i := range states {
		states[i] = ScoredRowState{QueryIndex: task.rowStart + i, State: rope.NewSoftmaxState(headDim)}
	}
	startKey := 0
	existingSeq := int64(0)
	if task.existing != nil && !task.existing.Complete {
		existingSeq = task.existing.Sequence
		startKey = task.existing.CoveredKeyEnd
		var payload ScorePayload
		if len(task.existing.ResultPayload) > 0 {
			if err := json.Unmarshal(task.existing.ResultPayload, &payload); err != nil {
				return 0, wrapIO(err)
			}
		}
		if len(payload.States) != len(states) {
			return 0, &model.AppError{Code: model.CodeState, ShardID: shard.ID, Stage: model.StageScore, Batch: task.key.batch, Head: task.key.head, RowBlock: task.key.rowBlock, Message: "partial score checkpoint lacks resumable row states"}
		}
		for i := range states {
			if payload.States[i].QueryIndex != states[i].QueryIndex {
				return 0, &model.AppError{Code: model.CodeState, ShardID: shard.ID, Stage: model.StageScore, Batch: task.key.batch, Head: task.key.head, RowBlock: task.key.rowBlock, Message: "partial score row coordinates do not match"}
			}
			states[i].State = payload.States[i].State
		}
	}
	var lastSeq int64 = existingSeq
	for keyStart := startKey; keyStart < seqLen; keyStart += cfg.KeyBlockSize {
		keyEnd := min(keyStart+cfg.KeyBlockSize, seqLen)
		for i := range states {
			row := states[i]
			qRow := q[i*headDim : (i+1)*headDim]
			if err := rope.ConsumeKeyRange(qRow, k, seqLen, headDim, row.QueryIndex, cfg.CausalMask, keyStart, keyEnd, &row.State, task.key.rowBlock, keyStart/cfg.KeyBlockSize, shard.ID); err != nil {
				app, _ := model.AsAppError(err)
				if app == nil {
					app = &model.AppError{Code: model.CodeNumeric, Message: err.Error()}
				}
				return lastSeq, enrichScoreError(app, shard.ID, task, keyStart/cfg.KeyBlockSize)
			}
			states[i] = row
		}
		complete := keyEnd >= seqLen
		var rows []model.AttentionRow
		if complete {
			rows = make([]model.AttentionRow, len(states))
			for i := range states {
				row := states[i]
				final, err := rope.FinalizeAttentionRow(q[i*headDim:(i+1)*headDim], k, seqLen, headDim, row.QueryIndex, cfg.CausalMask, row.State, shard.ID, task.key.rowBlock)
				if err != nil {
					app, _ := model.AsAppError(err)
					return lastSeq, enrichScoreError(app, shard.ID, task, keyEnd/cfg.KeyBlockSize)
				}
				rows[i] = final
			}
		}
		payload := ScorePayload{States: states, Rows: rows, Complete: complete}
		if complete {
			payload.Digest = rope.ScoreBlockDigest(rows)
		}
		payloadJSON, err := json.Marshal(payload)
		if err != nil {
			return lastSeq, err
		}
		partialDigest := scoreStateDigest(states)
		aggMax, aggSum := aggregateStates(states)
		req := model.PublishRequest{
			Checkpoint: model.Checkpoint{ShardID: shard.ID, Stage: model.StageScore, Batch: task.key.batch, Head: task.key.head, RowBlock: task.key.rowBlock,
				RowStart: task.rowStart, RowEnd: task.rowEnd, CoveredKeyStart: 0, CoveredKeyEnd: keyEnd,
				RunningMax: aggMax, ExpSum: aggSum, LSE: aggregateLSE(states), PartialDigest: partialDigest,
				InputFingerprint: model.InputDigest(shard), VersionFingerprint: fingerprint, Complete: complete,
				ConfigVersion: cfg.SnapshotVersion, RunID: runID},
			AssignedShardID: shard.ID, AssignedRowBlock: task.key.rowBlock, ExpectedSequence: lastSeq + 1, ResultPayload: payloadJSON,
		}
		seq, err := e.store.PublishCheckpoint(ctx, req)
		if err != nil {
			return lastSeq, err
		}
		lastSeq = seq
		if hook != nil {
			hook(model.StageScore, shard.ID, task.key.batch, task.key.head, task.key.rowBlock, seq)
		}
	}
	return lastSeq, nil
}

func (e *Engine) loadRotatedQK(ctx context.Context, shard model.Shard, cfg model.Config, task rowTask) ([]float64, []float64, error) {
	rotationFP := model.ComputeFingerprints(shard, cfg).Rotation
	records, err := e.store.ListCheckpoints(ctx, shard.ID, model.StageRotation)
	if err != nil {
		return nil, nil, err
	}
	dim := shard.Shape.HeadDim()
	seqLen := shard.Shape.Seq()
	k := make([]float64, seqLen*dim)
	covered := make([]bool, seqLen)
	var q []float64
	for i := range records {
		rec := &records[i]
		if rec.Batch != task.key.batch || rec.Head != task.key.head || !rec.Complete || rec.VersionFingerprint != rotationFP {
			continue
		}
		var payload RotationPayload
		if err := json.Unmarshal(rec.ResultPayload, &payload); err != nil {
			return nil, nil, wrapIO(err)
		}
		for row := rec.RowStart; row < rec.RowEnd; row++ {
			offset := (row - rec.RowStart) * dim
			for d := 0; d < dim; d++ {
				k[row*dim+d] = float64(payload.K.Data[offset+d]) / float64(payload.K.Scale)
				if row >= task.rowStart && row < task.rowEnd {
					if len(q) == 0 {
						q = make([]float64, (task.rowEnd-task.rowStart)*dim)
					}
					q[(row-task.rowStart)*dim+d] = float64(payload.Q.Data[offset+d]) / float64(payload.Q.Scale)
				}
			}
			covered[row] = true
		}
	}
	if len(q) == 0 {
		return nil, nil, &model.AppError{Code: model.CodeState, ShardID: shard.ID, Stage: model.StageScore, Batch: task.key.batch, Head: task.key.head, RowBlock: task.key.rowBlock, Message: "rotated Q block is not committed"}
	}
	for row, ok := range covered {
		if !ok {
			return nil, nil, &model.AppError{Code: model.CodeState, ShardID: shard.ID, Stage: model.StageScore, RowBlock: task.key.rowBlock, Message: fmt.Sprintf("rotated K row %d is not committed", row)}
		}
	}
	return q, k, nil
}

func (e *Engine) assertRotationReady(ctx context.Context, shard model.Shard, cfg model.Config) error {
	fp := model.ComputeFingerprints(shard, cfg).Rotation
	records, err := e.store.ListCheckpoints(ctx, shard.ID, model.StageRotation)
	if err != nil {
		return err
	}
	expected := shard.Shape.Batch() * shard.Shape.Heads() * int(math.Ceil(float64(shard.Shape.Seq())/float64(cfg.RowBlockSize)))
	valid := 0
	for i := range records {
		rec := &records[i]
		start, end := expectedRowRange(rec.RowBlock, shard.Shape.Seq(), cfg.RowBlockSize)
		if rec.Complete && rec.VersionFingerprint == fp && rec.RowStart == start && rec.RowEnd == end {
			valid++
		}
	}
	if valid < expected {
		return &model.AppError{Code: model.CodeState, ShardID: shard.ID, Stage: model.StageScore, Message: fmt.Sprintf("rotation stage incomplete: %d/%d row blocks", valid, expected)}
	}
	return nil
}

func (e *Engine) InvalidateMismatched(ctx context.Context, reason string) ([]model.InvalidationEvent, error) {
	cfg, err := e.store.CurrentConfig(ctx)
	if err != nil {
		return nil, err
	}
	shards, err := e.store.ListShards(ctx)
	if err != nil {
		return nil, err
	}
	var events []model.InvalidationEvent
	for _, shard := range shards {
		fps := model.ComputeFingerprints(shard, cfg)
		for _, stage := range []string{model.StageRotation, model.StageScore} {
			records, err := e.store.ListCheckpoints(ctx, shard.ID, stage)
			if err != nil {
				return nil, err
			}
			var invalid []store.CheckpointKey
			earliestRow := math.MaxInt
			colStart, colEnd := math.MaxInt, -1
			for i := range records {
				rec := &records[i]
				expectedStart, expectedEnd := expectedRowRange(rec.RowBlock, shard.Shape.Seq(), cfg.RowBlockSize)
				if rec.VersionFingerprint != fps.ForStage(stage) || rec.InputFingerprint != fps.Input || rec.RowStart != expectedStart || rec.RowEnd != expectedEnd {
					invalid = append(invalid, store.CheckpointKey{Batch: rec.Batch, Head: rec.Head, RowBlock: rec.RowBlock})
					if rec.RowBlock < earliestRow {
						earliestRow = rec.RowBlock
					}
					if stage == model.StageScore {
						if rec.CoveredKeyStart < colStart {
							colStart = rec.CoveredKeyStart
						}
						if rec.CoveredKeyEnd-1 > colEnd {
							colEnd = rec.CoveredKeyEnd - 1
						}
					}
				}
			}
			if len(invalid) == 0 {
				continue
			}
			if earliestRow == math.MaxInt {
				earliestRow = 0
			}
			blockSize := cfg.KeyBlockSize
			if stage == model.StageRotation {
				blockSize = cfg.AngleBlockSize
				colStart = 0
				colEnd = ((shard.Shape.HeadDim()+cfg.AngleBlockSize-1)/cfg.AngleBlockSize - 1) * cfg.AngleBlockSize
			} else {
				if colStart == math.MaxInt {
					colStart = 0
				}
				if colEnd < 0 {
					colEnd = 0
				}
			}
			event := model.InvalidationEvent{
				ConfigVersion: cfg.SnapshotVersion, Stage: stage, EarliestRowBlock: earliestRow,
				ColBlockStart: colStart / blockSize, ColBlockEnd: colEnd / blockSize,
				Reason: reason, ReusedRows: len(records) - len(invalid),
			}
			written, err := e.store.InvalidateCheckpoints(ctx, shard.ID, stage, invalid, event)
			if err != nil {
				return nil, err
			}
			events = append(events, written)
		}
	}
	return events, nil
}

func expectedRowRange(rowBlock, seqLen, rowBlockSize int) (int, int) {
	start := rowBlock * rowBlockSize
	end := min(start+rowBlockSize, seqLen)
	return start, end
}

func (e *Engine) Status(ctx context.Context, shardIDs []string) (StatusReport, error) {
	cfg, err := e.store.CurrentConfig(ctx)
	if err != nil {
		return StatusReport{}, err
	}
	shards, err := e.selectShards(ctx, shardIDs)
	if err != nil {
		return StatusReport{}, err
	}
	report := StatusReport{Config: cfg, ConfigDigest: model.ConfigDigest(cfg)}
	for _, shard := range shards {
		fps := model.ComputeFingerprints(shard, cfg)
		ss := ShardStatus{ShardID: shard.ID, Shape: shard.Shape, Fingerprints: fps}
		for _, stage := range []string{model.StageRotation, model.StageScore} {
			records, err := e.store.ListCheckpoints(ctx, shard.ID, stage)
			if err != nil {
				return StatusReport{}, err
			}
			expected := shard.Shape.Batch() * shard.Shape.Heads() * int(math.Ceil(float64(shard.Shape.Seq())/float64(cfg.RowBlockSize)))
			st := StageStatus{Stage: stage, Fingerprint: fps.ForStage(stage), ExpectedRows: expected}
			for i := range records {
				rec := &records[i]
				start, end := expectedRowRange(rec.RowBlock, shard.Shape.Seq(), cfg.RowBlockSize)
				if rec.VersionFingerprint != fps.ForStage(stage) || rec.InputFingerprint != fps.Input || rec.RowStart != start || rec.RowEnd != end {
					st.StaleRows++
				} else if rec.Complete {
					st.CompleteRows++
				} else {
					st.IncompleteRows++
				}
			}
			ss.Stages = append(ss.Stages, st)
		}
		report.Shards = append(report.Shards, ss)
	}
	return report, nil
}

func (e *Engine) VerifyReferences(ctx context.Context, shardIDs []string) (VerificationReport, error) {
	cfg, err := e.store.CurrentConfig(ctx)
	if err != nil {
		return VerificationReport{}, err
	}
	shards, err := e.selectShards(ctx, shardIDs)
	if err != nil {
		return VerificationReport{}, err
	}
	report := VerificationReport{ConfigDigest: model.ConfigDigest(cfg), AllMatch: true}
	for _, shard := range shards {
		select {
		case <-ctx.Done():
			return report, ctx.Err()
		default:
		}
		q, _, err := rope.RotateTensor(shard.Q, cfg)
		if err != nil {
			return report, err
		}
		k, _, err := rope.RotateTensor(shard.K, cfg)
		if err != nil {
			return report, err
		}
		vr := ReferenceVerification{ShardID: shard.ID, DigestMatch: true}
		if shard.ReferenceQ == nil || shard.ReferenceK == nil {
			vr.FirstMismatch = "reference tensors are absent"
			vr.DigestMatch = false
		} else {
			compareTensor(q, *shard.ReferenceQ, "q", &vr)
			compareTensor(k, *shard.ReferenceK, "k", &vr)
			vr.DigestMatch = vr.Mismatches == 0
		}
		if !vr.DigestMatch {
			report.AllMatch = false
		}
		report.Shards = append(report.Shards, vr)
	}
	return report, nil
}

func compareTensor(got, want model.QuantTensor, role string, report *ReferenceVerification) {
	if !reflect.DeepEqual(got.Shape, want.Shape) || got.Scale != want.Scale || len(got.Data) != len(want.Data) {
		report.Mismatches++
		if report.FirstMismatch == "" {
			report.FirstMismatch = role + " tensor shape/scale mismatch"
		}
		return
	}
	for i := range got.Data {
		report.CheckedElements++
		if got.Data[i] != want.Data[i] {
			report.Mismatches++
			if report.FirstMismatch == "" {
				report.FirstMismatch = fmt.Sprintf("%s[%d] got %d want %d", role, i, got.Data[i], want.Data[i])
			}
		}
	}
}

func (e *Engine) selectShards(ctx context.Context, ids []string) ([]model.Shard, error) {
	if len(ids) == 0 {
		return e.store.ListShards(ctx)
	}
	out := make([]model.Shard, 0, len(ids))
	for _, id := range ids {
		shard, err := e.store.LoadShard(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, shard)
	}
	return out, nil
}

func (e *Engine) stopRequested(ctx context.Context) bool {
	if e.stopped.Load() {
		return true
	}
	return errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded)
}

func combineRotationDigests(q, k string) string {
	return q + ":" + k
}

func scoreStateDigest(states []ScoredRowState) string {
	data, _ := json.Marshal(states)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func aggregateStates(states []ScoredRowState) (float64, float64) {
	maxScore := math.Inf(-1)
	sum := 0.0
	for _, row := range states {
		maxScore = math.Max(maxScore, row.State.Max)
		sum += row.State.SumExp
	}
	if math.IsInf(maxScore, -1) {
		maxScore = 0
	}
	return maxScore, sum
}

func aggregateLSE(states []ScoredRowState) float64 {
	for _, row := range states {
		if !math.IsInf(row.State.Max, -1) && row.State.SumExp > 0 {
			return row.State.Max + math.Log(row.State.SumExp)
		}
	}
	return 0
}

func enrichScoreError(app *model.AppError, shardID string, task rowTask, colBlock int) *model.AppError {
	if app == nil {
		return &model.AppError{Code: model.CodeNumeric, ShardID: shardID, Stage: model.StageScore, Batch: task.key.batch, Head: task.key.head, RowBlock: task.key.rowBlock, ColBlock: colBlock, Message: "numeric failure"}
	}
	app.ShardID = shardID
	app.Stage = model.StageScore
	app.Batch = task.key.batch
	app.Head = task.key.head
	app.RowBlock = task.key.rowBlock
	app.ColBlock = colBlock
	return app
}

func wrapIO(err error) *model.AppError {
	return &model.AppError{Code: model.CodeIO, Message: err.Error()}
}

func newRunID() string {
	var buf [12]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("run-%d", time.Now().UnixNano())
	}
	return "run-" + hex.EncodeToString(buf[:])
}

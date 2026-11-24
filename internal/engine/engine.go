package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"hostacl-workbench/internal/apperr"
	"hostacl-workbench/internal/fsx"
	"hostacl-workbench/internal/model"
	"hostacl-workbench/internal/store"
)

type Config struct {
	Root                  string
	DBPath                string
	MaxConcurrency        int
	QueueSize             int
	VerifyBudget          time.Duration
	CrashAfterMaterialize bool
}

type Engine struct {
	cfg   Config
	store *store.Store
	fs    *fsx.Materializer
	mu    sync.RWMutex
}

type CommitResult struct {
	Status        string              `json:"status"`
	Version       int64               `json:"version"`
	BaseVersion   int64               `json:"base_version"`
	Digest        string              `json:"digest"`
	ViewDigest    string              `json:"view_digest"`
	TreePath      string              `json:"tree_path"`
	Cascade       model.CascadeReport `json:"cascade"`
	Apply         model.ApplyReport   `json:"apply"`
	IntentID      string              `json:"intent_id"`
	CommittedAt   string              `json:"committed_at"`
	AffectedCount int                 `json:"affected_count"`
}

type ReadResult struct {
	SubjectID       string              `json:"subject_id"`
	Path            string              `json:"path"`
	ObservedVersion int64               `json:"observed_version"`
	UsedVersion     int64               `json:"used_version"`
	Safe            bool                `json:"safe"`
	View            model.EffectiveView `json:"view"`
}

type RecoveryIntent struct {
	ID            string `json:"id"`
	Kind          string `json:"kind"`
	BaseVersion   int64  `json:"base_version"`
	TargetVersion int64  `json:"target_version"`
	CreatedAt     string `json:"created_at"`
	Action        string `json:"action"`
	Evidence      string `json:"evidence"`
}

type RecoveryReport struct {
	ID                  string           `json:"id"`
	StartedAt           string           `json:"started_at"`
	FinishedAt          string           `json:"finished_at"`
	Status              string           `json:"status"`
	LastCompleteVersion int64            `json:"last_complete_version"`
	LastCompleteTree    string           `json:"last_complete_tree,omitempty"`
	PendingIntents      []RecoveryIntent `json:"pending_intents"`
	OrphanTreesRemoved  []string         `json:"orphan_trees_removed"`
	Facts               []string         `json:"facts"`
	Failure             string           `json:"failure,omitempty"`
}

func New(cfg Config) (*Engine, error) {
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 4
	}
	if cfg.QueueSize < 0 {
		return nil, fmt.Errorf("queue size must be >= 0")
	}
	if cfg.VerifyBudget <= 0 {
		cfg.VerifyBudget = 30 * time.Second
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	materializer, err := fsx.New(cfg.Root)
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	return &Engine{cfg: cfg, store: st, fs: materializer}, nil
}

func (e *Engine) Close() error { return e.store.Close() }

func (e *Engine) Store() *store.Store { return e.store }

func (e *Engine) Root() string { return e.fs.Root }

func (e *Engine) Platform() string { return e.fs.Platform }

func (e *Engine) Cleanup() error { return e.fs.Cleanup() }

func (e *Engine) Head(ctx context.Context) (store.Head, error) { return e.store.Head(ctx) }

func (e *Engine) CurrentPolicy(ctx context.Context) (*model.Policy, error) {
	return e.store.CurrentPolicy(ctx)
}

func (e *Engine) CurrentViews(ctx context.Context) ([]model.EffectiveView, error) {
	h, err := e.store.Head(ctx)
	if err != nil {
		return nil, err
	}
	return e.store.LoadViews(ctx, h.Version)
}

func (e *Engine) SubmitSnapshot(ctx context.Context, input model.Policy) (CommitResult, error) {
	policy, err := model.ValidateSnapshot(input)
	if err != nil {
		return CommitResult{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	h, err := e.store.Head(ctx)
	if err != nil {
		return CommitResult{}, err
	}
	if policy.Version <= h.Version {
		return CommitResult{}, apperr.With(apperr.StaleBase, "snapshot version is not newer than the committed head", map[string]any{
			"requested_version": policy.Version, "current_version": h.Version,
		})
	}
	oldPolicy, err := e.store.LoadPolicy(ctx, h.Version)
	if err != nil {
		return CommitResult{}, err
	}
	var oldViews []model.EffectiveView
	if oldPolicy != nil {
		oldViews, err = e.store.LoadViews(ctx, h.Version)
		if err != nil {
			return CommitResult{}, err
		}
	}
	return e.commitPolicyLocked(ctx, "snapshot", h.Version, policy, oldPolicy, oldViews)
}

func (e *Engine) SubmitIncrement(ctx context.Context, input model.Increment) (CommitResult, error) {
	input = *model.CloneIncrement(&input)
	if _, err := model.ValidateIncrementDigest(&input); err != nil {
		return CommitResult{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	h, err := e.store.Head(ctx)
	if err != nil {
		return CommitResult{}, err
	}
	if input.BaseVersion != h.Version {
		return CommitResult{}, apperr.With(apperr.StaleBase, "increment base version does not match the committed head", map[string]any{
			"base_version": input.BaseVersion, "current_version": h.Version, "target_version": input.Version,
		})
	}
	oldPolicy, err := e.store.LoadPolicy(ctx, h.Version)
	if err != nil {
		return CommitResult{}, err
	}
	if oldPolicy == nil {
		return CommitResult{}, apperr.With(apperr.StaleBase, "no committed policy exists for the increment base", map[string]any{
			"base_version": input.BaseVersion,
		})
	}
	next, err := model.ApplyIncrement(oldPolicy, &input)
	if err != nil {
		return CommitResult{}, err
	}
	oldViews, err := e.store.LoadViews(ctx, h.Version)
	if err != nil {
		return CommitResult{}, err
	}
	return e.commitPolicyLocked(ctx, "increment", h.Version, next, oldPolicy, oldViews)
}

func (e *Engine) commitPolicyLocked(ctx context.Context, kind string, baseVersion int64, policy *model.Policy, oldPolicy *model.Policy, oldViews []model.EffectiveView) (CommitResult, error) {
	intentID := newID(kind)
	intent := store.Intent{
		ID: intentID, Kind: kind, BaseVersion: baseVersion, TargetVersion: policy.Version,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		PayloadJSON: mustJSON(map[string]any{"digest": policy.Digest, "generated_at": policy.GeneratedAt}),
	}
	if err := e.store.StartIntent(ctx, intent); err != nil {
		return CommitResult{}, err
	}
	committed := false
	defer func() {
		if !committed {
			abortCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = e.store.AbortIntent(abortCtx, intentID, errors.New("policy operation did not commit"), nil)
			cancel()
		}
	}()
	newViews := model.ComputeViews(policy)
	cascade := model.CascadeDiff(oldPolicy, policy, oldViews, newViews)
	finalName := fmt.Sprintf("v%06d-%s", policy.Version, shortID(intentID))
	report, tree, err := e.fs.Materialize(ctx, policy, newViews, intentID, finalName)
	if err != nil {
		return CommitResult{}, err
	}
	e.maybeCrash()
	if err := e.store.CommitPolicy(ctx, intentID, baseVersion, policy, newViews, cascade, tree); err != nil {
		return CommitResult{}, err
	}
	committed = true
	return CommitResult{
		Status: "committed", Version: policy.Version, BaseVersion: baseVersion,
		Digest: policy.Digest, ViewDigest: cascade.ViewDigest, TreePath: tree,
		Cascade: cascade, Apply: report, IntentID: intentID,
		CommittedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		AffectedCount: len(cascade.AffectedSubjects) + len(cascade.AffectedPaths),
	}, nil
}

func (e *Engine) Rematerialize(ctx context.Context) (model.ApplyReport, string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	h, err := e.store.Head(ctx)
	if err != nil {
		return model.ApplyReport{}, "", err
	}
	if h.Version == 0 {
		return model.ApplyReport{}, "", apperr.New(apperr.NotFound, "no committed policy")
	}
	policy, err := e.store.LoadPolicy(ctx, h.Version)
	if err != nil || policy == nil {
		return model.ApplyReport{}, "", fmt.Errorf("load policy: %w", err)
	}
	views, err := e.store.LoadViews(ctx, h.Version)
	if err != nil {
		return model.ApplyReport{}, "", err
	}
	intentID := newID("apply")
	intent := store.Intent{ID: intentID, Kind: "apply", BaseVersion: h.Version, TargetVersion: h.Version,
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), PayloadJSON: mustJSON(map[string]any{"version": h.Version})}
	if err := e.store.StartIntent(ctx, intent); err != nil {
		return model.ApplyReport{}, "", err
	}
	committed := false
	defer func() {
		if !committed {
			abortCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = e.store.AbortIntent(abortCtx, intentID, errors.New("apply did not commit"), nil)
			cancel()
		}
	}()
	finalName := fmt.Sprintf("v%06d-apply-%s", h.Version, shortID(intentID))
	report, tree, err := e.fs.Materialize(ctx, policy, views, intentID, finalName)
	if err != nil {
		return model.ApplyReport{}, "", err
	}
	e.maybeCrash()
	if err := e.store.CommitApply(ctx, intentID, h.Version, tree, report); err != nil {
		return model.ApplyReport{}, "", err
	}
	committed = true
	return report, tree, nil
}

func (e *Engine) RebuildCascade(ctx context.Context) (model.CascadeReport, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	h, err := e.store.Head(ctx)
	if err != nil {
		return model.CascadeReport{}, err
	}
	if h.Version == 0 {
		return model.CascadeReport{}, apperr.New(apperr.NotFound, "no committed policy")
	}
	policy, err := e.store.LoadPolicy(ctx, h.Version)
	if err != nil || policy == nil {
		return model.CascadeReport{}, fmt.Errorf("load policy: %w", err)
	}
	oldViews, err := e.store.LoadViews(ctx, h.Version)
	if err != nil {
		return model.CascadeReport{}, err
	}
	newViews := model.ComputeViews(policy)
	cascade := model.CascadeDiff(policy, policy, oldViews, newViews)
	intentID := newID("cascade")
	intent := store.Intent{ID: intentID, Kind: "cascade", BaseVersion: h.Version, TargetVersion: h.Version,
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), PayloadJSON: mustJSON(map[string]any{"version": h.Version, "views": len(newViews)})}
	if err := e.store.StartIntent(ctx, intent); err != nil {
		return model.CascadeReport{}, err
	}
	if err := e.store.CommitCascade(ctx, intentID, policy, newViews, cascade); err != nil {
		abortCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = e.store.AbortIntent(abortCtx, intentID, err, nil)
		cancel()
		return model.CascadeReport{}, err
	}
	return cascade, nil
}

func (e *Engine) Read(ctx context.Context, subjectID, path string, observedVersion int64) (ReadResult, error) {
	if observedVersion < 0 {
		return ReadResult{}, apperr.New(apperr.SpecInvalid, "observed_version must be >= 0")
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	h, err := e.store.Head(ctx)
	if err != nil {
		return ReadResult{}, err
	}
	policy, err := e.store.LoadPolicy(ctx, h.Version)
	if err != nil {
		return ReadResult{}, err
	}
	if policy == nil {
		return ReadResult{}, apperr.New(apperr.NotFound, "no committed policy")
	}
	if _, ok := model.SubjectMap(policy)[subjectID]; !ok {
		return ReadResult{}, apperr.With(apperr.NotFound, "subject not found", map[string]any{"subject_id": subjectID})
	}
	if _, ok := model.PathMap(policy)[path]; !ok {
		return ReadResult{}, apperr.With(apperr.NotFound, "managed path not found", map[string]any{"path": path})
	}
	view, err := e.store.GetView(ctx, h.Version, subjectID, path)
	if err != nil {
		return ReadResult{}, err
	}
	if view == nil {
		return ReadResult{}, apperr.New(apperr.Internal, "effective view is missing")
	}
	if view.Version < observedVersion {
		return ReadResult{}, apperr.With(apperr.StaleRead, "effective view has not reached the reader's observed policy version", map[string]any{
			"view_version": view.Version, "observed_version": observedVersion,
			"subject_id": subjectID, "path": path,
		})
	}
	return ReadResult{
		SubjectID: subjectID, Path: path, ObservedVersion: observedVersion,
		UsedVersion: view.Version, Safe: true, View: *view,
	}, nil
}

func (e *Engine) Verify(ctx context.Context, options model.VerificationOptions) (model.VerificationReport, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	h, err := e.store.Head(ctx)
	if err != nil {
		return model.VerificationReport{}, err
	}
	policy, err := e.store.LoadPolicy(ctx, h.Version)
	if err != nil {
		return model.VerificationReport{}, err
	}
	if policy == nil {
		return model.VerificationReport{}, apperr.New(apperr.NotFound, "no committed policy")
	}
	views, err := e.store.LoadViews(ctx, h.Version)
	if err != nil {
		return model.VerificationReport{}, err
	}
	maxConcurrency := options.MaxConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = e.cfg.MaxConcurrency
	}
	if maxConcurrency > e.cfg.MaxConcurrency {
		maxConcurrency = e.cfg.MaxConcurrency
	}
	queueSize := options.QueueSize
	if queueSize <= 0 {
		queueSize = e.cfg.QueueSize
	}
	if queueSize > e.cfg.QueueSize {
		queueSize = e.cfg.QueueSize
	}
	budget := options.Budget
	if budget <= 0 {
		budget = e.cfg.VerifyBudget
	}
	if budget > e.cfg.VerifyBudget {
		budget = e.cfg.VerifyBudget
	}
	tasks, err := e.verificationTasks(policy, options)
	if err != nil {
		return model.VerificationReport{}, err
	}
	capacity := maxConcurrency + queueSize
	if len(tasks) > capacity {
		return model.VerificationReport{}, apperr.With(apperr.VerifyQueueFull, "verification queue capacity is exhausted", map[string]any{
			"tasks": len(tasks), "max_concurrency": maxConcurrency,
			"queue_size": queueSize, "capacity": capacity, "version": h.Version,
		})
	}
	if err := e.fs.KernelVerificationAvailable(); err != nil {
		return model.VerificationReport{}, err
	}
	intentID := newID("verify")
	intent := store.Intent{ID: intentID, Kind: "verify", BaseVersion: h.Version, TargetVersion: h.Version,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		PayloadJSON: mustJSON(map[string]any{"version": h.Version, "tasks": len(tasks), "budget_ms": budget.Milliseconds()})}
	if err := e.store.StartIntent(ctx, intent); err != nil {
		return model.VerificationReport{}, err
	}
	runID := "verify-" + shortID(intentID)
	report := e.runVerification(ctx, runID, h, policy, views, tasks, maxConcurrency, queueSize, budget)
	if err := e.store.CommitVerification(ctx, intentID, report); err != nil {
		abortCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = e.store.AbortIntent(abortCtx, intentID, err, nil)
		cancel()
		return model.VerificationReport{}, err
	}
	if report.Mismatches > 0 {
		return report, apperr.With(apperr.ApplyMismatch, "kernel open results disagree with effective policy views", map[string]any{
			"run_id": report.RunID, "mismatches": report.Mismatches, "version": report.Version,
		})
	}
	return report, nil
}

type taskResult struct {
	task     model.VerifyTask
	expected string
	result   model.OpenResult
}

func (e *Engine) runVerification(parent context.Context, runID string, h store.Head, policy *model.Policy, views []model.EffectiveView, tasks []model.VerifyTask, maxConcurrency, queueSize int, budget time.Duration) model.VerificationReport {
	deadline := time.Now().Add(budget)
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	viewMap := map[string]model.EffectiveView{}
	kindMap := map[string]string{}
	for _, view := range views {
		viewMap[view.SubjectID+"\x00"+view.Path] = view
	}
	for _, managed := range policy.Paths {
		kindMap[managed.Path] = managed.Kind
	}
	jobs := make(chan model.VerifyTask, queueSize)
	results := make(chan taskResult, len(tasks))
	var skippedMu sync.Mutex
	var skipped []model.SkippedTask
	var workers sync.WaitGroup
	for i := 0; i < maxConcurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for task := range jobs {
				if !time.Now().Before(deadline) || ctx.Err() != nil {
					skippedMu.Lock()
					skipped = append(skipped, model.SkippedTask{SubjectID: task.SubjectID, Path: task.Path, Priority: task.Priority, Reason: "time_budget"})
					skippedMu.Unlock()
					continue
				}
				view := viewMap[task.SubjectID+"\x00"+task.Path]
				expectedRights := model.Rights{Read: view.Rights.Read, Write: view.Rights.Write}
				expected := "denied"
				if (task.Mode == "read" && expectedRights.Read) || (task.Mode == "write" && expectedRights.Write) {
					expected = "allowed"
				}
				actual, kernelDenied, engineName, identity, openErr := e.fs.Open(ctx, h.TreePath, task.Path, kindMap[task.Path], task.SubjectID, task.Mode)
				if openErr != nil && ctx.Err() != nil {
					skippedMu.Lock()
					skipped = append(skipped, model.SkippedTask{SubjectID: task.SubjectID, Path: task.Path, Priority: task.Priority, Reason: "time_budget"})
					skippedMu.Unlock()
					continue
				}
				result := model.OpenResult{
					SubjectID: task.SubjectID, Path: task.Path, OpenMode: task.Mode,
					Requested: task.Mode, Actual: actual, KernelDenied: kernelDenied,
					Expected: expected, Identity: identity, Engine: engineName,
				}
				if openErr != nil {
					result.Error = openErr.Error()
				}
				result.Match = actual == expected
				results <- taskResult{task: task, expected: expected, result: result}
			}
		}()
	}
	dispatched := 0
	for i, task := range tasks {
		if !time.Now().Before(deadline) || ctx.Err() != nil {
			for _, remaining := range tasks[i:] {
				skippedMu.Lock()
				skipped = append(skipped, model.SkippedTask{SubjectID: remaining.SubjectID, Path: remaining.Path, Priority: remaining.Priority, Reason: "time_budget"})
				skippedMu.Unlock()
			}
			break
		}
		select {
		case jobs <- task:
			dispatched++
		case <-ctx.Done():
			for _, remaining := range tasks[i:] {
				skippedMu.Lock()
				skipped = append(skipped, model.SkippedTask{SubjectID: remaining.SubjectID, Path: remaining.Path, Priority: remaining.Priority, Reason: "time_budget"})
				skippedMu.Unlock()
			}
			i = len(tasks) - 1
		}
	}
	close(jobs)
	workers.Wait()
	close(results)
	var collected []taskResult
	for result := range results {
		collected = append(collected, result)
	}
	sort.Slice(collected, func(i, j int) bool {
		a, b := collected[i].task, collected[j].task
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		if a.SubjectID != b.SubjectID {
			return a.SubjectID < b.SubjectID
		}
		if a.Path != b.Path {
			return a.Path < b.Path
		}
		return a.Mode < b.Mode
	})
	report := model.VerificationReport{
		RunID: runID, Version: h.Version, BudgetMS: budget.Milliseconds(),
		MaxConcurrency: maxConcurrency, QueueSize: queueSize,
		Results: make([]model.OpenResult, 0, len(collected)), Skipped: skipped,
	}
	for _, item := range collected {
		report.Results = append(report.Results, item.result)
		if !item.result.Match {
			report.Mismatches++
		}
	}
	sort.Slice(report.Skipped, func(i, j int) bool {
		a, b := report.Skipped[i], report.Skipped[j]
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		if a.SubjectID != b.SubjectID {
			return a.SubjectID < b.SubjectID
		}
		return a.Path < b.Path
	})
	report.Degraded = len(report.Skipped) > 0
	switch {
	case report.Mismatches > 0:
		report.Status = "mismatch"
	case report.Degraded:
		report.Status = "degraded"
	default:
		report.Status = "complete"
	}
	return report
}

func (e *Engine) verificationTasks(policy *model.Policy, options model.VerificationOptions) ([]model.VerifyTask, error) {
	subjectFilter := map[string]bool{}
	for _, id := range options.SubjectIDs {
		subjectFilter[id] = true
	}
	pathFilter := map[string]bool{}
	for _, path := range options.Paths {
		pathFilter[path] = true
	}
	subjects := model.SubjectMap(policy)
	paths := model.PathMap(policy)
	for id := range subjectFilter {
		if _, ok := subjects[id]; !ok {
			return nil, apperr.With(apperr.NotFound, "verification subject not found", map[string]any{"subject_id": id})
		}
	}
	for path := range pathFilter {
		if _, ok := paths[path]; !ok {
			return nil, apperr.With(apperr.NotFound, "verification path not found", map[string]any{"path": path})
		}
	}
	subjectIDs := make([]string, 0, len(policy.Subjects))
	for _, subject := range policy.Subjects {
		if len(subjectFilter) > 0 && !subjectFilter[subject.ID] {
			continue
		}
		subjectIDs = append(subjectIDs, subject.ID)
	}
	pathIDs := make([]string, 0, len(policy.Paths))
	for _, managed := range policy.Paths {
		if len(pathFilter) > 0 && !pathFilter[managed.Path] {
			continue
		}
		pathIDs = append(pathIDs, managed.Path)
	}
	sort.Slice(subjectIDs, func(i, j int) bool {
		a, b := subjects[subjectIDs[i]], subjects[subjectIDs[j]]
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		return a.ID < b.ID
	})
	sort.Strings(pathIDs)
	tasks := make([]model.VerifyTask, 0, len(subjectIDs)*len(pathIDs)*2)
	for _, subjectID := range subjectIDs {
		for _, path := range pathIDs {
			tasks = append(tasks,
				model.VerifyTask{SubjectID: subjectID, Path: path, Mode: "read", Priority: subjects[subjectID].Priority},
				model.VerifyTask{SubjectID: subjectID, Path: path, Mode: "write", Priority: subjects[subjectID].Priority},
			)
		}
	}
	return tasks, nil
}

func (e *Engine) Recover(ctx context.Context) (RecoveryReport, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	started := time.Now().UTC()
	report := RecoveryReport{
		ID: "recovery-" + started.Format("20060102T150405.000000000Z"), StartedAt: started.Format(time.RFC3339Nano),
		PendingIntents: []RecoveryIntent{}, OrphanTreesRemoved: []string{}, Facts: []string{},
	}
	h, err := e.store.Head(ctx)
	if err != nil {
		return report, err
	}
	report.LastCompleteVersion = h.Version
	report.LastCompleteTree = h.TreePath
	pending, err := e.store.PendingIntents(ctx)
	if err != nil {
		return report, err
	}
	needsRematerialize := false
	var policy *model.Policy
	var views []model.EffectiveView
	if h.Version > 0 {
		policy, err = e.store.LoadPolicy(ctx, h.Version)
		if err != nil || policy == nil {
			return report, fmt.Errorf("load last complete policy: %w", err)
		}
		views, err = e.store.LoadViews(ctx, h.Version)
		if err != nil {
			return report, err
		}
	}
	for _, intent := range pending {
		action := "abort_pending_intent"
		evidence := "intent remained pending; no matching committed version or verification run exists"
		if (intent.Kind == "snapshot" || intent.Kind == "increment") && h.Version == intent.TargetVersion {
			action = "record_committed_evidence"
			evidence = "policy head already references the intent target version"
			if err := e.store.MarkRecoveryCommittedEvidence(ctx, intent.ID, evidence); err != nil {
				return report, err
			}
		} else {
			if err := e.store.MarkRecoveryAborted(ctx, intent.ID, "aborted by startup recovery: "+evidence); err != nil {
				return report, err
			}
			if intent.Kind == "snapshot" || intent.Kind == "increment" || intent.Kind == "apply" {
				needsRematerialize = true
			}
		}
		report.PendingIntents = append(report.PendingIntents, RecoveryIntent{
			ID: intent.ID, Kind: intent.Kind, BaseVersion: intent.BaseVersion,
			TargetVersion: intent.TargetVersion, CreatedAt: intent.CreatedAt, Action: action, Evidence: evidence,
		})
		report.Facts = append(report.Facts, fmt.Sprintf("intent %s (%s) found pending at startup; action=%s", intent.ID, intent.Kind, action))
	}
	if h.Version > 0 {
		if _, statErr := os.Stat(h.TreePath); statErr != nil {
			needsRematerialize = true
			report.Facts = append(report.Facts, fmt.Sprintf("committed tree missing: %s", h.TreePath))
		} else {
			report.Facts = append(report.Facts, fmt.Sprintf("committed tree exists on disk: %s", h.TreePath))
		}
		if needsRematerialize {
			recoveryIntent := "recovery-" + shortID(newID("tree"))
			finalName := fmt.Sprintf("v%06d-recovered-%s", h.Version, shortID(recoveryIntent))
			_, tree, err := e.fs.Materialize(ctx, policy, views, recoveryIntent, finalName)
			if err != nil {
				report.Status = "failed"
				report.Failure = err.Error()
				report.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
				_ = e.store.SaveRecoveryReport(context.Background(), report.ID, report)
				return report, &apperr.Error{Code: apperr.RecoveryRequired, Message: "failed to rematerialize last complete policy", Context: map[string]any{"report": report, "error": err.Error()}}
			}
			if err := e.store.UpdateTreePath(ctx, h.Version, tree); err != nil {
				return report, err
			}
			report.LastCompleteTree = tree
			report.Facts = append(report.Facts, fmt.Sprintf("rematerialized last complete version %d at %s", h.Version, tree))
		}
	} else {
		report.Facts = append(report.Facts, "no committed policy exists; only pending intents were closed")
	}
	referenced := map[string]bool{}
	versions, err := e.store.ListVersions(ctx)
	if err != nil {
		return report, err
	}
	for _, version := range versions {
		if version.TreePath != "" {
			referenced[filepath.Clean(version.TreePath)] = true
		}
	}
	if report.LastCompleteTree != "" {
		referenced[filepath.Clean(report.LastCompleteTree)] = true
	}
	treeRoot := filepath.Join(e.fs.Root, "trees")
	entries, err := os.ReadDir(treeRoot)
	if err != nil && !os.IsNotExist(err) {
		return report, err
	}
	for _, entry := range entries {
		abs := filepath.Join(treeRoot, entry.Name())
		if referenced[filepath.Clean(abs)] {
			continue
		}
		if err := os.RemoveAll(abs); err != nil {
			return report, err
		}
		report.OrphanTreesRemoved = append(report.OrphanTreesRemoved, abs)
		report.Facts = append(report.Facts, "removed unreferenced tree "+abs)
	}
	sort.Strings(report.OrphanTreesRemoved)
	report.Status = "complete"
	report.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := e.store.SaveRecoveryReport(ctx, report.ID, report); err != nil {
		return report, err
	}
	return report, nil
}

func (e *Engine) LatestRecovery(ctx context.Context) (json.RawMessage, error) {
	return e.store.LatestRecoveryReport(ctx)
}

func (e *Engine) maybeCrash() {
	if e.cfg.CrashAfterMaterialize {
		fmt.Fprintln(os.Stderr, "WORKBENCH_CRASH_AFTER_MATERIALIZE: terminating before policy transaction commit")
		os.Exit(137)
	}
}

func newID(prefix string) string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return prefix + "-" + hex.EncodeToString(b[:])
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[len(id)-8:]
	}
	return id
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func CleanupRoot(root string) error {
	root = filepath.Clean(root)
	if root == "/" || root == "." || root == "" {
		return fmt.Errorf("refusing to clean unsafe root %q", root)
	}
	if _, err := os.Stat(filepath.Join(root, ".workbench-root.json")); err != nil {
		return fmt.Errorf("root is not a workbench root: %w", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == "trees" || entry.Name() == ".workbench-root.json" {
			if err := os.RemoveAll(filepath.Join(root, entry.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func SummarizeViews(views []model.EffectiveView) string {
	lines := make([]string, 0, len(views))
	for _, view := range views {
		lines = append(lines, fmt.Sprintf("%s %-23s read=%-5t write=%-5t", view.SubjectID, view.Path, view.Rights.Read, view.Rights.Write))
	}
	return strings.Join(lines, "\n")
}

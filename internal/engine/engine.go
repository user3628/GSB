package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"local/windowengine/internal/domain"
	"local/windowengine/internal/store"
)

type Budgets struct {
	GlobalConcurrency int   `json:"global_concurrency"`
	TenantConcurrency int   `json:"tenant_concurrency"`
	TimeBudgetMS      int64 `json:"time_budget_ms"`
	QueueCapacity     int   `json:"queue_capacity"`
	TaskDelayMS       int64 `json:"task_delay_ms,omitempty"`
}

type TriggerOptions struct {
	SnapshotID              string   `json:"snapshot_id"`
	Budgets                 Budgets  `json:"budgets"`
	TenantIDs               []string `json:"tenant_ids,omitempty"`
	SpecIDs                 []string `json:"spec_ids,omitempty"`
	GroupKeys               []string `json:"group_keys,omitempty"`
	Force                   bool     `json:"force,omitempty"`
	ExpectedSnapshotVersion *int64   `json:"expected_snapshot_version,omitempty"`
}

type TriggerResult struct {
	Round      store.Round          `json:"round"`
	Tasks      []store.Task         `json:"tasks"`
	Trace      []store.TraceEvent   `json:"trace"`
	Shed       []store.ShedDecision `json:"shed_decisions"`
	NoOp       bool                 `json:"no_op"`
	ElapsedMS  int64                `json:"elapsed_wall_ms"`
	TaskErrors []*domain.Error      `json:"task_errors,omitempty"`
}

type Engine struct {
	store         *store.Store
	roundMu       sync.Mutex
	tenantLocksMu sync.Mutex
	tenantLocks   map[string]*sync.Mutex
	hardMaxGlobal int
}

type taskExecution struct {
	task store.Task
	err  error
}

func New(database *store.Store, hardMaxGlobal int) *Engine {
	if hardMaxGlobal <= 0 {
		hardMaxGlobal = 256
	}
	return &Engine{
		store: database, tenantLocks: make(map[string]*sync.Mutex), hardMaxGlobal: hardMaxGlobal,
	}
}

func (e *Engine) Store() *store.Store {
	return e.store
}

func (e *Engine) LoadSnapshot(ctx context.Context, raw []byte) (*store.LoadResult, *domain.Snapshot, error) {
	snapshot, parseErr := domain.ParseSnapshot(raw)
	if parseErr != nil {
		return nil, nil, parseErr
	}
	windows, planErr := domain.PlanWindows(snapshot)
	if planErr != nil {
		return nil, nil, planErr
	}
	result, err := e.store.LoadSnapshot(ctx, snapshot, windows)
	if err != nil {
		return nil, nil, err
	}
	return result, snapshot, nil
}

func (e *Engine) Recover(ctx context.Context) (store.RecoveryReport, error) {
	return e.store.Recover(ctx)
}

func (e *Engine) Trigger(ctx context.Context, options TriggerOptions) (*TriggerResult, error) {
	if err := validateBudgets(options.Budgets); err != nil {
		return nil, err
	}
	if options.Budgets.GlobalConcurrency > e.hardMaxGlobal {
		return nil, domain.NewError(domain.CodeBadRequest, "global_concurrency exceeds engine hard limit", map[string]any{
			"requested": options.Budgets.GlobalConcurrency, "hard_max_global_concurrency": e.hardMaxGlobal,
		})
	}

	e.roundMu.Lock()
	defer e.roundMu.Unlock()

	snapshotID := options.SnapshotID
	if snapshotID == "" {
		latest, err := e.store.LatestSnapshotID(ctx)
		if err != nil {
			return nil, err
		}
		snapshotID = latest
	}
	record, snapshotErr := e.store.SnapshotRecord(ctx, snapshotID)
	if snapshotErr != nil {
		return nil, snapshotErr
	}
	if options.ExpectedSnapshotVersion != nil && *options.ExpectedSnapshotVersion != record.LoadVersion {
		return nil, domain.NewError(domain.CodeStaleVersion, "snapshot load version is stale", map[string]any{
			"snapshot_id": snapshotID, "expected_version": *options.ExpectedSnapshotVersion,
			"current_version": record.LoadVersion,
		})
	}
	snapshot, snapshotErr := e.store.LoadSnapshotData(ctx, snapshotID)
	if snapshotErr != nil {
		return nil, snapshotErr
	}
	allWindows, _, err := e.store.WindowsAndSpecs(ctx, snapshotID)
	if err != nil {
		if business, ok := domain.BusinessError(err); ok {
			return nil, business
		}
		return nil, domain.NewError(domain.CodeInternal, "load windows failed", map[string]any{"reason": err.Error()})
	}
	statuses, err := e.store.WindowStatuses(ctx, snapshotID)
	if err != nil {
		return nil, domain.NewError(domain.CodeInternal, "load window statuses failed", map[string]any{"reason": err.Error()})
	}
	tenants := make(map[string]domain.Tenant, len(snapshot.Tenants))
	for _, tenant := range snapshot.Tenants {
		tenants[tenant.ID] = tenant
	}
	tenantFilter := stringSet(options.TenantIDs)
	specFilter := stringSet(options.SpecIDs)
	groupFilter := stringSet(options.GroupKeys)

	candidates := make([]domain.Window, 0)
	for _, window := range allWindows {
		status := statuses[window.WindowID]
		if !options.Force && status != "PENDING" && status != "SHED" && status != "FAILED" {
			continue
		}
		if len(tenantFilter) > 0 && !tenantFilter[window.TenantID] {
			continue
		}
		if len(specFilter) > 0 && !specFilter[window.SpecID] {
			continue
		}
		if len(groupFilter) > 0 && !groupFilter[window.GroupKey] {
			continue
		}
		candidates = append(candidates, window)
	}
	sort.Slice(candidates, func(i, j int) bool {
		left, right := candidates[i], candidates[j]
		if tenants[left.TenantID].Priority != tenants[right.TenantID].Priority {
			return tenants[left.TenantID].Priority < tenants[right.TenantID].Priority
		}
		if left.StartMS != right.StartMS {
			return left.StartMS < right.StartMS
		}
		if left.TenantID != right.TenantID {
			return left.TenantID < right.TenantID
		}
		if left.SpecID != right.SpecID {
			return left.SpecID < right.SpecID
		}
		if left.GroupKey != right.GroupKey {
			return left.GroupKey < right.GroupKey
		}
		return left.WindowID < right.WindowID
	})

	roundID := newRoundID()
	startedAt := time.Now()
	round := store.Round{
		RoundID: roundID, SnapshotID: snapshotID, Status: "RUNNING",
		GlobalConcurrency: options.Budgets.GlobalConcurrency,
		TenantConcurrency: options.Budgets.TenantConcurrency,
		TimeBudgetMS:      options.Budgets.TimeBudgetMS, QueueCapacity: options.Budgets.QueueCapacity,
		TaskDelayMS: options.Budgets.TaskDelayMS, ForceRecalc: options.Force,
		ConsideredCount: len(candidates), RemainingBudgetMS: options.Budgets.TimeBudgetMS,
		BudgetsJSON: mustJSON(options.Budgets), StartedAtMS: startedAt.UnixMilli(),
	}
	tasks := make([]store.Task, 0, len(candidates))
	for _, window := range candidates {
		tasks = append(tasks, store.Task{
			TaskID:  newTaskID(roundID, window.WindowID, window.GroupKey),
			RoundID: roundID, SnapshotID: snapshotID, TenantID: window.TenantID,
			Priority: tenants[window.TenantID].Priority, WindowID: window.WindowID,
			GroupKey: window.GroupKey, SpecID: window.SpecID, WindowType: string(window.WindowType),
			StartMS: window.StartMS, EndMS: window.EndMS, CostMS: taskCost(window),
		})
	}
	selectedCount, consumed := budgetPrefix(tasks, options.Budgets.TimeBudgetMS)
	waiting := selectedCount - min(options.Budgets.GlobalConcurrency, selectedCount)
	if waiting > options.Budgets.QueueCapacity {
		return nil, domain.NewError(domain.CodeRecalcQueueFull, "bounded recalculation wait queue is full", map[string]any{
			"snapshot_id": snapshotID, "candidate_tasks": len(candidates),
			"budget_selected_tasks": selectedCount,
			"global_concurrency":    options.Budgets.GlobalConcurrency,
			"queue_capacity":        options.Budgets.QueueCapacity,
			"would_wait":            waiting,
		})
	}
	if err := e.store.CreateRound(ctx, round, tasks); err != nil {
		return nil, err
	}

	result := &TriggerResult{
		Round: round, Tasks: tasks, Trace: []store.TraceEvent{}, Shed: []store.ShedDecision{},
		NoOp: len(tasks) == 0,
	}
	var traceSequence int64
	trace := func(event store.TraceEvent) {
		traceSequence++
		if err := e.store.RecordTrace(context.Background(), roundID, traceSequence, event); err != nil {
			// Trace failure never corrupts domain state. It is surfaced in the
			// final task error list if all other work succeeded.
			result.TaskErrors = append(result.TaskErrors, domain.NewError(domain.CodeInternal, "write scheduler trace failed", map[string]any{
				"round_id": roundID, "reason": err.Error(),
			}))
		}
	}
	trace(store.TraceEvent{
		EventType: "ROUND_START", ConsumedBeforeMS: 0, ConsumedAfterMS: 0,
		RemainingBudgetMS: options.Budgets.TimeBudgetMS,
		Reason:            "priority ascending, window start ascending", AtMS: startedAt.UnixMilli(),
		Detail: map[string]any{
			"candidate_tasks": len(tasks), "queue_capacity": options.Budgets.QueueCapacity,
			"global_concurrency": options.Budgets.GlobalConcurrency,
			"tenant_concurrency": options.Budgets.TenantConcurrency,
			"time_budget_ms":     options.Budgets.TimeBudgetMS,
		},
	})

	selected := tasks[:selectedCount]
	shedTasks := tasks[selectedCount:]
	remaining := options.Budgets.TimeBudgetMS - consumed
	round.ConsumedBudgetMS = consumed
	round.RemainingBudgetMS = remaining
	trace(store.TraceEvent{
		EventType: "BUDGET_CUTOFF", ConsumedBeforeMS: 0, ConsumedAfterMS: consumed,
		RemainingBudgetMS: remaining, Reason: "deterministic accounted prefix",
		Detail: map[string]any{
			"selected_tasks": len(selected), "shed_tasks": len(shedTasks),
			"accounted_cost_ms": consumed, "time_budget_ms": options.Budgets.TimeBudgetMS,
			"rule": "tenant priority ascending, window start ascending; first task that exceeds remaining budget defines the SHED suffix",
		},
	})

	// Degradation order is the reverse of reservation order: lowest priority
	// and latest window start are shed first.
	reversed := append([]store.Task(nil), shedTasks...)
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}
	for index, task := range reversed {
		basis := map[string]any{
			"rule":      "reverse reservation order: highest numeric priority first, then latest window start",
			"tenant_id": task.TenantID, "priority": task.Priority,
			"window_start_ms": task.StartMS, "window_id": task.WindowID,
			"group_key": task.GroupKey, "task_cost_ms": task.CostMS,
			"consumed_budget_ms": consumed, "remaining_budget_ms": remaining,
			"time_budget_ms":   options.Budgets.TimeBudgetMS,
			"reservation_rank": selectedCount + len(shedTasks) - index,
			"decision_order":   index + 1,
		}
		decision := store.ShedDecision{
			RoundID: roundID, TaskID: task.TaskID, SnapshotID: snapshotID,
			TenantID: task.TenantID, WindowID: task.WindowID, GroupKey: task.GroupKey,
			Priority: task.Priority, WindowStartMS: task.StartMS, DecisionOrder: index + 1,
			ReasonCode: "BUDGET_EXHAUSTED", Basis: basis,
			ConsumedBudgetMS: consumed, RemainingBudgetMS: remaining, CreatedAtMS: time.Now().UnixMilli(),
		}
		if err := e.store.ShedTask(context.Background(), task, decision); err != nil {
			round.Status = "FAILED"
			round.ErrorJSON = mustJSON(err)
			round.FinishedAtMS = time.Now().UnixMilli()
			_ = e.store.FinishRound(context.Background(), round)
			return result, err
		}
		result.Shed = append(result.Shed, decision)
		trace(store.TraceEvent{
			EventType: "SHED", TenantID: task.TenantID, WindowID: task.WindowID,
			TaskID: task.TaskID, GroupKey: task.GroupKey, Priority: task.Priority,
			CostMS: task.CostMS, ConsumedBeforeMS: consumed, ConsumedAfterMS: consumed,
			RemainingBudgetMS: remaining, Reason: "BUDGET_EXHAUSTED", Detail: basis,
		})
	}

	eventsByTenantGroup := make(map[string][]domain.Event)
	for _, event := range snapshot.Events {
		key := event.TenantID + "\x1f" + event.GroupKey
		eventsByTenantGroup[key] = append(eventsByTenantGroup[key], event)
	}

	done := make(chan taskExecution, len(selected))
	pending := append([]store.Task(nil), selected...)
	tenantRunning := make(map[string]int)
	running := 0
	accounted := int64(0)

	for len(pending) > 0 || running > 0 {
		launched := false
		for len(pending) > 0 && running < options.Budgets.GlobalConcurrency {
			head := pending[0]
			if tenantRunning[head.TenantID] >= options.Budgets.TenantConcurrency {
				break
			}
			pending = pending[1:]
			if err := e.store.StartTask(context.Background(), head); err != nil {
				business := coerceBusiness(err)
				_ = e.store.FailTask(context.Background(), head.TaskID, business)
				result.TaskErrors = append(result.TaskErrors, business)
				result.Round.FailedCount++
				trace(store.TraceEvent{
					EventType: "START_FAILED", TenantID: head.TenantID, WindowID: head.WindowID,
					TaskID: head.TaskID, GroupKey: head.GroupKey, Priority: head.Priority,
					CostMS: head.CostMS, ConsumedBeforeMS: accounted, ConsumedAfterMS: accounted,
					RemainingBudgetMS: options.Budgets.TimeBudgetMS - accounted,
					Reason:            string(business.Code), Detail: business.Context,
				})
				continue
			}
			before := accounted
			accounted += head.CostMS
			tenantRunning[head.TenantID]++
			running++
			if running > round.MaxGlobalRunning {
				round.MaxGlobalRunning = running
			}
			if tenantRunning[head.TenantID] > round.MaxTenantRunning {
				round.MaxTenantRunning = tenantRunning[head.TenantID]
			}
			launched = true
			result.Round.DispatchedCount++
			trace(store.TraceEvent{
				EventType: "DISPATCH", TenantID: head.TenantID, WindowID: head.WindowID,
				TaskID: head.TaskID, GroupKey: head.GroupKey, Priority: head.Priority,
				CostMS: head.CostMS, ConsumedBeforeMS: before, ConsumedAfterMS: accounted,
				RemainingBudgetMS: options.Budgets.TimeBudgetMS - accounted,
				Reason:            "start intent committed before execution",
			})
			task := head
			events := eventsByTenantGroup[task.TenantID+"\x1f"+task.GroupKey]
			go func() {
				done <- e.executeTask(task, events, options.Budgets.TaskDelayMS)
			}()
		}
		if running == 0 {
			if len(pending) > 0 {
				// This should be unreachable because global and tenant limits are positive.
				for _, task := range pending {
					result.TaskErrors = append(result.TaskErrors, domain.NewError(domain.CodeInternal, "scheduler stalled with pending tasks", map[string]any{
						"task_id": task.TaskID,
					}))
				}
				break
			}
			break
		}
		if !launched || len(pending) == 0 || running >= options.Budgets.GlobalConcurrency {
			finished := <-done
			running--
			tenantRunning[finished.task.TenantID]--
			if finished.err != nil {
				business := coerceBusiness(finished.err)
				_ = e.store.FailTask(context.Background(), finished.task.TaskID, business)
				result.TaskErrors = append(result.TaskErrors, business)
				result.Round.FailedCount++
				trace(store.TraceEvent{
					EventType: "FAILED", TenantID: finished.task.TenantID, WindowID: finished.task.WindowID,
					TaskID: finished.task.TaskID, GroupKey: finished.task.GroupKey,
					Priority: finished.task.Priority, CostMS: finished.task.CostMS,
					ConsumedBeforeMS: accounted - finished.task.CostMS, ConsumedAfterMS: accounted,
					RemainingBudgetMS: options.Budgets.TimeBudgetMS - accounted,
					Reason:            string(business.Code), Detail: business.Context,
				})
			} else {
				result.Round.CommittedCount++
				trace(store.TraceEvent{
					EventType: "COMMITTED", TenantID: finished.task.TenantID, WindowID: finished.task.WindowID,
					TaskID: finished.task.TaskID, GroupKey: finished.task.GroupKey,
					Priority: finished.task.Priority, CostMS: finished.task.CostMS,
					ConsumedBeforeMS: accounted - finished.task.CostMS, ConsumedAfterMS: accounted,
					RemainingBudgetMS: options.Budgets.TimeBudgetMS - accounted, Reason: "atomic tenant commit",
				})
			}
		}
	}

	round = result.Round
	round.Status = "COMPLETED"
	round.ConsumedBudgetMS = accounted
	round.RemainingBudgetMS = options.Budgets.TimeBudgetMS - accounted
	round.ShedCount = len(shedTasks)
	round.FinishedAtMS = time.Now().UnixMilli()
	if len(result.TaskErrors) > 0 {
		round.Status = "COMPLETED_WITH_ERRORS"
		round.ErrorJSON = mustJSON(result.TaskErrors)
	}
	if err := e.store.FinishRound(context.Background(), round); err != nil {
		return result, domain.NewError(domain.CodeInternal, "finish round failed", map[string]any{"reason": err.Error()})
	}
	result.Round = round
	result.ElapsedMS = time.Since(startedAt).Milliseconds()
	traceSequence++
	_ = e.store.RecordTrace(context.Background(), roundID, traceSequence, store.TraceEvent{
		EventType: "ROUND_FINISH", ConsumedBeforeMS: accounted, ConsumedAfterMS: accounted,
		RemainingBudgetMS: round.RemainingBudgetMS, Reason: round.Status,
		AtMS: time.Now().UnixMilli(),
	})
	finalTasks, err := e.store.TasksForRound(context.Background(), roundID)
	if err != nil {
		return result, domain.NewError(domain.CodeInternal, "load final tasks failed", map[string]any{"reason": err.Error()})
	}
	result.Tasks = finalTasks
	result.Trace, _ = e.store.Trace(context.Background(), roundID)
	result.Shed, _ = e.store.ShedDecisions(context.Background(), roundID)
	if len(result.TaskErrors) > 0 {
		first := result.TaskErrors[0]
		return result, domain.WithContext(first, map[string]any{"round_id": roundID})
	}
	return result, nil
}

func (e *Engine) executeTask(task store.Task, events []domain.Event, delayMS int64) taskExecution {
	var result taskExecution
	result.task = task
	if delayMS > 0 {
		time.Sleep(time.Duration(delayMS) * time.Millisecond)
	}
	window := domain.Window{
		SnapshotID: task.SnapshotID, WindowID: task.WindowID, TenantID: task.TenantID,
		SpecID: task.SpecID, GroupKey: task.GroupKey, StartMS: task.StartMS,
		EndMS: task.EndMS, Boundary: domain.BoundaryLeftClosedRightOpen,
		WindowType: domain.WindowType(task.WindowType),
	}
	aggregate, aggregateErr := domain.AggregateWindow(window, events)
	if aggregateErr != nil {
		result.err = aggregateErr
		return result
	}
	attemptID, err := e.store.StageAggregate(context.Background(), task.TaskID, aggregate)
	if err != nil {
		result.err = err
		return result
	}
	if err := e.store.PrepareCommitIntent(context.Background(), task.TaskID, attemptID, aggregate); err != nil {
		result.err = err
		return result
	}
	lock := e.tenantLock(task.TenantID)
	lock.Lock()
	_, err = e.store.CommitTask(context.Background(), task.TaskID, attemptID, aggregate)
	lock.Unlock()
	if err != nil {
		result.err = err
		return result
	}
	return result
}

func (e *Engine) tenantLock(tenantID string) *sync.Mutex {
	e.tenantLocksMu.Lock()
	defer e.tenantLocksMu.Unlock()
	lock := e.tenantLocks[tenantID]
	if lock == nil {
		lock = &sync.Mutex{}
		e.tenantLocks[tenantID] = lock
	}
	return lock
}

func validateBudgets(budgets Budgets) error {
	if budgets.GlobalConcurrency <= 0 {
		return domain.NewError(domain.CodeBadRequest, "global_concurrency must be positive", map[string]any{
			"global_concurrency": budgets.GlobalConcurrency,
		})
	}
	if budgets.TenantConcurrency <= 0 {
		return domain.NewError(domain.CodeBadRequest, "tenant_concurrency must be positive", map[string]any{
			"tenant_concurrency": budgets.TenantConcurrency,
		})
	}
	if budgets.TenantConcurrency > budgets.GlobalConcurrency {
		return domain.NewError(domain.CodeBadRequest, "tenant_concurrency cannot exceed global_concurrency", map[string]any{
			"tenant_concurrency": budgets.TenantConcurrency,
			"global_concurrency": budgets.GlobalConcurrency,
		})
	}
	if budgets.TimeBudgetMS < 0 {
		return domain.NewError(domain.CodeBadRequest, "time_budget_ms must be non-negative", map[string]any{
			"time_budget_ms": budgets.TimeBudgetMS,
		})
	}
	if budgets.QueueCapacity < 0 {
		return domain.NewError(domain.CodeBadRequest, "queue_capacity must be non-negative", map[string]any{
			"queue_capacity": budgets.QueueCapacity,
		})
	}
	if budgets.TaskDelayMS < 0 || budgets.TaskDelayMS > 60_000 {
		return domain.NewError(domain.CodeBadRequest, "task_delay_ms must be between 0 and 60000", map[string]any{
			"task_delay_ms": budgets.TaskDelayMS,
		})
	}
	return nil
}

func taskCost(window domain.Window) int64 {
	if window.EventCount < 1 {
		return 1
	}
	return int64(window.EventCount)
}

func budgetPrefix(tasks []store.Task, budget int64) (int, int64) {
	var consumed int64
	for index, task := range tasks {
		if task.CostMS > budget-consumed {
			return index, consumed
		}
		consumed += task.CostMS
	}
	return len(tasks), consumed
}

func stringSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			set[value] = true
		}
	}
	return set
}

func coerceBusiness(err error) *domain.Error {
	var business *domain.Error
	if errors.As(err, &business) {
		return business
	}
	return domain.NewError(domain.CodeInternal, err.Error(), nil)
}

func mustJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(data)
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func newRoundID() string {
	return fmt.Sprintf("round_%d_%d", time.Now().UnixMilli(), time.Now().UnixNano()%1_000_000)
}

func newTaskID(roundID, windowID, groupKey string) string {
	raw := roundID + "\x1f" + windowID + "\x1f" + groupKey + "\x1f" + fmt.Sprint(atomic.AddUint64(&taskCounter, 1))
	sum := sha256.Sum256([]byte(raw))
	return "t_" + hex.EncodeToString(sum[:12])
}

var taskCounter uint64

package engine

import (
	"context"
	"sort"
	"sync"

	"alarmengine/internal/errcode"
	"alarmengine/internal/model"
)

// PatrolRound executes one bounded, budgeted patrol round.
//
// Concurrency: at most policy.ConcurrencyLimit workers run simultaneously;
// overflow tasks enter a bounded waiting queue and E_PATROL_QUEUE_FULL is
// returned when the queue is full (no silent skipping).
//
// Budget: each task costs a deterministic number of milliseconds derived from
// its service weight. When the remaining budget cannot cover a task, the
// task is degraded in a deterministic order: sensitivity tier (HIGH before
// MEDIUM before LOW), then zone lexicographic rotation, then service id.
func (e *Engine) PatrolRound(ctx context.Context, roundID string) (model.PatrolTrace, error) {
	policy := e.Snapshot()
	tasks := e.buildPatrolTasks()
	trace := model.PatrolTrace{
		RoundID: roundID, ConcurrencyLimit: policy.ConcurrencyLimit,
		QueueCapacity: e.cfg.PatrolQueueCapacity, BudgetMS: policy.RoundBudgetMS,
		RemainingBudget: policy.RoundBudgetMS,
	}
	for _, t := range tasks {
		trace.Enqueued = append(trace.Enqueued, t.ServiceID)
	}

	// Deterministic budget degradation before dispatch: cheapest/most-sensitive
	// tasks are kept; the tail is marked degraded with explicit remaining budget.
	ordered := orderForDegradation(tasks)
	budget := policy.RoundBudgetMS
	var runnable, degraded []model.PatrolTask
	for _, t := range ordered {
		cost := taskCostMS(t)
		if cost <= budget {
			budget -= cost
			runnable = append(runnable, t)
		} else {
			degraded = append(degraded, t)
		}
	}
	for _, t := range degraded {
		trace.Degraded = append(trace.Degraded, t.ServiceID)
	}
	trace.RemainingBudget = budget

	// Bounded queue admission with explicit failure on saturation.
	sem := make(chan struct{}, policy.ConcurrencyLimit)
	queue := make(chan patrolEnvelope, e.cfg.PatrolQueueCapacity)
	type done struct {
		id  string
		err error
	}
	doneCh := make(chan done, len(runnable))

	// Producer fills the bounded queue; full queue is a hard error.
	if e.cfg.PatrolQueueCapacity <= 0 {
		return model.PatrolTrace{}, errcode.New(errcode.EPatrolQueueFull,
			"patrol queue capacity %d exhausted", e.cfg.PatrolQueueCapacity)
	}
	for _, t := range runnable {
		env := patrolEnvelope{ctx: ctx, task: t, ch: make(chan patrolResult, 1)}
		select {
		case queue <- env:
		default:
			return model.PatrolTrace{}, errcode.New(errcode.EPatrolQueueFull,
				"patrol queue capacity %d exhausted", e.cfg.PatrolQueueCapacity)
		}
	}
	close(queue)

	// Fixed, self-contained worker pool sized by the policy concurrency limit.
	var roundWg sync.WaitGroup
	for w := 0; w < policy.ConcurrencyLimit; w++ {
		roundWg.Add(1)
		go func() {
			defer roundWg.Done()
			for env := range queue {
				select {
				case sem <- struct{}{}:
				case <-env.ctx.Done():
					doneCh <- done{id: env.task.ServiceID, err: env.ctx.Err()}
					continue
				}
				res := patrolWork(env.task)
				<-sem
				env.ch <- res
				doneCh <- done{id: env.task.ServiceID}
			}
		}()
	}
	roundWg.Wait()
	close(doneCh)

	completedSet := map[string]bool{}
	for d := range doneCh {
		if d.err == nil {
			completedSet[d.id] = true
		}
	}
	for _, t := range runnable {
		if completedSet[t.ServiceID] {
			trace.Completed = append(trace.Completed, t.ServiceID)
		}
	}
	sort.Strings(trace.Completed)
	return trace, nil
}

// SubmitPatrol enqueues a single task on the engine-level bounded queue used
// by the long-running worker pool and returns E_PATROL_QUEUE_FULL when full.
func (e *Engine) SubmitPatrol(ctx context.Context, task model.PatrolTask) (patrolResult, error) {
	env := patrolEnvelope{ctx: ctx, task: task, ch: make(chan patrolResult, 1)}
	select {
	case e.patrolQ <- env:
	default:
		return patrolResult{}, errcode.New(errcode.EPatrolQueueFull,
			"engine patrol queue capacity %d full", e.cfg.PatrolQueueCapacity)
	}
	select {
	case res := <-env.ch:
		return res, nil
	case <-ctx.Done():
		return patrolResult{}, ctx.Err()
	}
}

func (e *Engine) startPatrolWorkers() {
	policy := e.Snapshot()
	n := policy.ConcurrencyLimit
	if n <= 0 {
		n = 1
	}
	for w := 0; w < n; w++ {
		e.patrolWg.Add(1)
		go func() {
			defer e.patrolWg.Done()
			for env := range e.patrolQ {
				select {
				case <-env.ctx.Done():
					env.ch <- patrolResult{task: env.task, cost: 0}
				default:
					env.ch <- patrolWork(env.task)
				}
			}
		}()
	}
}

// Close stops background workers.
func (e *Engine) Close() {
	close(e.patrolQ)
	e.patrolWg.Wait()
}

func (e *Engine) buildPatrolTasks() []model.PatrolTask {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]model.PatrolTask, 0, len(e.services))
	for _, s := range e.services {
		out = append(out, model.PatrolTask{
			ServiceID: s.ID, Zone: s.Zone, Sensitivity: s.Sensitivity, Weight: s.WeightPerMille,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ServiceID < out[j].ServiceID })
	return out
}

// orderForDegradation defines the deterministic degradation priority:
// sensitivity tier (HIGH=1 first), then zone lexicographic (rotation anchor),
// then service id. Tasks kept longest come first.
func orderForDegradation(tasks []model.PatrolTask) []model.PatrolTask {
	out := append([]model.PatrolTask(nil), tasks...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Sensitivity != out[j].Sensitivity {
			return out[i].Sensitivity < out[j].Sensitivity
		}
		if out[i].Zone != out[j].Zone {
			return out[i].Zone < out[j].Zone
		}
		return out[i].ServiceID < out[j].ServiceID
	})
	return out
}

// taskCostMS maps the per-mille weight to a deterministic integer cost in
// [1,10] milliseconds.
func taskCostMS(t model.PatrolTask) int64 {
	cost := 1 + t.Weight/100
	if cost > 10 {
		cost = 10
	}
	return cost
}

func patrolWork(t model.PatrolTask) patrolResult {
	return patrolResult{task: t, cost: taskCostMS(t)}
}

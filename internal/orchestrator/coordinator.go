package orchestrator

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"leasearbiter/internal/core"
)

type Config struct {
	Concurrency   int
	QueueCapacity int
	Budget        time.Duration
}

type TaskResult struct {
	HolderID       string
	Deferred       bool
	DeferredReason string
	Error          *core.BizError
	Heal           *core.HealExecution
	Rebuild        *core.RebuildResult
}

type Task struct {
	HolderID         string
	DriftMagnitudeMS int64
	Run              func(context.Context) TaskResult
}

type Outcome struct {
	Completed         []TaskResult
	Deferred          []core.DeferredEntry
	RemainingBudgetMS int64
	Err               *core.BizError
}

// Coordinator bounds admitted work globally. Waiting tasks occupy the bounded
// queue; once both worker slots and queue slots are full, a whole new batch is
// rejected before any holder starts.
type Coordinator struct {
	config      Config
	sem         chan struct{}
	mu          sync.Mutex
	admitted    int
	maxInflight int
	now         func() time.Time
}

func New(config Config) (*Coordinator, error) {
	if config.Concurrency <= 0 {
		return nil, fmt.Errorf("concurrency must be positive")
	}
	if config.QueueCapacity < 0 {
		return nil, fmt.Errorf("queue capacity must be non-negative")
	}
	if config.Budget <= 0 {
		return nil, fmt.Errorf("budget must be positive")
	}
	return &Coordinator{
		config:      config,
		sem:         make(chan struct{}, config.Concurrency),
		maxInflight: config.Concurrency + config.QueueCapacity,
		now:         time.Now,
	}, nil
}

func (c *Coordinator) Config() Config {
	return c.config
}

func (c *Coordinator) Execute(ctx context.Context, tasks []Task) Outcome {
	outcome := Outcome{Completed: []TaskResult{}, Deferred: []core.DeferredEntry{}}
	if len(tasks) == 0 {
		outcome.RemainingBudgetMS = c.config.Budget.Milliseconds()
		return outcome
	}
	if !c.reserve(len(tasks)) {
		outcome.Err = core.Biz(core.CodeQueueFull, fmt.Sprintf("bounded queue is full: concurrency=%d queue_capacity=%d", c.config.Concurrency, c.config.QueueCapacity))
		return outcome
	}

	start := c.now()
	batchDeadline := start.Add(c.config.Budget)
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(batchDeadline) {
		batchDeadline = parentDeadline
	}
	jobCtx, cancel := context.WithDeadline(ctx, batchDeadline)
	defer cancel()

	results := make([]TaskResult, len(tasks))
	var wg sync.WaitGroup
	wg.Add(len(tasks))
	for i := range tasks {
		i := i
		task := tasks[i]
		go func() {
			defer wg.Done()
			defer c.release()
			if err := jobCtx.Err(); err != nil {
				results[i] = deferredTask(task, budgetDeferredReason(err))
				return
			}
			timer := time.NewTimer(time.Until(batchDeadline))
			defer timer.Stop()
			select {
			case c.sem <- struct{}{}:
				defer func() { <-c.sem }()
			case <-jobCtx.Done():
				results[i] = deferredTask(task, budgetDeferredReason(jobCtx.Err()))
				return
			case <-timer.C:
				results[i] = deferredTask(task, "ROUND_BUDGET_EXHAUSTED")
				return
			}
			if jobCtx.Err() != nil {
				results[i] = deferredTask(task, budgetDeferredReason(jobCtx.Err()))
				return
			}
			result := task.Run(jobCtx)
			if result.HolderID == "" {
				result.HolderID = task.HolderID
			}
			if result.Deferred && result.DeferredReason == "" {
				result.DeferredReason = "ROUND_BUDGET_EXHAUSTED"
			}
			results[i] = result
		}()
	}
	wg.Wait()

	remaining := time.Until(batchDeadline)
	if remaining < 0 {
		remaining = 0
	}
	outcome.RemainingBudgetMS = remaining.Milliseconds()
	for _, result := range results {
		if result.Deferred {
			outcome.Deferred = append(outcome.Deferred, core.DeferredEntry{
				HolderID:          result.HolderID,
				DriftMagnitudeMS:  tasksMagnitude(tasks, result.HolderID),
				Reason:            result.DeferredReason,
				RemainingBudgetMS: outcome.RemainingBudgetMS,
			})
			continue
		}
		outcome.Completed = append(outcome.Completed, result)
	}
	core.SortDeferred(outcome.Deferred)
	sort.SliceStable(outcome.Completed, func(i, j int) bool {
		return outcome.Completed[i].HolderID < outcome.Completed[j].HolderID
	})
	return outcome
}

func (c *Coordinator) reserve(count int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.admitted+count > c.maxInflight {
		return false
	}
	c.admitted += count
	return true
}

func (c *Coordinator) release() {
	c.mu.Lock()
	c.admitted--
	c.mu.Unlock()
}

func deferredTask(task Task, reason string) TaskResult {
	return TaskResult{HolderID: task.HolderID, Deferred: true, DeferredReason: reason}
}

func budgetDeferredReason(err error) string {
	if err == context.DeadlineExceeded {
		return "ROUND_BUDGET_EXHAUSTED"
	}
	return "ROUND_CANCELED"
}

func tasksMagnitude(tasks []Task, holderID string) int64 {
	for _, task := range tasks {
		if task.HolderID == holderID {
			return task.DriftMagnitudeMS
		}
	}
	return 0
}

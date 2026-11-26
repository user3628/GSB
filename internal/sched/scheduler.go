package sched

import (
	"context"
	"sort"
	"sync"

	"logagg/internal/errs"
)

type Task struct {
	ID           string `json:"id"`
	SessionID    string `json:"session_id"`
	ShardID      string `json:"shard_id"`
	Version      int64  `json:"version"`
	Partition    int    `json:"partition"`
	BlockNo      int    `json:"block_no"`
	SegmentStart int    `json:"segment_start"`
	SegmentEnd   int    `json:"segment_end"`
	Bytes        int64  `json:"bytes"`
	BlockStarted bool   `json:"block_started"`
}

type PlannedTask struct {
	Task
	Admission string `json:"admission"`
}

type DeferredTask struct {
	Task
	Reason           string `json:"reason"`
	ContinueFromRow  int    `json:"continue_from_row"`
	RemainingBudget  int64  `json:"remaining_budget"`
	StableOrderIndex int    `json:"stable_order_index"`
}

type RoundPlan struct {
	MaxConcurrency      int            `json:"max_concurrency"`
	ActiveCount         int            `json:"active_count"`
	QueueCapacity       int            `json:"queue_capacity"`
	ByteBudget          int64          `json:"byte_budget"`
	RemainingBudget     int64          `json:"remaining_budget"`
	Run                 []PlannedTask  `json:"run"`
	Queued              []PlannedTask  `json:"queued"`
	Deferred            []DeferredTask `json:"deferred"`
	MaxTaskBytes        int64          `json:"max_task_bytes"`
	ConcurrentPeakBytes int64          `json:"concurrent_peak_bytes"`
}

func PlanRound(tasks []Task, activeCount, maxConcurrency, queueCapacity int, byteBudget int64) (*RoundPlan, error) {
	if maxConcurrency <= 0 {
		return nil, errs.New(errs.EInvalidRequest, "max concurrency must be positive")
	}
	if queueCapacity < 0 {
		return nil, errs.New(errs.EInvalidRequest, "queue capacity must be non-negative")
	}
	if activeCount < 0 || activeCount > maxConcurrency {
		return nil, errs.New(errs.EInvalidRequest, "active count %d outside [0,%d]", activeCount, maxConcurrency)
	}
	if byteBudget < 0 {
		return nil, errs.New(errs.EInvalidRequest, "byte budget must be non-negative")
	}
	for _, task := range tasks {
		if task.Bytes < 0 {
			return nil, errs.New(errs.EInvalidRequest, "task %s has negative byte estimate", task.ID)
		}
	}

	ordered := append([]Task(nil), tasks...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Partition != ordered[j].Partition {
			return ordered[i].Partition < ordered[j].Partition
		}
		if ordered[i].BlockNo != ordered[j].BlockNo {
			return ordered[i].BlockNo < ordered[j].BlockNo
		}
		if ordered[i].SegmentStart != ordered[j].SegmentStart {
			return ordered[i].SegmentStart < ordered[j].SegmentStart
		}
		return ordered[i].ID < ordered[j].ID
	})

	plan := &RoundPlan{
		MaxConcurrency:  maxConcurrency,
		ActiveCount:     activeCount,
		QueueCapacity:   queueCapacity,
		ByteBudget:      byteBudget,
		RemainingBudget: byteBudget,
		Run:             []PlannedTask{},
		Queued:          []PlannedTask{},
		Deferred:        []DeferredTask{},
	}

	for index, task := range ordered {
		if task.Bytes <= plan.RemainingBudget {
			plan.RemainingBudget -= task.Bytes
			if task.Bytes > plan.MaxTaskBytes {
				plan.MaxTaskBytes = task.Bytes
			}
			continue
		}
		for deferredIndex := index; deferredIndex < len(ordered); deferredIndex++ {
			deferredTask := ordered[deferredIndex]
			plan.Deferred = append(plan.Deferred, DeferredTask{
				Task:             deferredTask,
				Reason:           "byte_budget_exhausted",
				ContinueFromRow:  deferredTask.SegmentStart,
				RemainingBudget:  plan.RemainingBudget,
				StableOrderIndex: deferredIndex,
			})
		}
		break
	}

	availableSlots := maxConcurrency - activeCount
	selectedCount := len(ordered) - len(plan.Deferred)
	for index := 0; index < selectedCount; index++ {
		task := ordered[index]
		planned := PlannedTask{Task: task}
		if index < availableSlots {
			planned.Admission = "run"
			plan.Run = append(plan.Run, planned)
		} else {
			planned.Admission = "queued"
			plan.Queued = append(plan.Queued, planned)
		}
	}
	if len(plan.Queued) > queueCapacity {
		coded := errs.New(errs.EDecodeQueueFull, "bounded decode queue requires %d waiting slots but capacity is %d", len(plan.Queued), queueCapacity)
		coded.WithDetail("queued_tasks", len(plan.Queued)).
			WithDetail("queue_capacity", queueCapacity).
			WithDetail("max_concurrency", maxConcurrency).
			WithDetail("active_count", activeCount)
		return plan, coded
	}
	peakConcurrency := len(plan.Run)
	if peakConcurrency > maxConcurrency {
		peakConcurrency = maxConcurrency
	}
	plan.ConcurrentPeakBytes = int64(peakConcurrency) * plan.MaxTaskBytes
	return plan, nil
}

type Job struct {
	Task Task
	Run  func(context.Context) error
}

type Pool struct {
	concurrency   int
	queueCapacity int

	mu        sync.Mutex
	active    int
	maxActive int
	queued    []Job
	closed    bool
	wg        sync.WaitGroup
}

func NewPool(concurrency, queueCapacity int) *Pool {
	if concurrency <= 0 {
		concurrency = 1
	}
	if queueCapacity < 0 {
		queueCapacity = 0
	}
	return &Pool{concurrency: concurrency, queueCapacity: queueCapacity}
}

func (p *Pool) Submit(ctx context.Context, job Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errs.New(errs.EInternal, "decode pool is closed")
	}
	if p.active < p.concurrency {
		p.active++
		if p.active > p.maxActive {
			p.maxActive = p.active
		}
		p.wg.Add(1)
		go p.run(job)
		return nil
	}
	if len(p.queued) >= p.queueCapacity {
		return errs.New(errs.EDecodeQueueFull, "decode queue capacity %d is full", p.queueCapacity).
			WithDetail("queue_capacity", p.queueCapacity).
			WithDetail("active", p.active).
			WithDetail("queued", len(p.queued))
	}
	p.queued = append(p.queued, job)
	return nil
}

func (p *Pool) run(first Job) {
	defer p.wg.Done()
	job := first
	for {
		if job.Run != nil {
			_ = job.Run(context.Background())
		}
		p.mu.Lock()
		if len(p.queued) > 0 {
			job = p.queued[0]
			p.queued = p.queued[1:]
			p.mu.Unlock()
			continue
		}
		p.active--
		p.mu.Unlock()
		return
	}
}

func (p *Pool) Active() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.active
}

func (p *Pool) MaxActive() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.maxActive
}

func (p *Pool) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.wg.Wait()
}

package convergence

import (
	"context"
	"sync"
	"sync/atomic"
)

type scheduledJob struct {
	id     string
	ctx    context.Context
	fn     func(context.Context) (*BatchApplyResult, error)
	done   chan struct{}
	result *BatchApplyResult
	err    error
}

type Scheduler struct {
	maxParallel   int
	queueCapacity int
	sem           chan struct{}
	queue         chan *scheduledJob
	admissionMu   sync.Mutex
	held          atomic.Int64
	closeOnce     sync.Once
}

func NewScheduler(maxParallel, queueCapacity int) *Scheduler {
	if maxParallel <= 0 {
		maxParallel = 1
	}
	if queueCapacity < 0 {
		queueCapacity = 0
	}
	scheduler := &Scheduler{
		maxParallel:   maxParallel,
		queueCapacity: queueCapacity,
		sem:           make(chan struct{}, maxParallel),
		queue:         make(chan *scheduledJob, queueCapacity),
	}
	go scheduler.dispatch()
	return scheduler
}

func (s *Scheduler) dispatch() {
	for job := range s.queue {
		s.held.Add(1)
		select {
		case s.sem <- struct{}{}:
			s.held.Add(-1)
			go s.run(job)
		case <-job.ctx.Done():
			s.held.Add(-1)
			job.err = WrapError(CodeRoundBudget, "queued batch did not start before context ended", job.ctx.Err(), map[string]any{"batch_id": job.id})
			close(job.done)
		}
	}
}

// Submit either starts immediately, places the job in the bounded FIFO queue, or returns E_WRITE_QUEUE_FULL.
func (s *Scheduler) Submit(ctx context.Context, batchID string, fn func(context.Context) (*BatchApplyResult, error)) (*scheduledJob, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	job := &scheduledJob{id: batchID, ctx: ctx, fn: fn, done: make(chan struct{})}
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	select {
	case s.sem <- struct{}{}:
		go s.run(job)
		return job, nil
	default:
	}
	waiting := len(s.queue) + int(s.held.Load())
	if waiting >= s.queueCapacity {
		return nil, NewError(CodeWriteQueueFull, "write queue is full", map[string]any{
			"batch_id":       batchID,
			"max_parallel":   s.maxParallel,
			"queue_capacity": s.queueCapacity,
			"active":         s.Active(),
			"queued":         waiting,
		})
	}
	select {
	case s.queue <- job:
		return job, nil
	default:
		return nil, NewError(CodeWriteQueueFull, "write queue is full", map[string]any{
			"batch_id":       batchID,
			"max_parallel":   s.maxParallel,
			"queue_capacity": s.queueCapacity,
			"active":         s.Active(),
			"queued":         s.Queued(),
		})
	}
}

func (s *Scheduler) run(job *scheduledJob) {
	defer func() { <-s.sem }()
	defer close(job.done)
	defer func() {
		if recovered := recover(); recovered != nil {
			job.err = NewError(CodeWriteFailed, "batch execution panicked", map[string]any{"batch_id": job.id, "panic": recovered})
		}
	}()
	job.result, job.err = job.fn(job.ctx)
}

func (s *Scheduler) Wait(ctx context.Context, job *scheduledJob) (*BatchApplyResult, error) {
	if job == nil {
		return nil, NewError(CodeNotFound, "scheduled job is nil", nil)
	}
	select {
	case <-job.done:
		return job.result, job.err
	case <-ctx.Done():
		return nil, WrapError(CodeRoundBudget, "waiting for batch failed", ctx.Err(), map[string]any{"batch_id": job.id})
	}
}

func (s *Scheduler) Active() int {
	return len(s.sem)
}

func (s *Scheduler) Queued() int {
	return len(s.queue) + int(s.held.Load())
}

func (s *Scheduler) Close() {
	s.closeOnce.Do(func() { close(s.queue) })
}

package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
)

type Job struct {
	Index int
	ID    string
	Run   func(context.Context) error
}

type Stats struct {
	Limit        int  `json:"limit"`
	InFlight     int  `json:"in_flight"`
	Queued       int  `json:"queued"`
	Completed    int  `json:"completed"`
	Failed       int  `json:"failed"`
	PeakInFlight int  `json:"peak_in_flight"`
	PeakQueued   int  `json:"peak_queued"`
	Saturated    bool `json:"saturated"`
}

type Scheduler struct {
	limit      int
	inFlight   atomic.Int64
	pending    atomic.Int64
	completed  atomic.Int64
	failed     atomic.Int64
	peak       atomic.Int64
	peakQueued atomic.Int64
}

func New(limit int) *Scheduler {
	if limit < 1 {
		limit = 1
	}
	return &Scheduler{limit: limit}
}

func (s *Scheduler) Limit() int { return s.limit }

func (s *Scheduler) Stats() Stats {
	inFlight := int(s.inFlight.Load())
	queued := int(s.pending.Load()) - inFlight
	if queued < 0 {
		queued = 0
	}
	return Stats{
		Limit:        s.limit,
		InFlight:     inFlight,
		Queued:       queued,
		Completed:    int(s.completed.Load()),
		Failed:       int(s.failed.Load()),
		PeakInFlight: int(s.peak.Load()),
		PeakQueued:   int(s.peakQueued.Load()),
		Saturated:    inFlight >= s.limit,
	}
}

// Run executes at most limit jobs concurrently. Jobs beyond the limit wait in
// the queue channel; they are never rejected solely because the scheduler is
// saturated.
func (s *Scheduler) Run(ctx context.Context, jobs []Job) []error {
	size := len(jobs)
	for _, job := range jobs {
		if job.Index+1 > size {
			size = job.Index + 1
		}
	}
	errs := make([]error, size)
	if len(jobs) == 0 {
		return errs
	}
	queue := make(chan Job, len(jobs))
	for _, job := range jobs {
		s.pending.Add(1)
		s.observeQueue()
		queue <- job
	}
	close(queue)

	var wg sync.WaitGroup
	var errMu sync.Mutex
	workers := s.limit
	if workers > len(jobs) {
		workers = len(jobs)
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range queue {
				s.pending.Add(-1)
				s.observeQueue()
				if ctx.Err() != nil {
					errMu.Lock()
					errs[job.Index] = ctx.Err()
					errMu.Unlock()
					s.failed.Add(1)
					continue
				}
				current := s.inFlight.Add(1)
				for {
					peak := s.peak.Load()
					if current <= peak || s.peak.CompareAndSwap(peak, current) {
						break
					}
				}
				err := runSafely(ctx, job)
				s.inFlight.Add(-1)
				if err != nil {
					s.failed.Add(1)
				} else {
					s.completed.Add(1)
				}
				errMu.Lock()
				errs[job.Index] = err
				errMu.Unlock()
			}
		}()
	}
	wg.Wait()
	return errs
}

func (s *Scheduler) observeQueue() {
	queued := s.pending.Load() - s.inFlight.Load()
	if queued < 0 {
		queued = 0
	}
	for {
		peak := s.peakQueued.Load()
		if queued <= peak || s.peakQueued.CompareAndSwap(peak, queued) {
			break
		}
	}
}

func runSafely(ctx context.Context, job Job) (err error) {
	defer func() {
		if panicValue := recover(); panicValue != nil {
			err = &panicError{value: panicValue}
		}
	}()
	if job.Run == nil {
		return nil
	}
	return job.Run(ctx)
}

type panicError struct{ value any }

func (e *panicError) Error() string {
	return "scheduler job panicked"
}

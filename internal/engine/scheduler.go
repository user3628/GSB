package engine

import (
	"sync"

	"driftengine/internal/errs"
)

type Scheduler struct {
	mu        sync.Mutex
	maxActive int
	maxQueue  int
	active    int
	queue     []*scheduledJob
}

type scheduledJob struct {
	id  string
	run func()
}

func NewScheduler(maxActive, maxQueue int) *Scheduler {
	if maxActive <= 0 {
		maxActive = 1
	}
	if maxQueue < 0 {
		maxQueue = 0
	}
	return &Scheduler{maxActive: maxActive, maxQueue: maxQueue}
}

func (s *Scheduler) Submit(id string, run func()) error {
	s.mu.Lock()
	if s.active < s.maxActive {
		s.active++
		s.mu.Unlock()
		go s.execute(&scheduledJob{id: id, run: run})
		return nil
	}
	if len(s.queue) >= s.maxQueue {
		s.mu.Unlock()
		return errs.New(errs.WriteQueueFull, "bounded write queue is full", map[string]any{
			"job":          id,
			"max_parallel": s.maxActive,
			"queue_size":   s.maxQueue,
			"queued":       len(s.queue),
		})
	}
	s.queue = append(s.queue, &scheduledJob{id: id, run: run})
	s.mu.Unlock()
	return nil
}

func (s *Scheduler) execute(job *scheduledJob) {
	defer s.finishOne()
	job.run()
}

func (s *Scheduler) finishOne() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
	if len(s.queue) == 0 {
		return
	}
	next := s.queue[0]
	s.queue = s.queue[1:]
	s.active++
	go s.execute(next)
}

func (s *Scheduler) Stats() (active, queued int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active, len(s.queue)
}

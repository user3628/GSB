package source

import (
	"context"
	"sync"
	"sync/atomic"

	"svcworkbench/internal/domain"
)

type BatchMetrics struct {
	MaxConcurrency      int `json:"max_concurrency"`
	ObservedMaxInFlight int `json:"observed_max_in_flight"`
	QueuedSources       int `json:"queued_sources"`
	CompletedSources    int `json:"completed_sources"`
}

type Scheduler struct {
	maxConcurrency int
	inFlight       atomic.Int64
	observedMax    atomic.Int64
}

func NewScheduler(maxConcurrency int) *Scheduler {
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}
	return &Scheduler{maxConcurrency: maxConcurrency}
}

func (s *Scheduler) InFlight() int {
	return int(s.inFlight.Load())
}

func (s *Scheduler) ParseAll(ctx context.Context, inputs []Input, options Options) ([]domain.SourceResult, BatchMetrics) {
	results := make([]domain.SourceResult, len(inputs))
	metrics := BatchMetrics{MaxConcurrency: s.maxConcurrency}
	if len(inputs) == 0 {
		return results, metrics
	}
	workers := s.maxConcurrency
	if workers > len(inputs) {
		workers = len(inputs)
	}
	metrics.QueuedSources = len(inputs) - workers
	jobs := make(chan int, len(inputs))
	for index := range inputs {
		jobs <- index
	}
	close(jobs)
	var workersGroup sync.WaitGroup
	workersGroup.Add(workers)
	var completed atomic.Int64
	var firstStarted atomic.Int64
	var releaseFirst sync.Once
	firstRelease := make(chan struct{})
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer workersGroup.Done()
			first := true
			for index := range jobs {
				current := s.inFlight.Add(1)
				for {
					observed := s.observedMax.Load()
					if current <= observed || s.observedMax.CompareAndSwap(observed, current) {
						break
					}
				}
				if first {
					started := firstStarted.Add(1)
					if int(started) >= workers {
						releaseFirst.Do(func() { close(firstRelease) })
					}
					<-firstRelease
					first = false
				}
				select {
				case <-ctx.Done():
					results[index] = domain.SourceResult{
						SourceID: inputs[index].ID,
						Kind:     inputs[index].Kind,
						Issues: []domain.Issue{{
							Code:       domain.CodeSourceMalformed,
							SourceID:   inputs[index].ID,
							SourceKind: inputs[index].Kind,
							Detail:     ctx.Err().Error(),
						}},
					}
					s.inFlight.Add(-1)
					completed.Add(1)
					continue
				default:
				}
				results[index] = ParseFile(ctx, inputs[index], options)
				s.inFlight.Add(-1)
				completed.Add(1)
			}
		}()
	}
	workersGroup.Wait()
	metrics.ObservedMaxInFlight = int(s.observedMax.Load())
	metrics.CompletedSources = int(completed.Load())
	return results, metrics
}

package dnsdetect

import (
	"context"
	"os"
	"sort"
	"sync"
	"sync/atomic"
)

type SourceResult struct {
	Spec    SourceSpec     `json:"spec"`
	Session *SourceSession `json:"-"`
	Err     error          `json:"-"`
}

type SourceBatch struct {
	Results  []SourceResult   `json:"-"`
	Sessions []*SourceSession `json:"-"`
	Events   []Event          `json:"-"`
	Ledger   ErrorLedger      `json:"-"`
	Stats    SchedulerStats   `json:"stats"`
}

func (b *SourceBatch) FirstError() error {
	for _, result := range b.Results {
		if result.Err != nil {
			return result.Err
		}
	}
	return nil
}

type SchedulerStats struct {
	MaxConcurrency  int   `json:"max_concurrency"`
	Queued          int64 `json:"queued"`
	InFlight        int64 `json:"in_flight"`
	InFlightBytes   int64 `json:"in_flight_bytes"`
	Completed       int64 `json:"completed"`
	Failed          int64 `json:"failed"`
	TotalInputBytes int64 `json:"total_input_bytes"`
}

// Scheduler parses sources in independent sessions. Sources above the
// concurrency limit wait in the queue; they are never rejected for backpressure.
type Scheduler struct {
	maxConcurrency  int
	options         LoadOptions
	queued          atomic.Int64
	inFlight        atomic.Int64
	inFlightBytes   atomic.Int64
	completed       atomic.Int64
	failed          atomic.Int64
	totalInputBytes atomic.Int64
}

func NewScheduler(maxConcurrency int, options LoadOptions) *Scheduler {
	if maxConcurrency <= 0 {
		maxConcurrency = DefaultMaxConcurrency
	}
	return &Scheduler{maxConcurrency: maxConcurrency, options: options.WithDefaults()}
}

func (s *Scheduler) Stats() SchedulerStats {
	return SchedulerStats{
		MaxConcurrency:  s.maxConcurrency,
		Queued:          s.queued.Load(),
		InFlight:        s.inFlight.Load(),
		InFlightBytes:   s.inFlightBytes.Load(),
		Completed:       s.completed.Load(),
		Failed:          s.failed.Load(),
		TotalInputBytes: s.totalInputBytes.Load(),
	}
}

func (s *Scheduler) Load(ctx context.Context, specs []SourceSpec) *SourceBatch {
	type job struct {
		index int
		spec  SourceSpec
	}
	jobs := make(chan job, len(specs))
	results := make([]SourceResult, len(specs))
	var waitGroup sync.WaitGroup
	workerCount := s.maxConcurrency
	if workerCount > len(specs) {
		workerCount = len(specs)
	}
	for worker := 0; worker < workerCount; worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for job := range jobs {
				s.queued.Add(-1)
				s.inFlight.Add(1)
				var size int64
				if info, statErr := os.Stat(job.spec.Path); statErr == nil {
					size = info.Size()
					s.inFlightBytes.Add(size)
				}
				session, err := LoadSource(ctx, job.spec, s.options)
				if size > 0 {
					s.inFlightBytes.Add(-size)
				}
				s.totalInputBytes.Add(size)
				s.inFlight.Add(-1)
				if err != nil {
					s.failed.Add(1)
				} else {
					s.completed.Add(1)
				}
				results[job.index] = SourceResult{Spec: job.spec, Session: session, Err: err}
			}
		}()
	}
	for index, spec := range specs {
		s.queued.Add(1)
		jobs <- job{index: index, spec: spec}
	}
	close(jobs)
	waitGroup.Wait()

	batch := &SourceBatch{Results: results, Stats: s.Stats()}
	for _, result := range results {
		if result.Err != nil {
			continue
		}
		batch.Sessions = append(batch.Sessions, result.Session)
		batch.Events = append(batch.Events, result.Session.Events...)
		batch.Ledger.Entries = append(batch.Ledger.Entries, result.Session.Ledger.Entries...)
	}
	batch.Ledger = MergeLedgers([]ErrorLedger{batch.Ledger})
	sort.Slice(batch.Sessions, func(i, j int) bool { return batch.Sessions[i].Path < batch.Sessions[j].Path })
	return batch
}

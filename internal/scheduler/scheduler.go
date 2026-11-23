package scheduler

import (
	"context"
	"errors"
	"sort"
	"sync"

	"internalpki/workbench/internal/model"
)

// Job is one queued audit unit. Execute receives the deterministic queue
// position and weighted wait ticks so report metering never depends on the
// number of workers or on wall-clock time.
type Job struct {
	Path    string
	Depth   int
	Execute func(ctx context.Context, queuePosition int, waitTicks int64) (model.SampleResult, error)
}

type completed struct {
	result model.SampleResult
	err    error
}

// Run executes jobs through a bounded worker pool. Jobs beyond the concurrency
// limit remain queued in the buffered channel; none are rejected. The job order
// is depth-descending, then path-ascending, with wait metering derived from the
// preceding weighted work.
func Run(ctx context.Context, jobs []Job, concurrency int) ([]model.SampleResult, error) {
	if concurrency < 1 {
		return nil, errors.New("concurrency must be at least 1")
	}
	if len(jobs) == 0 {
		return nil, nil
	}
	ordered := append([]Job(nil), jobs...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Depth == ordered[j].Depth {
			return ordered[i].Path < ordered[j].Path
		}
		return ordered[i].Depth > ordered[j].Depth
	})

	var waitAccumulator int64
	for i := range ordered {
		depth := int64(ordered[i].Depth)
		if depth < 1 {
			depth = 1
		}
		ordered[i].Execute = wrapExecution(ordered[i].Execute, i, waitAccumulator)
		waitAccumulator += depth * depth * 10
	}

	jobQueue := make(chan Job, len(ordered))
	resultQueue := make(chan completed, len(ordered))
	var workers sync.WaitGroup
	workerCount := concurrency
	if workerCount > len(ordered) {
		workerCount = len(ordered)
	}
	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobQueue {
				if err := ctx.Err(); err != nil {
					resultQueue <- completed{err: err}
					continue
				}
				result, err := job.Execute(ctx, 0, 0)
				resultQueue <- completed{result: result, err: err}
			}
		}()
	}
	for _, job := range ordered {
		jobQueue <- job
	}
	close(jobQueue)
	workers.Wait()
	close(resultQueue)

	results := make([]model.SampleResult, 0, len(ordered))
	var firstErr error
	for item := range resultQueue {
		if item.err != nil {
			if firstErr == nil {
				firstErr = item.err
			}
			continue
		}
		results = append(results, item.result)
	}
	if firstErr != nil {
		return results, firstErr
	}
	return results, nil
}

func wrapExecution(execute func(context.Context, int, int64) (model.SampleResult, error), position int, waitTicks int64) func(context.Context, int, int64) (model.SampleResult, error) {
	return func(ctx context.Context, _ int, _ int64) (model.SampleResult, error) {
		return execute(ctx, position, waitTicks)
	}
}

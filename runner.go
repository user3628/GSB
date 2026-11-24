package abeval

import (
	"context"
	"errors"
	"sync"
)

type Runner struct {
	store          *Store
	batchID        string
	writerID       string
	stopAfterStage int

	stopOnce sync.Once
	stopCh   chan struct{}
}

func NewRunner(store *Store, batchID, writerID string) *Runner {
	return &Runner{
		store:    store,
		batchID:  batchID,
		writerID: writerID,
		stopCh:   make(chan struct{}),
	}
}

func NewRunnerWithStopAfterStage(store *Store, batchID, writerID string, stopAfterStage int) *Runner {
	runner := NewRunner(store, batchID, writerID)
	runner.stopAfterStage = stopAfterStage
	return runner
}

func (r *Runner) RequestStop() {
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
}

func (r *Runner) StopChannel() <-chan struct{} {
	return r.stopCh
}

func (r *Runner) Run(ctx context.Context) (*Report, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-r.stopCh:
			status, err := r.store.MarkGracefulStopped(ctx, r.batchID)
			if err != nil {
				return nil, err
			}
			report, err := r.store.BuildReport(ctx, r.batchID)
			if err != nil {
				return nil, err
			}
			_ = status
			return report, ErrGracefulStop
		default:
		}

		checkpoint, err := r.store.AdvanceStage(ctx, r.batchID, r.writerID)
		if err != nil {
			if errors.Is(err, ErrCompleted) {
				return r.store.BuildReport(ctx, r.batchID)
			}
			return nil, err
		}

		if r.stopAfterStage > 0 && checkpoint.StageIndex >= r.stopAfterStage {
			r.RequestStop()
		}
		select {
		case <-r.stopCh:
			if checkpoint.StageIndex > 0 {
				if _, err := r.store.MarkGracefulStopped(ctx, r.batchID); err != nil {
					return nil, err
				}
			}
			report, buildErr := r.store.BuildReport(ctx, r.batchID)
			if buildErr != nil {
				return nil, buildErr
			}
			return report, ErrGracefulStop
		default:
		}
	}
}

type RunOptions struct {
	WriterID       string
	StopAfterStage int
}

func RunBatch(ctx context.Context, store *Store, batchID string, options RunOptions) (*Report, error) {
	runner := NewRunnerWithStopAfterStage(store, batchID, options.WriterID, options.StopAfterStage)
	return runner.Run(ctx)
}

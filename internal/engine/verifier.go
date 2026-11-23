package engine

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"sort"
	"sync"
	"time"

	"local/objectstore/internal/model"
)

type verifyJob struct {
	part     model.Part
	deadline time.Time
	result   chan verifyResult
}

type verifyResult struct {
	partNumber int64
	err        error
	skipped    bool
}

type Verifier struct {
	concurrency   int
	queueCapacity int
	now           func() time.Time
	verifyFn      func(context.Context, model.Part) error

	mu     sync.Mutex
	cond   *sync.Cond
	active int
	queue  []*verifyJob
	closed bool
	wg     sync.WaitGroup
}

func NewVerifier(concurrency, queueCapacity int, now func() time.Time) *Verifier {
	if concurrency < 1 {
		concurrency = 1
	}
	if queueCapacity < 0 {
		queueCapacity = 0
	}
	if now == nil {
		now = time.Now
	}
	verifier := &Verifier{
		concurrency: concurrency, queueCapacity: queueCapacity, now: now, verifyFn: verifyPartContent,
	}
	verifier.cond = sync.NewCond(&verifier.mu)
	return verifier
}

func (v *Verifier) Close() {
	v.mu.Lock()
	v.closed = true
	queued := v.queue
	v.queue = nil
	v.mu.Unlock()
	for _, job := range queued {
		job.result <- verifyResult{partNumber: job.part.PartNumber, err: model.NewError(model.CodeInternal, "part verifier is closed")}
		close(job.result)
	}
	v.cond.Broadcast()
	v.wg.Wait()
}

func (v *Verifier) Submit(ctx context.Context, part model.Part) (<-chan verifyResult, error) {
	return v.submit(ctx, part, time.Time{})
}

func (v *Verifier) submit(ctx context.Context, part model.Part, deadline time.Time) (<-chan verifyResult, error) {
	job := &verifyJob{part: part, deadline: deadline, result: make(chan verifyResult, 1)}
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return nil, model.NewError(model.CodeInternal, "part verifier is closed")
	}
	if v.active < v.concurrency {
		v.active++
		v.mu.Unlock()
		v.launch(ctx, job)
		return job.result, nil
	}
	if len(v.queue) >= v.queueCapacity {
		v.mu.Unlock()
		return nil, model.NewError(model.CodeVerifyQueueFull, "global part verification queue is full").
			WithContext("part_number", part.PartNumber).
			WithContext("concurrency", v.concurrency).
			WithContext("queue_capacity", v.queueCapacity)
	}
	v.queue = append(v.queue, job)
	v.mu.Unlock()
	return job.result, nil
}

func (v *Verifier) launch(ctx context.Context, job *verifyJob) {
	v.wg.Add(1)
	go func() {
		defer v.wg.Done()
		if !job.deadline.IsZero() && !v.now().Before(job.deadline) {
			job.result <- verifyResult{partNumber: job.part.PartNumber, skipped: true}
		} else {
			verifyFn := v.verifyFn
			if verifyFn == nil {
				verifyFn = verifyPartContent
			}
			err := verifyFn(ctx, job.part)
			job.result <- verifyResult{partNumber: job.part.PartNumber, err: err}
		}
		close(job.result)

		v.mu.Lock()
		v.active--
		if !v.closed && len(v.queue) > 0 {
			next := v.queue[0]
			v.queue = v.queue[1:]
			v.active++
			v.mu.Unlock()
			v.launch(ctx, next)
			return
		}
		v.cond.Broadcast()
		v.mu.Unlock()
	}()
}

func verifyPartContent(ctx context.Context, part model.Part) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	sum := sha256.Sum256(part.Content)
	actual := hex.EncodeToString(sum[:])
	if subtle.ConstantTimeCompare([]byte(actual), []byte(model.NormalizeSHA256(part.SHA256))) != 1 {
		return model.NewError(model.CodePartMismatch, "stored part content failed digest verification").
			WithContext("part_number", part.PartNumber).
			WithContext("recorded_sha256", part.SHA256).
			WithContext("actual_sha256", actual)
	}
	return nil
}

type verificationHandle struct {
	partNumber int64
	result     <-chan verifyResult
}

func (v *Verifier) Verify(ctx context.Context, parts []model.Part, budget time.Duration) (model.VerificationReport, error) {
	report := model.VerificationReport{
		Concurrency:         v.concurrency,
		QueueCapacity:       v.queueCapacity,
		BudgetMS:            budget.Milliseconds(),
		VerifiedPartNumbers: []int64{},
		SkippedPartNumbers:  []int64{},
	}
	started := v.now()
	if budget <= 0 {
		for _, part := range parts {
			report.SkippedPartNumbers = append(report.SkippedPartNumbers, part.PartNumber)
		}
		report.Degraded = len(report.SkippedPartNumbers) > 0
		report.DurationMS = v.now().Sub(started).Milliseconds()
		report.Normalize()
		return report, nil
	}
	deadline := started.Add(budget)
	ordered := append([]model.Part(nil), parts...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].PartNumber < ordered[j].PartNumber })

	handles := make([]verificationHandle, 0, len(ordered))
	for index, part := range ordered {
		if !v.now().Before(deadline) {
			for _, skipped := range ordered[index:] {
				report.SkippedPartNumbers = append(report.SkippedPartNumbers, skipped.PartNumber)
			}
			break
		}
		result, err := v.submit(ctx, part, deadline)
		if err != nil {
			report.Degraded = true
			report.DurationMS = v.now().Sub(started).Milliseconds()
			report.Normalize()
			return report, err
		}
		handles = append(handles, verificationHandle{partNumber: part.PartNumber, result: result})
	}

	var failures []*model.Error
	for _, handle := range handles {
		select {
		case <-ctx.Done():
			report.DurationMS = v.now().Sub(started).Milliseconds()
			report.Normalize()
			return report, ctx.Err()
		case result, ok := <-handle.result:
			if !ok {
				continue
			}
			if result.skipped {
				report.SkippedPartNumbers = append(report.SkippedPartNumbers, handle.partNumber)
				continue
			}
			if result.err != nil {
				business := model.AsError(result.err)
				if business.Code == model.CodePartMismatch {
					failures = append(failures, business)
				} else {
					report.DurationMS = v.now().Sub(started).Milliseconds()
					report.Normalize()
					return report, result.err
				}
				continue
			}
			report.VerifiedPartNumbers = append(report.VerifiedPartNumbers, handle.partNumber)
		}
	}
	report.DurationMS = v.now().Sub(started).Milliseconds()
	report.Degraded = len(report.SkippedPartNumbers) > 0
	report.Normalize()
	if len(failures) > 0 {
		failedParts := make([]int64, 0, len(failures))
		details := make([]map[string]any, 0, len(failures))
		for _, failure := range failures {
			if partNumber, ok := failure.Context["part_number"]; ok {
				if number, ok := partNumber.(int64); ok {
					failedParts = append(failedParts, number)
				}
			}
			details = append(details, failure.Context)
		}
		return report, model.NewError(model.CodePartMismatch, "one or more part digests failed verification; merge was aborted").
			WithContext("failed_part_numbers", failedParts).
			WithContext("failures", details)
	}
	return report, nil
}

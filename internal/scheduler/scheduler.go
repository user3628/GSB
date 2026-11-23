// Package scheduler runs adjudication batches in isolated sessions with
// bounded concurrency, a bounded waiting queue and per-batch budgets.
package scheduler

import (
	"fmt"
	"sort"
	"sync"

	"injectctl/internal/metrics"
	"injectctl/internal/model"
	"injectctl/internal/session"
	"injectctl/internal/store"
)

// Config controls scheduler resource limits.
type Config struct {
	MaxConcurrent      int // maximum in-flight batches
	MaxQueueDepth      int // waiting queue capacity
	MaxSamplesPerBatch int // per-batch sample budget
	MaxEncodedBytes    int // per-sample encoded byte limit, <=0 disables
}

// Job is one batch submitted for adjudication.
type Job struct {
	BatchID        string
	ContextVersion string
	Samples        []model.Sample
}

// BatchResult is the terminal outcome of one batch.
type BatchResult struct {
	BatchID     string
	Code        string
	Verdicts    []model.Verdict
	Corrections []session.Correction
	Version     int
	Stats       []metrics.Stats
}

// Scheduler coordinates workers, the waiting queue and snapshot commits.
type Scheduler struct {
	cfg          Config
	store        *store.Store
	recoveryMode bool

	mu         sync.Mutex
	cond       *sync.Cond
	inflight   int
	queue      []Job
	closed     bool
	nextSeq    int
	commitMu   sync.Mutex
	commitCond *sync.Cond
	commitTurn int

	resultsMu sync.Mutex
	results   map[string]BatchResult
}

// New constructs a scheduler backed by the given store.
func New(cfg Config, st *store.Store) *Scheduler {
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 1
	}
	if cfg.MaxQueueDepth < 0 {
		cfg.MaxQueueDepth = 0
	}
	s := &Scheduler{cfg: cfg, store: st, results: map[string]BatchResult{}}
	s.cond = sync.NewCond(&s.mu)
	s.commitCond = sync.NewCond(&s.commitMu)
	return s
}

// SetRecoveryMode forces strict FIFO, single-worker processing so snapshot
// versions follow batch submission order after a forced restart.
func (s *Scheduler) SetRecoveryMode(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recoveryMode = on
}

// Start launches the worker pool.
func (s *Scheduler) Start() {
	workers := s.cfg.MaxConcurrent
	if s.recoveryMode {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		go s.worker(i)
	}
}

// Submit enqueues a job. Over-capacity jobs wait in the queue rather than
// being rejected; only a full queue rejects synchronously.
func (s *Scheduler) Submit(job Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("scheduler closed")
	}
	if len(s.queue) >= s.cfg.MaxQueueDepth && s.cfg.MaxQueueDepth > 0 {
		return fmt.Errorf("queue full")
	}
	s.queue = append(s.queue, job)
	s.cond.Signal()
	return nil
}

// Inflight returns the observable in-flight batch count.
func (s *Scheduler) Inflight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight
}

// beginCommit blocks until this batch reaches its FIFO commit turn.
func (s *Scheduler) beginCommit(seq int) {
	s.commitMu.Lock()
	for s.commitTurn != seq {
		s.commitCond.Wait()
	}
}

// endCommit advances the FIFO turn and wakes the next waiter.
func (s *Scheduler) endCommit() {
	s.commitTurn++
	s.commitCond.Broadcast()
	s.commitMu.Unlock()
}

// Queued returns the observable waiting queue length.
func (s *Scheduler) Queued() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

func (s *Scheduler) worker(id int) {
	for {
		s.mu.Lock()
		for len(s.queue) == 0 && !s.closed {
			s.cond.Wait()
		}
		if s.closed && len(s.queue) == 0 {
			s.mu.Unlock()
			return
		}
		if s.recoveryMode && id != 0 {
			s.mu.Unlock()
			return
		}
		job := s.queue[0]
		s.queue = s.queue[1:]
		s.inflight++
		seq := s.nextSeq
		s.nextSeq++
		s.mu.Unlock()

		result := s.runBatch(job, seq)

		s.resultsMu.Lock()
		s.results[job.BatchID] = result
		s.resultsMu.Unlock()

		s.mu.Lock()
		s.inflight--
		s.cond.Broadcast()
		s.mu.Unlock()
	}
}

func (s *Scheduler) runBatch(job Job, seq int) BatchResult {
	res := BatchResult{BatchID: job.BatchID}

	// Recovery path for a durably committed batch. Persisted verdicts are
	// never recomputed; if the snapshot transaction was lost to a forced
	// kill, it is appended here exactly once, keeping the version chain
	// continuous and restart idempotent.
	if exists, status, err := s.store.BatchExists(job.BatchID); err == nil && exists {
		verdicts, _ := s.store.LoadVerdicts(job.BatchID)
		corrections, _ := s.store.LoadCorrections(job.BatchID)
		res.Code = model.CodeOK
		res.Verdicts = verdicts
		res.Corrections = corrections
		s.beginCommit(seq)
		if status == store.StatusBudget {
			res.Code = model.CodeSampleBudget
			s.endCommit()
			return res
		}
		if snap, ok, _ := s.store.SnapshotForBatch(job.BatchID); ok {
			res.Version = snap.Version
			s.endCommit()
			return res
		}
		chain, _ := s.store.LoadChain()
		runningHash := ""
		for _, sp := range chain.Snapshots {
			runningHash = store.ChainSummaryHash(runningHash, sp.ContentHash)
		}
		snap := store.NewSnapshot(0, job.BatchID, job.ContextVersion, verdicts, corrections, runningHash)
		version, _, err := s.store.AppendSnapshotForBatch(snap)
		if err != nil {
			res.Code = model.CodeSnapshotVersion
			s.endCommit()
			return res
		}
		res.Version = version
		s.endCommit()
		return res
	}

	// Per-batch sample budget: terminate only this batch.
	if s.cfg.MaxSamplesPerBatch > 0 && len(job.Samples) > s.cfg.MaxSamplesPerBatch {
		s.beginCommit(seq)
		res.Code = model.CodeSampleBudget
		_ = s.store.SaveSettlement(job.BatchID, store.StatusBudget, nil, nil)
		s.endCommit()
		return res
	}

	rec := metrics.NewRecorder()
	sess := session.New(job.BatchID, job.ContextVersion, s.cfg.MaxEncodedBytes, rec)

	sort.SliceStable(job.Samples, func(i, j int) bool {
		return job.Samples[i].Seq < job.Samples[j].Seq
	})
	for _, sample := range job.Samples {
		if _, err := sess.AdmitInteractive(sample); err != nil {
			res.Code = model.CodeContextInvalid
			return res
		}
	}

	final, corrections, err := sess.Close(job.Samples)
	if err != nil {
		res.Code = model.CodeContextInvalid
		return res
	}
	verdicts := make([]model.Verdict, 0, len(final))
	for _, v := range final {
		verdicts = append(verdicts, v)
	}
	sort.Slice(verdicts, func(i, j int) bool { return verdicts[i].Seq < verdicts[j].Seq })

	// FIFO commit gate: adjudication runs concurrently, but durable
	// settlement and snapshot commits follow submission order, so snapshot
	// versions and hashes are deterministic across runs.
	s.beginCommit(seq)

	// Single transaction for the settlement commit.
	if err := s.store.SaveSettlement(job.BatchID, store.StatusSettled, verdicts, corrections); err != nil {
		res.Code = model.CodeContextInvalid
		res.Stats = rec.Aggregate()
		s.endCommit()
		return res
	}

	// The snapshot commit computes the running hash and next version
	// atomically inside one SQLite transaction.
	snap := store.NewSnapshot(0, job.BatchID, job.ContextVersion, verdicts, corrections, "")
	nextVersion, err := s.store.SaveSnapshotAtomic(snap)
	if err != nil {
		res.Code = model.CodeSnapshotVersion
		res.Stats = rec.Aggregate()
		s.endCommit()
		return res
	}

	// The snapshot version is the globally ordered, deterministic reason
	// version; stamp every verdict so exported reports do not depend on the
	// local session counter or worker scheduling order.
	for i := range verdicts {
		verdicts[i].ReasonVersion = nextVersion
	}
	res.Code = model.CodeOK
	res.Verdicts = verdicts
	res.Corrections = corrections
	res.Version = nextVersion
	res.Stats = rec.Aggregate()
	s.endCommit()
	return res
}

// Shutdown drains the queue and stops workers after in-flight work finishes.
func (s *Scheduler) Shutdown() {
	s.mu.Lock()
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

// WaitIdle blocks until queue and in-flight work are drained.
func (s *Scheduler) WaitIdle() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.queue) > 0 || s.inflight > 0 {
		s.cond.Wait()
	}
}

// Result returns the terminal result of a batch.
func (s *Scheduler) Result(batchID string) (BatchResult, bool) {
	s.resultsMu.Lock()
	defer s.resultsMu.Unlock()
	r, ok := s.results[batchID]
	return r, ok
}

// AllResults returns results sorted by batch id.
func (s *Scheduler) AllResults() []BatchResult {
	s.resultsMu.Lock()
	defer s.resultsMu.Unlock()
	out := make([]BatchResult, 0, len(s.results))
	for _, r := range s.results {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BatchID < out[j].BatchID })
	return out
}

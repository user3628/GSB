package scheduler

import (
	"context"
	"sort"
	"sync"
	"time"

	"cdn-edge-consistency/internal/model"
)

type TaskKind string

const (
	KindOrigin TaskKind = "origin"
	KindPurge  TaskKind = "purge"
)

type Task struct {
	ID       string
	Kind     TaskKind
	OriginID string
	NodeID   string
	Key      string
	Priority int
	Heat     int64
	CostMs   int64
	Run      func(context.Context) error
}

type SkippedTask struct {
	Order    int      `json:"order"`
	TaskID   string   `json:"task_id"`
	Kind     TaskKind `json:"kind"`
	NodeID   string   `json:"node_id"`
	Key      string   `json:"key"`
	Priority int      `json:"priority"`
	Heat     int64    `json:"heat"`
	CostMs   int64    `json:"cost_ms"`
	Reason   string   `json:"reason"`
}

type RoundReport struct {
	RoundID     int64         `json:"round_id"`
	BudgetMs    int64         `json:"budget_ms"`
	UsedCostMs  int64         `json:"used_cost_ms"`
	Started     []SkippedTask `json:"started"`
	Skipped     []SkippedTask `json:"skipped"`
	Completed   int           `json:"completed"`
	Failed      int           `json:"failed"`
	Determinism string        `json:"determinism"`
}

type Config struct {
	GlobalLimit     int
	PerOriginLimit  int
	QueueCapacity   int
	RoundBudgetMs   int64
	DefaultTaskCost int64
}

type Scheduler struct {
	cfg Config

	mu             sync.Mutex
	queue          []*task
	activeGlobal   int
	activeByOrigin map[string]int
	stopping       bool
	wake           chan struct{}
	completion     chan completed
	ctx            context.Context
	cancel         context.CancelFunc
	done           chan struct{}

	roundID    int64
	roundUsed  int64
	roundStart []SkippedTask
	roundSkip  []SkippedTask
	completedN int
	failedN    int
	lastReport RoundReport
}

type task struct {
	Task
	order  int64
	result chan error
}

type completed struct {
	t      *task
	err    error
	origin string
}

var seqMu sync.Mutex
var taskSeq int64

func nextSeq() int64 {
	seqMu.Lock()
	defer seqMu.Unlock()
	taskSeq++
	return taskSeq
}

func New(cfg Config) *Scheduler {
	if cfg.GlobalLimit <= 0 {
		cfg.GlobalLimit = 1
	}
	if cfg.PerOriginLimit <= 0 || cfg.PerOriginLimit > cfg.GlobalLimit {
		cfg.PerOriginLimit = cfg.GlobalLimit
	}
	if cfg.QueueCapacity < 0 {
		cfg.QueueCapacity = 0
	}
	if cfg.RoundBudgetMs <= 0 {
		cfg.RoundBudgetMs = 1
	}
	if cfg.DefaultTaskCost <= 0 {
		cfg.DefaultTaskCost = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Scheduler{
		cfg:            cfg,
		activeByOrigin: map[string]int{},
		wake:           make(chan struct{}, 1),
		completion:     make(chan completed, 128),
		ctx:            ctx,
		cancel:         cancel,
		done:           make(chan struct{}),
	}
	go s.loop()
	return s
}

func (s *Scheduler) Config() Config { return s.cfg }

func (s *Scheduler) Submit(ctx context.Context, t Task) error {
	if t.Run == nil {
		return model.NewBiz(model.CodeBadRequest, "scheduler task has no Run function")
	}
	if t.CostMs <= 0 {
		t.CostMs = s.cfg.DefaultTaskCost
	}
	if t.OriginID == "" {
		t.OriginID = "default"
	}
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return model.NewBiz(model.CodeBudgetExhausted, "scheduler is stopping")
	}
	if len(s.queue) >= s.cfg.QueueCapacity {
		depth := len(s.queue)
		s.mu.Unlock()
		return model.WithDetails(model.NewBiz(model.CodeOriginQueueFull, "deterministic waiting queue is full"), map[string]any{
			"queue_depth": depth,
			"queue_limit": s.cfg.QueueCapacity,
			"task_id":     t.ID,
		})
	}
	st := &task{Task: t, order: nextSeq(), result: make(chan error, 1)}
	s.queue = append(s.queue, st)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return nil
}

func (s *Scheduler) SubmitTask(ctx context.Context, t Task) (<-chan error, error) {
	if t.Run == nil {
		return nil, model.NewBiz(model.CodeBadRequest, "scheduler task has no Run function")
	}
	if t.CostMs <= 0 {
		t.CostMs = s.cfg.DefaultTaskCost
	}
	if t.OriginID == "" {
		t.OriginID = "default"
	}
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return nil, model.NewBiz(model.CodeBudgetExhausted, "scheduler is stopping")
	}
	if len(s.queue) >= s.cfg.QueueCapacity {
		depth := len(s.queue)
		s.mu.Unlock()
		return nil, model.WithDetails(model.NewBiz(model.CodeOriginQueueFull, "deterministic waiting queue is full"), map[string]any{
			"queue_depth": depth,
			"queue_limit": s.cfg.QueueCapacity,
			"task_id":     t.ID,
		})
	}
	st := &task{Task: t, order: nextSeq(), result: make(chan error, 1)}
	s.queue = append(s.queue, st)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return st.result, nil
}

func (s *Scheduler) loop() {
	defer close(s.done)
	for {
		select {
		case <-s.ctx.Done():
			s.failAll("scheduler stopped")
			return
		case <-s.wake:
			s.dispatch()
		case c := <-s.completion:
			s.mu.Lock()
			s.activeGlobal--
			s.activeByOrigin[c.origin]--
			if s.activeByOrigin[c.origin] <= 0 {
				delete(s.activeByOrigin, c.origin)
			}
			if c.err != nil {
				s.failedN++
			} else {
				s.completedN++
			}
			s.mu.Unlock()
			select {
			case c.t.result <- c.err:
			default:
			}
			s.dispatch()
		}
	}
}

func (s *Scheduler) dispatch() {
	for {
		s.mu.Lock()
		if s.stopping {
			s.mu.Unlock()
			return
		}
		if len(s.queue) == 0 {
			if s.activeGlobal == 0 {
				s.finishRoundLocked()
			}
			s.mu.Unlock()
			return
		}
		if s.roundID == 0 {
			s.roundID = nextSeq()
			s.roundUsed = 0
			s.roundStart = nil
			s.roundSkip = nil
			s.completedN = 0
			s.failedN = 0
		}
		sort.SliceStable(s.queue, func(i, j int) bool {
			a, b := s.queue[i], s.queue[j]
			if a.Priority != b.Priority {
				return a.Priority > b.Priority
			}
			if a.Heat != b.Heat {
				return a.Heat > b.Heat
			}
			if a.Key != b.Key {
				return a.Key < b.Key
			}
			if a.NodeID != b.NodeID {
				return a.NodeID < b.NodeID
			}
			if a.Kind != b.Kind {
				return a.Kind < b.Kind
			}
			return a.order < b.order
		})

		startedAny := false
		remaining := make([]*task, 0, len(s.queue))
		for _, t := range s.queue {
			origin := t.OriginID
			if s.activeGlobal >= s.cfg.GlobalLimit {
				remaining = append(remaining, t)
				continue
			}
			if s.activeByOrigin[origin] >= s.cfg.PerOriginLimit {
				remaining = append(remaining, t)
				continue
			}
			if s.roundUsed+t.CostMs > s.cfg.RoundBudgetMs {
				reason := "round budget exhausted before deterministic admission"
				sk := skippedFor(t, len(s.roundSkip), reason)
				s.roundSkip = append(s.roundSkip, sk)
				t.result <- model.WithDetails(model.NewBiz(model.CodeBudgetExhausted, reason), map[string]any{
					"task":         t.ID,
					"skip_order":   len(s.roundSkip) - 1,
					"skipped":      append([]SkippedTask(nil), s.roundSkip...),
					"round_id":     s.roundID,
					"budget_ms":    s.cfg.RoundBudgetMs,
					"used_cost_ms": s.roundUsed,
				})
				continue
			}
			s.roundUsed += t.CostMs
			s.activeGlobal++
			s.activeByOrigin[origin]++
			s.roundStart = append(s.roundStart, skippedFor(t, len(s.roundStart), "admitted"))
			startedAny = true
			go func(st *task) {
				err := st.Run(s.ctx)
				s.completion <- completed{t: st, err: err, origin: st.OriginID}
			}(t)
		}
		s.queue = remaining
		if len(s.queue) > 0 && s.activeGlobal >= s.cfg.GlobalLimit {
			s.mu.Unlock()
			return
		}
		if len(s.queue) > 0 && !startedAny {
			// All queued tasks are waiting for an origin/global slot. Release
			// the lock and wait for a completion event.
			s.mu.Unlock()
			return
		}
		// Loop again: budget/admission may allow more tasks after skipped
		// entries were removed, and concurrency slots may remain.
		if len(s.queue) == 0 {
			if s.activeGlobal == 0 {
				s.finishRoundLocked()
			}
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
	}
}

func skippedFor(t *task, order int, reason string) SkippedTask {
	return SkippedTask{Order: order, TaskID: t.ID, Kind: t.Kind, NodeID: t.NodeID, Key: t.Key, Priority: t.Priority, Heat: t.Heat, CostMs: t.CostMs, Reason: reason}
}

func (s *Scheduler) finishRoundLocked() {
	if s.roundID == 0 {
		return
	}
	s.lastReport = RoundReport{
		RoundID:     s.roundID,
		BudgetMs:    s.cfg.RoundBudgetMs,
		UsedCostMs:  s.roundUsed,
		Started:     append([]SkippedTask(nil), s.roundStart...),
		Skipped:     append([]SkippedTask(nil), s.roundSkip...),
		Completed:   s.completedN,
		Failed:      s.failedN,
		Determinism: "admission uses virtual task cost; ordering is priority desc, heat desc, key, node, kind, sequence",
	}
	s.roundID = 0
	s.roundUsed = 0
	s.roundStart = nil
	s.roundSkip = nil
	s.completedN = 0
	s.failedN = 0
}

func (s *Scheduler) LastReport() RoundReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := s.lastReport
	cp.Started = append([]SkippedTask(nil), cp.Started...)
	cp.Skipped = append([]SkippedTask(nil), cp.Skipped...)
	return cp
}

func (s *Scheduler) QueueDepth() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

func (s *Scheduler) Active() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeGlobal
}

func (s *Scheduler) failAll(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopping = true
	for _, t := range s.queue {
		select {
		case t.result <- model.NewBiz(model.CodeBudgetExhausted, reason):
		default:
		}
	}
	s.queue = nil
}

func (s *Scheduler) Close() {
	s.cancel()
	<-s.done
}

func (s *Scheduler) WaitIdle(ctx context.Context) error {
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		if s.QueueDepth() == 0 && s.Active() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// UpdateConfig applies a newer policy snapshot. Existing queued work keeps its
// deterministic order; new admissions observe the new limits and budget.
func (s *Scheduler) UpdateConfig(cfg Config) {
	if cfg.GlobalLimit <= 0 {
		cfg.GlobalLimit = 1
	}
	if cfg.PerOriginLimit <= 0 || cfg.PerOriginLimit > cfg.GlobalLimit {
		cfg.PerOriginLimit = cfg.GlobalLimit
	}
	if cfg.QueueCapacity < 0 {
		cfg.QueueCapacity = 0
	}
	if cfg.RoundBudgetMs <= 0 {
		cfg.RoundBudgetMs = 1
	}
	if cfg.DefaultTaskCost <= 0 {
		cfg.DefaultTaskCost = 1
	}
	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()
}

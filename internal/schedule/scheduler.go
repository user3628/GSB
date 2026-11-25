package schedule

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"auditdrift/internal/model"
)

type Config struct {
	MaxConcurrency int
	QueueSize      int
	RoundBudget    time.Duration
	RepairQuota    int
}

type Outcome struct {
	Conclusion   model.Conclusion
	RepairNeeded bool
}

type Task struct {
	Key        string
	Host       string
	BaselineID string
	Weight     int
	RepairCost int
	Order      int
	Execute    func(context.Context) (Outcome, error)
}

type Result struct {
	Task              Task
	Status            model.TaskStatus
	Outcome           *Outcome
	ErrorCode         model.ErrorCode
	Err               error
	DegradationReason string
	QuotaCharged      bool
}

type Degradation struct {
	Order          int    `json:"order"`
	TaskKey        string `json:"task_key"`
	Host           string `json:"host"`
	BaselineID     string `json:"baseline_id"`
	SeverityWeight int    `json:"severity_weight"`
	RepairCost     int    `json:"repair_cost"`
	RemainingQuota int    `json:"remaining_quota"`
	Basis          string `json:"basis"`
}

type Report struct {
	Results        []Result      `json:"-"`
	OrderedKeys    []string      `json:"ordered_keys"`
	Degradations   []Degradation `json:"degradations"`
	QuotaInitial   int           `json:"repair_quota_initial"`
	QuotaConsumed  int           `json:"repair_quota_consumed_net"`
	QuotaRefunded  int           `json:"repair_quota_refunded"`
	QuotaRemaining int           `json:"repair_quota_remaining"`
	MaxConcurrency int           `json:"max_concurrency"`
	QueueSize      int           `json:"queue_size"`
	PeakActive     int           `json:"peak_active"`
}

type Scheduler struct {
	cfg       Config
	ctx       context.Context
	cancel    context.CancelFunc
	startCh   chan struct{}
	startOnce sync.Once

	mu             sync.Mutex
	active         int
	peakActive     int
	queue          []Task
	wg             sync.WaitGroup
	results        []Result
	orderedKeys    []string
	quotaRemaining int
	quotaConsumed  int
	degradations   []Degradation
}

func New(parent context.Context, cfg Config) *Scheduler {
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 1
	}
	if cfg.QueueSize < 0 {
		cfg.QueueSize = 0
	}
	ctx, cancel := context.WithTimeout(parent, cfg.RoundBudget)
	if cfg.RoundBudget <= 0 {
		ctx, cancel = context.WithCancel(parent)
	}
	return &Scheduler{
		cfg: cfg, ctx: ctx, cancel: cancel, startCh: make(chan struct{}),
		quotaRemaining: cfg.RepairQuota,
	}
}

func OrderTasks(tasks []Task) []Task {
	weights := make(map[int]struct{})
	for _, task := range tasks {
		weights[task.Weight] = struct{}{}
	}
	weightOrder := make([]int, 0, len(weights))
	for weight := range weights {
		weightOrder = append(weightOrder, weight)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(weightOrder)))

	ordered := make([]Task, 0, len(tasks))
	rank := 0
	for _, weight := range weightOrder {
		hostQueues := make(map[string][]Task)
		hostSet := make(map[string]struct{})
		for _, task := range tasks {
			if task.Weight != weight {
				continue
			}
			hostQueues[task.Host] = append(hostQueues[task.Host], task)
			hostSet[task.Host] = struct{}{}
		}
		hosts := make([]string, 0, len(hostSet))
		for host := range hostSet {
			hosts = append(hosts, host)
		}
		sort.Strings(hosts)
		for host := range hostQueues {
			queue := hostQueues[host]
			sort.Slice(queue, func(i, j int) bool {
				if queue[i].BaselineID != queue[j].BaselineID {
					return queue[i].BaselineID < queue[j].BaselineID
				}
				return queue[i].Key < queue[j].Key
			})
			hostQueues[host] = queue
		}
		for {
			progressed := false
			for _, host := range hosts {
				queue := hostQueues[host]
				if len(queue) == 0 {
					continue
				}
				task := queue[0]
				hostQueues[host] = queue[1:]
				rank++
				task.Order = rank
				ordered = append(ordered, task)
				progressed = true
			}
			if !progressed {
				break
			}
		}
	}
	return ordered
}

func RunRound(parent context.Context, cfg Config, tasks []Task) (*Report, error) {
	ordered := OrderTasks(tasks)
	scheduler := New(parent, cfg)
	var queueErr error
	for _, task := range ordered {
		if err := scheduler.Submit(task); err != nil {
			queueErr = err
			break
		}
	}
	report := scheduler.Wait()
	if queueErr != nil {
		return &report, queueErr
	}
	return &report, nil
}

func (s *Scheduler) Submit(task Task) error {
	s.mu.Lock()
	s.orderedKeys = append(s.orderedKeys, task.Key)

	if s.active < s.cfg.MaxConcurrency {
		s.active++
		if s.active > s.peakActive {
			s.peakActive = s.active
		}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.run(task)
		return nil
	}
	if len(s.queue) < s.cfg.QueueSize {
		s.queue = append(s.queue, task)
		s.wg.Add(1)
		s.mu.Unlock()
		return nil
	}
	active := s.active
	queued := len(s.queue)
	s.mu.Unlock()
	return model.NewCodedError(model.ErrAuditQueueFull, "bounded audit queue is full").
		WithDetails(map[string]any{
			"active": active, "max_concurrency": s.cfg.MaxConcurrency,
			"queued": queued, "queue_size": s.cfg.QueueSize, "task_key": task.Key,
		})
}

func (s *Scheduler) Wait() Report {
	s.startOnce.Do(func() { close(s.startCh) })
	s.wg.Wait()
	s.cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	sort.Slice(s.results, func(i, j int) bool {
		return s.results[i].Task.Order < s.results[j].Task.Order
	})
	s.allocateQuotaLocked()
	return Report{
		Results:        append([]Result{}, s.results...),
		OrderedKeys:    append([]string{}, s.orderedKeys...),
		Degradations:   append([]Degradation{}, s.degradations...),
		QuotaInitial:   s.cfg.RepairQuota,
		QuotaConsumed:  s.quotaConsumed,
		QuotaRefunded:  0,
		QuotaRemaining: s.quotaRemaining,
		MaxConcurrency: s.cfg.MaxConcurrency,
		QueueSize:      s.cfg.QueueSize,
		PeakActive:     s.peakActive,
	}
}

func (s *Scheduler) run(task Task) {
	defer s.wg.Done()

	select {
	case <-s.startCh:
	case <-s.ctx.Done():
		s.completeDeferred(task, "round_budget_exhausted", s.currentRemaining(), s.ctx.Err())
		s.pump()
		return
	}
	if err := s.ctx.Err(); err != nil {
		s.completeDeferred(task, "round_budget_exhausted", s.currentRemaining(), err)
		s.pump()
		return
	}

	outcome, err := task.Execute(s.ctx)

	if err != nil {
		status, reason := classifyFailure(s.ctx, err)
		result := Result{Task: task, Status: status, Err: err, DegradationReason: reason}
		var coded *model.CodedError
		if errors.As(err, &coded) {
			result.ErrorCode = coded.Code
		}
		s.record(result)
	} else {
		status := model.TaskDone
		if outcome.Conclusion.Status != "" {
			status = outcome.Conclusion.Status
		}
		s.record(Result{Task: task, Status: status, Outcome: &outcome})
	}
	s.pump()
}

func (s *Scheduler) currentRemaining() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.quotaRemaining
}

func (s *Scheduler) allocateQuotaLocked() {
	s.quotaRemaining = s.cfg.RepairQuota
	s.quotaConsumed = 0
	quotaClosed := false
	for i := range s.results {
		result := &s.results[i]
		if result.Status != model.TaskDone || result.Outcome == nil || !result.Outcome.RepairNeeded {
			continue
		}
		cost := result.Task.RepairCost
		if cost <= 0 {
			cost = result.Task.Weight
		}
		if !quotaClosed && s.quotaRemaining >= cost {
			s.quotaRemaining -= cost
			s.quotaConsumed += cost
			result.QuotaCharged = true
			continue
		}
		quotaClosed = true
		result.Status = model.TaskDeferred
		result.QuotaCharged = false
		result.DegradationReason = "repair_quota_exhausted"
		basis := "strict_priority: repair quota exhausted; this and later repair-required tasks are DEFERRED"
		if s.quotaRemaining < cost {
			basis = fmt.Sprintf("strict_priority: repair cost %d exceeds remaining quota %d; this and later repair-required tasks are DEFERRED", cost, s.quotaRemaining)
		}
		s.degradations = append(s.degradations, Degradation{
			Order: result.Task.Order, TaskKey: result.Task.Key, Host: result.Task.Host, BaselineID: result.Task.BaselineID,
			SeverityWeight: result.Task.Weight, RepairCost: cost, RemainingQuota: s.quotaRemaining, Basis: basis,
		})
	}
}

func (s *Scheduler) pump() {
	s.mu.Lock()
	s.active--
	var next Task
	haveNext := len(s.queue) > 0
	if haveNext {
		next = s.queue[0]
		s.queue = s.queue[1:]
		s.active++
		if s.active > s.peakActive {
			s.peakActive = s.active
		}
	}
	s.mu.Unlock()
	if haveNext {
		go s.run(next)
	}
}

func (s *Scheduler) record(result Result) {
	s.mu.Lock()
	s.results = append(s.results, result)
	s.mu.Unlock()
}

func (s *Scheduler) addQuotaDeferredLocked(task Task, cost, remaining int) {
	degradation := Degradation{
		Order:          task.Order,
		TaskKey:        task.Key,
		Host:           task.Host,
		BaselineID:     task.BaselineID,
		SeverityWeight: task.Weight,
		RepairCost:     cost,
		RemainingQuota: remaining,
		Basis:          "strict_priority: repair cost exceeds available quota; this and later tasks are DEFERRED",
	}
	s.degradations = append(s.degradations, degradation)
	s.results = append(s.results, Result{
		Task: task, Status: model.TaskDeferred, DegradationReason: "repair_quota_exhausted",
		Err: model.NewCodedError("REPAIR_QUOTA_EXHAUSTED", "%s", degradation.Basis),
	})
}

func (s *Scheduler) completeDeferred(task Task, reason string, remaining int, cause error) {
	s.mu.Lock()
	s.degradations = append(s.degradations, Degradation{
		Order: task.Order, TaskKey: task.Key, Host: task.Host, BaselineID: task.BaselineID,
		SeverityWeight: task.Weight, RepairCost: task.RepairCost, RemainingQuota: remaining,
		Basis: reason,
	})
	s.results = append(s.results, Result{Task: task, Status: model.TaskDeferred, DegradationReason: reason, Err: cause})
	s.mu.Unlock()
}

func classifyFailure(roundCtx context.Context, err error) (model.TaskStatus, string) {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(roundCtx.Err(), context.DeadlineExceeded) {
		return model.TaskDeferred, "round_budget_exhausted"
	}
	if errors.Is(err, context.Canceled) || errors.Is(roundCtx.Err(), context.Canceled) {
		return model.TaskDeferred, "round_context_canceled"
	}
	return model.TaskError, "probe_or_execution_error"
}

var _ = fmt.Sprintf

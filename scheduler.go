package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Scheduler struct {
	service *Service
	mu      sync.Mutex
	cfg     SchedulerConfig
	queue   []*ScheduledTask
	enqueue int64
	rounds  int64
	last    []BudgetRound
	running map[string]bool
}

func NewScheduler(service *Service, cfg SchedulerConfig) *Scheduler {
	normalizeSchedulerConfig(&cfg)
	return &Scheduler{service: service, cfg: cfg, running: map[string]bool{}}
}

func normalizeSchedulerConfig(cfg *SchedulerConfig) {
	if cfg.GlobalConcurrency <= 0 {
		cfg.GlobalConcurrency = 2
	}
	if cfg.PerPartitionLeaseLimit <= 0 {
		cfg.PerPartitionLeaseLimit = 1
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 32
	}
	if cfg.RoundBudgetMS <= 0 {
		cfg.RoundBudgetMS = 250
	}
	if cfg.QueryTimeoutMS <= 0 {
		cfg.QueryTimeoutMS = 150
	}
	if cfg.MinDispatchMS <= 0 {
		cfg.MinDispatchMS = 2
	}
}

func (s *Scheduler) Config() SchedulerConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg
}

func (s *Scheduler) SetConfig(cfg SchedulerConfig) SchedulerConfig {
	normalizeSchedulerConfig(&cfg)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg = cfg
	return s.cfg
}

func taskKey(kind, topic string, partition int, txnID string) string {
	return fmt.Sprintf("%s|%s|%d|%s", kind, topic, partition, txnID)
}

func (s *Scheduler) Enqueue(ctx context.Context, req SchedulerTaskRequest) (ScheduledTask, error) {
	if req.Kind == "" {
		if req.TxnID != "" {
			req.Kind = "txn"
		} else {
			req.Kind = "lease_expire"
		}
	}
	var topic string
	var partition int
	var priority int
	switch req.Kind {
	case "txn":
		if req.TxnID == "" {
			return ScheduledTask{}, newErr(CodeInvalidArgument, "txn_id is required")
		}
		txn, err := s.service.GetTransaction(ctx, req.TxnID)
		if err != nil {
			return ScheduledTask{}, err
		}
		if txn.Status != TxnPending {
			return ScheduledTask{}, newErrDetails(CodeInvalidTransition,
				"only pending transactions can be scheduled",
				map[string]any{"txn_id": txn.TxnID, "status": txn.Status})
		}
		topic, partition = txn.Topic, txn.Partition
	case "lease_expire":
		if req.Topic == "" {
			return ScheduledTask{}, newErr(CodeInvalidArgument, "topic is required")
		}
		topic, partition = req.Topic, req.Partition
	default:
		return ScheduledTask{}, newErr(CodeInvalidArgument, "task kind must be txn or lease_expire")
	}
	view, err := s.service.GetPartition(ctx, topic, partition)
	if err != nil {
		return ScheduledTask{}, err
	}
	priority = view.Priority
	key := taskKey(req.Kind, topic, partition, req.TxnID)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.queue {
		if taskKey(existing.Kind, existing.Topic, existing.Partition, existing.TxnID) == key {
			copy := *existing
			copy.Deduped = true
			return copy, nil
		}
	}
	if s.running[key] {
		return ScheduledTask{Kind: req.Kind, Topic: topic, Partition: partition,
			Priority: priority, TxnID: req.TxnID, RequestID: req.RequestID,
			Status: "RUNNING", Deduped: true}, nil
	}
	if len(s.queue) >= s.cfg.QueueCapacity {
		return ScheduledTask{}, newErrDetails(CodeQueueFull,
			"bounded scheduler queue is full",
			map[string]any{"capacity": s.cfg.QueueCapacity, "queued": len(s.queue)})
	}
	s.enqueue++
	requestID := req.RequestID
	if requestID == "" {
		requestID = newID("schedreq")
	}
	task := &ScheduledTask{ID: newID("task"), Kind: req.Kind, Topic: topic,
		Partition: partition, Priority: priority, TxnID: req.TxnID,
		RequestID: requestID, EnqueueSeq: s.enqueue, Status: "QUEUED"}
	s.queue = append(s.queue, task)
	return *task, nil
}

func (s *Scheduler) orderedQueueLocked(round int64) []*ScheduledTask {
	if len(s.queue) == 0 {
		return nil
	}
	partitionOrder := map[string]int{}
	names := make([]string, 0)
	for _, task := range s.queue {
		k := fmt.Sprintf("%s/%d", task.Topic, task.Partition)
		if _, ok := partitionOrder[k]; !ok {
			partitionOrder[k] = len(names)
			names = append(names, k)
		}
	}
	offset := int(round % int64(len(names)))
	rotation := func(k string) int {
		return (partitionOrder[k] - offset + len(names)) % len(names)
	}
	groups := map[string][]*ScheduledTask{}
	for _, task := range s.queue {
		k := fmt.Sprintf("%s/%d", task.Topic, task.Partition)
		groups[k] = append(groups[k], task)
	}
	keys := append([]string(nil), names...)
	priorities := map[int]bool{}
	for _, k := range keys {
		sort.SliceStable(groups[k], func(i, j int) bool {
			return groups[k][i].EnqueueSeq < groups[k][j].EnqueueSeq
		})
		priorities[groups[k][0].Priority] = true
	}
	bands := make([]int, 0, len(priorities))
	for priority := range priorities {
		bands = append(bands, priority)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(bands)))
	out := make([]*ScheduledTask, 0, len(s.queue))
	for _, priority := range bands {
		bandKeys := []string{}
		for _, k := range keys {
			if len(groups[k]) > 0 && groups[k][0].Priority == priority {
				bandKeys = append(bandKeys, k)
			}
		}
		sort.Slice(bandKeys, func(i, j int) bool {
			return rotation(bandKeys[i]) < rotation(bandKeys[j])
		})
		for {
			advanced := false
			for _, k := range bandKeys {
				if len(groups[k]) == 0 {
					continue
				}
				out = append(out, groups[k][0])
				groups[k] = groups[k][1:]
				advanced = true
			}
			if !advanced {
				break
			}
		}
	}
	return out
}

func (s *Scheduler) removeQueuedLocked(taskID string) {
	for i, task := range s.queue {
		if task.ID == taskID {
			s.queue = append(s.queue[:i], s.queue[i+1:]...)
			return
		}
	}
}

type taskCompletion struct {
	task  *ScheduledTask
	step  BudgetStep
	key   string
	lease bool
}

func (s *Scheduler) executeTask(ctx context.Context, task *ScheduledTask,
	timeoutMS int, beforeMS int64) taskCompletion {
	start := time.Now()
	step := BudgetStep{TaskID: task.ID, Kind: task.Kind, Topic: task.Topic,
		Partition: task.Partition, Priority: task.Priority, TxnID: task.TxnID,
		BudgetBeforeMS: beforeMS}
	switch task.Kind {
	case "txn":
		_, err := s.service.ResolveTransaction(ctx, task.TxnID, ResolveRequest{
			RequestID: task.RequestID, QueryTimeoutMS: timeoutMS,
		})
		if err != nil {
			step.Result = "FAILED"
			step.Code = asWorkbenchError(err).Code
			step.Reason = err.Error()
		} else {
			step.Result = "DONE"
		}
	case "lease_expire":
		expired, _, err := s.service.ExpireLease(ctx, task.Topic, task.Partition)
		if err != nil {
			step.Result = "FAILED"
			step.Code = asWorkbenchError(err).Code
			step.Reason = err.Error()
		} else if expired {
			step.Result = "DONE"
			step.Reason = "lease_expired"
		} else {
			step.Result = "DONE"
			step.Reason = "lease_not_expired"
		}
	default:
		step.Result = "FAILED"
		step.Code = CodeInvalidArgument
		step.Reason = "unknown task kind"
	}
	step.DurationMS = time.Since(start).Milliseconds()
	key := taskKey(task.Kind, task.Topic, task.Partition, task.TxnID)
	return taskCompletion{task: task, step: step, key: key, lease: task.Kind == "lease_expire"}
}

func (s *Scheduler) RunRound(ctx context.Context, budgetOverrideMS int) (BudgetRound, error) {
	s.mu.Lock()
	s.rounds++
	roundNo := s.rounds
	cfg := s.cfg
	ordered := s.orderedQueueLocked(roundNo)
	if len(ordered) == 0 {
		round := BudgetRound{StartedAtMS: nowMS(time.Now()), BudgetMS: int64(cfg.RoundBudgetMS),
			RemainingMS: int64(cfg.RoundBudgetMS), Status: "idle", Steps: []BudgetStep{}}
		s.mu.Unlock()
		return round, nil
	}
	s.mu.Unlock()

	budget := cfg.RoundBudgetMS
	if budgetOverrideMS > 0 {
		budget = budgetOverrideMS
	}
	startedAt := time.Now()
	deadline := startedAt.Add(time.Duration(budget) * time.Millisecond)
	remainingMS := func() int64 {
		left := time.Until(deadline).Milliseconds()
		if left < 0 {
			return 0
		}
		return left
	}

	completions := make(chan taskCompletion, len(ordered))
	steps := make([]BudgetStep, 0, len(ordered))
	inFlight := 0
	leaseInFlight := map[string]int{}
	dispatched := map[string]bool{}
	finished := map[string]bool{}
	canceled := false

	process := func(done taskCompletion) {
		finished[done.task.ID] = true
		done.step.BudgetAfterMS = remainingMS()
		steps = append(steps, done.step)
		s.mu.Lock()
		s.removeQueuedLocked(done.task.ID)
		delete(s.running, done.key)
		s.mu.Unlock()
	}

	for {
		for {
			select {
			case done := <-completions:
				inFlight--
				if done.lease {
					leaseInFlight[fmt.Sprintf("%s/%d", done.task.Topic, done.task.Partition)]--
				}
				process(done)
			default:
				goto drained
			}
		}
	drained:
		if !canceled && remainingMS() >= int64(cfg.MinDispatchMS) {
			for _, task := range ordered {
				if inFlight >= cfg.GlobalConcurrency {
					break
				}
				if dispatched[task.ID] || finished[task.ID] {
					continue
				}
				if task.Kind == "lease_expire" {
					k := fmt.Sprintf("%s/%d", task.Topic, task.Partition)
					if leaseInFlight[k] >= cfg.PerPartitionLeaseLimit {
						continue
					}
				}
				dispatched[task.ID] = true
				inFlight++
				if task.Kind == "lease_expire" {
					leaseInFlight[fmt.Sprintf("%s/%d", task.Topic, task.Partition)]++
				}
				key := taskKey(task.Kind, task.Topic, task.Partition, task.TxnID)
				s.mu.Lock()
				task.Status = "RUNNING"
				s.running[key] = true
				s.mu.Unlock()
				before := remainingMS()
				go func(t *ScheduledTask) {
					completions <- s.executeTask(ctx, t, cfg.QueryTimeoutMS, before)
				}(task)
			}
		}
		if inFlight == 0 {
			break
		}
		select {
		case <-ctx.Done():
			canceled = true
		case done := <-completions:
			inFlight--
			if done.lease {
				leaseInFlight[fmt.Sprintf("%s/%d", done.task.Topic, done.task.Partition)]--
			}
			process(done)
		}
	}

	status := "completed"
	left := remainingMS()
	if canceled {
		status = "canceled"
	} else if left < int64(cfg.MinDispatchMS) {
		status = "degraded_budget"
	}
	round := s.finishRound(ctx, roundNo, startedAt, int64(budget), left, steps, ordered,
		dispatched, finished, status)
	if canceled {
		return round, ctx.Err()
	}
	return round, nil
}

func (s *Scheduler) finishRound(ctx context.Context, roundNo int64, startedAt time.Time,
	budget int64, remaining int64, steps []BudgetStep, ordered []*ScheduledTask,
	dispatched, finished map[string]bool, status string) BudgetRound {
	for _, task := range ordered {
		if dispatched[task.ID] || finished[task.ID] {
			continue
		}
		steps = append(steps, BudgetStep{TaskID: task.ID, Kind: task.Kind,
			Topic: task.Topic, Partition: task.Partition, Priority: task.Priority,
			TxnID: task.TxnID, Result: "SKIPPED", Reason: "round_budget_exhausted",
			BudgetBeforeMS: remaining, BudgetAfterMS: remaining})
	}
	consumed := time.Since(startedAt).Milliseconds()
	round := BudgetRound{StartedAtMS: nowMS(startedAt), BudgetMS: budget,
		ConsumedMS: consumed, RemainingMS: remaining, Status: status, Steps: steps}
	raw, _ := json.Marshal(round)
	id := s.service.SaveSchedulerRound(context.Background(), round, raw)
	s.mu.Lock()
	if id == 0 {
		round.ID = roundNo
	}
	round.Steps = steps
	s.last = append([]BudgetRound{round}, s.last...)
	if len(s.last) > 10 {
		s.last = s.last[:10]
	}
	s.mu.Unlock()
	return round
}

func (s *Service) SaveSchedulerRound(ctx context.Context, round BudgetRound, raw []byte) int64 {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	saveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var id int64
	if err := s.db.QueryRowContext(saveCtx, `INSERT INTO scheduler_rounds
		(started_at_ms, budget_ms, consumed_ms, remaining_ms, status, trace_json)
		VALUES (?,?,?,?,?,?) RETURNING id`, round.StartedAtMS, round.BudgetMS,
		round.ConsumedMS, round.RemainingMS, round.Status, string(raw)).Scan(&id); err != nil {
		return 0
	}
	return id
}

func (s *Scheduler) View() SchedulerView {
	s.mu.Lock()
	defer s.mu.Unlock()
	queue := make([]ScheduledTask, 0, len(s.queue))
	for _, task := range s.queue {
		queue = append(queue, *task)
	}
	rounds := append([]BudgetRound(nil), s.last...)
	return SchedulerView{Config: s.cfg, Queue: queue, InFlight: len(s.running), LastRounds: rounds}
}

func (s *Service) ListBudgetRounds(ctx context.Context, limit int) ([]BudgetRound, error) {
	if limit <= 0 || limit > 100 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, started_at_ms, budget_ms, consumed_ms,
		remaining_ms, status, trace_json FROM scheduler_rounds ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []BudgetRound{}
	for rows.Next() {
		var round BudgetRound
		var raw string
		if err := rows.Scan(&round.ID, &round.StartedAtMS, &round.BudgetMS, &round.ConsumedMS,
			&round.RemainingMS, &round.Status, &raw); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(raw), &round)
		if round.Steps == nil {
			round.Steps = []BudgetStep{}
		}
		out = append(out, round)
	}
	return out, rows.Err()
}

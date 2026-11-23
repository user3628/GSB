// Package scheduler 在全局并发、每实例并发与轮次时间预算三重约束下，
// 按确定性权重快照把请求真实派发到回环上游实例。
package scheduler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"gatewayworkbench/internal/model"
	"gatewayworkbench/internal/store"
)

type job struct {
	seq      int
	req      model.RequestRecord
	decision *model.Decision
}

type Scheduler struct {
	store      *store.Store
	cfg        model.RuntimeConfig
	httpClient *http.Client

	// 单飞：同一轮并发触发时仅一个实际执行，其余等待复用结果。
	roundLocks map[string]*sync.Mutex
	rlMu       sync.Mutex
}

func New(st *store.Store, cfg model.RuntimeConfig) *Scheduler {
	return &Scheduler{
		store:      st,
		cfg:        cfg,
		httpClient: &http.Client{Timeout: time.Duration(cfg.RoundBudgetMillis) * time.Millisecond},
		roundLocks: make(map[string]*sync.Mutex),
	}
}

func (s *Scheduler) roundMu(roundID string) *sync.Mutex {
	s.rlMu.Lock()
	defer s.rlMu.Unlock()
	mu, ok := s.roundLocks[roundID]
	if !ok {
		mu = &sync.Mutex{}
		s.roundLocks[roundID] = mu
	}
	return mu
}

// DispatchResult 是一轮调度的汇总。
type DispatchResult struct {
	RoundID         string           `json:"round_id"`
	SnapshotVersion int64            `json:"snapshot_version"`
	BudgetMillis    int              `json:"budget_millis"`
	ElapsedMillis   int64            `json:"elapsed_millis"`
	Accepted        int              `json:"accepted"`
	Queued          int              `json:"queued"`
	Completed       int              `json:"completed"`
	Failed          int              `json:"failed"`
	SkippedBudget   int              `json:"skipped_budget"`
	NoCandidate     int              `json:"no_candidate"`
	QueueFullError  *model.BizError  `json:"queue_full_error,omitempty"`
	Notes           []string         `json:"notes,omitempty"`
	Decisions       []model.Decision `json:"decisions"`
}

// RunRound 执行一轮调度。并发重复触发同一轮时复用已有结果或等待。
func (s *Scheduler) RunRound(parent context.Context, roundID string) (*DispatchResult, error) {
	mu := s.roundMu(roundID)
	if !mu.TryLock() {
		// 已有同轮执行：等待其完成后返回当前持久化状态，不重复派发。
		mu.Lock()
		defer mu.Unlock()
		if _, err := s.store.RoundStatus(parent, roundID); err != nil {
			return nil, err
		}
		return &DispatchResult{RoundID: roundID, Decisions: []model.Decision{},
			ElapsedMillis: 0, BudgetMillis: -1}, nil
	}
	defer mu.Unlock()

	if _, err := s.store.RoundStatus(parent, roundID); err == nil {
		return &DispatchResult{RoundID: roundID, Decisions: []model.Decision{}, BudgetMillis: -1}, nil
	}

	snapshot, err := s.store.LatestSnapshot(parent)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, model.NewBizError(model.CodeNoCandidate, "no committed snapshot, run a probe round first", "")
	}
	instances, err := s.store.ListInstances(parent)
	if err != nil {
		return nil, err
	}
	portByID := make(map[string]int, len(instances))
	for _, ins := range instances {
		portByID[ins.ID] = ins.Port
	}

	budget := time.Duration(s.cfg.RoundBudgetMillis) * time.Millisecond
	ctx, cancel := context.WithTimeout(parent, budget)
	defer cancel()
	roundStart := time.Now()

	// 计划事务：OPEN 意图 + 轮次 + 每个请求的派发占位与决策，先全部落盘再真实发起。
	tx, intentID, err := s.store.BeginIntent(ctx, roundID, "dispatch",
		map[string]any{"snapshot": snapshot.Version})
	if err != nil {
		return nil, err
	}
	if err := s.store.CreateRound(ctx, tx, roundID, "dispatch",
		fmt.Sprintf(`{"snapshot":%d}`, snapshot.Version)); err != nil {
		store.AbortIntent(tx)
		return nil, err
	}

	pending, err := s.store.PendingRequests(ctx, tx)
	if err != nil {
		store.AbortIntent(tx)
		return nil, err
	}
	// 时间戳顺序读取已由 SQL 保证；再按可用区名做确定性轮转打散。
	ordered := rotateByZone(pending, snapshot)

	jobs := make([]job, 0, len(ordered))
	queued := 0
	noCandidate := 0
	var queueFull *model.BizError
	for _, item := range ordered {
		seq, req := item.seq, item.req
		decision, err := model.ChooseInstance(snapshot, req.ClientID, seq)
		if err != nil {
			noCandidate++
			continue
		}
		// 有界等待队列：计划阶段超过队列容量直接返回 E_DISPATCH_QUEUE_FULL，
		// 已入队计划保留在事务内随提交落盘，超出部分不再受理。
		if queued >= s.cfg.QueueCapacity {
			queueFull = model.NewBizError(model.CodeDispatchQueueFull,
				"bounded dispatch queue is full",
				fmt.Sprintf("queue_capacity=%d request_seq=%d", s.cfg.QueueCapacity, seq))
			break
		}
		if _, err := s.store.InsertDispatch(ctx, tx, roundID, seq, decision.SelectedInstance); err != nil {
			store.AbortIntent(tx)
			return nil, err
		}
		if err := s.store.MarkRequestDispatched(ctx, tx, seq, roundID, decision); err != nil {
			store.AbortIntent(tx)
			return nil, err
		}
		jobs = append(jobs, job{seq: seq, req: req, decision: decision})
		queued++
	}
	if err := s.store.FinishRound(ctx, tx, roundID, "PLANNED"); err != nil {
		store.AbortIntent(tx)
		return nil, err
	}
	// 第一阶段提交：计划与 OPEN 意图一起落盘，但意图尚未 COMMITTED；
	// 进程在此之后、第二阶段之前被强杀，恢复流程将依据 OPEN 意图回滚整轮。
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	result := &DispatchResult{
		RoundID: roundID, SnapshotVersion: snapshot.Version,
		BudgetMillis: s.cfg.RoundBudgetMillis, Queued: queued,
		Accepted: queued, NoCandidate: noCandidate, QueueFullError: queueFull,
	}
	for _, j := range jobs {
		result.Decisions = append(result.Decisions, *j.decision)
	}

	// 执行阶段：全局信号量 + 每实例信号量构成三重并发约束中的前两重。
	globalSem := make(chan struct{}, s.cfg.GlobalConcurrency)
	instanceSem := make(map[string]chan struct{})
	for _, ins := range instances {
		instanceSem[ins.ID] = make(chan struct{}, s.cfg.PerInstanceLimit)
	}

	// 强杀演示点：计划已落盘且 OPEN 意图保留，只执行部分派发后立即退出进程，
	// 由重启恢复扫描 OPEN 意图并回滚未提交派发。
	if cp := os.Getenv("WB_KILL_AFTER_PLAN"); cp != "" && cp == roundID {
		s.execHalfAndExit(ctx, roundID, jobs, globalSem, instanceSem, portByID)
	}
	var wg sync.WaitGroup
	for _, j := range jobs {
		if ctx.Err() != nil {
			result.SkippedBudget++
			continue
		}
		wg.Add(1)
		go func(jb job) {
			defer wg.Done()
			select {
			case globalSem <- struct{}{}:
			case <-ctx.Done():
				result.SkippedBudget++
				return
			}
			defer func() { <-globalSem }()
			select {
			case instanceSem[jb.decision.SelectedInstance] <- struct{}{}:
			case <-ctx.Done():
				result.SkippedBudget++
				return
			}
			defer func() { <-instanceSem[jb.decision.SelectedInstance] }()

			dispatchID, dErr := s.findDispatchID(ctx, roundID, jb.seq)
			if dErr != nil {
				result.Failed++
				return
			}
			if err := s.store.MarkDispatchRunning(ctx, dispatchID); err != nil {
				result.Failed++
				return
			}
			status, errCode, errMsg := s.callUpstream(ctx, portByID[jb.decision.SelectedInstance], jb.req)
			finalStatus := "SUCCEEDED"
			if errCode != "" {
				finalStatus = "FAILED"
			}
			if err := s.store.CompleteDispatch(ctx, dispatchID, finalStatus, status, errCode, errMsg); err != nil {
				result.Failed++
				return
			}
			if errCode != "" {
				result.Failed++
			} else {
				result.Completed++
			}
		}(j)
	}
	wg.Wait()
	if ctx.Err() != nil {
		if err := s.markBudgetSkipped(parent, roundID); err != nil {
			return nil, err
		}
		result.Notes = append(result.Notes, "round time budget exhausted before all requests completed")
	}
	if err := s.store.CommitOpenIntent(parent, intentID); err != nil {
		return nil, err
	}
	result.ElapsedMillis = time.Since(roundStart).Milliseconds()
	return result, nil
}

// markBudgetSkipped 把预算耗尽时仍处于 PENDING 的派发显式标记为 BUDGET_SKIPPED，
// 避免它们与强杀未完成派发混淆；意图随后仍推进到 COMMITTED 完整边界。
func (s *Scheduler) markBudgetSkipped(ctx context.Context, roundID string) error {
	_, err := s.store.DB().ExecContext(ctx,
		`UPDATE dispatches SET status='BUDGET_SKIPPED', err_code=?, err_msg='round budget exhausted',
		 finished_at=? WHERE round_id=? AND status='PENDING'`,
		model.CodeBudgetExceeded, time.Now().UnixMilli(), roundID)
	return err
}

func (s *Scheduler) findDispatchID(ctx context.Context, roundID string, seq int) (int64, error) {
	var id int64
	err := s.store.DB().QueryRowContext(ctx,
		`SELECT id FROM dispatches WHERE round_id=? AND request_seq=?`, roundID, seq).Scan(&id)
	return id, err
}

// callUpstream 真实向上游回环端点发起 POST，请求体承载请求字节数与权重。
func (s *Scheduler) callUpstream(ctx context.Context, port int, req model.RequestRecord) (int, string, string) {
	payload := bytes.Repeat([]byte("x"), int(clampBytes(req.RequestBytes)))
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/upstream", port), bytes.NewReader(payload))
	if err != nil {
		return 0, model.CodeProbeFailed, err.Error()
	}
	httpReq.Header.Set("X-Client-Id", req.ClientID)
	httpReq.Header.Set("X-Request-Weight", fmt.Sprintf("%d", req.RequestWeight))
	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			return 0, model.CodeBudgetExceeded, "round budget exhausted: " + err.Error()
		}
		return 0, model.CodeProbeFailed, err.Error()
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp.StatusCode, "", ""
	}
	return resp.StatusCode, model.CodeProbeFailed, "upstream non-2xx status"
}

func clampBytes(n int64) int64 {
	if n > 4096 {
		return 4096
	}
	return n
}

type orderedItem struct {
	seq int
	req model.RequestRecord
}

// rotateByZone 按首选可用区轮转，保证同区请求不连续垄断，同时保持时间戳优先。
func rotateByZone(reqs []model.RequestRecord, snap *model.SnapshotVersion) []orderedItem {
	items := make([]orderedItem, len(reqs))
	for i, req := range reqs {
		items[i] = orderedItem{seq: i, req: req}
	}
	buckets := map[string][]orderedItem{}
	for _, it := range items {
		hash := model.DeterministicHash(it.req.ClientID, it.seq)
		zone := model.PickZone(snap.ZoneRatios, hash)
		buckets[zone] = append(buckets[zone], it)
	}
	zones := make([]string, 0, len(buckets))
	for z := range buckets {
		zones = append(zones, z)
	}
	sort.Strings(zones)
	out := make([]orderedItem, 0, len(items))
	zoneIdx := map[string]int{}
	for len(out) < len(items) {
		progressed := false
		for _, z := range zones {
			idx := zoneIdx[z]
			if idx < len(buckets[z]) {
				out = append(out, buckets[z][idx])
				zoneIdx[z] = idx + 1
				progressed = true
			}
		}
		if !progressed {
			break
		}
	}
	return out
}

// execHalfAndExit 真实派发前一半计划，随后以非零状态退出，模拟 SIGKILL 后无清理机会。
func (s *Scheduler) execHalfAndExit(ctx context.Context, roundID string, jobs []job,
	globalSem chan struct{}, instanceSem map[string]chan struct{}, portByID map[string]int) {
	half := len(jobs) / 2
	if half == 0 {
		half = 1
	}
	var wg sync.WaitGroup
	for idx := 0; idx < half && idx < len(jobs); idx++ {
		jb := jobs[idx]
		wg.Add(1)
		go func(item job) {
			defer wg.Done()
			globalSem <- struct{}{}
			defer func() { <-globalSem }()
			instanceSem[item.decision.SelectedInstance] <- struct{}{}
			defer func() { <-instanceSem[item.decision.SelectedInstance] }()
			dispatchID, err := s.findDispatchID(ctx, roundID, item.seq)
			if err != nil {
				return
			}
			_ = s.store.MarkDispatchRunning(ctx, dispatchID)
			status, errCode, errMsg := s.callUpstream(ctx, portByID[item.decision.SelectedInstance], item.req)
			finalStatus := "SUCCEEDED"
			if errCode != "" {
				finalStatus = "FAILED"
			}
			_ = s.store.CompleteDispatch(ctx, dispatchID, finalStatus, status, errCode, errMsg)
		}(jb)
	}
	wg.Wait()
	fmt.Fprintf(os.Stderr, "WB_KILL_AFTER_PLAN triggered: round=%s executed=%d/%d, exiting without commit cleanup\n",
		roundID, half, len(jobs))
	os.Exit(97)
}

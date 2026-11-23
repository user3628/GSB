package scheduler

import (
	"sort"
	"sync"

	"mitmpin/internal/engine"
	"mitmpin/internal/sample"
)

// UnitCost 是每个事件消耗的全局内存预算单位。
const UnitCost = 1

// PlannedConn 是预算规划阶段对单条连接的裁决。
type PlannedConn struct {
	ConnID        string
	EventCount    int
	NeedBudget    int
	BudgetCode    string // E_EVENT_BUDGET / E_GLOBAL_BUDGET / E_OK
	AllowedEvents int    // 实际允许处理的事件数（含预算截断）
}

// Plan 是确定性预算规划结果。规划只依赖事件数与连接标识，
// 与并发度无关，因此并发度变化时业务码序列保持稳定。
type Plan struct {
	Items []PlannedConn
}

// NewPlan 按连接事件数加权（少者优先）排队并裁决预算。
func NewPlan(obs []sample.Observation, budget sample.Budget) Plan {
	type row struct {
		conn sample.Observation
		n    int
	}
	rows := make([]row, len(obs))
	for i, o := range obs {
		rows[i] = row{conn: o, n: len(o.Events)}
	}
	// 加权排队：事件数升序，同数按连接标识稳定排序。
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].n != rows[j].n {
			return rows[i].n < rows[j].n
		}
		return rows[i].conn.ConnID < rows[j].conn.ConnID
	})

	items := make([]PlannedConn, 0, len(rows))
	remaining := budget.GlobalMemory
	for _, r := range rows {
		item := PlannedConn{ConnID: r.conn.ConnID, EventCount: r.n}

		// 单连接事件数上限：超限截断并标记 E_EVENT_BUDGET，保留已处理事件结论。
		capped := r.n
		if budget.MaxEventPerConn > 0 && r.n > budget.MaxEventPerConn {
			capped = budget.MaxEventPerConn
			item.BudgetCode = sample.E_EVENT_BUDGET
		}

		need := capped * UnitCost
		item.NeedBudget = need
		if remaining >= need {
			remaining -= need
			item.AllowedEvents = capped
			if item.BudgetCode == "" {
				item.BudgetCode = sample.E_OK
			}
		} else {
			// 全局预算不足：按事件数从少到多降级，分配剩余预算后标记 E_GLOBAL_BUDGET。
			allowed := remaining / UnitCost
			if allowed > capped {
				allowed = capped
			}
			item.AllowedEvents = allowed
			item.NeedBudget = allowed * UnitCost
			remaining -= item.NeedBudget
			// 全局预算码优先级高于单连接码，体现降级裁决来源。
			item.BudgetCode = sample.E_GLOBAL_BUDGET
		}
		items = append(items, item)
	}
	return Plan{Items: items}
}

// Find 返回某连接的规划项。
func (p Plan) Find(connID string) (PlannedConn, bool) {
	for _, item := range p.Items {
		if item.ConnID == connID {
			return item, true
		}
	}
	return PlannedConn{}, false
}

// Job 是调度单元。
type Job struct {
	Obs       sample.Observation
	Bound     sample.Snapshot
	MaxEvents int
}

// Runner 抽象引擎执行，便于测试替换。
type Runner func(job Job) (engine.ConnResult, error)

// Run 使用有界 worker 池并发执行任务。超过并发上限的连接进入等待队列，
// 而非直接拒绝；结果按连接标识稳定排序返回，保证报告确定性。
func Run(jobs []Job, concurrency int, runner Runner) ([]engine.ConnResult, []error) {
	if concurrency <= 0 {
		concurrency = 1
	}
	// 等待队列：按入队顺序（调用方已做加权排序）保持 FIFO。
	queue := make(chan Job, len(jobs))
	for _, job := range jobs {
		queue <- job
	}
	close(queue)

	var mu sync.Mutex
	results := make(map[string]engine.ConnResult, len(jobs))
	var errs []error

	var wg sync.WaitGroup
	for worker := 0; worker < concurrency && worker < len(jobs); worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range queue {
				res, err := runner(job)
				mu.Lock()
				if err != nil {
					errs = append(errs, err)
				}
				results[job.Obs.ConnID] = res
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	ordered := make([]engine.ConnResult, 0, len(jobs))
	ids := make([]string, 0, len(jobs))
	for id := range results {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		ordered = append(ordered, results[id])
	}
	sort.Slice(errs, func(i, j int) bool { return errs[i].Error() < errs[j].Error() })
	return ordered, errs
}

package scheduler

import (
	"sync/atomic"
	"testing"

	"mitmpin/internal/engine"
	"mitmpin/internal/generator"
	"mitmpin/internal/sample"
)

func observations(t *testing.T) []sample.Observation {
	m, err := generator.BuildMaterial()
	if err != nil {
		t.Fatal(err)
	}
	return []sample.Observation{
		generator.LongConn(m, "conn-long", 10),
		generator.NormalConn(m, "conn-short-a"),
		generator.NormalConn(m, "conn-short-b"),
	}
}

func TestPlan_WeightedOrderAndEventBudget(t *testing.T) {
	obs := observations(t)
	plan := NewPlan(obs, sample.Budget{Concurrency: 2, GlobalMemory: 100000, MaxEventPerConn: 6})

	// 加权排队：4 事件连接应排在 10 事件连接之前。
	if plan.Items[0].EventCount != 4 || plan.Items[2].EventCount != 10 {
		t.Fatalf("应按事件数升序排队，实际 %+v", plan.Items)
	}
	long, _ := plan.Find("conn-long")
	if long.BudgetCode != sample.E_EVENT_BUDGET || long.AllowedEvents != 6 {
		t.Fatalf("超长连接应 E_EVENT_BUDGET 且截断到 6，实际 %+v", long)
	}
}

func TestPlan_GlobalBudgetDegradesFewestFirst(t *testing.T) {
	obs := observations(t)
	// 全局预算只够两条短连接（各 4，共 8），长连接被降级。
	plan := NewPlan(obs, sample.Budget{Concurrency: 1, GlobalMemory: 8, MaxEventPerConn: 100})

	short, _ := plan.Find("conn-short-a")
	if short.BudgetCode != sample.E_OK {
		t.Fatalf("短连接应正常通过，实际 %s", short.BudgetCode)
	}
	long, _ := plan.Find("conn-long")
	if long.BudgetCode != sample.E_GLOBAL_BUDGET {
		t.Fatalf("全局预算不足应 E_GLOBAL_BUDGET，实际 %s", long.BudgetCode)
	}
	if long.AllowedEvents != 0 {
		t.Fatalf("预算耗尽时长连接允许事件应为 0，实际 %d", long.AllowedEvents)
	}
}

func TestPlan_StableAcrossConcurrency(t *testing.T) {
	obs := observations(t)
	plan1 := NewPlan(obs, sample.Budget{Concurrency: 1, GlobalMemory: 9, MaxEventPerConn: 5})
	plan8 := NewPlan(obs, sample.Budget{Concurrency: 8, GlobalMemory: 9, MaxEventPerConn: 5})

	for i := range plan1.Items {
		a, b := plan1.Items[i], plan8.Items[i]
		if a.ConnID != b.ConnID || a.BudgetCode != b.BudgetCode || a.AllowedEvents != b.AllowedEvents {
			t.Fatalf("并发度变化时业务码序列必须稳定: %+v vs %+v", a, b)
		}
	}
}

func TestRun_WaitingQueueNotRejecting(t *testing.T) {
	obs := observations(t)
	jobs := make([]Job, len(obs))
	for i, o := range obs {
		jobs[i] = Job{Obs: o, MaxEvents: len(o.Events)}
	}
	var concurrent int32
	var maxObserved int32
	runner := func(job Job) (engine.ConnResult, error) {
		cur := atomic.AddInt32(&concurrent, 1)
		for {
			old := atomic.LoadInt32(&maxObserved)
			if cur <= old || atomic.CompareAndSwapInt32(&maxObserved, old, cur) {
				break
			}
		}
		defer atomic.AddInt32(&concurrent, -1)
		return engine.ConnResult{ConnID: job.Obs.ConnID, FinalCode: sample.E_OK,
			ProcessedEvents: len(job.Obs.Events), Trace: []engine.TraceEntry{}}, nil
	}

	results, errs := Run(jobs, 2, runner)
	if len(errs) != 0 {
		t.Fatalf("等待队列不应产生错误: %v", errs)
	}
	if len(results) != len(jobs) {
		t.Fatalf("超并发连接必须入队处理而非拒绝，期望 %d 实际 %d", len(jobs), len(results))
	}
	if maxObserved > 2 {
		t.Fatalf("实际并发度 %d 超过上限 2", maxObserved)
	}
	// 结果按连接标识稳定排序。
	if results[0].ConnID != "conn-long" {
		t.Fatalf("结果应按连接标识排序，实际首条 %s", results[0].ConnID)
	}
}

package pipeline

import (
	"path/filepath"
	"testing"

	"mitmpin/internal/generator"
	"mitmpin/internal/sample"
	"mitmpin/internal/store"
)

func freshPipeline(t *testing.T) *Pipeline {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "b.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return New(st)
}

func TestBudgetDemo_TwoCodesAndStable(t *testing.T) {
	m, _ := generator.BuildMaterial()
	base, _ := generator.Batch(m)
	base.Observations = append(base.Observations,
		generator.LongConn(m, "conn-long-a", 12),
		generator.LongConn(m, "conn-long-b", 20))

	run := func(concurrency int) (map[string]string, map[string]int) {
		p := freshPipeline(t)
		result, err := p.Run(base, sample.Budget{
			Concurrency: concurrency, GlobalMemory: 60, MaxEventPerConn: 8,
		})
		if err != nil {
			t.Fatal(err)
		}
		codes := make(map[string]string)
		allowed := make(map[string]int)
		for _, item := range result.BudgetPlan.Items {
			codes[item.ConnID] = item.BudgetCode
			allowed[item.ConnID] = item.AllowedEvents
		}
		return codes, allowed
	}

	codes1, allowed1 := run(1)
	codes8, allowed8 := run(8)
	for connID := range codes1 {
		if codes1[connID] != codes8[connID] || allowed1[connID] != allowed8[connID] {
			t.Fatalf("并发度变化时预算裁决不稳定: %s %s/%d vs %s/%d",
				connID, codes1[connID], allowed1[connID], codes8[connID], allowed8[connID])
		}
	}
	if codes1["conn-long-a"] != sample.E_EVENT_BUDGET ||
		codes1["conn-long-b"] != sample.E_EVENT_BUDGET {
		t.Fatalf("超长连接应 E_EVENT_BUDGET，实际 %s %s",
			codes1["conn-long-a"], codes1["conn-long-b"])
	}

	// 全局预算耗尽：只给极少预算，长连接被按事件数从少到多降级。
	p := freshPipeline(t)
	small, _ := generator.Batch(m)
	small.Observations = append(small.Observations,
		generator.LongConn(m, "conn-long-a", 12),
		generator.LongConn(m, "conn-long-b", 20))
	result, err := p.Run(small, sample.Budget{
		Concurrency: 2, GlobalMemory: 10, MaxEventPerConn: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := result.BudgetPlan
	degradedGlobal := 0
	for _, item := range plan.Items {
		if item.BudgetCode == sample.E_GLOBAL_BUDGET {
			degradedGlobal++
		}
	}
	if degradedGlobal == 0 {
		t.Fatal("全局预算不足时必须出现 E_GLOBAL_BUDGET 降级连接")
	}
}

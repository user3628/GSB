// Package pipeline 编排观测加载、策略应用、固定校验、快照回放与报告导出。
package pipeline

import (
	"fmt"
	"sort"

	"mitmpin/internal/engine"
	"mitmpin/internal/policy"
	"mitmpin/internal/replay"
	"mitmpin/internal/report"
	"mitmpin/internal/sample"
	"mitmpin/internal/scheduler"
	"mitmpin/internal/store"
)

// Fault 种类常量，与配置/命令行注入字段对齐。
const (
	FaultPinMismatch        = "pin_mismatch"
	FaultHotUpdateRace      = "hot_update_race"
	FaultChainBroken        = "chain_broken"
	FaultEventSeqGap        = "event_seq_gap"
	FaultSnapshotVersionGap = "snapshot_version_gap"
)

// Result 是一次管线运行的完整产物。
type Result struct {
	Report       report.Report
	Replays      []replay.ConnReplay
	Results      []engine.ConnResult
	Bindings     map[string]sample.Snapshot
	BudgetPlan   scheduler.Plan
	SnapshotInfo []report.SnapshotInfo
	// FaultCodes 记录注入故障直接导致的业务码（按连接标识）。
	FaultCodes map[string]string
}

// Pipeline 持有存储与运行状态。
type Pipeline struct {
	store *store.Store
	eng   *engine.Engine
}

// New 构造管线。
func New(st *store.Store) *Pipeline {
	return &Pipeline{store: st, eng: engine.New(st)}
}

// Store 暴露底层存储，便于宿主服务做检查点/快照操作。
func (p *Pipeline) Store() *store.Store { return p.store }

// commitSnapshots 按版本顺序在独立事务中提交初始快照与热更新快照。
// 任一更新被拒绝（空更新/版本冲突）时整体不推进已提交版本链。
func (p *Pipeline) commitSnapshots(b sample.Batch) error {
	snaps := append([]sample.Snapshot(nil), b.Snapshots...)
	for _, update := range b.Updates {
		snaps = append(snaps, update.Snapshot)
	}
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].Version < snaps[j].Version })
	for _, snap := range snaps {
		if err := p.store.CommitSnapshot(snap); err != nil {
			return fmt.Errorf("策略快照提交失败: %w", err)
		}
	}
	return nil
}

// bindPolicies 按全局调度序（观测切片顺序）应用热更新，确定每条连接绑定的快照。
// 已进入握手阶段的连接沿用开始时版本；新连接使用最新版本。
func bindPolicies(observations []sample.Observation, b sample.Batch) map[string]sample.Snapshot {
	updates := append([]sample.UpdateStep(nil), b.Updates...)
	sort.Slice(updates, func(i, j int) bool { return updates[i].AfterSeq < updates[j].AfterSeq })

	latest := policy.LatestSnapshot(b.Snapshots)
	bindings := make(map[string]sample.Snapshot, len(observations))
	updateIdx := 0
	for order, obs := range observations {
		// order 为 0 基；AfterSeq=N 表示第 N 个连接开始前生效，即 order>=N 时采用新快照。
		for updateIdx < len(updates) && order >= updates[updateIdx].AfterSeq {
			latest = updates[updateIdx].Snapshot
			updateIdx++
		}
		bindings[obs.ConnID] = latest
	}
	return bindings
}

func removeIntermediate(pemStr string) string {
	blocks := decodeBlocks(pemStr)
	if len(blocks) <= 2 {
		return pemStr
	}
	// 保留叶（首）与根（末），移除中间证书。
	return blocks[0] + blocks[len(blocks)-1]
}

// Run 执行完整管线并产出可回放结果与报告。
func (p *Pipeline) Run(b sample.Batch, budget sample.Budget) (*Result, error) {
	if err := sample.ValidateBatch(b); err != nil {
		return nil, err
	}
	batch := cloneBatch(b)
	faultCodes := applyFaults(&batch)
	// 注入口仅校验快照本身；观测级异常（序号跳变等）下沉到逐连接解析，保证同批隔离。
	for _, snap := range batch.Snapshots {
		if err := sample.ValidateSnapshot(snap); err != nil {
			return nil, err
		}
	}
	for _, update := range batch.Updates {
		if err := sample.ValidateSnapshot(update.Snapshot); err != nil {
			return nil, err
		}
	}

	if err := p.commitSnapshots(batch); err != nil {
		return nil, err
	}
	for _, obs := range batch.Observations {
		if err := p.store.SaveObservation(obs); err != nil {
			return nil, err
		}
	}

	bindings := bindPolicies(batch.Observations, batch)
	plan := scheduler.NewPlan(batch.Observations, budget)

	// 调度任务按加权规划顺序入队，超并发连接在等待队列中排队。
	jobs := make([]scheduler.Job, 0, len(batch.Observations))
	for _, item := range plan.Items {
		obs := findObservation(batch.Observations, item.ConnID)
		jobs = append(jobs, scheduler.Job{
			Obs: obs, Bound: bindingFor(item.ConnID, bindings[item.ConnID]),
			MaxEvents: item.AllowedEvents,
		})
	}

	runner := func(job scheduler.Job) (engine.ConnResult, error) {
		result, err := p.eng.Process(job.Obs, job.Bound, job.MaxEvents)
		// 解析类错误（策略缺失/序号跳变/链损坏）记录终态但不中断整批。
		if err != nil {
			result.ConnID = job.Obs.ConnID
			result.PolicyVersion = job.Bound.Version
			if result.FinalCode == "" || result.FinalCode == sample.E_OK {
				result.FinalCode = sample.CodeOf(err)
			}
			if result.Trace == nil {
				result.Trace = []engine.TraceEntry{}
			}
		}
		// 预算降级码覆盖：全局预算优先，其次单连接事件预算。
		if planned, ok := plan.Find(job.Obs.ConnID); ok &&
			(planned.BudgetCode == sample.E_GLOBAL_BUDGET || planned.BudgetCode == sample.E_EVENT_BUDGET) &&
			result.FinalCode == sample.E_OK {
			result.FinalCode = planned.BudgetCode
		}
		return result, nil
	}
	results, runErrs := scheduler.Run(jobs, budget.Concurrency, runner)
	if len(runErrs) > 0 {
		return nil, runErrs[0]
	}

	// 交互侧独立重放并与结算侧逐条比对。
	settled := make(map[string]engine.ConnResult, len(results))
	for _, r := range results {
		settled[r.ConnID] = r
	}
	replays, err := replay.BatchReplay(batch.Observations, bindings, settled)
	if err != nil {
		return nil, err
	}

	snapshotInfo, err := p.loadSnapshotInfo()
	if err != nil {
		return nil, err
	}
	budgetCodes := make(map[string]string)
	for _, item := range plan.Items {
		if item.BudgetCode != sample.E_OK {
			budgetCodes[item.ConnID] = item.BudgetCode
		}
	}
	rep := report.Build(report.Options{
		Batch: batch, Budget: budget, Snapshots: snapshotInfo,
		Results: results, BudgetCodes: budgetCodes, Replays: replays,
	})

	return &Result{
		Report: rep, Replays: replays, Results: results, Bindings: bindings,
		BudgetPlan: plan, SnapshotInfo: snapshotInfo, FaultCodes: faultCodes,
	}, nil
}

func (p *Pipeline) loadSnapshotInfo() ([]report.SnapshotInfo, error) {
	stored, err := p.store.ListSnapshots()
	if err != nil {
		return nil, err
	}
	info := make([]report.SnapshotInfo, 0, len(stored))
	for _, item := range stored {
		info = append(info, report.SnapshotInfo{
			Version: item.Snapshot.Version, Marker: item.Snapshot.Marker,
			ContentHash: item.ContentHash,
		})
	}
	return info, nil
}

func findObservation(obs []sample.Observation, connID string) sample.Observation {
	for _, o := range obs {
		if o.ConnID == connID {
			return o
		}
	}
	return sample.Observation{}
}

func cloneBatch(b sample.Batch) sample.Batch {
	// 深拷贝，确保故障注入不污染调用方输入。
	data := jsonMarshal(b)
	var cloned sample.Batch
	jsonUnmarshal(data, &cloned)
	return cloned
}

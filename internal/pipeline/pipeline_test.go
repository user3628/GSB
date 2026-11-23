package pipeline

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"mitmpin/internal/engine"
	"mitmpin/internal/generator"
	"mitmpin/internal/report"
	"mitmpin/internal/sample"
	"mitmpin/internal/store"
)

func openPipeline(t *testing.T) (*Pipeline, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "pipeline.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return New(st), st
}

func standardBudget() sample.Budget {
	return sample.Budget{Concurrency: 4, GlobalMemory: 100000, MaxEventPerConn: 100}
}

func resultCode(results []engine.ConnResult, connID string) string {
	for _, r := range results {
		if r.ConnID == connID {
			return r.FinalCode
		}
	}
	return "NOT_FOUND"
}

func TestRun_HotUpdateBindingAndReplay(t *testing.T) {
	p, _ := openPipeline(t)
	m, _ := generator.BuildMaterial()
	batch, err := generator.Batch(m)
	if err != nil {
		t.Fatal(err)
	}
	result, err := p.Run(batch, standardBudget())
	if err != nil {
		t.Fatalf("管线运行失败: %v", err)
	}

	// 前 4 条连接绑定 v1，其后绑定 v2（热更新在第 5 条开始前生效）。
	for i, obs := range batch.Observations {
		bound := result.Bindings[obs.ConnID]
		want := 1
		if i >= 4 {
			want = 2
		}
		if bound.Version != want {
			t.Fatalf("连接 %s 应绑定 v%d，实际 v%d", obs.ConnID, want, bound.Version)
		}
	}
	// 每条事件轨迹都携带绑定策略版本。
	for _, conn := range result.Results {
		if len(conn.Trace) == 0 {
			t.Fatalf("连接 %s 轨迹为空", conn.ConnID)
		}
		for _, entry := range conn.Trace {
			if entry.PolicyVersion != conn.PolicyVersion {
				t.Fatalf("连接 %s 事件 %d 策略版本不可追溯", conn.ConnID, entry.Seq)
			}
		}
	}
	// 交互侧重放与结算侧逐条一致。
	for _, r := range result.Replays {
		if !r.AllConsistent {
			t.Fatalf("连接 %s 重放结论与结算侧不一致: %+v", r.ConnID, r.Checks)
		}
	}
	// 终态业务码断言。
	cases := map[string]string{
		"conn-normal-1": sample.E_OK,
		"conn-mitm-1":   sample.E_MITM_SUSPECTED,
		"conn-broken-1": sample.E_CHAIN_BROKEN,
		"conn-seqgap-1": sample.E_EVENT_SEQ_GAP,
		"conn-sni-1":    sample.E_SNI_MISMATCH,
		"conn-proto-1":  sample.E_PROTOCOL_NOT_ALLOWED,
	}
	for connID, want := range cases {
		if got := resultCode(result.Results, connID); got != want {
			t.Fatalf("连接 %s 期望 %s 实际 %s", connID, want, got)
		}
	}
}

func TestRun_ReportByteDeterministic(t *testing.T) {
	m, _ := generator.BuildMaterial()
	batch, _ := generator.Batch(m)

	runOnce := func() []byte {
		dir := t.TempDir()
		st, err := store.Open(filepath.Join(dir, "d.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		p := New(st)
		result, err := p.Run(batch, standardBudget())
		if err != nil {
			t.Fatal(err)
		}
		data, err := result.Report.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		return data
	}

	first := runOnce()
	second := runOnce()
	if string(first) != string(second) {
		t.Fatal("相同输入与参数下报告必须字节级一致")
	}
	// 报告结构含快照版本链与逐连接重放结论。
	var rep report.Report
	if err := json.Unmarshal(first, &rep); err != nil {
		t.Fatal(err)
	}
	if len(rep.Snapshots) != 2 || rep.Snapshots[0].Version != 1 || rep.Snapshots[1].Version != 2 {
		t.Fatalf("快照版本链异常: %+v", rep.Snapshots)
	}
}

func TestFaultInjection_FiveKinds(t *testing.T) {
	m, _ := generator.BuildMaterial()
	base, _ := generator.Batch(m)

	cases := []struct {
		kind   string
		target string
		code   string
	}{
		{FaultPinMismatch, "conn-normal-1", sample.E_MITM_SUSPECTED},
		{FaultHotUpdateRace, "conn-normal-2", sample.E_OK},
		{FaultChainBroken, "conn-normal-3", sample.E_CHAIN_BROKEN},
		{FaultEventSeqGap, "conn-normal-1", sample.E_EVENT_SEQ_GAP},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			p, st := openPipeline(t)
			batch := cloneBatch(base)
			batch.Faults = []sample.FaultSpec{{Kind: tc.kind, Target: tc.target}}
			result, err := p.Run(batch, standardBudget())
			if err != nil {
				t.Fatalf("故障注入运行失败: %v", err)
			}
			if got := resultCode(result.Results, tc.target); got != tc.code {
				t.Fatalf("故障 %s 期望 %s 实际 %s", tc.kind, tc.code, got)
			}
			// 同批次正常连接终态未被污染。
			for _, conn := range result.Results {
				if conn.ConnID == tc.target {
					continue
				}
				expected := expectedCleanCode(conn.ConnID)
				if conn.FinalCode != expected {
					t.Fatalf("故障 %s 污染了连接 %s：期望 %s 实际 %s",
						tc.kind, conn.ConnID, expected, conn.FinalCode)
				}
			}
			// 已提交策略快照：热更新竞争会推进版本，其余不影响快照数量基线。
			snaps, _ := st.ListSnapshots()
			if tc.kind == FaultHotUpdateRace {
				if len(snaps) != 3 {
					t.Fatalf("热更新竞争应提交 3 个快照，实际 %d", len(snaps))
				}
			}
		})
	}
}

func TestFaultInjection_SnapshotVersionGap(t *testing.T) {
	p, st := openPipeline(t)
	m, _ := generator.BuildMaterial()
	batch, _ := generator.Batch(m)
	result, err := p.Run(batch, standardBudget())
	if err != nil {
		t.Fatal(err)
	}
	target := "conn-normal-1"
	if err := p.InjectSnapshotGap(target); err != nil {
		t.Fatal(err)
	}
	bound := result.Bindings[target]
	obs := findObservation(batch.Observations, target)
	_, err = p.eng.Process(obs, bound, 100)
	if sample.CodeOf(err) != sample.E_SNAPSHOT_VERSION {
		t.Fatalf("期望 E_SNAPSHOT_VERSION，实际 %v", err)
	}
	// 已提交策略快照版本未被推进。
	snaps, _ := st.ListSnapshots()
	if len(snaps) != 2 {
		t.Fatalf("快照版本不应因检查点故障推进，实际 %d", len(snaps))
	}
}

func expectedCleanCode(connID string) string {
	switch connID {
	case "conn-mitm-1":
		return sample.E_MITM_SUSPECTED
	case "conn-broken-1":
		return sample.E_CHAIN_BROKEN
	case "conn-seqgap-1":
		return sample.E_EVENT_SEQ_GAP
	case "conn-sni-1":
		return sample.E_SNI_MISMATCH
	case "conn-proto-1":
		return sample.E_PROTOCOL_NOT_ALLOWED
	default:
		return sample.E_OK
	}
}

package engine

import (
	"path/filepath"
	"testing"

	"mitmpin/internal/generator"
	"mitmpin/internal/sample"
	"mitmpin/internal/store"
)

func newEngine(t *testing.T) (*Engine, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "engine.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return New(st), st
}

func TestProcess_ReplayAndResume(t *testing.T) {
	eng, st := newEngine(t)
	m, _ := generator.BuildMaterial()
	snap, _ := generator.SnapshotV1(m)
	obs := generator.NormalConn(m, "conn-resume")
	mustSave(t, st, obs)

	result, err := eng.Process(obs, snap, 0)
	if err != nil {
		t.Fatalf("首次处理失败: %v", err)
	}
	if result.FinalCode != sample.E_OK || result.ProcessedEvents != 4 {
		t.Fatalf("首次处理结论异常: %+v", result)
	}

	// 模拟重启：新引擎实例从检查点恢复，已提交事件不重做。
	eng2 := New(st)
	result2, err := eng2.Process(obs, snap, 0)
	if err != nil {
		t.Fatalf("恢复处理失败: %v", err)
	}
	if result2.ProcessedEvents != 4 {
		t.Fatalf("恢复后事件数应为 4，实际 %d", result2.ProcessedEvents)
	}
	cps, _ := st.ListCheckpoints(obs.ConnID)
	if len(cps) != 4 {
		t.Fatalf("检查点应保持 4 个，实际 %d", len(cps))
	}
}

func TestProcess_SnapshotGapRecovery(t *testing.T) {
	eng, st := newEngine(t)
	m, _ := generator.BuildMaterial()
	snap, _ := generator.SnapshotV1(m)
	obs := generator.LongConn(m, "conn-long", 6)
	mustSave(t, st, obs)

	// 先处理前 3 个事件并落检查点。
	if _, err := eng.Process(obs, snap, 3); err != nil {
		t.Fatal(err)
	}
	// 注入检查点版本跳号：把第 3 个检查点改成版本 5。
	if _, err := st.DB().Exec(
		`UPDATE checkpoints SET checkpoint_version = 5 WHERE conn_id = ? AND checkpoint_version = 3`,
		obs.ConnID); err != nil {
		t.Fatal(err)
	}

	// 恢复应返回 E_SNAPSHOT_VERSION，并回退到最近连续版本（1、2）。
	_, err := eng.Process(obs, snap, 0)
	if sample.CodeOf(err) != sample.E_SNAPSHOT_VERSION {
		t.Fatalf("期望 E_SNAPSHOT_VERSION，实际 %v", err)
	}
	cps, _ := st.ListCheckpoints(obs.ConnID)
	if len(cps) != 2 {
		t.Fatalf("跳号回退后应保留 2 个连续检查点，实际 %d", len(cps))
	}

	// 再次恢复：从最近连续版本继续，已提交结论不回退，最终成功完成。
	result, err := eng.Process(obs, snap, 0)
	if err != nil {
		t.Fatalf("回退后重做失败: %v", err)
	}
	if result.ProcessedEvents != 6 {
		t.Fatalf("重做后应处理全部 6 个事件，实际 %d", result.ProcessedEvents)
	}
}

func TestProcess_TerminalCodes(t *testing.T) {
	eng, st := newEngine(t)
	m, _ := generator.BuildMaterial()
	snap, _ := generator.SnapshotV1(m)

	mitm := generator.MITMConn(m, "conn-mitm")
	mustSave(t, st, mitm)
	result, err := eng.Process(mitm, snap, 0)
	if err != nil {
		t.Fatal(err)
	}
	if result.FinalCode != sample.E_MITM_SUSPECTED {
		t.Fatalf("期望 E_MITM_SUSPECTED，实际 %s", result.FinalCode)
	}
	// 致命码在 pin_check 处终止，握手完成事件不应出现。
	for _, entry := range result.Trace {
		if entry.Type == sample.EvHandshakeDone {
			t.Fatal("MITM 终止后不应出现握手完成事件")
		}
	}
}

func mustSave(t *testing.T, st *store.Store, obs sample.Observation) {
	t.Helper()
	if err := st.SaveObservation(obs); err != nil {
		t.Fatal(err)
	}
}

package store

import (
	"path/filepath"
	"testing"

	"mitmpin/internal/generator"
	"mitmpin/internal/sample"
)

func openTemp(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	st, err := Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestCommitSnapshot_TransactionAndReplacement(t *testing.T) {
	st := openTemp(t)
	m, err := generator.BuildMaterial()
	if err != nil {
		t.Fatal(err)
	}
	v1, _ := generator.SnapshotV1(m)
	v2 := generator.SnapshotV2(m)

	if err := st.CommitSnapshot(v1); err != nil {
		t.Fatalf("v1 提交失败: %v", err)
	}
	// 相同内容再次提交 -> E_POLICY_UNCHANGED，版本链不推进。
	again := v1
	again.Version = 2
	if sample.CodeOf(st.CommitSnapshot(again)) != sample.E_POLICY_UNCHANGED {
		t.Fatal("内容未变化应拒绝并返回 E_POLICY_UNCHANGED")
	}
	// 版本号回退 -> 冲突。
	if err := st.CommitSnapshot(v1); sample.CodeOf(err) != sample.E_POLICY_VERSION_CONFLICT &&
		sample.CodeOf(err) != sample.E_POLICY_UNCHANGED {
		t.Fatalf("重复/回退版本应被拒绝，实际 %v", err)
	}
	// 整体替换内容提交成功。
	if err := st.CommitSnapshot(v2); err != nil {
		t.Fatalf("v2 提交失败: %v", err)
	}
	snaps, err := st.ListSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 2 {
		t.Fatalf("应只有 2 个快照，实际 %d", len(snaps))
	}
	chain, err := st.SnapshotChain()
	if err != nil || len(chain) != 2 {
		t.Fatalf("快照版本链异常: %v %v", chain, err)
	}
}

func TestCheckpointGap_Rollback(t *testing.T) {
	st := openTemp(t)
	connID := "conn-gap"
	for seq := 1; seq <= 3; seq++ {
		if err := st.SaveCheckpoint(Checkpoint{
			ConnID: connID, CheckpointVer: seq, EventSeq: seq,
			EventType: sample.EvPinCheck, PolicyVersion: 1,
			TraceJSON: "[]", Marker: "m",
		}); err != nil {
			t.Fatal(err)
		}
	}
	// 制造跳号：第 4 个检查点版本写成 5。
	if err := st.SaveCheckpoint(Checkpoint{
		ConnID: connID, CheckpointVer: 5, EventSeq: 4,
		EventType: sample.EvPinCheck, PolicyVersion: 1,
		TraceJSON: "[]", Marker: "m",
	}); err != nil {
		t.Fatal(err)
	}
	cps, _ := st.ListCheckpoints(connID)
	if len(cps) != 4 {
		t.Fatalf("应有 4 个检查点，实际 %d", len(cps))
	}
	if err := st.RollbackCheckpointsAfter(connID, 3); err != nil {
		t.Fatal(err)
	}
	cps, _ = st.ListCheckpoints(connID)
	if len(cps) != 3 {
		t.Fatalf("回退后应保留 3 个连续检查点，实际 %d", len(cps))
	}
}

package router

import (
	"path/filepath"
	"testing"

	"gsb/internal/format"
	"gsb/internal/gwerr"
	"gsb/internal/store"
)

func setup(t *testing.T) (*Router, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return New(st), st
}

func rec(id, tc string, lastWrite int64) format.RouteRecord {
	return format.RouteRecord{
		ModelID: id, Endpoint: "http://127.0.0.1:1",
		VRAMBytes: 8192 << 20, Concurrency: 8, Toolchain: tc,
		BaseTimeMs: 1735689600000, RetentionMs: 259200000,
		ExpiresAtMs: 1735689600000 + 259200000, LastWriteMs: lastWrite,
	}
}

func TestImportInsertAndVersionBump(t *testing.T) {
	r, _ := setup(t)
	entries, version, err := r.ImportBatch([]format.RouteRecord{rec("m1", "tc-1.0.0", 100)})
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 || entries[0].Outcome != OutcomeInsert {
		t.Fatalf("version=%d entries=%+v", version, entries)
	}
	if r.Version() != 1 {
		t.Fatal("内存版本未推进")
	}
}

func TestConflictToolchainPriority(t *testing.T) {
	r, _ := setup(t)
	_, _, _ = r.ImportBatch([]format.RouteRecord{rec("m1", "tc-1.0.0", 100)})
	// 更高工具链 → 覆盖
	entries, _, err := r.ImportBatch([]format.RouteRecord{rec("m1", "tc-2.0.0", 50)})
	if err != nil || entries[0].Outcome != OutcomeOverwrite {
		t.Fatalf("期望覆盖: %v %+v", err, entries)
	}
	// 更低工具链 → 跳过
	entries, _, err = r.ImportBatch([]format.RouteRecord{rec("m1", "tc-1.0.0", 999)})
	if err != nil || entries[0].Outcome != OutcomeSkip {
		t.Fatalf("期望跳过: %v %+v", err, entries)
	}
}

func TestConflictLastWriteTiebreak(t *testing.T) {
	r, _ := setup(t)
	_, _, _ = r.ImportBatch([]format.RouteRecord{rec("m1", "tc-1.0.0", 100)})
	in := rec("m1", "tc-1.0.0", 200)
	in.VRAMBytes = 4096 << 20
	entries, _, err := r.ImportBatch([]format.RouteRecord{in})
	if err != nil || entries[0].Outcome != OutcomeOverwrite {
		t.Fatalf("期望按最后写入时间覆盖: %v %+v", err, entries)
	}
}

func TestConflictRejectRollsBackBatch(t *testing.T) {
	r, st := setup(t)
	_, _, _ = r.ImportBatch([]format.RouteRecord{rec("m1", "tc-1.0.0", 100)})
	// 平局但内容不同 → 拒绝，整批回滚
	bad := rec("m1", "tc-1.0.0", 100)
	bad.VRAMBytes = 4096 << 20
	_, version, err := r.ImportBatch([]format.RouteRecord{
		rec("m2", "tc-1.0.0", 100), bad,
	})
	ge, ok := err.(*gwerr.Error)
	if !ok || ge.Code != gwerr.EConflictingImport {
		t.Fatalf("期望 E_CONFLICTING_IMPORT, 得到 %v", err)
	}
	if version != 1 || r.Version() != 1 {
		t.Fatalf("版本未保持: %d", r.Version())
	}
	if _, err := r.Decide("m2"); err == nil {
		t.Fatal("整批回滚失败: m2 不应存在")
	}
	if st.TableVersion() != 1 {
		t.Fatal("SQLite 版本未保持")
	}
}

func TestSnapshotSwitchAndValidation(t *testing.T) {
	r, st := setup(t)
	_, _, _ = r.ImportBatch([]format.RouteRecord{rec("m1", "tc-1.0.0", 100)})
	_ = st.RecordHit("m1", 1)

	snap := &SnapshotFile{
		Format: "grt-snapshot", FormatVersion: 2, SnapshotSeq: 1,
		GeneratedMs: 1735689600000,
		Records:     []format.RouteRecord{rec("m9", "tc-2.0.0", 300)},
	}
	version, err := r.LoadSnapshot(snap)
	if err != nil {
		t.Fatal(err)
	}
	if version != 2 || r.SnapshotSeq() != 1 {
		t.Fatalf("切换失败: version=%d seq=%d", version, r.SnapshotSeq())
	}
	if _, err := r.Decide("m1"); err == nil {
		t.Fatal("旧路由未释放")
	}
	if _, err := r.Decide("m9"); err != nil {
		t.Fatal("新快照路由未生效")
	}
	hits, _ := st.LoadHits()
	if len(hits) != 0 {
		t.Fatal("派生统计未在切换后释放")
	}
	// 序号单调：旧序号拒绝
	if _, err := r.LoadSnapshot(&SnapshotFile{
		Format: "grt-snapshot", FormatVersion: 2, SnapshotSeq: 1,
	}); err == nil || err.(*gwerr.Error).Code != gwerr.ESnapshotInvalid {
		t.Fatalf("期望 E_SNAPSHOT_INVALID, 得到 %v", err)
	}
	// 不兼容版本拒绝且旧快照保持可用
	if _, err := r.LoadSnapshot(&SnapshotFile{
		Format: "grt-snapshot", FormatVersion: 99, SnapshotSeq: 5,
	}); err == nil || err.(*gwerr.Error).Code != gwerr.ESnapshotInvalid {
		t.Fatalf("期望 E_SNAPSHOT_INVALID, 得到 %v", err)
	}
	if _, err := r.Decide("m9"); err != nil {
		t.Fatal("校验失败后旧快照不可用")
	}
}

func TestRecoveryAfterForcedKill(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "crash.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	r := New(st)
	_, _, _ = r.ImportBatch([]format.RouteRecord{rec("m1", "tc-1.0.0", 100)})
	// 模拟强制终止：写入挂起意图后进程退出（不提交数据事务）
	if _, err := st.BeginIntent("import", "target_version=2 records=1"); err != nil {
		t.Fatal(err)
	}
	st.Close()

	// 重启：扫描未完成意图并回滚
	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	r2 := New(st2)
	rep, err := r2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.RolledBack) != 1 {
		t.Fatalf("应回滚 1 条意图: %+v", rep)
	}
	if rep.CompleteVersion != 1 || r2.Version() != 1 {
		t.Fatalf("未回滚到最后完整版本: %d", rep.CompleteVersion)
	}
	if _, err := r2.Decide("m1"); err != nil {
		t.Fatal("旧路由表不可用")
	}
	// 再次恢复应无未完成意图
	rep2, _ := r2.Recover()
	if len(rep2.RolledBack) != 0 {
		t.Fatal("恢复报告应只基于已落盘事实")
	}
}

func TestReplayDeterministicDecision(t *testing.T) {
	r, _ := setup(t)
	recs := []format.RouteRecord{rec("m1", "tc-1.0.0", 100), rec("m2", "tc-1.0.0", 100)}
	_, _, _ = r.ImportBatch(recs)
	// 后端不可达时回放返回 E_BACKEND_UNAVAILABLE
	_, err := r.Replay(42, 4)
	if ge, ok := err.(*gwerr.Error); !ok || ge.Code != gwerr.EBackendUnavailable {
		t.Fatalf("期望 E_BACKEND_UNAVAILABLE, 得到 %v", err)
	}
}

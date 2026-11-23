package capver

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCommitSnapshotVersioning(t *testing.T) {
	s := openTestStore(t)
	p := GenerateSamplePolicy()

	v1, err := s.CommitSnapshot(p)
	if err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	if v1 != 1 {
		t.Fatalf("expected version 1, got %d", v1)
	}

	// 内容变化 -> 新版本；旧快照整体保留。
	p2 := ApplyInjection(p, InjectUnauthorizedWrite)
	v2, err := s.CommitSnapshot(p2)
	if err != nil {
		t.Fatalf("commit v2: %v", err)
	}
	if v2 != 2 {
		t.Fatalf("expected version 2, got %d", v2)
	}

	snap1, err := s.LoadVersion(1)
	if err != nil {
		t.Fatalf("load v1: %v", err)
	}
	if !snap1.Policy.EqualContent(p) {
		t.Fatal("v1 content mismatch")
	}
	snap2, err := s.LoadVersion(2)
	if err != nil {
		t.Fatalf("load v2: %v", err)
	}
	if !snap2.Policy.EqualContent(p2) {
		t.Fatal("v2 content mismatch")
	}

	// 进程记录随快照保存。
	recs, err := s.ProcessRecords(2)
	if err != nil {
		t.Fatalf("process records: %v", err)
	}
	if len(recs) != len(p2.Processes) {
		t.Fatalf("expected %d process records, got %d", len(p2.Processes), len(recs))
	}
}

func TestSnapshotGapFallback(t *testing.T) {
	s := openTestStore(t)
	p := GenerateSamplePolicy()
	if _, err := s.CommitSnapshot(p); err != nil {
		t.Fatal(err)
	}
	p2 := ApplyInjection(p, InjectChildEscape)
	if _, err := s.CommitSnapshot(p2); err != nil {
		t.Fatal(err)
	}

	// 注入跳号：写入 version 4（跳过 3）。
	gapV, err := s.InjectSnapshotGap(2)
	if err != nil {
		t.Fatalf("inject gap: %v", err)
	}
	if gapV != 4 {
		t.Fatalf("expected gap version 4, got %d", gapV)
	}

	snap, err := s.LoadLatest()
	if CodeOf(err) != CodeSnapshotVersion {
		t.Fatalf("expected E_SNAPSHOT_VERSION, got %v", err)
	}
	if snap == nil || snap.Version != 2 {
		t.Fatalf("expected fallback to version 2, got %+v", snap)
	}

	// 恢复结论：回退到最近连续快照，已提交快照不污染。
	snap2, conclusion, err := s.Recover()
	if conclusion != RecoveryFallback {
		t.Fatalf("expected fallback conclusion, got %s", conclusion)
	}
	if CodeOf(err) != CodeSnapshotVersion {
		t.Fatalf("expected E_SNAPSHOT_VERSION, got %v", err)
	}
	if !snap2.Policy.EqualContent(p2) {
		t.Fatal("fallback snapshot content polluted")
	}

	// 版本链保留跳号供审计。
	versions, last, gap, err := s.Chain()
	if err != nil {
		t.Fatal(err)
	}
	if !gap || last != 2 || len(versions) != 3 {
		t.Fatalf("unexpected chain: versions=%v last=%d gap=%v", versions, last, gap)
	}
}

func TestCrashRecoveryKeepsCommitted(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "crash.db")
	p := GenerateSamplePolicy()

	// 第一次“进程”：提交快照后模拟强制终止（直接关闭）。
	s1, err := OpenStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s1.CommitSnapshot(p); err != nil {
		t.Fatal(err)
	}
	s1.Close()

	// 第二次“进程”：已提交快照不回滚。
	s2, err := OpenStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	snap, conclusion, err := s2.Recover()
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if conclusion != RecoveryClean || snap.Version != 1 {
		t.Fatalf("unexpected recovery: %s v%d", conclusion, snap.Version)
	}
	if !snap.Policy.EqualContent(p) {
		t.Fatal("recovered policy mismatch")
	}
}

func TestUncommittedChangesDiscarded(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tx.db")
	s, err := OpenStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	p := GenerateSamplePolicy()
	if _, err := s.CommitSnapshot(p); err != nil {
		t.Fatal(err)
	}

	// 模拟未提交事务：独立连接开启事务写入后不回滚直接关闭。
	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := raw.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO snapshots(version, hash, commit_id, payload) VALUES(99,'x',99,'{}')"); err != nil {
		t.Fatal(err)
	}
	raw.Close() // 未提交 -> 整体丢弃

	snap, err := s.LoadLatest()
	if err != nil {
		t.Fatalf("load latest: %v", err)
	}
	if snap.Version != 1 {
		t.Fatalf("uncommitted change leaked: got version %d", snap.Version)
	}
	s.Close()
}

func TestContentCompareNotVersionNumber(t *testing.T) {
	s := openTestStore(t)
	p := GenerateSamplePolicy()
	v1, _ := s.CommitSnapshot(p)

	// 语义相同但字段顺序不同的策略 -> 哈希相同（整体替换语义识别）。
	p2 := &Policy{Name: p.Name, Processes: make([]ProcessDecl, len(p.Processes))}
	for i, d := range p.Processes {
		nd := d
		// 逆序能力列表：规范化后内容一致。
		for l, r := 0, len(nd.Capabilities)-1; l < r; l, r = l+1, r-1 {
			nd.Capabilities[l], nd.Capabilities[r] = nd.Capabilities[r], nd.Capabilities[l]
		}
		p2.Processes[len(p.Processes)-1-i] = nd
	}
	if !p.EqualContent(p2) {
		t.Fatal("canonical content should be equal regardless of ordering")
	}
	v2, _ := s.CommitSnapshot(p2)
	snap, err := s.LoadVersion(v2)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Hash == "" || snap.Hash != s.mustHash(t, v1) {
		t.Fatal("hash should match for equal content")
	}
}

func (s *Store) mustHash(t *testing.T, v int64) string {
	t.Helper()
	snap, err := s.LoadVersion(v)
	if err != nil {
		t.Fatal(err)
	}
	return snap.Hash
}

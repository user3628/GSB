package smuggle

import (
	"path/filepath"
	"testing"
)

func openTempStore(t *testing.T) *Store {
	t.Helper()
	st, err := OpenStore(filepath.Join(t.TempDir(), "snap.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestSnapshotVersionChain(t *testing.T) {
	st := openTempStore(t)
	s1, err := st.Commit("c1", []byte(`{"a":1}`))
	if err != nil || s1.Version != 1 {
		t.Fatalf("v1: %v %+v", err, s1)
	}
	s2, err := st.Commit("c1", []byte(`{"a":2}`))
	if err != nil || s2.Version != 2 {
		t.Fatalf("v2: %v %+v", err, s2)
	}
	snaps, code := st.LoadChain("c1")
	if code != CodeOK || len(snaps) != 2 {
		t.Fatalf("chain: %s %d", code, len(snaps))
	}
	if !IsWholesaleReplacement(snaps[0], snaps[1]) {
		t.Fatal("expected wholesale replacement detection on content change")
	}
	if IsWholesaleReplacement(snaps[1], snaps[1]) {
		t.Fatal("identical snapshot must not be a replacement")
	}
}

func TestSnapshotVersionGap(t *testing.T) {
	st := openTempStore(t)
	if _, err := st.Commit("c1", []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Commit("c1", []byte(`{"a":2}`)); err != nil {
		t.Fatal(err)
	}
	// 直接注入跳号版本，模拟版本链断裂。
	if _, err := st.db.Exec(`INSERT INTO snapshots(conn_id,version,payload,hash,marker) VALUES('c1',5,'{}','x','m')`); err != nil {
		t.Fatal(err)
	}
	snaps, code := st.LoadChain("c1")
	if code != CodeSnapshotVersion {
		t.Fatalf("got code=%s", code)
	}
	if len(snaps) != 2 || snaps[len(snaps)-1].Version != 2 {
		t.Fatalf("rollback prefix wrong: %+v", snaps)
	}
}

func TestSnapshotNoPartialOverwrite(t *testing.T) {
	st := openTempStore(t)
	if _, err := st.Commit("c1", []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	// 主键冲突：已提交快照不得被覆盖。
	if _, err := st.db.Exec(`INSERT INTO snapshots(conn_id,version,payload,hash,marker) VALUES('c1',1,'{}','y','m')`); err == nil {
		t.Fatal("expected primary key conflict")
	}
	snaps, code := st.LoadChain("c1")
	if code != CodeOK || len(snaps) != 1 || string(snaps[0].Payload) != `{"a":1}` {
		t.Fatalf("snapshot overwritten: %+v", snaps)
	}
}

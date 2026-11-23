package writeback

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/ntfsacl/internal/audit"
	"example.com/ntfsacl/internal/domain"
	"example.com/ntfsacl/internal/fixture"
	"example.com/ntfsacl/internal/store"
)

type crashSignal struct{}

func setupWorkdir(t *testing.T) (string, audit.Result, int64) {
	t.Helper()
	root := t.TempDir()
	if _, err := fixture.Generate(root, fixture.Options{}); err != nil {
		t.Fatal(err)
	}
	ar, err := audit.Run(context.Background(), root, nil, audit.Config{Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	fs, err := fixture.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(fs.DBPath())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	snap, err := db.CommitSnapshot(context.Background(), store.KindAudit, func(v, pv int64, ph string) ([]byte, error) {
		r, _ := ar.Report(v, pv, ph)
		return r.Marshal()
	})
	if err != nil {
		t.Fatal(err)
	}
	return root, ar, snap.Version
}

func TestSuccessfulWritebackAndSnapshotComparison(t *testing.T) {
	root, ar, version := setupWorkdir(t)
	res, err := Run(context.Background(), root, ar, Options{AuditVersion: version, Owner: "test", LockTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != "COMPLETED" || res.Code != "" || len(res.Plan) != 4 {
		t.Fatalf("unexpected writeback result: %+v", res)
	}
	if encoded, err := json.Marshal(res); err != nil || bytes.Contains(encoded, []byte("null")) {
		t.Fatalf("writeback JSON contains null arrays or failed: %v %s", err, encoded)
	}
	if res.BeforeSnapshotVersion == 0 || res.AfterSnapshotVersion <= res.BeforeSnapshotVersion {
		t.Fatalf("missing before/after snapshots: %+v", res)
	}
	if res.BeforeTreeHash == res.AfterTreeHash {
		t.Fatal("tree hash did not change after writeback")
	}
	after, err := audit.Run(context.Background(), root, nil, audit.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Conflicts) != 0 {
		t.Fatalf("conflicts remain after repair: %+v", after.Conflicts)
	}
	fs, _ := fixture.Open(root)
	db, _ := store.Open(fs.DBPath())
	defer db.Close()
	chain, err := db.SnapshotChain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 3 {
		t.Fatalf("snapshot chain=%d, want 3", len(chain))
	}
	if chain[1].Kind != store.KindWritebackBefore || chain[2].Kind != store.KindWritebackAfter {
		t.Fatalf("unexpected snapshot kinds: %+v", chain)
	}
}

func TestWritebackFailureRollsBackAllAppliedNodes(t *testing.T) {
	root, ar, version := setupWorkdir(t)
	before := descriptorHashes(t, root, ar)
	res, err := Run(context.Background(), root, ar, Options{
		AuditVersion: version, Owner: "test", FailNode: "finance/budget.txt", LockTimeout: time.Second,
	})
	if err == nil {
		t.Fatal("expected injected failure")
	}
	if res.Status != "ROLLED_BACK" || res.Code != CodeWritebackRollback {
		t.Fatalf("unexpected rollback status: %+v", res)
	}
	if len(res.Rollback.Restored) == 0 || len(res.Rollback.AffectedRange) < 2 {
		t.Fatalf("rollback scope missing: %+v", res.Rollback)
	}
	after := descriptorHashes(t, root, ar)
	for path, hash := range before {
		if after[path] != hash {
			t.Fatalf("%s not restored: before=%s after=%s", path, hash, after[path])
		}
	}
	reAudit, err := audit.Run(context.Background(), root, nil, audit.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if reAudit.TreeHash != ar.TreeHash {
		t.Fatal("rollback did not restore the audit tree hash")
	}
	if len(reAudit.Conflicts) != 4 {
		t.Fatalf("rollback should restore all conflicts, got %+v", reAudit.Conflicts)
	}
}

func TestRecoveryAfterWriteAndAfterCommit(t *testing.T) {
	t.Run("after-write rolls back pending node", func(t *testing.T) {
		root, ar, version := setupWorkdir(t)
		before := descriptorHashes(t, root, ar)
		plan, err := BuildPlan(mustFixture(t, root), ar)
		if err != nil {
			t.Fatal(err)
		}
		runCrash(t, root, ar, version, plan.Nodes[0].NodePath, "after-write")
		rec, err := Recover(context.Background(), root)
		if err != nil {
			t.Fatal(err)
		}
		if rec.ExitReportCommitted || rec.Code != "" || len(rec.Sessions) != 1 {
			t.Fatalf("unexpected recovery result: %+v", rec)
		}
		if encoded, err := json.Marshal(rec); err != nil || bytes.Contains(encoded, []byte("null")) {
			t.Fatalf("recovery JSON contains null arrays or failed: %v %s", err, encoded)
		}
		s := rec.Sessions[0]
		if len(s.RolledBack) != 1 || s.RolledBack[0] != plan.Nodes[0].NodePath || len(s.Requeue) != 1 {
			t.Fatalf("pending node not requeued: %+v", s)
		}
		if got := descriptorHashes(t, root, ar)[plan.Nodes[0].NodePath]; got != before[plan.Nodes[0].NodePath] {
			t.Fatal("pending node descriptor not restored to before state")
		}
	})
	t.Run("after-commit keeps committed node", func(t *testing.T) {
		root, ar, version := setupWorkdir(t)
		plan, err := BuildPlan(mustFixture(t, root), ar)
		if err != nil {
			t.Fatal(err)
		}
		target := plan.Nodes[0]
		runCrash(t, root, ar, version, target.NodePath, "after-commit")
		rec, err := Recover(context.Background(), root)
		if err != nil {
			t.Fatal(err)
		}
		s := rec.Sessions[0]
		if len(s.Committed) != 1 || s.Committed[0] != target.NodePath || len(s.Requeue) != 0 || len(s.RolledBack) != 0 {
			t.Fatalf("committed node was not preserved: %+v", s)
		}
		if got := descriptorHashes(t, root, ar)[target.NodePath]; got != target.AfterSHA {
			t.Fatalf("committed descriptor=%s want %s", got, target.AfterSHA)
		}
	})
}

func TestRecoveryKeepsCommittedAndRollsBackPendingNodes(t *testing.T) {
	root, ar, version := setupWorkdir(t)
	plan, err := BuildPlan(mustFixture(t, root), ar)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Nodes) < 2 {
		t.Fatal("fixture needs at least two conflicting nodes")
	}
	before := descriptorHashes(t, root, ar)
	runCrashAt(t, root, ar, version, 2, "after-write")
	rec, err := Recover(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Sessions) != 1 {
		t.Fatalf("missing recovery session: %+v", rec)
	}
	s := rec.Sessions[0]
	if len(s.Committed) != 1 || s.Committed[0] != plan.Nodes[0].NodePath {
		t.Fatalf("committed prefix not preserved: %+v", s)
	}
	if len(s.RolledBack) != 1 || s.RolledBack[0] != plan.Nodes[1].NodePath {
		t.Fatalf("pending node not rolled back: %+v", s)
	}
	if len(s.Requeue) != 1 || s.Requeue[0] != plan.Nodes[1].NodePath {
		t.Fatalf("pending node not requeued: %+v", s)
	}
	hashes := descriptorHashes(t, root, ar)
	if hashes[plan.Nodes[0].NodePath] != plan.Nodes[0].AfterSHA {
		t.Fatal("committed node was reverted during recovery")
	}
	if hashes[plan.Nodes[1].NodePath] != before[plan.Nodes[1].NodePath] {
		t.Fatal("pending node was not restored to its before revision")
	}
	if hashes[plan.Nodes[2].NodePath] != before[plan.Nodes[2].NodePath] {
		t.Fatal("unstarted node changed during recovery")
	}
}

func TestRecoveryAfterPendingDoesNotWrite(t *testing.T) {
	root, ar, version := setupWorkdir(t)
	plan, err := BuildPlan(mustFixture(t, root), ar)
	if err != nil {
		t.Fatal(err)
	}
	before := descriptorHashes(t, root, ar)
	runCrashAt(t, root, ar, version, 1, "after-pending")
	rec, err := Recover(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Sessions) != 1 || len(rec.Sessions[0].Requeue) != 1 {
		t.Fatalf("pending intent not requeued: %+v", rec)
	}
	if got := descriptorHashes(t, root, ar)[plan.Nodes[0].NodePath]; got != before[plan.Nodes[0].NodePath] {
		t.Fatal("after-pending recovery changed the descriptor")
	}
}

func TestRecoveryMarksResidualIntermediateState(t *testing.T) {
	root, ar, version := setupWorkdir(t)
	plan, err := BuildPlan(mustFixture(t, root), ar)
	if err != nil {
		t.Fatal(err)
	}
	runCrash(t, root, ar, version, plan.Nodes[0].NodePath, "after-write")
	fs := mustFixture(t, root)
	d, _, err := fs.LoadDescriptor(plan.Nodes[0].NodePath)
	if err != nil {
		t.Fatal(err)
	}
	d.ACEs = append(d.ACEs, domain.ACE{Index: len(d.ACEs), Type: "ALLOW", SID: fixture.SIDIntern, AccessMask: domain.FileReadAccess})
	if err := fs.SaveDescriptor(plan.Nodes[0].NodePath, d); err != nil {
		t.Fatal(err)
	}
	rec, err := Recover(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Code != CodeRecoveryIncomplete || len(rec.Sessions) != 1 || len(rec.Sessions[0].Residual) != 1 {
		t.Fatalf("residual state not identified: %+v", rec)
	}
	if rec.Sessions[0].Residual[0] != plan.Nodes[0].NodePath {
		t.Fatalf("wrong residual path: %+v", rec.Sessions[0].Residual)
	}
}

func TestNodeLocksSerializeAndRecordWait(t *testing.T) {
	db, err := store.Open(t.TempDir() + "/locks.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	first, err := db.AcquireNodeLock(ctx, "s1", "finance", "finance/budget.txt", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	var second store.NodeLock
	var secondErr error
	go func() {
		defer wg.Done()
		second, secondErr = db.AcquireNodeLock(ctx, "s2", "finance", "finance/budget.txt", 2*time.Second)
	}()
	time.Sleep(80 * time.Millisecond)
	if err := db.ReleaseNodeLock(ctx, "s1", first.Path); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if secondErr != nil {
		t.Fatal(secondErr)
	}
	if second.Waited < 40*time.Millisecond {
		t.Fatalf("second lock did not wait: %s", second.Waited)
	}
	if err := db.ReleaseNodeLock(ctx, "s2", second.Path); err != nil {
		t.Fatal(err)
	}
}

func runCrash(t *testing.T, root string, ar audit.Result, version int64, node, point string) {
	t.Helper()
	runCrashAt(t, root, ar, version, 1, point)
}

func runCrashAt(t *testing.T, root string, ar audit.Result, version int64, crashAt int, point string) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("writeback did not crash")
		} else if _, ok := r.(crashSignal); !ok {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	_, _ = Run(context.Background(), root, ar, Options{
		AuditVersion: version, Owner: "crash-test", CrashAt: crashAt, CrashPoint: point, LockTimeout: time.Second,
		CrashHook: func() { panic(crashSignal{}) },
	})
}

func descriptorHashes(t *testing.T, root string, ar audit.Result) map[string]string {
	t.Helper()
	fs := mustFixture(t, root)
	out := map[string]string{}
	for _, n := range ar.Nodes {
		_, sha, err := fs.LoadDescriptor(n.RelPath)
		if err != nil {
			t.Fatal(err)
		}
		out[n.RelPath] = sha
	}
	return out
}

func mustFixture(t *testing.T, root string) *fixture.Store {
	t.Helper()
	fs, err := fixture.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	return fs
}

func TestPlanOnlyTargetsConflictNodes(t *testing.T) {
	root, ar, _ := setupWorkdir(t)
	plan, err := BuildPlan(mustFixture(t, root), ar)
	if err != nil {
		t.Fatal(err)
	}
	conflictNodes := map[string]bool{}
	for _, c := range ar.Conflicts {
		conflictNodes[c.NodePath] = true
	}
	for _, n := range plan.Nodes {
		if !conflictNodes[n.NodePath] {
			t.Fatalf("plan touched non-conflict node %s", n.NodePath)
		}
		if !n.Changed {
			t.Fatalf("plan node %s has no change", n.NodePath)
		}
	}
	for _, n := range plan.Nodes {
		for _, a := range n.Actions {
			if strings.TrimSpace(a.Reason) == "" {
				t.Fatalf("action without reason: %+v", a)
			}
		}
	}
}

func TestConcurrentIdenticalWritebacksSerializePerNode(t *testing.T) {
	root, ar, version := setupWorkdir(t)
	plan, err := BuildPlan(mustFixture(t, root), ar)
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		res Result
		err error
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	for _, owner := range []string{"concurrent-a", "concurrent-b"} {
		owner := owner
		go func() {
			<-start
			res, err := Run(context.Background(), root, ar, Options{
				AuditVersion: version, Owner: owner, Subtree: ".", LockTimeout: 3 * time.Second,
				PreparedPlan: &plan,
			})
			results <- outcome{res: res, err: err}
		}()
	}
	close(start)
	a, b := <-results, <-results
	for _, o := range []outcome{a, b} {
		if o.err != nil {
			t.Fatalf("concurrent writeback failed: %v (%+v)", o.err, o.res)
		}
		if o.res.Status != "COMPLETED" || len(o.res.LockWaits) != len(plan.Nodes) {
			t.Fatalf("bad concurrent result: %+v", o.res)
		}
	}
	committed, skipped := 0, 0
	for _, o := range []outcome{a, b} {
		for _, n := range o.res.Nodes {
			switch n.Status {
			case "COMMITTED":
				committed++
			case "ALREADY_APPLIED":
				skipped++
			}
		}
	}
	if committed != len(plan.Nodes) || skipped != len(plan.Nodes) {
		t.Fatalf("per-node serialization mismatch: committed=%d skipped=%d", committed, skipped)
	}
	after, err := audit.Run(context.Background(), root, nil, audit.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Conflicts) != 0 {
		t.Fatalf("concurrent writeback left conflicts: %+v", after.Conflicts)
	}
}

func TestWritebackRejectsStaleAuditSnapshot(t *testing.T) {
	root, ar, version := setupWorkdir(t)
	fs := mustFixture(t, root)
	d, _, err := fs.LoadDescriptor("docs/archive/old.txt")
	if err != nil {
		t.Fatal(err)
	}
	d.ACEs = append(d.ACEs, domain.ACE{Index: len(d.ACEs), Type: "ALLOW", SID: fixture.SIDDocs, AccessMask: domain.FileReadAccess})
	if err := fs.SaveDescriptor("docs/archive/old.txt", d); err != nil {
		t.Fatal(err)
	}
	res, err := Run(context.Background(), root, ar, Options{AuditVersion: version, LockTimeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), CodeStaleAudit) {
		t.Fatalf("want %s, got %v", CodeStaleAudit, err)
	}
	if res.Code != CodeStaleAudit || res.Status != "STALE_AUDIT" || res.BeforeSnapshotVersion != 0 {
		t.Fatalf("unexpected stale audit result: %+v", res)
	}
}

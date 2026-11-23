package writeback

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"example.com/ntfsacl/internal/audit"
	"example.com/ntfsacl/internal/domain"
	"example.com/ntfsacl/internal/fixture"
	"example.com/ntfsacl/internal/store"
)

const SchemaResult = "ntfsacl/writeback/v1"

type Options struct {
	AuditVersion int64
	Owner        string
	Subtree      string
	FailNode     string
	CrashAt      int
	CrashPoint   string
	LockTimeout  time.Duration
	CrashHook    func()
	AuditConfig  audit.Config
	Requests     []audit.Request
	PreparedPlan *Plan
}

type LockWait struct {
	Subtree   string `json:"subtree"`
	NodePath  string `json:"node_path"`
	Owner     string `json:"owner"`
	WaitMilli int64  `json:"wait_ms"`
}

type NodeDiff struct {
	NodePath       string   `json:"node_path"`
	Status         string   `json:"status"`
	BeforeSHA256   string   `json:"before_sha256"`
	AfterSHA256    string   `json:"after_sha256"`
	BeforeRevision int64    `json:"before_revision,omitempty"`
	AfterRevision  int64    `json:"after_revision,omitempty"`
	Actions        []Action `json:"actions"`
	Error          string   `json:"error,omitempty"`
}

type RollbackInfo struct {
	Requested     bool     `json:"requested"`
	FailedNode    string   `json:"failed_node,omitempty"`
	AffectedRange []string `json:"affected_range"`
	Restored      []string `json:"restored"`
	RestoreErrors []string `json:"restore_errors"`
}

type Result struct {
	Schema                string               `json:"schema"`
	SessionID             string               `json:"session_id"`
	AuditVersion          int64                `json:"audit_version"`
	BeforeSnapshotVersion int64                `json:"before_snapshot_version"`
	AfterSnapshotVersion  int64                `json:"after_snapshot_version"`
	PlanHash              string               `json:"plan_hash"`
	Status                string               `json:"status"`
	Code                  string               `json:"code,omitempty"`
	Plan                  []Action             `json:"plan"`
	Nodes                 []NodeDiff           `json:"nodes"`
	LockWaits             []LockWait           `json:"lock_waits"`
	Rollback              RollbackInfo         `json:"rollback"`
	BeforeTreeHash        string               `json:"before_tree_hash"`
	AfterTreeHash         string               `json:"after_tree_hash"`
	SnapshotChain         []store.SnapshotInfo `json:"snapshot_chain"`
}

func (r Result) MarshalJSON() ([]byte, error) {
	type wireResult Result
	v := wireResult(r)
	if v.Plan == nil {
		v.Plan = []Action{}
	}
	if v.Nodes == nil {
		v.Nodes = []NodeDiff{}
	}
	if v.LockWaits == nil {
		v.LockWaits = []LockWait{}
	}
	if v.Rollback.AffectedRange == nil {
		v.Rollback.AffectedRange = []string{}
	}
	if v.Rollback.Restored == nil {
		v.Rollback.Restored = []string{}
	}
	if v.Rollback.RestoreErrors == nil {
		v.Rollback.RestoreErrors = []string{}
	}
	if v.SnapshotChain == nil {
		v.SnapshotChain = []store.SnapshotInfo{}
	}
	return domain.CanonicalJSON(v)
}

func (r *Result) Marshal() ([]byte, error) { return r.MarshalJSON() }

// Run commits a writeback-before snapshot, executes each conflict repair with
// per-node SQLite intent logging, and commits a writeback-after snapshot.
func Run(ctx context.Context, workdir string, audited audit.Result, opts Options) (Result, error) {
	fs, err := fixture.Open(workdir)
	if err != nil {
		return Result{}, err
	}
	db, err := store.Open(fs.DBPath())
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	if opts.PreparedPlan == nil {
		current, auditErr := audit.Run(ctx, workdir, opts.Requests, opts.AuditConfig)
		if auditErr != nil {
			return Result{}, fmt.Errorf("pre-writeback freshness audit: %w", auditErr)
		}
		if current.TreeHash != audited.TreeHash {
			return Result{
				Schema: SchemaResult, AuditVersion: opts.AuditVersion, Status: "STALE_AUDIT",
				Code: CodeStaleAudit, BeforeTreeHash: audited.TreeHash,
			}, fmt.Errorf("%s: audit snapshot tree %s does not match current tree %s", CodeStaleAudit, audited.TreeHash, current.TreeHash)
		}
	}
	var plan Plan
	if opts.PreparedPlan != nil {
		plan = *opts.PreparedPlan
	} else {
		plan, err = BuildPlan(fs, audited)
		if err != nil {
			return Result{}, err
		}
	}
	res := Result{
		Schema: SchemaResult, AuditVersion: opts.AuditVersion, PlanHash: plan.PlanHash,
		BeforeTreeHash: audited.TreeHash, Status: "NO_CONFLICTS",
	}
	if len(plan.Nodes) == 0 {
		res.AfterTreeHash = audited.TreeHash
		res.SnapshotChain, _ = db.ListSnapshotInfo(ctx)
		return res, nil
	}
	for _, np := range plan.Nodes {
		res.Plan = append(res.Plan, np.Actions...)
	}

	beforeSnap, err := db.CommitSnapshot(ctx, store.KindWritebackBefore, func(version, parentVersion int64, parentHash string) ([]byte, error) {
		rep, err := audited.ReportAs(store.KindWritebackBefore, version, parentVersion, parentHash)
		if err != nil {
			return nil, err
		}
		return rep.Marshal()
	})
	if err != nil {
		return res, err
	}
	res.BeforeSnapshotVersion = beforeSnap.Version
	res.SessionID = fmt.Sprintf("wb-v%d-b%d-%s", opts.AuditVersion, beforeSnap.Version, plan.PlanHash[:12])
	if _, err := db.StartWritebackSession(ctx, res.SessionID, opts.AuditVersion, plan.PlanHash); err != nil {
		return res, err
	}
	if err := db.SetSessionStatus(ctx, res.SessionID, store.SessionRunning, "", beforeSnap.Version, 0); err != nil {
		return res, err
	}

	execRes, execErr := execute(ctx, fs, db, res.SessionID, plan, opts)
	mergeExecution(&res, execRes)

	// Re-audit the real tree after either success or rollback. This is the
	// state used by the writeback-after snapshot.
	afterAudit, auditErr := audit.Run(ctx, workdir, opts.Requests, opts.AuditConfig)
	if auditErr != nil {
		return res, fmt.Errorf("post-writeback audit: %w", auditErr)
	}
	res.AfterTreeHash = afterAudit.TreeHash
	afterSnap, snapErr := db.CommitSnapshot(ctx, store.KindWritebackAfter, func(version, parentVersion int64, parentHash string) ([]byte, error) {
		rep, err := afterAudit.ReportAs(store.KindWritebackAfter, version, parentVersion, parentHash)
		if err != nil {
			return nil, err
		}
		return rep.Marshal()
	})
	if snapErr != nil {
		return res, snapErr
	}
	res.AfterSnapshotVersion = afterSnap.Version
	res.SnapshotChain, _ = db.ListSnapshotInfo(ctx)
	status := store.SessionCompleted
	code := ""
	if execErr != nil {
		status = store.SessionRolledBack
		code = CodeWritebackRollback
		res.Status = "ROLLED_BACK"
		res.Code = CodeWritebackRollback
	} else {
		res.Status = "COMPLETED"
	}
	if err := db.SetSessionStatus(ctx, res.SessionID, status, code, beforeSnap.Version, afterSnap.Version); err != nil {
		return res, err
	}
	if execErr != nil {
		return res, execErr
	}
	return res, nil
}

type execution struct {
	Nodes    []NodeDiff
	Waits    []LockWait
	Rollback RollbackInfo
}

type appliedNode struct {
	plan NodePlan
	seq  int
	rev  store.Revision
}

func execute(ctx context.Context, fs *fixture.Store, db *store.DB, sessionID string, plan Plan, opts Options) (execution, error) {
	var out execution
	owner := sessionID
	lockTimeout := opts.LockTimeout
	if lockTimeout <= 0 {
		lockTimeout = 10 * time.Second
	}
	var applied []appliedNode
	fail := func(failed string, cause error) (execution, error) {
		out.Rollback = restoreAll(ctx, fs, db, applied, sessionID, plan, failed)
		out.Rollback.Requested = true
		return out, cause
	}
	for i, np := range plan.Nodes {
		if np.NodePath == opts.FailNode {
			err := fmt.Errorf("injected writeback failure at %s", np.NodePath)
			out.Nodes = append(out.Nodes, NodeDiff{NodePath: np.NodePath, Status: "FAILED", BeforeSHA256: np.BeforeSHA, Actions: np.Actions, Error: err.Error()})
			return fail(np.NodePath, err)
		}
		lock, err := db.AcquireNodeLock(ctx, owner, opts.Subtree, np.NodePath, lockTimeout)
		if err != nil {
			out.Nodes = append(out.Nodes, NodeDiff{NodePath: np.NodePath, Status: "FAILED", BeforeSHA256: np.BeforeSHA, Actions: np.Actions, Error: err.Error()})
			return fail(np.NodePath, err)
		}
		out.Waits = append(out.Waits, LockWait{Subtree: opts.Subtree, NodePath: np.NodePath, Owner: owner, WaitMilli: lock.Waited.Milliseconds()})
		current, err := fs.LoadDescriptorBytes(np.NodePath)
		if err != nil {
			_ = db.ReleaseNodeLock(ctx, owner, np.NodePath)
			out.Nodes = append(out.Nodes, NodeDiff{NodePath: np.NodePath, Status: "FAILED", BeforeSHA256: np.BeforeSHA, Actions: np.Actions, Error: err.Error()})
			return fail(np.NodePath, err)
		}
		if domain.HashBytes(current) == np.AfterSHA {
			_ = db.ReleaseNodeLock(ctx, owner, np.NodePath)
			out.Nodes = append(out.Nodes, NodeDiff{
				NodePath: np.NodePath, Status: "ALREADY_APPLIED", BeforeSHA256: np.BeforeSHA,
				AfterSHA256: np.AfterSHA, Actions: np.Actions,
			})
			continue
		}
		if domain.HashBytes(current) != np.BeforeSHA {
			_ = db.ReleaseNodeLock(ctx, owner, np.NodePath)
			err := fmt.Errorf("%s: descriptor changed after plan generation", CodeStalePlan)
			out.Nodes = append(out.Nodes, NodeDiff{NodePath: np.NodePath, Status: "STALE", BeforeSHA256: np.BeforeSHA, Actions: np.Actions, Error: err.Error()})
			return fail(np.NodePath, err)
		}
		revBefore, err := db.BeginNode(ctx, sessionID, np.NodePath, i+1, np.Before, np.After)
		if err != nil {
			_ = db.ReleaseNodeLock(ctx, owner, np.NodePath)
			out.Nodes = append(out.Nodes, NodeDiff{NodePath: np.NodePath, Status: "FAILED", BeforeSHA256: np.BeforeSHA, Actions: np.Actions, Error: err.Error()})
			return fail(np.NodePath, err)
		}
		applied = append(applied, appliedNode{plan: np, seq: i + 1, rev: revBefore})
		np.BeforeRevision = revBefore.Revision
		if opts.CrashAt == i+1 && opts.CrashPoint == "after-pending" {
			crash(opts)
		}
		if err := fs.SaveDescriptor(np.NodePath, planDescriptor(np)); err != nil {
			_ = db.ReleaseNodeLock(ctx, owner, np.NodePath)
			out.Nodes = append(out.Nodes, NodeDiff{NodePath: np.NodePath, Status: "FAILED", BeforeSHA256: np.BeforeSHA, Actions: np.Actions, Error: err.Error()})
			return fail(np.NodePath, err)
		}
		written, err := fs.LoadDescriptorBytes(np.NodePath)
		if err != nil || domain.HashBytes(written) != np.AfterSHA {
			if err == nil {
				err = errors.New("post-write descriptor hash mismatch")
			}
			_ = db.ReleaseNodeLock(ctx, owner, np.NodePath)
			out.Nodes = append(out.Nodes, NodeDiff{NodePath: np.NodePath, Status: "FAILED", BeforeSHA256: np.BeforeSHA, Actions: np.Actions, Error: err.Error()})
			return fail(np.NodePath, err)
		}
		if opts.CrashAt == i+1 && opts.CrashPoint == "after-write" {
			crash(opts)
		}
		revAfter, err := db.CommitNode(ctx, sessionID, np.NodePath, i+1, np.After)
		if err != nil {
			_ = db.ReleaseNodeLock(ctx, owner, np.NodePath)
			out.Nodes = append(out.Nodes, NodeDiff{NodePath: np.NodePath, Status: "FAILED", BeforeSHA256: np.BeforeSHA, Actions: np.Actions, Error: err.Error()})
			return fail(np.NodePath, err)
		}
		np.AfterRevision = revAfter.Revision
		_ = db.ReleaseNodeLock(ctx, owner, np.NodePath)
		out.Nodes = append(out.Nodes, NodeDiff{
			NodePath: np.NodePath, Status: "COMMITTED", BeforeSHA256: np.BeforeSHA, AfterSHA256: np.AfterSHA,
			BeforeRevision: revBefore.Revision, AfterRevision: revAfter.Revision, Actions: np.Actions,
		})
		if opts.CrashAt == i+1 && (opts.CrashPoint == "" || opts.CrashPoint == "after-commit") {
			crash(opts)
		}
	}
	return out, nil
}

func crash(opts Options) {
	if opts.CrashHook != nil {
		opts.CrashHook()
	}
	// A real forced-termination path: no deferred rollback and no exit report.
	os.Exit(137)
}

func restoreAll(ctx context.Context, fs *fixture.Store, db *store.DB, applied []appliedNode, sessionID string, plan Plan, failed string) RollbackInfo {
	info := RollbackInfo{Requested: true, FailedNode: failed, AffectedRange: []string{}, Restored: []string{}, RestoreErrors: []string{}}
	for _, np := range plan.Nodes {
		if failed == "" || np.NodePath == failed || strings.HasPrefix(np.NodePath, failed+"/") {
			info.AffectedRange = append(info.AffectedRange, np.NodePath)
		}
	}
	// Stable depth-descending restore reproduces the pre-writeback snapshot.
	sort.SliceStable(applied, func(i, j int) bool { return applied[i].plan.Depth > applied[j].plan.Depth })
	for _, a := range applied {
		info.AffectedRange = append(info.AffectedRange, a.plan.NodePath)
		if err := fs.RestoreDescriptor(a.plan.NodePath, a.plan.Before); err != nil {
			info.RestoreErrors = append(info.RestoreErrors, a.plan.NodePath+": "+err.Error())
			continue
		}
		got, err := fs.LoadDescriptorBytes(a.plan.NodePath)
		if err != nil || domain.HashBytes(got) != a.plan.BeforeSHA {
			info.RestoreErrors = append(info.RestoreErrors, a.plan.NodePath+": restored hash mismatch")
			continue
		}
		info.Restored = append(info.Restored, a.plan.NodePath)
		_ = db.MarkNodeRolledBack(ctx, sessionID, a.plan.NodePath, a.seq, "rolled back to writeback-before snapshot")
	}
	sort.Strings(info.AffectedRange)
	sort.Strings(info.Restored)
	sort.Strings(info.RestoreErrors)
	return info
}

func planDescriptor(np NodePlan) domain.Descriptor {
	// SaveDescriptor canonicalizes the descriptor; the already validated plan
	// bytes are the canonical representation.
	var d domain.Descriptor
	_ = domain.DecodeJSON(bytes.NewReader(np.After), &d)
	return d
}

func mergeExecution(dst *Result, src execution) {
	dst.Nodes = append(dst.Nodes, src.Nodes...)
	dst.LockWaits = append(dst.LockWaits, src.Waits...)
	if src.Rollback.Requested {
		dst.Rollback = src.Rollback
	}
}

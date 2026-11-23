package capver

import (
	"context"
	"os"
	"path/filepath"
	"sync"
)

// VerifyOptions 一次验证运行的选项。
type VerifyOptions struct {
	SessionID string
	Inject    InjectKind
	Limits    SessionLimits
	ExePath   string // "@self" 解析后的组件二进制路径
	TempRoot  string // 临时目录父路径；为空使用系统临时目录
	SlowTest  bool   // 追加一个长睡眠进程用于超时验证
	SlowMS    int
}

// Verifier 是导出的领域组件：策略加载、进程验证、快照回放、报告导出。
type Verifier struct {
	Store  *Store
	Runner Runner
}

// NewVerifier 创建验证器。
func NewVerifier(store *Store, runner Runner) *Verifier {
	return &Verifier{Store: store, Runner: runner}
}

// ensureSnapshot 加载或提交策略快照。
// 返回快照版本、恢复结论；版本链跳号时返回 E_SNAPSHOT_VERSION 并拒绝继续加载。
func (v *Verifier) ensureSnapshot(p *Policy) (version int64, recovery RecoveryConclusion, err error) {
	snap, err := v.Store.LoadLatest()
	if err != nil {
		if CodeOf(err) == CodeSnapshotVersion && snap != nil {
			// 版本链跳号：回退到最近连续快照，拒绝继续加载。
			return snap.Version, RecoveryFallback, err
		}
		if CodeOf(err) == CodeSnapshotVersion && snap == nil {
			// 无任何已提交快照：提交初始版本。
			ver, cerr := v.Store.CommitSnapshot(p)
			if cerr != nil {
				return 0, RecoveryFailed, cerr
			}
			return ver, RecoveryClean, nil
		}
		return 0, RecoveryFailed, err
	}
	// 内容比较基于整体哈希：识别整体替换式版本变化。
	if !snap.Policy.EqualContent(p) {
		ver, cerr := v.Store.CommitSnapshot(p)
		if cerr != nil {
			return 0, RecoveryFailed, cerr
		}
		return ver, RecoveryClean, nil
	}
	return snap.Version, RecoveryClean, nil
}

// Verify 执行一次完整验证会话：策略快照 -> 受限进程探测 -> 确定性报告。
// 快照版本链跳号时返回部分报告与 E_SNAPSHOT_VERSION 错误。
func (v *Verifier) Verify(ctx context.Context, policy *Policy, opts VerifyOptions) (*Report, error) {
	if opts.SessionID == "" {
		opts.SessionID = "sess-1"
	}
	if opts.ExePath == "" {
		exe, err := os.Executable()
		if err != nil {
			return nil, newError(CodePolicyInvalid, "resolve executable", err)
		}
		opts.ExePath = exe
	}
	if opts.Limits.MaxConcurrentSessions == 0 {
		opts.Limits = DefaultSessionLimits()
	}
	if !opts.Inject.Valid() {
		return nil, newError(CodePolicyInvalid, "unknown inject kind: "+opts.Inject.String(), nil)
	}

	effective := ApplyInjection(policy, opts.Inject)
	if opts.SlowTest {
		effective = &Policy{
			Name: effective.Name,
			Processes: append(append([]ProcessDecl(nil), effective.Processes...), ProcessDecl{
				ID:            "proc-slow",
				Command:       probeCommand("-plan", "sleep"),
				Capabilities:  []Capability{CapFileRead},
				RestrictedDir: "self",
			}),
		}
	}
	if err := effective.Validate(); err != nil {
		return nil, err
	}

	version, recovery, err := v.ensureSnapshot(effective)
	versions, _, _, chainErr := v.Store.Chain()
	if chainErr != nil {
		return nil, chainErr
	}
	report := &Report{
		Component:       ComponentID,
		PolicyName:      effective.Name,
		PolicyHash:      effective.Hash(),
		SnapshotVersion: version,
		VersionChain:    versions,
		Recovery:        recovery,
		Inject:          opts.Inject,
		Events:          []Event{},
		Sessions:        []SessionRecord{},
	}
	if err != nil {
		// 版本链跳号：拒绝继续加载，返回携带回退信息的报告。
		return report, err
	}

	// 会话临时目录与路径规范化。
	tmpParent := opts.TempRoot
	if tmpParent == "" {
		tmpParent = os.TempDir()
	}
	sessionRoot, err := os.MkdirTemp(tmpParent, "capver-"+opts.SessionID+"-")
	if err != nil {
		return nil, newError(CodeStore, "create session temp dir", err)
	}
	defer os.RemoveAll(sessionRoot)

	norm := NewPathNormalizer()
	norm.Map(sessionRoot, "<SESSION>")
	norm.Map(opts.ExePath, "<SELF>")

	mgr := NewSessionManager(opts.Limits)
	if err := mgr.CheckProcessCount(opts.SessionID, len(effective.Processes)); err != nil {
		return report, err
	}
	release, err := mgr.Acquire(ctx, opts.SessionID)
	if err != nil {
		return report, err
	}
	defer release()

	// 并发执行受测进程；报告排序保证确定性。
	var wg sync.WaitGroup
	var mu sync.Mutex
	records := make([]ProcessRecord, 0, len(effective.Processes))
	for _, decl := range effective.Processes {
		decl := decl
		workDir := filepath.Join(sessionRoot, decl.ID)
		outsideDir := filepath.Join(sessionRoot, decl.ID+"-outside")
		norm.Map(workDir, "<WORK:"+decl.ID+">")
		norm.Map(outsideDir, "<OUTSIDE:"+decl.ID+">")

		pctx, cancel := mgr.ProcessContext(ctx, opts.SessionID, decl.ID)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer cancel()
			req := RunRequest{
				SessionID:  opts.SessionID,
				Decl:       decl,
				ExePath:    opts.ExePath,
				WorkDir:    workDir,
				OutsideDir: outsideDir,
				Timeout:    mgr.Limits().ProcessTimeout,
				Inject:     injectForProc(opts.Inject, decl.ID),
				Norm:       norm,
			}
			if opts.SlowTest && decl.ID == "proc-slow" {
				req.SlowMS = opts.SlowMS
				if req.SlowMS <= 0 {
					req.SlowMS = 30000
				}
			}
			rec, rerr := v.Runner.Run(pctx, req)
			if rerr != nil {
				rec = ProcessRecord{
					ProcID:   decl.ID,
					Declared: sortedCaps(decl.Capabilities),
					Code:     CodeOf(rerr),
					Probes:   []ProbeResult{},
				}
			}
			if rec.TimedOut {
				mgr.RecordTimeout(opts.SessionID, decl.ID)
			}
			if rec.Terminated {
				mgr.RecordKill(opts.SessionID, "process "+decl.ID+" terminated: "+string(rec.Code))
			}
			mu.Lock()
			records = append(records, rec)
			mu.Unlock()
		}()
	}
	wg.Wait()
	mgr.RecordFinish(opts.SessionID)

	report.Events = mgr.Events()
	report.Sessions = []SessionRecord{{SessionID: opts.SessionID, Processes: records}}
	return report, nil
}

// injectForProc 判断某个进程是否为注入进程。
func injectForProc(kind InjectKind, procID string) InjectKind {
	if kind.InjectedProcID() == procID {
		return kind
	}
	return InjectNone
}

// Replay 回放指定版本（或最近连续版本）的策略快照。
func (v *Verifier) Replay(version int64) (*Snapshot, error) {
	if version > 0 {
		return v.Store.LoadVersion(version)
	}
	return v.Store.LoadLatest()
}

// ExportReport 导出确定性 JSON 报告字节。
func ExportReport(r *Report) []byte { return r.Bytes() }

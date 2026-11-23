package capver

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"
)

// fakeRunner 在测试中以确定性方式模拟真实运行器的探针结果。
// 真实隔离行为由 Windows 运行器在目标平台验证；此处验证编排与判定链路。
type fakeRunner struct{}

func (fakeRunner) Run(ctx context.Context, req RunRequest) (ProcessRecord, error) {
	if req.Decl.ID == "proc-slow" {
		select {
		case <-ctx.Done():
			return EvaluateRecord(req.Decl, nil, true), nil
		case <-time.After(30 * time.Second):
		}
	}
	probes := []ProbeResult{}
	for _, c := range AllCapabilities() {
		declared := false
		for _, d := range req.Decl.Capabilities {
			if d == c {
				declared = true
			}
		}
		if !declared {
			continue
		}
		probes = append(probes, ProbeResult{Token: c, Target: "<WORK:" + req.Decl.ID + ">/ok",
			Scenario: ScenarioBaseline, Allowed: true, Expected: true})
	}
	switch req.Inject {
	case InjectUnauthorizedWrite:
		probes = append(probes, ProbeResult{Token: CapFileWrite, Target: "<OUTSIDE:" + req.Decl.ID + ">/escape.txt",
			Scenario: ScenarioUnauthorizedWrite, Allowed: false, Expected: false,
			DeniedBy: "component", RawError: "denied-by-policy"})
	case InjectChildEscape:
		probes = append(probes, ProbeResult{Token: CapChildCreate, Target: "capver-not-whitelisted.exe",
			Scenario: ScenarioChildEscape, Allowed: false, Expected: false,
			DeniedBy: "component", RawError: "denied-by-policy"})
	case InjectHandleEscape:
		probes = append(probes, ProbeResult{Token: CapFileWrite, Target: "inherited-handle:231",
			Scenario: ScenarioHandleEscape, Allowed: false, Expected: false,
			DeniedBy: "kernel", RawError: "invalid handle"})
	case InjectCapLeak:
		probes = append(probes, ProbeResult{Token: CapRegistryWrite, Target: `HKCU\Software\capver-probe`,
			Scenario: ScenarioCapLeak, Allowed: true, Expected: false})
	}
	return EvaluateRecord(req.Decl, probes, false), nil
}

func newTestVerifier(t *testing.T) (*Verifier, *Store) {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), "verify.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return NewVerifier(store, fakeRunner{}), store
}

func testOptions(inject InjectKind) VerifyOptions {
	return VerifyOptions{
		SessionID: "sess-1",
		Inject:    inject,
		ExePath:   "/bin/capver",
		TempRoot:  "",
		Limits: SessionLimits{
			MaxConcurrentSessions:  2,
			MaxProcessesPerSession: 8,
			ProcessTimeout:         2 * time.Second,
		},
	}
}

func TestVerifyBaseline(t *testing.T) {
	v, store := newTestVerifier(t)
	report, err := v.Verify(context.Background(), GenerateSamplePolicy(), testOptions(InjectNone))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if report.SnapshotVersion != 1 {
		t.Fatalf("expected snapshot v1, got %d", report.SnapshotVersion)
	}
	if report.Recovery != RecoveryClean {
		t.Fatalf("expected clean recovery, got %s", report.Recovery)
	}
	for _, s := range report.Sessions {
		for _, p := range s.Processes {
			if p.Code != CodeOK {
				t.Fatalf("process %s: expected OK, got %s", p.ProcID, p.Code)
			}
		}
	}
	// 重复运行且输入不变：报告字节级一致，快照版本不增长。
	report2, err := v.Verify(context.Background(), GenerateSamplePolicy(), testOptions(InjectNone))
	if err != nil {
		t.Fatal(err)
	}
	if report2.SnapshotVersion != 1 {
		t.Fatalf("identical policy should reuse snapshot v1, got %d", report2.SnapshotVersion)
	}
	if !bytes.Equal(report.Bytes(), report2.Bytes()) {
		t.Fatalf("reports differ:\n%s\n---\n%s", report.Bytes(), report2.Bytes())
	}
	_ = store
}

func TestVerifyInjections(t *testing.T) {
	for _, kind := range []InjectKind{InjectUnauthorizedWrite, InjectChildEscape, InjectHandleEscape, InjectCapLeak} {
		t.Run(string(kind), func(t *testing.T) {
			v, store := newTestVerifier(t)
			policy := GenerateSamplePolicy()
			report, err := v.Verify(context.Background(), policy, testOptions(kind))
			if err != nil {
				t.Fatalf("verify: %v", err)
			}
			var injected *ProcessRecord
			for si := range report.Sessions {
				for pi := range report.Sessions[si].Processes {
					pr := &report.Sessions[si].Processes[pi]
					if pr.ProcID == kind.InjectedProcID() {
						injected = pr
					}
				}
			}
			if injected == nil {
				t.Fatal("injected process record missing")
			}
			if injected.Code != kind.ExpectedCode() {
				t.Fatalf("expected %s, got %s", kind.ExpectedCode(), injected.Code)
			}
			if !injected.Terminated {
				t.Fatal("violating process must be terminated")
			}
			// 基线进程不受影响。
			for _, s := range report.Sessions {
				for _, pr := range s.Processes {
					if pr.ProcID != kind.InjectedProcID() && pr.Code != CodeOK {
						t.Fatalf("baseline process %s polluted: %s", pr.ProcID, pr.Code)
					}
				}
			}
			// 已提交快照未污染：最新快照内容 == 注入后策略，且链连续。
			snap, err := store.LoadLatest()
			if err != nil {
				t.Fatalf("load latest: %v", err)
			}
			if !snap.Policy.EqualContent(ApplyInjection(policy, kind)) {
				t.Fatal("committed snapshot content mismatch")
			}
			_, _, gap, err := store.Chain()
			if err != nil || gap {
				t.Fatalf("chain broken: gap=%v err=%v", gap, err)
			}
		})
	}
}

func TestVerifySnapshotGapInjection(t *testing.T) {
	v, store := newTestVerifier(t)
	policy := GenerateSamplePolicy()

	// 基线提交。
	if _, err := v.Verify(context.Background(), policy, testOptions(InjectNone)); err != nil {
		t.Fatal(err)
	}
	baseHash := policy.Hash()

	// 注入跳号后：拒绝继续加载，回退到最近连续快照。
	if _, err := store.InjectSnapshotGap(2); err != nil {
		t.Fatal(err)
	}
	report, err := v.Verify(context.Background(), policy, testOptions(InjectSnapshotGap))
	if CodeOf(err) != CodeSnapshotVersion {
		t.Fatalf("expected E_SNAPSHOT_VERSION, got %v", err)
	}
	if report == nil {
		t.Fatal("partial report expected")
	}
	if report.Recovery != RecoveryFallback {
		t.Fatalf("expected fallback recovery, got %s", report.Recovery)
	}
	if report.SnapshotVersion != 1 {
		t.Fatalf("expected fallback snapshot v1, got %d", report.SnapshotVersion)
	}
	// 已提交快照未被污染。
	snap, rerr := store.LoadVersion(1)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if snap.Hash != baseHash {
		t.Fatal("committed snapshot polluted by gap injection")
	}
}

func TestVerifyProcessTimeout(t *testing.T) {
	v, _ := newTestVerifier(t)
	opts := testOptions(InjectNone)
	opts.SlowTest = true
	opts.Limits.ProcessTimeout = 200 * time.Millisecond
	report, err := v.Verify(context.Background(), GenerateSamplePolicy(), opts)
	if err != nil {
		t.Fatal(err)
	}
	var slow *ProcessRecord
	for si := range report.Sessions {
		for pi := range report.Sessions[si].Processes {
			if report.Sessions[si].Processes[pi].ProcID == "proc-slow" {
				slow = &report.Sessions[si].Processes[pi]
			}
		}
	}
	if slow == nil || slow.Code != CodeProcessTimeout || !slow.Terminated {
		t.Fatalf("expected E_PROCESS_TIMEOUT for proc-slow, got %+v", slow)
	}
	// 超时事件记录会话标识与原因。
	found := false
	for _, ev := range report.Events {
		if ev.Kind == "timeout" && ev.SessionID == "sess-1" {
			found = true
		}
	}
	if !found {
		t.Fatal("timeout event with session id missing")
	}
}

func TestSessionQueueAndLimits(t *testing.T) {
	m := NewSessionManager(SessionLimits{
		MaxConcurrentSessions:  1,
		MaxProcessesPerSession: 2,
		ProcessTimeout:         time.Second,
	})
	ctx := context.Background()
	rel1, err := m.Acquire(ctx, "sess-1")
	if err != nil {
		t.Fatal(err)
	}
	// 第二个会话进入等待队列。
	acquired := make(chan struct{})
	go func() {
		rel2, err := m.Acquire(ctx, "sess-2")
		if err != nil {
			t.Error(err)
		}
		close(acquired)
		rel2()
	}()
	select {
	case <-acquired:
		t.Fatal("sess-2 should be queued, not admitted")
	case <-time.After(100 * time.Millisecond):
	}
	rel1()
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("sess-2 should be admitted after sess-1 released")
	}
	// 等待事件记录。
	var queued bool
	for _, ev := range m.Events() {
		if ev.SessionID == "sess-2" && ev.Kind == "queued" {
			queued = true
		}
	}
	if !queued {
		t.Fatal("queued event for sess-2 missing")
	}
	// 单会话进程数上限。
	if err := m.CheckProcessCount("sess-1", 3); CodeOf(err) != CodeSessionLimit {
		t.Fatalf("expected E_SESSION_LIMIT, got %v", err)
	}
}

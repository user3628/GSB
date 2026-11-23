package capver

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestHelperProcess 是探针子进程的测试载体：以测试二进制自身充当组件可执行文件。
func TestHelperProcess(t *testing.T) {
	if os.Getenv("CAPVER_TEST_PROBE") != "1" {
		return
	}
	for i, a := range os.Args {
		if a == "--" {
			os.Exit(RunProbe(os.Args[i+1:]))
		}
	}
	os.Exit(2)
}

// startProbeChild 以真实子进程方式启动探针（非隔离，仅验证中介协议与探测逻辑）。
func startProbeChild(t *testing.T, workDir, outside, caps, whitelist, plan string) (stdin *os.File, stdout *os.File, wait func()) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"-test.run=TestHelperProcess", "--"}
	args = append(args, "-plan", plan,
		"-workdir", workDir, "-outside", outside,
		"-caps", caps, "-whitelist", whitelist)
	cmd := exec.Command(exe, args...)
	cmd.Env = append(os.Environ(), "CAPVER_TEST_PROBE=1",
		"CAPVER_PROBE_NOOP_ARGS=-test.run=TestHelperProcess -- -plan noop")
	cmd.Stderr = os.Stderr
	inW, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	outR, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return inW.(*os.File), outR.(*os.File), func() { cmd.Wait() }
}

func TestProbeMediationUnauthorizedWrite(t *testing.T) {
	root := t.TempDir()
	work := filepath.Join(root, "work")
	outside := filepath.Join(root, "outside")
	os.MkdirAll(work, 0o700)
	os.MkdirAll(outside, 0o700)
	os.WriteFile(filepath.Join(work, "seed.txt"), []byte("capver-seed\n"), 0o600)

	stdin, stdout, wait := startProbeChild(t, work, outside, "file.read,file.write", "", "inject-unauthorized-write")
	pol := &mediationPolicy{workDir: work, exePath: "/bin/capver"}
	norm := NewPathNormalizer()
	norm.Map(work, "<WORK>")
	norm.Map(outside, "<OUTSIDE>")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	probes, err := mediateSession(ctx, stdin, stdout, pol, norm)
	wait()
	if err != nil {
		t.Fatalf("mediate: %v", err)
	}
	decl := decl("proc-inject-write", CapFileRead, CapFileWrite)
	rec := EvaluateRecord(decl, probes, false)
	if rec.Code != CodeCapDenied {
		t.Fatalf("expected E_CAP_DENIED, got %s (probes=%+v)", rec.Code, probes)
	}
	// 越权写被组件在真实边界拦截，目标文件不存在。
	if _, err := os.Stat(filepath.Join(outside, "escape.txt")); !os.IsNotExist(err) {
		t.Fatal("escape file should not exist after component interception")
	}
	// 路径已规范化。
	for _, p := range rec.Probes {
		if p.Token == CapFileWrite && p.Scenario == ScenarioUnauthorizedWrite {
			if p.Target != "<OUTSIDE>/escape.txt" {
				t.Fatalf("target not normalized: %q", p.Target)
			}
			if p.DeniedBy != "component" {
				t.Fatalf("expected component denial, got %q", p.DeniedBy)
			}
		}
	}
}

func TestProbeMediationChildEscape(t *testing.T) {
	root := t.TempDir()
	work := filepath.Join(root, "work")
	outside := filepath.Join(root, "outside")
	os.MkdirAll(work, 0o700)
	os.MkdirAll(outside, 0o700)
	os.WriteFile(filepath.Join(work, "seed.txt"), []byte("capver-seed\n"), 0o600)

	stdin, stdout, wait := startProbeChild(t, work, outside, "file.read,process.create", "@self", "inject-child-escape")
	exe, _ := os.Executable()
	pol := &mediationPolicy{workDir: work, exePath: exe, whitelist: []string{"@self"}}
	norm := NewPathNormalizer()
	norm.Map(work, "<WORK>")
	norm.Map(outside, "<OUTSIDE>")
	norm.Map(exe, "<SELF>")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	probes, err := mediateSession(ctx, stdin, stdout, pol, norm)
	wait()
	if err != nil {
		t.Fatalf("mediate: %v", err)
	}
	decl := decl("proc-inject-child", CapFileRead, CapChildCreate)
	decl.ChildWhitelist = []string{"@self"}
	rec := EvaluateRecord(decl, probes, false)
	if rec.Code != CodeCapDenied {
		t.Fatalf("expected E_CAP_DENIED, got %s (probes=%+v)", rec.Code, probes)
	}
	// 白名单内子进程真实创建成功。
	var whitelistedOK bool
	for _, p := range rec.Probes {
		if p.Token == CapChildCreate && p.Scenario == ScenarioBaseline && p.Allowed {
			whitelistedOK = true
		}
	}
	if !whitelistedOK {
		t.Fatal("whitelisted child spawn should succeed")
	}
}

func TestProbeBaselineFileOps(t *testing.T) {
	root := t.TempDir()
	work := filepath.Join(root, "work")
	outside := filepath.Join(root, "outside")
	os.MkdirAll(work, 0o700)
	os.MkdirAll(outside, 0o700)
	os.WriteFile(filepath.Join(work, "seed.txt"), []byte("capver-seed\n"), 0o600)

	stdin, stdout, wait := startProbeChild(t, work, outside, "file.read,file.write", "", "baseline")
	pol := &mediationPolicy{workDir: work, exePath: "/bin/capver"}
	norm := NewPathNormalizer()
	norm.Map(work, "<WORK>")
	norm.Map(outside, "<OUTSIDE>")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	probes, err := mediateSession(ctx, stdin, stdout, pol, norm)
	wait()
	if err != nil {
		t.Fatalf("mediate: %v", err)
	}
	rec := EvaluateRecord(decl("proc-writer", CapFileRead, CapFileWrite), probes, false)
	if rec.Code != CodeOK {
		t.Fatalf("expected OK, got %s (probes=%+v)", rec.Code, probes)
	}
	// 声明内写真实落盘。
	if _, err := os.Stat(filepath.Join(work, "out.txt")); err != nil {
		t.Fatal("in-scope write should have succeeded")
	}
}

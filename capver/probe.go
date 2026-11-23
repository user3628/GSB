package capver

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// 探针（受测子进程）入口。由组件以隐藏子命令形式启动：
//
//	capver probe -plan <plan> -workdir <dir> -outside <dir> -caps a,b -whitelist @self -inherit-handle N
//
// 探针通过 stdout/stdin 与父进程的中介协议交互，最终以一行 JSON 上报结果。
func RunProbe(args []string) int {
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	plan := fs.String("plan", "baseline", "probe plan")
	workDir := fs.String("workdir", "", "restricted work dir")
	outside := fs.String("outside", "", "outside target dir")
	caps := fs.String("caps", "", "comma-separated declared capabilities")
	whitelist := fs.String("whitelist", "", "comma-separated child whitelist")
	inheritHandle := fs.Uint64("inherit-handle", 0, "inherited handle value to test")
	sleepMS := fs.Int("sleep-ms", 30000, "sleep plan duration")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	p := &probe{
		workDir:       *workDir,
		outside:       *outside,
		declared:      parseCaps(*caps),
		whitelist:     splitCSV(*whitelist),
		inheritHandle: *inheritHandle,
		out:           json.NewEncoder(os.Stdout),
		in:            bufio.NewScanner(os.Stdin),
	}
	p.in.Buffer(make([]byte, 0, 4096), 1024*1024)

	switch *plan {
	case "noop":
		return 0
	case "sleep":
		time.Sleep(time.Duration(*sleepMS) * time.Millisecond)
		return 0
	case "baseline":
		p.runBaseline()
	case "inject-unauthorized-write":
		p.runBaseline()
		p.probeFileWrite(filepath.Join(*outside, "escape.txt"), ScenarioUnauthorizedWrite)
	case "inject-child-escape":
		p.runBaseline()
		p.probeChildCreate("capver-not-whitelisted.exe", ScenarioChildEscape)
	case "inject-handle-escape":
		p.runBaseline()
		p.probeInheritedHandle()
	case "inject-cap-leak":
		p.runBaseline()
		p.probeRegistryWrite(ScenarioCapLeak)
	default:
		fmt.Fprintf(os.Stderr, "unknown plan %q\n", *plan)
		return 2
	}
	// 最终上报。
	_ = p.out.Encode(wireMessage{Type: "results", Probes: p.results})
	return 0
}

type probe struct {
	workDir       string
	outside       string
	declared      []Capability
	whitelist     []string
	inheritHandle uint64
	out           *json.Encoder
	in            *bufio.Scanner
	results       []ProbeResult
}

func parseCaps(s string) []Capability {
	var out []Capability
	for _, c := range splitCSV(s) {
		out = append(out, Capability(c))
	}
	return out
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (p *probe) declaredHas(c Capability) bool {
	for _, d := range p.declared {
		if d == c {
			return true
		}
	}
	return false
}

// checkWithParent 请求父进程（组件）对敏感操作做策略判定。
func (p *probe) checkWithParent(op Capability, target string) bool {
	if err := p.out.Encode(wireMessage{Type: "check", Op: op, Target: target}); err != nil {
		return false
	}
	if !p.in.Scan() {
		return false
	}
	var resp wireResponse
	if err := json.Unmarshal(p.in.Bytes(), &resp); err != nil {
		return false
	}
	return resp.Allow
}

func (p *probe) record(r ProbeResult) { p.results = append(p.results, r) }

// runBaseline 执行声明内能力的合法探测。
func (p *probe) runBaseline() {
	for _, c := range AllCapabilities() {
		if !p.declaredHas(c) {
			continue
		}
		switch c {
		case CapFileRead:
			p.probeFileRead(filepath.Join(p.workDir, "seed.txt"))
		case CapFileWrite:
			p.probeFileWrite(filepath.Join(p.workDir, "out.txt"), ScenarioBaseline)
		case CapChildCreate:
			p.probeChildCreate("@self", ScenarioBaseline)
		case CapRegistryWrite:
			p.probeRegistryWrite(ScenarioBaseline)
		}
	}
}

func (p *probe) probeFileRead(target string) {
	expected := p.declaredHas(CapFileRead) && withinDir(p.workDir, target)
	if !p.checkWithParent(CapFileRead, target) {
		p.record(ProbeResult{Token: CapFileRead, Target: target, Scenario: ScenarioBaseline,
			Allowed: false, Expected: expected, DeniedBy: "component", RawError: "denied-by-policy"})
		return
	}
	_, err := os.ReadFile(target)
	if err != nil {
		p.record(ProbeResult{Token: CapFileRead, Target: target, Scenario: ScenarioBaseline,
			Allowed: false, Expected: expected, DeniedBy: "kernel", RawError: err.Error()})
		return
	}
	p.record(ProbeResult{Token: CapFileRead, Target: target, Scenario: ScenarioBaseline,
		Allowed: true, Expected: expected})
}

func (p *probe) probeFileWrite(target, scenario string) {
	expected := p.declaredHas(CapFileWrite) && withinDir(p.workDir, target)
	if !p.checkWithParent(CapFileWrite, target) {
		p.record(ProbeResult{Token: CapFileWrite, Target: target, Scenario: scenario,
			Allowed: false, Expected: expected, DeniedBy: "component", RawError: "denied-by-policy"})
		return
	}
	err := os.WriteFile(target, []byte("capver-probe\n"), 0o600)
	if err != nil {
		p.record(ProbeResult{Token: CapFileWrite, Target: target, Scenario: scenario,
			Allowed: false, Expected: expected, DeniedBy: "kernel", RawError: err.Error()})
		return
	}
	p.record(ProbeResult{Token: CapFileWrite, Target: target, Scenario: scenario,
		Allowed: true, Expected: expected})
}

func (p *probe) probeChildCreate(image, scenario string) {
	expected := p.declaredHas(CapChildCreate) && p.whitelisted(image)
	resolved := image
	if image == "@self" {
		if exe, err := os.Executable(); err == nil {
			resolved = exe
		}
	}
	if !p.checkWithParent(CapChildCreate, resolved) {
		p.record(ProbeResult{Token: CapChildCreate, Target: resolved, Scenario: scenario,
			Allowed: false, Expected: expected, DeniedBy: "component", RawError: "denied-by-policy"})
		return
	}
	noopArgs := []string{"probe", "-plan", "noop"}
	if ov := os.Getenv("CAPVER_PROBE_NOOP_ARGS"); ov != "" {
		noopArgs = strings.Split(ov, " ")
	}
	cmd := exec.Command(resolved, noopArgs...)
	cmd.Dir = p.workDir
	err := cmd.Run()
	if err != nil {
		p.record(ProbeResult{Token: CapChildCreate, Target: resolved, Scenario: scenario,
			Allowed: false, Expected: expected, DeniedBy: "kernel", RawError: err.Error()})
		return
	}
	p.record(ProbeResult{Token: CapChildCreate, Target: resolved, Scenario: scenario,
		Allowed: true, Expected: expected})
}

func (p *probe) whitelisted(image string) bool {
	for _, w := range p.whitelist {
		if w == image {
			return true
		}
	}
	return false
}

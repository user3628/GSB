package capver

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"
)

// RunRequest 描述一次受测进程运行。
type RunRequest struct {
	SessionID  string
	Decl       ProcessDecl
	ExePath    string        // "@self" 解析后的组件二进制路径
	WorkDir    string        // 进程专属临时工作目录（限制目录）
	OutsideDir string        // 限制目录之外的目标目录
	Timeout    time.Duration // 单进程运行时长上限
	Inject     InjectKind    // 该进程对应的异常注入场景
	SlowMS     int           // sleep 计划时长（超时测试）
	Norm       *PathNormalizer
}

// Runner 在真实边界上启动受限进程并执行探测。
type Runner interface {
	Run(ctx context.Context, req RunRequest) (ProcessRecord, error)
}

// NewRunner 返回当前平台的运行器。
// Windows：作业对象 + 受限令牌的真实隔离；其他平台：返回 E_UNSUPPORTED。
func NewRunner() Runner { return newPlatformRunner() }

// ---- 组件-探针中介协议 ----
// 探针（受测子进程）对敏感操作先向父进程发起 check 请求，
// 父进程依据策略在真实边界上主动拦截（应用层拦截）；
// 注册表与继承句柄等内核边界操作由探针直接尝试并记录原始错误。

type wireMessage struct {
	Type   string        `json:"type"` // check | results
	Op     Capability    `json:"op,omitempty"`
	Target string        `json:"target,omitempty"`
	Probes []ProbeResult `json:"probes,omitempty"`
}

type wireResponse struct {
	Allow bool `json:"allow"`
}

// mediationPolicy 是父进程侧的策略判定视图。
type mediationPolicy struct {
	workDir   string
	exePath   string
	whitelist []string
}

// withinDir 判断 target 是否位于 dir 之内（大小写不敏感，消除平台差异）。
func withinDir(dir, target string) bool {
	d := strings.ToLower(filepath.Clean(dir))
	t := strings.ToLower(filepath.Clean(target))
	return t == d || strings.HasPrefix(t, d+string(filepath.Separator))
}

// check 组件侧拦截判定：文件操作须在限制目录内，子进程须在白名单内。
func (p *mediationPolicy) check(op Capability, target string) bool {
	switch op {
	case CapFileRead, CapFileWrite:
		return withinDir(p.workDir, target)
	case CapChildCreate:
		for _, w := range p.whitelist {
			if w == "@self" {
				w = p.exePath
			}
			if strings.EqualFold(w, target) {
				return true
			}
		}
		return false
	}
	return false
}

// mediateSession 运行父进程侧中介循环，直到探针上报最终结果。
// 返回的探针结果已做路径规范化。
func mediateSession(ctx context.Context, stdin io.Writer, stdout io.Reader, pol *mediationPolicy, norm *PathNormalizer) ([]ProbeResult, error) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	enc := json.NewEncoder(stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var msg wireMessage
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			continue // 忽略非协议输出
		}
		switch msg.Type {
		case "check":
			allow := pol.check(msg.Op, msg.Target)
			if err := enc.Encode(wireResponse{Allow: allow}); err != nil {
				return nil, fmt.Errorf("mediate: write response: %w", err)
			}
		case "results":
			for i := range msg.Probes {
				msg.Probes[i].Target = norm.Normalize(msg.Probes[i].Target)
				msg.Probes[i].RawError = norm.Normalize(msg.Probes[i].RawError)
			}
			return msg.Probes, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("mediate: read stdout: %w", err)
	}
	return nil, fmt.Errorf("mediate: probe exited without results")
}

// ---- 判定逻辑（平台无关，可测试） ----

// 探针场景标识。
const (
	ScenarioBaseline          = "baseline"
	ScenarioUnauthorizedWrite = "unauthorized-write"
	ScenarioChildEscape       = "child-escape"
	ScenarioHandleEscape      = "handle-escape"
	ScenarioCapLeak           = "cap-leak"
)

// EvaluateRecord 由探针结果计算进程判定记录（稳定业务码）。
// 规则：
//  1. 超时 -> E_PROCESS_TIMEOUT；
//  2. 实际获得未声明能力（Allowed && !Expected）-> E_CAP_LEAK；
//  3. 声明内操作被拒（!Allowed && Expected）-> E_CAP_DENIED；
//  4. 越权尝试被真实拒绝（!Allowed && !Expected）-> 场景对应业务码；
//  5. 全部符合声明 -> OK。
func EvaluateRecord(decl ProcessDecl, probes []ProbeResult, timedOut bool) ProcessRecord {
	rec := ProcessRecord{
		ProcID:   decl.ID,
		Declared: sortedCaps(decl.Capabilities),
		Probes:   probes,
	}
	if rec.Declared == nil {
		rec.Declared = []Capability{}
	}
	if rec.Probes == nil {
		rec.Probes = []ProbeResult{}
	}
	switch {
	case timedOut:
		rec.Code = CodeProcessTimeout
		rec.TimedOut = true
		rec.Terminated = true
		return rec
	}
	for _, p := range probes {
		if p.Allowed && !p.Expected {
			rec.Code = CodeCapLeak
			rec.Terminated = true
			return rec
		}
	}
	for _, p := range probes {
		if !p.Allowed && p.Expected {
			rec.Code = CodeCapDenied
			rec.Terminated = true
			return rec
		}
	}
	for _, p := range probes {
		if p.Allowed || p.Expected {
			continue
		}
		switch p.Scenario {
		case ScenarioHandleEscape:
			rec.Code = CodeHandleEscape
		case ScenarioUnauthorizedWrite, ScenarioChildEscape:
			rec.Code = CodeCapDenied
		default:
			rec.Code = CodeCapDenied
		}
		rec.Terminated = true
		return rec
	}
	rec.Code = CodeOK
	return rec
}

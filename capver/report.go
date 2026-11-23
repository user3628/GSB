package capver

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
)

// ComponentID 报告中的组件标识（固定，保证字节级可重复）。
const ComponentID = "capver/1"

// ProbeResult 记录一次能力探测的判定结果。
type ProbeResult struct {
	Token    Capability `json:"token"`
	Target   string     `json:"target"`             // 已规范化的目标路径/对象
	Scenario string     `json:"scenario,omitempty"` // 触发场景（注入类型）
	Allowed  bool       `json:"allowed"`            // 操作是否成功
	Expected bool       `json:"expected"`           // 按声明是否应当成功
	DeniedBy string     `json:"deniedBy,omitempty"` // "kernel" | "component"
	RawError string     `json:"rawError,omitempty"` // 原始错误码/信息
}

// ProcessRecord 是单个受测进程的判定记录。
type ProcessRecord struct {
	ProcID     string        `json:"procId"`
	Declared   []Capability  `json:"declared"`
	Code       Code          `json:"code"`
	Probes     []ProbeResult `json:"probes"`
	Terminated bool          `json:"terminated"`
	TimedOut   bool          `json:"timedOut"`
}

// Event 记录等待与终止等会话事件。
type Event struct {
	SessionID string `json:"sessionId"`
	Kind      string `json:"kind"` // queued | started | finished | killed | timeout
	Reason    string `json:"reason,omitempty"`
}

// SessionRecord 记录一个验证会话。
type SessionRecord struct {
	SessionID string          `json:"sessionId"`
	Processes []ProcessRecord `json:"processes"`
}

// Report 是导出的确定性 JSON 报告。
// 不包含时间戳、随机路径等不稳定字段；所有路径经规范化处理。
type Report struct {
	Component       string             `json:"component"`
	PolicyName      string             `json:"policyName"`
	PolicyHash      string             `json:"policyHash"`
	SnapshotVersion int64              `json:"snapshotVersion"`
	VersionChain    []int64            `json:"versionChain"`
	Recovery        RecoveryConclusion `json:"recovery"`
	Inject          InjectKind         `json:"inject,omitempty"`
	Events          []Event            `json:"events"`
	Sessions        []SessionRecord    `json:"sessions"`
}

// MarshalJSON 保证报告字节级稳定：进程按 (procId, code) 排序，
// 探针按 (token, target) 排序，事件按 (sessionId, kind) 排序。
func (r *Report) MarshalJSON() ([]byte, error) {
	type alias Report
	out := alias(*r)
	out.Events = append([]Event{}, r.Events...)
	sort.SliceStable(out.Events, func(i, j int) bool {
		if out.Events[i].SessionID != out.Events[j].SessionID {
			return out.Events[i].SessionID < out.Events[j].SessionID
		}
		return out.Events[i].Kind < out.Events[j].Kind
	})
	out.Sessions = append([]SessionRecord{}, r.Sessions...)
	sort.SliceStable(out.Sessions, func(i, j int) bool {
		return out.Sessions[i].SessionID < out.Sessions[j].SessionID
	})
	for si := range out.Sessions {
		procs := append([]ProcessRecord{}, out.Sessions[si].Processes...)
		sort.SliceStable(procs, func(i, j int) bool {
			if procs[i].ProcID != procs[j].ProcID {
				return procs[i].ProcID < procs[j].ProcID
			}
			return procs[i].Code < procs[j].Code
		})
		for pi := range procs {
			pr := append([]ProbeResult{}, procs[pi].Probes...)
			sort.SliceStable(pr, func(i, j int) bool {
				if pr[i].Token != pr[j].Token {
					return pr[i].Token < pr[j].Token
				}
				return pr[i].Target < pr[j].Target
			})
			procs[pi].Probes = pr
		}
		out.Sessions[si].Processes = procs
	}
	return json.Marshal(out)
}

// Bytes 返回报告的确定性 JSON 字节（带尾部换行）。
func (r *Report) Bytes() []byte {
	b, err := r.MarshalJSON()
	if err != nil {
		panic(err)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(json.RawMessage(b)); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// PathNormalizer 把机器相关的绝对路径替换为稳定逻辑占位符，
// 保证报告在不同机器、不同临时目录下字节级一致。
type PathNormalizer struct {
	repl []pathReplacement
}

type pathReplacement struct {
	from string
	to   string
}

// NewPathNormalizer 创建路径规范化器。
func NewPathNormalizer() *PathNormalizer { return &PathNormalizer{} }

// Map 注册一条 绝对路径 -> 逻辑占位符 映射（先注册先生效，长路径优先）。
func (n *PathNormalizer) Map(absPath, placeholder string) {
	n.repl = append(n.repl, pathReplacement{from: absPath, to: placeholder})
}

// Normalize 应用全部映射（按源路径长度降序，避免前缀误替换）。
func (n *PathNormalizer) Normalize(s string) string {
	repl := append([]pathReplacement(nil), n.repl...)
	sort.SliceStable(repl, func(i, j int) bool { return len(repl[i].from) > len(repl[j].from) })
	out := s
	for _, r := range repl {
		if r.from == "" {
			continue
		}
		out = strings.ReplaceAll(out, r.from, r.to)
	}
	// 统一路径分隔符，消除平台差异。
	out = strings.ReplaceAll(out, "\\", "/")
	return out
}

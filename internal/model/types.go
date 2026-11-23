package model

// Component 是清单规范化后的组件模型。
type Component struct {
	ID         string `json:"id"`
	Version    []int  `json:"version"`
	DigestAlgo string `json:"digest_algo"`
	Digest     []byte `json:"digest"`
	// RegisteredDigest 是清单登记摘要，用于核验报告中的摘要登记来源。
	RegisteredDigest []byte `json:"registered_digest,omitempty"`
}

// Edge 是依赖边声明规范化后的模型。
type Edge struct {
	Line       int    `json:"line"`
	Parent     string `json:"parent"`
	Child      string `json:"child"`
	Constraint []int  `json:"constraint"`
}

// EdgeKey 用于重复边判等（与行号无关）。
type EdgeKey struct {
	Parent string
	Child  string
}

// ParseIssue 记录解析期产生的业务问题。
type ParseIssue struct {
	Code     string `json:"code"`
	Offset   int    `json:"offset,omitempty"`
	Line     int    `json:"line,omitempty"`
	Field    string `json:"field,omitempty"`
	Message  string `json:"message"`
	Resolved int    `json:"resolved,omitempty"`
}

// Issue 是核验结论中的单条问题，序列化为稳定 JSON。
type Issue struct {
	Code        string    `json:"code"`
	Component   string    `json:"component,omitempty"`
	Parent      string    `json:"parent,omitempty"`
	Child       string    `json:"child,omitempty"`
	EdgeLines   []int     `json:"edge_lines,omitempty"`
	Actual      []int     `json:"actual,omitempty"`
	Constraint  []int     `json:"constraint,omitempty"`
	Constraints [][]int   `json:"constraints,omitempty"`
	Proof       []EdgeRef `json:"proof,omitempty"`
	CycleNodes  []string  `json:"cycle_nodes,omitempty"`
	CycleEdges  []EdgeRef `json:"cycle_edges,omitempty"`
	Message     string    `json:"message"`
}

// EdgeRef 是边来源的稳定引用。
type EdgeRef struct {
	Line   int    `json:"line"`
	Parent string `json:"parent"`
	Child  string `json:"child"`
}

// ClosureResult 是单轮核验的规范化结论。
type ClosureResult struct {
	Roots         []string    `json:"roots"`
	Closure       []string    `json:"closure"`
	Issues        []Issue     `json:"issues"`
	CodeSequence  []string    `json:"code_sequence"`
	Path          string      `json:"path"`
	Threshold     int         `json:"threshold"`
	ClosureSize   int         `json:"closure_size"`
	DigestSources []DigestRef `json:"digest_sources"`
	SampleMarker  string      `json:"sample_marker"`
}

// DigestRef 描述摘要登记来源。
type DigestRef struct {
	Component string `json:"component"`
	Algo      string `json:"algo"`
	Digest    []byte `json:"digest"`
	Match     bool   `json:"match"`
}

// CompareVersion 比较两个整数序列版本：逐段比较；前缀一致时长者为大。
// 返回 -1 / 0 / 1，分别表示 a 小于 / 等于 / 大于 b。
func CompareVersion(a, b []int) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}

// NormalizeKey 组件标识统一小写，保留命名空间分隔符（/、:、.）。
func NormalizeKey(raw string) string {
	b := make([]byte, len(raw))
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

package capver

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// Capability 是声明式能力令牌。
type Capability string

const (
	CapFileRead      Capability = "file.read"
	CapFileWrite     Capability = "file.write"
	CapChildCreate   Capability = "process.create"
	CapRegistryWrite Capability = "registry.write"
)

// AllCapabilities 返回全部已知能力令牌（稳定顺序）。
func AllCapabilities() []Capability {
	return []Capability{CapFileRead, CapFileWrite, CapChildCreate, CapRegistryWrite}
}

func validCapability(c Capability) bool {
	for _, k := range AllCapabilities() {
		if k == c {
			return true
		}
	}
	return false
}

// ProcessDecl 描述一个受测进程。
type ProcessDecl struct {
	ID             string       `json:"id"`
	Command        []string     `json:"command"`
	Capabilities   []Capability `json:"capabilities"`
	RestrictedDir  string       `json:"restrictedDir"`  // "self" 表示进程专属临时工作目录
	ChildWhitelist []string     `json:"childWhitelist"` // 允许的子进程映像名
}

// Policy 是一组进程声明构成的能力策略。
type Policy struct {
	Name      string        `json:"name"`
	Processes []ProcessDecl `json:"processes"`
}

// Validate 校验策略声明的合法性。
func (p *Policy) Validate() error {
	if p.Name == "" {
		return newError(CodePolicyInvalid, "policy name is empty", nil)
	}
	seen := map[string]bool{}
	for _, d := range p.Processes {
		if d.ID == "" {
			return newError(CodePolicyInvalid, "process id is empty", nil)
		}
		if seen[d.ID] {
			return newError(CodePolicyInvalid, "duplicate process id: "+d.ID, nil)
		}
		seen[d.ID] = true
		if len(d.Command) == 0 {
			return newError(CodePolicyInvalid, fmt.Sprintf("process %s: empty command", d.ID), nil)
		}
		for _, c := range d.Capabilities {
			if !validCapability(c) {
				return newError(CodePolicyInvalid, fmt.Sprintf("process %s: unknown capability %q", d.ID, c), nil)
			}
		}
	}
	return nil
}

// canonical 返回策略的规范化副本：进程按 ID 排序，能力与白名单排序去重。
// 快照哈希与内容比较均基于规范化形式，识别整体替换式变化而非仅版本号。
func (p *Policy) canonical() Policy {
	out := Policy{Name: p.Name, Processes: make([]ProcessDecl, len(p.Processes))}
	for i, d := range p.Processes {
		nd := d
		nd.Capabilities = sortedCaps(d.Capabilities)
		nd.ChildWhitelist = sortedStrings(d.ChildWhitelist)
		nd.Command = append([]string(nil), d.Command...)
		out.Processes[i] = nd
	}
	sort.Slice(out.Processes, func(i, j int) bool { return out.Processes[i].ID < out.Processes[j].ID })
	return out
}

// CanonicalJSON 返回策略规范化后的 JSON 字节（确定性）。
func (p *Policy) CanonicalJSON() []byte {
	b, err := json.Marshal(p.canonical())
	if err != nil {
		panic(err)
	}
	return b
}

// Hash 返回策略规范化内容的 SHA-256。
func (p *Policy) Hash() string {
	sum := sha256.Sum256(p.CanonicalJSON())
	return hex.EncodeToString(sum[:])
}

// EqualContent 比较两个策略的规范化内容是否一致（整体替换语义）。
func (p *Policy) EqualContent(other *Policy) bool {
	return p.Hash() == other.Hash()
}

func sortedCaps(in []Capability) []Capability {
	out := append([]Capability(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	// 去重
	dst := out[:0]
	for i, c := range out {
		if i == 0 || c != out[i-1] {
			dst = append(dst, c)
		}
	}
	return dst
}

func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	dst := out[:0]
	for i, s := range out {
		if i == 0 || s != out[i-1] {
			dst = append(dst, s)
		}
	}
	return dst
}

// ParsePolicy 从 JSON 字节解析并校验策略。
func ParsePolicy(data []byte) (*Policy, error) {
	var p Policy
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, newError(CodePolicyInvalid, "invalid policy json", err)
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return &p, nil
}

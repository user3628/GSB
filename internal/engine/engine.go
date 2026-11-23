package engine

import (
	"crypto/sha256"
	"crypto/sha512"
	"sort"

	"depverify/internal/closure"
	"depverify/internal/model"
)

// DefaultThreshold 是双路径切换的默认闭包组件数阈值，可被参数覆盖。
const DefaultThreshold = 32

// Engine 持有图模型与可调参数。
type Engine struct {
	Graph     *closure.Graph
	Threshold int
}

// New 构建引擎；threshold <= 0 时使用默认阈值。
func New(g *closure.Graph, threshold int) *Engine {
	if threshold <= 0 {
		threshold = DefaultThreshold
	}
	return &Engine{Graph: g, Threshold: threshold}
}

// Verify 执行一轮完整核验并输出规范化结论。
func (e *Engine) Verify(roots []string, maxItems int, parseIssues []model.ParseIssue) model.ClosureResult {
	return e.VerifyWithPath(roots, maxItems, parseIssues, "")
}

// VerifyWithPath 执行核验；forcePath 为空时按阈值自动选择路径。
func (e *Engine) VerifyWithPath(roots []string, maxItems int, parseIssues []model.ParseIssue, forcePath string) model.ClosureResult {
	normRoots := normalizeRoots(roots)
	closureNodes, buildIssues := e.Graph.Build(normRoots, maxItems)
	path := e.selectPath(len(closureNodes), forcePath)

	var issues []model.Issue
	issues = append(issues, parseToIssues(parseIssues)...)
	issues = append(issues, buildIssues...)

	if path == model.PathPrecise {
		issues = append(issues, e.precise(closureNodes)...)
	} else {
		issues = append(issues, e.batched(closureNodes)...)
	}
	issues = canonicalize(issues)

	return model.ClosureResult{
		Roots:         normRoots,
		Closure:       closureNodes,
		Issues:        issues,
		CodeSequence:  codeSequence(issues),
		Path:          path,
		Threshold:     e.Threshold,
		ClosureSize:   len(closureNodes),
		DigestSources: e.digestSources(closureNodes),
		SampleMarker:  model.FixedSampleMarker,
	}
}

func (e *Engine) selectPath(size int, forcePath string) string {
	if forcePath == model.PathPrecise || forcePath == model.PathBatched {
		return forcePath
	}
	if size >= e.Threshold {
		return model.PathBatched
	}
	return model.PathPrecise
}

// precise 逐边精算路径：直接遍历邻接边即时判断。
func (e *Engine) precise(nodes []string) []model.Issue {
	var issues []model.Issue
	issues = append(issues, e.Graph.Downgrades(nodes)...)
	issues = append(issues, e.Graph.Cycles(nodes)...)
	issues = append(issues, e.Graph.Conflicts(nodes)...)
	return issues
}

// batched 批量路径：先对组件与边建立排序索引，再在索引上批量核验。
func (e *Engine) batched(nodes []string) []model.Issue {
	indexedNodes := append([]string(nil), nodes...)
	sort.Strings(indexedNodes)
	var indexedEdges []model.Edge
	for _, parent := range indexedNodes {
		indexedEdges = append(indexedEdges, e.Graph.EdgesOf(parent)...)
	}
	indexed := closure.NewGraph(indexedEdges, e.Graph.Components)
	var issues []model.Issue
	issues = append(issues, indexed.Downgrades(indexedNodes)...)
	issues = append(issues, indexed.Cycles(indexedNodes)...)
	issues = append(issues, indexed.Conflicts(indexedNodes)...)
	return issues
}

func (e *Engine) digestSources(nodes []string) []model.DigestRef {
	out := make([]model.DigestRef, 0, len(nodes))
	for _, id := range nodes {
		c := e.Graph.Components[id]
		match := verifyDigest(c)
		out = append(out, model.DigestRef{
			Component: id, Algo: c.DigestAlgo,
			Digest: append([]byte(nil), c.RegisteredDigest...), Match: match,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Component < out[j].Component })
	return out
}

func verifyDigest(c model.Component) bool {
	if len(c.RegisteredDigest) == 0 {
		return false
	}
	var sum []byte
	switch c.DigestAlgo {
	case "SHA256":
		d := sha256.Sum256(digestPayload(c))
		sum = d[:]
	case "SHA512":
		d := sha512.Sum512(digestPayload(c))
		sum = d[:]
	default:
		return false
	}
	if len(sum) != len(c.RegisteredDigest) {
		return false
	}
	for i := range sum {
		if sum[i] != c.RegisteredDigest[i] {
			return false
		}
	}
	return true
}

// digestPayload 摘要对象为组件标识与版本序列的确定性字节串。
func digestPayload(c model.Component) []byte {
	buf := make([]byte, 0, len(c.ID)+len(c.Version)*8)
	buf = append(buf, c.ID...)
	for _, v := range c.Version {
		buf = append(buf, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
	return buf
}

func parseToIssues(in []model.ParseIssue) []model.Issue {
	out := make([]model.Issue, 0, len(in))
	for _, p := range in {
		out = append(out, model.Issue{
			Code: p.Code, Component: p.Field, Parent: "", Child: "",
			EdgeLines: lineOrOffset(p), Message: p.Message,
		})
	}
	return out
}

func lineOrOffset(p model.ParseIssue) []int {
	if p.Line > 0 {
		return []int{p.Line}
	}
	if p.Offset > 0 {
		return []int{p.Offset}
	}
	return nil
}

func normalizeRoots(roots []string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(roots))
	for _, r := range roots {
		k := model.NormalizeKey(r)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func canonicalize(issues []model.Issue) []model.Issue {
	sort.SliceStable(issues, func(i, j int) bool {
		a, b := issues[i], issues[j]
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		ak := a.Parent + "|" + a.Child + "|" + a.Component
		bk := b.Parent + "|" + b.Child + "|" + b.Component
		return ak < bk
	})
	return issues
}

func codeSequence(issues []model.Issue) []string {
	out := make([]string, 0, len(issues))
	for _, is := range issues {
		out = append(out, is.Code)
	}
	return out
}

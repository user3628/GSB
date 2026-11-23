package closure

import (
	"sort"

	"depverify/internal/model"
)

// Graph 是规范化后的依赖图（去重边）。
type Graph struct {
	Edges      []model.Edge
	Components map[string]model.Component
	adj        map[string][]model.Edge
}

// NewGraph 按确定性顺序构建邻接表：同父组件的出边按子组件键排序。
func NewGraph(edges []model.Edge, components map[string]model.Component) *Graph {
	g := &Graph{Edges: edges, Components: components, adj: make(map[string][]model.Edge)}
	for _, e := range edges {
		g.adj[e.Parent] = append(g.adj[e.Parent], e)
	}
	for p := range g.adj {
		sort.Slice(g.adj[p], func(i, j int) bool { return g.adj[p][i].Child < g.adj[p][j].Child })
	}
	return g
}

// Build 以 roots 出发做 BFS 闭包，visited 保证环边不会无限展开。
// maxItems 为单会话组件数上限，超限时返回 E_ITEM_BUDGET。
func (g *Graph) Build(roots []string, maxItems int) ([]string, []model.Issue) {
	seen := make(map[string]struct{})
	var ordered []string
	queue := append([]string(nil), roots...)
	var issues []model.Issue
	for _, r := range roots {
		if _, ok := g.Components[r]; !ok {
			issues = append(issues, model.Issue{
				Code: model.ERootMissing, Component: r,
				Message: "root component not present in manifest",
			})
		}
	}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		if _, ok := seen[node]; ok {
			continue
		}
		if _, ok := g.Components[node]; !ok {
			continue // 悬挂边组件不入闭包，问题已在解析期记录
		}
		if maxItems > 0 && len(ordered) >= maxItems {
			issues = append(issues, model.Issue{
				Code: model.EItemBudget, Component: node,
				Message: "session component budget exceeded",
			})
			break
		}
		seen[node] = struct{}{}
		ordered = append(ordered, node)
		for _, e := range g.adj[node] {
			if _, ok := g.Components[e.Child]; ok {
				queue = append(queue, e.Child)
			}
		}
	}
	sort.Strings(ordered)
	return ordered, issues
}

// Downgrades 在闭包内逐条边比较子组件实际版本与约束版本。
func (g *Graph) Downgrades(closureNodes []string) []model.Issue {
	inSet := toSet(closureNodes)
	var issues []model.Issue
	for _, parent := range closureNodes {
		for _, e := range g.adj[parent] {
			if _, ok := inSet[e.Child]; !ok {
				continue
			}
			child := g.Components[e.Child]
			if model.CompareVersion(child.Version, e.Constraint) < 0 {
				issues = append(issues, model.Issue{
					Code: model.EVersionDowngrade, Parent: e.Parent, Child: e.Child,
					EdgeLines: []int{e.Line}, Actual: append([]int(nil), child.Version...),
					Constraint: append([]int(nil), e.Constraint...),
					Message:    "actual version is below declared constraint",
				})
			}
		}
	}
	return issues
}

// Cycles 用 Tarjan 算法在闭包子图上求强连通分量，提取每个非平凡 SCC 的
// 确定性环路径（最小字典序起点 + 每步最小字典序后继）。
func (g *Graph) Cycles(closureNodes []string) []model.Issue {
	inSet := toSet(closureNodes)
	index := 0
	indices := make(map[string]int)
	low := make(map[string]int)
	onStack := make(map[string]bool)
	var stack []string
	var sccs [][]string

	var strong func(v string)
	strong = func(v string) {
		indices[v] = index
		low[v] = index
		index++
		stack = append(stack, v)
		onStack[v] = true
		for _, e := range g.adj[v] {
			w := e.Child
			if _, ok := inSet[w]; !ok {
				continue
			}
			if _, seen := indices[w]; !seen {
				strong(w)
				if low[w] < low[v] {
					low[v] = low[w]
				}
			} else if onStack[w] {
				if indices[w] < low[v] {
					low[v] = indices[w]
				}
			}
		}
		if low[v] == indices[v] {
			var comp []string
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[w] = false
				comp = append(comp, w)
				if w == v {
					break
				}
			}
			sccs = append(sccs, comp)
		}
	}
	// 固定遍历顺序，保证 Tarjan 行为不依赖输入顺序。
	for _, node := range closureNodes {
		if _, seen := indices[node]; !seen {
			strong(node)
		}
	}

	var issues []model.Issue
	for _, comp := range sccs {
		if len(comp) < 2 {
			// 单节点 SCC 只有存在自环边时才算环。
			node := comp[0]
			selfLoop := false
			for _, e := range g.adj[node] {
				if e.Child == node {
					selfLoop = true
				}
			}
			if !selfLoop {
				continue
			}
		}
		issues = append(issues, g.describeCycle(comp, inSet))
	}
	sort.Slice(issues, func(i, j int) bool { return issues[i].Component < issues[j].Component })
	return issues
}

func (g *Graph) describeCycle(comp []string, inSet map[string]struct{}) model.Issue {
	members := append([]string(nil), comp...)
	sort.Strings(members)
	start := members[0]
	// 从最小节点出发，每一步选择 SCC 内最小字典序后继，构造规范环。
	nodes := []string{start}
	edgeRefs := []model.EdgeRef{}
	visited := map[string]bool{start: true}
	current := start
	for {
		var next string
		var via model.Edge
		for _, e := range g.adj[current] {
			if _, ok := inSet[e.Child]; !ok {
				continue
			}
			if !visited[e.Child] || (e.Child == start && len(edgeRefs) > 0) {
				if next == "" || e.Child < next {
					next = e.Child
					via = e
				}
			}
		}
		edgeRefs = append(edgeRefs, model.EdgeRef{Line: via.Line, Parent: via.Parent, Child: via.Child})
		if next == start {
			break
		}
		visited[next] = true
		nodes = append(nodes, next)
		current = next
	}
	return model.Issue{
		Code: model.ECycle, Component: start,
		CycleNodes: nodes, CycleEdges: edgeRefs,
		Message: "directed dependency cycle detected within closure",
	}
}

// Conflicts 按子组件聚合约束：同一子组件存在两个及以上互斥约束时裁决冲突。
// 输出约束并集与两个不可满足证明边，全部按确定性规则排序。
func (g *Graph) Conflicts(closureNodes []string) []model.Issue {
	inSet := toSet(closureNodes)
	grouped := make(map[string][]model.Edge)
	for _, parent := range closureNodes {
		for _, e := range g.adj[parent] {
			if _, ok := inSet[e.Child]; ok {
				grouped[e.Child] = append(grouped[e.Child], e)
			}
		}
	}
	children := make([]string, 0, len(grouped))
	for child := range grouped {
		children = append(children, child)
	}
	sort.Strings(children)

	var issues []model.Issue
	for _, child := range children {
		edgesToChild := grouped[child]
		distinct := distinctConstraints(edgesToChild)
		if len(distinct) < 2 {
			continue
		}
		// 证明边：约束最小与约束最大的两条边；同约束取行号最小，再取父键最小。
		sort.Slice(edgesToChild, func(i, j int) bool {
			c := model.CompareVersion(edgesToChild[i].Constraint, edgesToChild[j].Constraint)
			if c != 0 {
				return c < 0
			}
			if edgesToChild[i].Line != edgesToChild[j].Line {
				return edgesToChild[i].Line < edgesToChild[j].Line
			}
			return edgesToChild[i].Parent < edgesToChild[j].Parent
		})
		lo, hi := edgesToChild[0], edgesToChild[len(edgesToChild)-1]
		issues = append(issues, model.Issue{
			Code: model.EConstraintConflict, Child: child,
			Constraints: distinct,
			Proof: []model.EdgeRef{
				{Line: lo.Line, Parent: lo.Parent, Child: lo.Child},
				{Line: hi.Line, Parent: hi.Parent, Child: hi.Child},
			},
			Message: "mutually exclusive constraints on the same child component",
		})
	}
	return issues
}

func distinctConstraints(edgeList []model.Edge) [][]int {
	seen := make(map[string][]int)
	var keys []string
	for _, e := range edgeList {
		key := versionKey(e.Constraint)
		if _, ok := seen[key]; !ok {
			seen[key] = append([]int(nil), e.Constraint...)
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out := make([][]int, 0, len(keys))
	for _, k := range keys {
		out = append(out, seen[k])
	}
	return out
}

func versionKey(v []int) string {
	b := make([]byte, 0, len(v)*4)
	for _, n := range v {
		b = append(b, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
	return string(b)
}

func toSet(nodes []string) map[string]struct{} {
	m := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		m[n] = struct{}{}
	}
	return m
}

// EdgesOf 返回某父组件的确定性出边列表（已按子组件排序）。
func (g *Graph) EdgesOf(parent string) []model.Edge {
	return append([]model.Edge(nil), g.adj[parent]...)
}

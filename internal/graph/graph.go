package graph

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"recoveryengine/internal/model"
)

const (
	BlockReasonCycle           = "cycle"
	BlockReasonUpstreamBlocked = "upstream_blocked"
)

type Cycle struct {
	ID         string   `json:"id"`
	Components []string `json:"components"`
}

type Block struct {
	Reason  string   `json:"reason"`
	CycleID string   `json:"cycle_id,omitempty"`
	Via     []string `json:"via,omitempty"`
}

type Diagnostic struct {
	Code       string   `json:"code"`
	Message    string   `json:"message"`
	Components []string `json:"components,omitempty"`
	CycleID    string   `json:"cycle_id,omitempty"`
}

type BatchGroup struct {
	Batch      int      `json:"batch"`
	Components []string `json:"components"`
}

type ImpactRow struct {
	Component      string `json:"component"`
	Hop            int    `json:"hop"`
	TopologyLevel  *int   `json:"topology_level,omitempty"`
	RecoveryBatch  *int   `json:"recovery_batch,omitempty"`
	Blocked        bool   `json:"blocked"`
	BlockedReason  string `json:"blocked_reason,omitempty"`
	BlockedByCycle string `json:"blocked_by_cycle,omitempty"`
}

type ImpactResult struct {
	Origin            string       `json:"origin"`
	Affected          []ImpactRow  `json:"affected"`
	RecoveryBatches   []BatchGroup `json:"recovery_batches"`
	BlockedComponents []string     `json:"blocked_components"`
	AffectedCount     int          `json:"affected_count"`
	MaxHop            int          `json:"max_hop"`
}

type Analysis struct {
	Nodes           []string                `json:"nodes"`
	TopologyBatches map[string]int          `json:"topology_batches"`
	BatchGroups     []BatchGroup            `json:"batch_groups"`
	Blocked         map[string]Block        `json:"blocked"`
	Cycles          []Cycle                 `json:"cycles"`
	Diagnostics     []Diagnostic            `json:"diagnostics"`
	Impacts         map[string]ImpactResult `json:"impacts"`
	HasCycle        bool                    `json:"has_cycle"`
	GraphDigest     string                  `json:"graph_digest"`
}

func Analyze(snapshot model.Snapshot) *Analysis {
	nodes := make([]string, 0, len(snapshot.Components))
	outgoing := make(map[string][]string, len(snapshot.Components))
	for _, component := range snapshot.SortedComponents() {
		nodes = append(nodes, component.ID)
		outgoing[component.ID] = nil
	}
	for _, edge := range snapshot.SortedEdges() {
		if _, ok := outgoing[edge.From]; !ok {
			continue
		}
		if _, ok := outgoing[edge.To]; !ok {
			continue
		}
		outgoing[edge.From] = append(outgoing[edge.From], edge.To)
	}
	for node := range outgoing {
		sort.Strings(outgoing[node])
	}

	analysis := &Analysis{
		Nodes:           append([]string(nil), nodes...),
		TopologyBatches: map[string]int{},
		Blocked:         map[string]Block{},
		Diagnostics:     []Diagnostic{},
		Impacts:         map[string]ImpactResult{},
	}

	cycles, cycleNodes := findCycles(nodes, outgoing)
	analysis.Cycles = cycles
	analysis.HasCycle = len(cycles) > 0
	for _, cycle := range cycles {
		for _, node := range cycle.Components {
			analysis.Blocked[node] = Block{Reason: BlockReasonCycle, CycleID: cycle.ID}
		}
		analysis.Diagnostics = append(analysis.Diagnostics, Diagnostic{
			Code:       model.CodeDependencyCycle,
			Message:    "dependency cycle blocks deterministic recovery ordering",
			Components: append([]string(nil), cycle.Components...),
			CycleID:    cycle.ID,
		})
	}

	markDownstreamBlocked(nodes, outgoing, cycleNodes, analysis.Blocked)
	for node, block := range analysis.Blocked {
		if block.Reason == BlockReasonUpstreamBlocked {
			analysis.Diagnostics = append(analysis.Diagnostics, Diagnostic{
				Code:       "E_BLOCKED_BY_CYCLE",
				Message:    "component depends on a cycle and is blocked from recovery ordering",
				Components: []string{node},
				CycleID:    block.CycleID,
			})
		}
	}
	sort.Slice(analysis.Diagnostics, func(i, j int) bool {
		left, right := analysis.Diagnostics[i], analysis.Diagnostics[j]
		if left.CycleID != right.CycleID {
			return left.CycleID < right.CycleID
		}
		if left.Code != right.Code {
			return left.Code < right.Code
		}
		return joined(left.Components) < joined(right.Components)
	})

	clean := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if _, blocked := analysis.Blocked[node]; !blocked {
			clean = append(clean, node)
		}
	}
	analysis.TopologyBatches, analysis.BatchGroups = computeLevels(clean, outgoing)
	for _, origin := range nodes {
		analysis.Impacts[origin] = impactFor(origin, outgoing, analysis.TopologyBatches, analysis.Blocked)
	}
	analysis.GraphDigest = digest(analysis)
	return analysis
}

func (a *Analysis) ImpactFor(origins []string) ([]ImpactResult, []string) {
	missing := make([]string, 0)
	results := make([]ImpactResult, 0, len(origins))
	seen := map[string]struct{}{}
	for _, origin := range origins {
		if _, duplicate := seen[origin]; duplicate {
			continue
		}
		seen[origin] = struct{}{}
		impact, ok := a.Impacts[origin]
		if !ok {
			missing = append(missing, origin)
			continue
		}
		results = append(results, impact)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Origin < results[j].Origin })
	sort.Strings(missing)
	return results, missing
}

func findCycles(nodes []string, outgoing map[string][]string) ([]Cycle, map[string]struct{}) {
	index := 0
	indices := make(map[string]int, len(nodes))
	lowlink := make(map[string]int, len(nodes))
	onStack := make(map[string]bool, len(nodes))
	stack := make([]string, 0, len(nodes))
	components := make([][]string, 0)

	var strongConnect func(string)
	strongConnect = func(node string) {
		indices[node] = index
		lowlink[node] = index
		index++
		stack = append(stack, node)
		onStack[node] = true
		for _, next := range outgoing[node] {
			if _, visited := indices[next]; !visited {
				strongConnect(next)
				if lowlink[next] < lowlink[node] {
					lowlink[node] = lowlink[next]
				}
			} else if onStack[next] && indices[next] < lowlink[node] {
				lowlink[node] = indices[next]
			}
		}
		if lowlink[node] != indices[node] {
			return
		}
		component := make([]string, 0)
		for {
			last := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			onStack[last] = false
			component = append(component, last)
			if last == node {
				break
			}
		}
		components = append(components, component)
	}
	for _, node := range nodes {
		if _, visited := indices[node]; !visited {
			strongConnect(node)
		}
	}

	cycles := make([]Cycle, 0)
	cycleNodes := map[string]struct{}{}
	for _, component := range components {
		sort.Strings(component)
		cyclic := len(component) > 1
		if !cyclic && len(component) == 1 {
			for _, next := range outgoing[component[0]] {
				if next == component[0] {
					cyclic = true
					break
				}
			}
		}
		if !cyclic {
			continue
		}
		for _, node := range component {
			cycleNodes[node] = struct{}{}
		}
		cycles = append(cycles, Cycle{Components: component})
	}
	sort.Slice(cycles, func(i, j int) bool { return joined(cycles[i].Components) < joined(cycles[j].Components) })
	for i := range cycles {
		cycles[i].ID = "cycle-" + itoa(i+1)
	}
	return cycles, cycleNodes
}

func markDownstreamBlocked(nodes []string, outgoing map[string][]string, cycleNodes map[string]struct{}, blocked map[string]Block) {
	type queueItem struct {
		node   string
		origin string
		cycle  string
		path   []string
	}
	queue := make([]queueItem, 0, len(cycleNodes))
	for _, node := range nodes {
		if _, cyclic := cycleNodes[node]; cyclic {
			block := blocked[node]
			queue = append(queue, queueItem{node: node, origin: node, cycle: block.CycleID, path: []string{node}})
		}
	}
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		for _, next := range outgoing[item.node] {
			if _, cyclic := cycleNodes[next]; cyclic {
				continue
			}
			if existing, ok := blocked[next]; ok && existing.Reason == BlockReasonUpstreamBlocked && existing.CycleID <= item.cycle {
				continue
			}
			path := append(append([]string(nil), item.path...), next)
			blocked[next] = Block{Reason: BlockReasonUpstreamBlocked, CycleID: item.cycle, Via: path}
			queue = append(queue, queueItem{node: next, origin: item.origin, cycle: item.cycle, path: path})
		}
	}
}

func computeLevels(clean []string, outgoing map[string][]string) (map[string]int, []BatchGroup) {
	levels := make(map[string]int, len(clean))
	if len(clean) == 0 {
		return levels, []BatchGroup{}
	}
	indegree := make(map[string]int, len(clean))
	cleanSet := make(map[string]bool, len(clean))
	for _, node := range clean {
		cleanSet[node] = true
		levels[node] = 0
	}
	for _, from := range clean {
		for _, to := range outgoing[from] {
			if cleanSet[to] {
				indegree[to]++
			}
		}
	}
	queue := make([]string, 0, len(clean))
	for _, node := range clean {
		if indegree[node] == 0 {
			queue = append(queue, node)
		}
	}
	processed := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		processed++
		for _, next := range outgoing[node] {
			if !cleanSet[next] {
				continue
			}
			if candidate := levels[node] + 1; candidate > levels[next] {
				levels[next] = candidate
			}
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	if processed != len(clean) {
		// This should not happen: every cyclic component and its downstream should be blocked.
		// Keeping a deterministic fallback avoids partial map mutation leaking non-determinism.
		levels = map[string]int{}
		return levels, nil
	}
	grouped := map[int][]string{}
	maxLevel := 0
	for _, node := range clean {
		level := levels[node]
		grouped[level] = append(grouped[level], node)
		if level > maxLevel {
			maxLevel = level
		}
	}
	groups := make([]BatchGroup, 0, maxLevel+1)
	for level := 0; level <= maxLevel; level++ {
		members := grouped[level]
		sort.Strings(members)
		groups = append(groups, BatchGroup{Batch: level, Components: members})
	}
	return levels, groups
}

func impactFor(origin string, outgoing map[string][]string, levels map[string]int, blocked map[string]Block) ImpactResult {
	hops := map[string]int{origin: 0}
	queue := []string{origin}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		for _, next := range outgoing[node] {
			candidate := hops[node] + 1
			if existing, seen := hops[next]; seen && existing <= candidate {
				continue
			}
			hops[next] = candidate
			queue = append(queue, next)
		}
	}

	rows := make([]ImpactRow, 0, len(hops))
	levelSet := map[int]struct{}{}
	for component, hop := range hops {
		row := ImpactRow{Component: component, Hop: hop}
		if block, isBlocked := blocked[component]; isBlocked {
			row.Blocked = true
			row.BlockedReason = block.Reason
			row.BlockedByCycle = block.CycleID
		} else if level, ok := levels[component]; ok {
			levelCopy := level
			row.TopologyLevel = &levelCopy
			levelSet[level] = struct{}{}
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Hop != rows[j].Hop {
			return rows[i].Hop < rows[j].Hop
		}
		return rows[i].Component < rows[j].Component
	})
	levelsSorted := make([]int, 0, len(levelSet))
	for level := range levelSet {
		levelsSorted = append(levelsSorted, level)
	}
	sort.Ints(levelsSorted)
	batchForLevel := make(map[int]int, len(levelsSorted))
	for i, level := range levelsSorted {
		batchForLevel[level] = i
	}
	maxHop := 0
	for i := range rows {
		if rows[i].Hop > maxHop {
			maxHop = rows[i].Hop
		}
		if rows[i].TopologyLevel != nil {
			batch := batchForLevel[*rows[i].TopologyLevel]
			rows[i].RecoveryBatch = &batch
		}
	}
	batchMembers := make([][]string, len(levelsSorted))
	blockedComponents := make([]string, 0)
	for _, row := range rows {
		if row.Blocked {
			blockedComponents = append(blockedComponents, row.Component)
			continue
		}
		if row.RecoveryBatch != nil {
			batchMembers[*row.RecoveryBatch] = append(batchMembers[*row.RecoveryBatch], row.Component)
		}
	}
	batches := make([]BatchGroup, 0, len(batchMembers))
	for batch, members := range batchMembers {
		sort.Strings(members)
		batches = append(batches, BatchGroup{Batch: batch, Components: members})
	}
	sort.Strings(blockedComponents)
	return ImpactResult{
		Origin:            origin,
		Affected:          rows,
		RecoveryBatches:   batches,
		BlockedComponents: blockedComponents,
		AffectedCount:     len(rows),
		MaxHop:            maxHop,
	}
}

func digest(analysis *Analysis) string {
	payload, _ := json.Marshal(struct {
		Nodes           []string                `json:"nodes"`
		TopologyBatches map[string]int          `json:"topology_batches"`
		Blocked         map[string]Block        `json:"blocked"`
		Cycles          []Cycle                 `json:"cycles"`
		Impacts         map[string]ImpactResult `json:"impacts"`
	}{
		Nodes: analysis.Nodes, TopologyBatches: analysis.TopologyBatches,
		Blocked: analysis.Blocked, Cycles: analysis.Cycles, Impacts: analysis.Impacts,
	})
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func joined(items []string) string {
	out := ""
	for i, item := range items {
		if i > 0 {
			out += "\x00"
		}
		out += item
	}
	return out
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := make([]byte, 0, 8)
	for value > 0 {
		digits = append(digits, byte('0'+value%10))
		value /= 10
	}
	for i, j := 0, len(digits)-1; i < j; i, j = i+1, j-1 {
		digits[i], digits[j] = digits[j], digits[i]
	}
	return string(digits)
}

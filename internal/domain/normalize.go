package domain

import (
	"fmt"
	"strings"
)

// NormalizedSet is the result of parsing a full LoadSpec. Nothing in this set
// has been persisted yet; the caller stores it atomically (all or nothing).
type NormalizedSet struct {
	Groups    []Group
	Nodes     []Node
	Requests  []Request
	Snapshots []PolicySnapshot
	Fragments []Fragment
}

// Normalize parses and validates a whole LoadSpec. Any problem returns
// E_SPEC_INVALID and the caller must persist nothing (whole-batch rejection).
func Normalize(spec LoadSpec) (*NormalizedSet, error) {
	problems := make([]string, 0)

	groups := make([]Group, 0, len(spec.Groups))
	groupIDs := map[string]bool{}
	for i, g := range spec.Groups {
		if g.ID == "" {
			problems = append(problems, fmt.Sprintf("groups[%d]: empty id", i))
			continue
		}
		if groupIDs[g.ID] {
			problems = append(problems, fmt.Sprintf("groups[%d]: duplicate group id %q", i, g.ID))
			continue
		}
		mem, err := ParseMemoryBytes(g.MemoryLimit)
		if err != nil {
			problems = append(problems, fmt.Sprintf("groups[%d] %q memory_limit: %v", i, g.ID, err))
			mem = -1
		}
		comp, err := ParseComputeMilli(g.ComputeLimit)
		if err != nil {
			problems = append(problems, fmt.Sprintf("groups[%d] %q compute_limit: %v", i, g.ID, err))
			comp = -1
		}
		if mem >= 0 && mem <= 0 {
			problems = append(problems, fmt.Sprintf("groups[%d] %q: memory limit must be positive", i, g.ID))
		}
		if comp >= 0 && comp <= 0 {
			problems = append(problems, fmt.Sprintf("groups[%d] %q: compute limit must be positive", i, g.ID))
		}
		groupIDs[g.ID] = true
		if mem > 0 && comp > 0 {
			groups = append(groups, Group{ID: g.ID, MemoryLimit: mem, ComputeLimitM: comp})
		}
	}

	nodes := make([]Node, 0, len(spec.Nodes))
	nodeIDs := map[string]bool{}
	nodeComputeSum := map[string]int64{}
	nodeMemorySum := map[string]int64{}
	for i, n := range spec.Nodes {
		if n.ID == "" {
			problems = append(problems, fmt.Sprintf("nodes[%d]: empty id", i))
			continue
		}
		if nodeIDs[n.ID] {
			problems = append(problems, fmt.Sprintf("nodes[%d]: duplicate node id %q", i, n.ID))
			continue
		}
		if !groupIDs[n.GroupID] {
			problems = append(problems, fmt.Sprintf("nodes[%d] %q: unknown quota group %q", i, n.ID, n.GroupID))
			continue
		}
		health := strings.ToUpper(strings.TrimSpace(n.Health))
		if health == "" {
			health = HealthHealthy
		}
		if !validHealth(health) {
			problems = append(problems, fmt.Sprintf("nodes[%d] %q: unknown health %q", i, n.ID, n.Health))
			continue
		}
		mem, err := ParseMemoryBytes(n.MemoryTotal)
		if err != nil {
			problems = append(problems, fmt.Sprintf("nodes[%d] %q memory_total: %v", i, n.ID, err))
			mem = -1
		}
		comp, err := ParseComputeMilli(n.ComputeTotal)
		if err != nil {
			problems = append(problems, fmt.Sprintf("nodes[%d] %q compute_total: %v", i, n.ID, err))
			comp = -1
		}
		if mem <= 0 || comp <= 0 {
		} else {
			nodeIDs[n.ID] = true
			nodes = append(nodes, Node{
				ID: n.ID, GroupID: n.GroupID, MemoryTotal: mem,
				ComputeTotalM: comp, Health: health,
			})
			nodeComputeSum[n.GroupID] += comp
			nodeMemorySum[n.GroupID] += mem
		}
	}

	// Group capacity cannot exceed the aggregate capacity of its member nodes.
	for _, g := range groups {
		if nodeMemorySum[g.ID] < g.MemoryLimit || nodeComputeSum[g.ID] < g.ComputeLimitM {
			problems = append(problems, fmt.Sprintf(
				"group %q: limit exceeds member capacity (memory members=%d limit=%d, compute members=%d limit=%d)",
				g.ID, nodeMemorySum[g.ID], g.MemoryLimit, nodeComputeSum[g.ID], g.ComputeLimitM))
		}
	}

	reqs := make([]Request, 0, len(spec.Requests))
	reqIDs := map[string]bool{}
	for i, r := range spec.Requests {
		if r.ID == "" {
			problems = append(problems, fmt.Sprintf("requests[%d]: empty id", i))
			continue
		}
		if reqIDs[r.ID] {
			problems = append(problems, fmt.Sprintf("requests[%d]: duplicate request id %q", i, r.ID))
			continue
		}
		if !groupIDs[r.GroupID] {
			problems = append(problems, fmt.Sprintf("requests[%d] %q: unknown quota group %q", i, r.ID, r.GroupID))
			continue
		}
		mem, err := ParseMemoryBytes(r.MemoryNeed)
		if err != nil {
			problems = append(problems, fmt.Sprintf("requests[%d] %q memory_need: %v", i, r.ID, err))
			mem = -1
		}
		comp, err := ParseComputeMilli(r.ComputeNeed)
		if err != nil {
			problems = append(problems, fmt.Sprintf("requests[%d] %q compute_need: %v", i, r.ID, err))
			comp = -1
		}
		if mem <= 0 || comp <= 0 {
			continue
		}
		if r.SamplesPerBatch <= 0 {
			problems = append(problems, fmt.Sprintf("requests[%d] %q: samples_per_batch must be positive", i, r.ID))
			continue
		}
		if r.LatencyLimitMs <= 0 {
			problems = append(problems, fmt.Sprintf("requests[%d] %q: latency_limit_ms must be positive", i, r.ID))
			continue
		}
		if r.Priority < 0 {
			problems = append(problems, fmt.Sprintf("requests[%d] %q: priority cannot be negative", i, r.ID))
			continue
		}
		reqIDs[r.ID] = true
		reqs = append(reqs, Request{
			ID: r.ID, ModelID: r.ModelID, GroupID: r.GroupID,
			MemoryNeed: mem, ComputeNeedM: comp,
			SamplesPerBatch: r.SamplesPerBatch, LatencyLimitMs: r.LatencyLimitMs,
			Priority: r.Priority, State: StatePending,
		})
	}

	snaps := make([]PolicySnapshot, 0, len(spec.Snapshots))
	versions := map[int64]bool{}
	lastVersion := int64(0)
	prevDigest := ""
	for i, s := range spec.Snapshots {
		if s.Version <= 0 {
			problems = append(problems, fmt.Sprintf("snapshots[%d]: version must be positive", i))
			continue
		}
		if versions[s.Version] {
			problems = append(problems, fmt.Sprintf("snapshots[%d]: duplicate version %d", i, s.Version))
			continue
		}
		if s.Version <= lastVersion {
			problems = append(problems, fmt.Sprintf("snapshots[%d]: version %d not monotonic (last=%d)", i, s.Version, lastVersion))
			continue
		}
		if i > 0 && s.PrevDigest != prevDigest {
			problems = append(problems, fmt.Sprintf("snapshots[%d]: prev_digest does not chain to previous digest", i))
			continue
		}
		if p := validateParams(s.Params); p != "" {
			problems = append(problems, fmt.Sprintf("snapshots[%d]: %s", i, p))
			continue
		}
		digest, err := DigestPolicy(s.Version, s.Params, s.PrevDigest)
		if err != nil {
			problems = append(problems, fmt.Sprintf("snapshots[%d]: digest: %v", i, err))
			continue
		}
		versions[s.Version] = true
		lastVersion = s.Version
		prevDigest = digest
		snaps = append(snaps, PolicySnapshot{
			Version: s.Version, Digest: digest, PrevDigest: s.PrevDigest, Params: s.Params,
		})
	}

	frags := make([]Fragment, 0, len(spec.Fragments))
	for i, f := range spec.Fragments {
		if f.Version <= 0 {
			problems = append(problems, fmt.Sprintf("fragments[%d]: version must be positive", i))
			continue
		}
		if len(f.Fields) == 0 {
			problems = append(problems, fmt.Sprintf("fragments[%d]: fragment must contain affected fields", i))
			continue
		}
		digest, err := DigestFragment(f.Version, f.BaseDigest, f.Fields)
		if err != nil {
			problems = append(problems, fmt.Sprintf("fragments[%d]: %v", i, err))
			continue
		}
		frags = append(frags, Fragment{
			Version: f.Version, BaseDigest: f.BaseDigest, Digest: digest, Fields: f.Fields,
		})
	}

	if len(problems) > 0 {
		return nil, NewError(ESpecInvalid,
			fmt.Sprintf("spec validation failed with %d problem(s)", len(problems)),
			map[string]any{"problems": problems})
	}
	return &NormalizedSet{Groups: groups, Nodes: nodes, Requests: reqs, Snapshots: snaps, Fragments: frags}, nil
}

func validateParams(p PolicyParams) string {
	switch {
	case p.GlobalMaxConcurrent <= 0:
		return "global_max_concurrent_batches must be positive"
	case p.WaitQueueCapacity < 0:
		return "wait_queue_capacity cannot be negative"
	case p.RoundBudgetMs <= 0:
		return "round_budget_ms must be positive"
	case p.LatencyBaseMs < 0:
		return "latency_base_ms cannot be negative"
	case p.LatencyPerSampleMs < 0:
		return "latency_per_sample_ms cannot be negative"
	}
	return ""
}

func validHealth(h string) bool {
	switch h {
	case HealthHealthy, HealthDegraded, HealthDraining, HealthUnhealthy:
		return true
	}
	return false
}

package qos

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const (
	maxQueueIDLength = 128
	maxRateBPS       = int64(1_000_000_000_000_000)
	maxBurstBytes    = int64(1_000_000_000_000_000)
)

// NormalizeAndValidate returns a deterministic, normalized policy. Validation is
// complete before any caller starts a write transaction.
func NormalizeAndValidate(in []Queue) ([]Queue, map[string]Queue, *BusinessError) {
	if len(in) == 0 {
		return nil, nil, policyInvalid("policy must contain at least one queue", map[string]any{"queue_count": 0})
	}
	out := make([]Queue, 0, len(in))
	seen := map[string]int{}
	for i, original := range in {
		q := original
		q.ID = strings.TrimSpace(q.ID)
		if q.ID == "" || len(q.ID) > maxQueueIDLength {
			return nil, nil, policyInvalid("queue_id must be non-empty and no longer than 128 bytes", map[string]any{
				"queue_index": i, "queue_id": original.ID,
			})
		}
		if first, ok := seen[q.ID]; ok {
			return nil, nil, policyInvalid("queue_id must be unique", map[string]any{
				"queue_id": q.ID, "first_index": first, "duplicate_index": i,
			})
		}
		seen[q.ID] = i
		if q.CIRBPS <= 0 || q.PIRBPS <= 0 || q.CBSBytes <= 0 || q.PBSBytes <= 0 {
			return nil, nil, policyInvalid("rates and burst sizes must be positive integers", map[string]any{
				"queue_id": q.ID, "cir_bps": q.CIRBPS, "pir_bps": q.PIRBPS,
				"cbs_bytes": q.CBSBytes, "pbs_bytes": q.PBSBytes,
			})
		}
		if q.CBSBytes > q.PBSBytes {
			return nil, nil, policyInvalid("CBS must not exceed PBS", map[string]any{
				"queue_id": q.ID, "cbs_bytes": q.CBSBytes, "pbs_bytes": q.PBSBytes,
			})
		}
		if q.CIRBPS > q.PIRBPS {
			return nil, nil, policyInvalid("CIR must not exceed PIR", map[string]any{
				"queue_id": q.ID, "cir_bps": q.CIRBPS, "pir_bps": q.PIRBPS,
			})
		}
		if q.CIRBPS > maxRateBPS || q.PIRBPS > maxRateBPS || q.CBSBytes > maxBurstBytes || q.PBSBytes > maxBurstBytes {
			return nil, nil, policyInvalid("rate or burst value exceeds the supported integer range", map[string]any{
				"queue_id": q.ID,
			})
		}
		if q.Priority < 1 || q.Priority > 7 {
			return nil, nil, policyInvalid("priority must be in the inclusive range 1..7", map[string]any{
				"queue_id": q.ID, "priority": q.Priority,
			})
		}
		if q.Match.SrcPortMin < 0 || q.Match.SrcPortMax > 65535 || q.Match.SrcPortMin > q.Match.SrcPortMax {
			return nil, nil, policyInvalid("source port range must satisfy 0 <= min <= max <= 65535", map[string]any{
				"queue_id": q.ID, "src_port_min": q.Match.SrcPortMin, "src_port_max": q.Match.SrcPortMax,
			})
		}
		dscp := append([]int(nil), q.Match.DSCPValues...)
		sort.Ints(dscp)
		uniq := dscp[:0]
		for _, v := range dscp {
			if v < 0 || v > 63 {
				return nil, nil, policyInvalid("DSCP values must be in the inclusive range 0..63", map[string]any{
					"queue_id": q.ID, "dscp": v,
				})
			}
			if len(uniq) == 0 || uniq[len(uniq)-1] != v {
				uniq = append(uniq, v)
			}
		}
		q.Match.DSCPValues = append([]int(nil), uniq...)
		if q.ParentID != nil {
			parent := strings.TrimSpace(*q.ParentID)
			if parent == "" {
				return nil, nil, policyInvalid("parent_id must be null or a non-empty queue_id", map[string]any{
					"queue_id": q.ID,
				})
			}
			q.ParentID = &parent
		}
		out = append(out, q)
	}

	policy := make(map[string]Queue, len(out))
	for _, q := range out {
		policy[q.ID] = q
	}
	for _, q := range out {
		if q.ParentID != nil {
			if _, ok := policy[*q.ParentID]; !ok {
				return nil, nil, policyInvalid("parent queue does not exist", map[string]any{
					"queue_id": q.ID, "parent_id": *q.ParentID,
				})
			}
		}
	}
	if cycle := findCycle(policy); len(cycle) > 0 {
		return nil, nil, cycleError(cycle)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, policy, nil
}

func findCycle(policy map[string]Queue) []string {
	ids := make([]string, 0, len(policy))
	for id := range policy {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	color := make(map[string]uint8, len(ids))
	stack := make([]string, 0, len(ids))
	position := make(map[string]int, len(ids))
	var found []string
	var visit func(string) bool
	visit = func(id string) bool {
		color[id] = 1
		position[id] = len(stack)
		stack = append(stack, id)
		q := policy[id]
		if q.ParentID != nil {
			parent := *q.ParentID
			switch color[parent] {
			case 0:
				if visit(parent) {
					return true
				}
			case 1:
				start := position[parent]
				found = append(found, stack[start:]...)
				found = append(found, parent)
				return true
			}
		}
		stack = stack[:len(stack)-1]
		delete(position, id)
		color[id] = 2
		return false
	}
	for _, id := range ids {
		if color[id] == 0 && visit(id) {
			return found
		}
	}
	return nil
}

func PolicyDigest(policy map[string]Queue) string {
	ids := SortedQueueIDs(policy)
	normalized := make([]Queue, 0, len(ids))
	for _, id := range ids {
		normalized = append(normalized, policy[id])
	}
	b, _ := json.Marshal(normalized)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func SortedQueueIDs(policy map[string]Queue) []string {
	ids := make([]string, 0, len(policy))
	for id := range policy {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func Children(policy map[string]Queue) map[string][]string {
	children := make(map[string][]string)
	for id, q := range policy {
		if q.ParentID != nil {
			children[*q.ParentID] = append(children[*q.ParentID], id)
		}
	}
	for k := range children {
		sort.Strings(children[k])
	}
	return children
}

func Descendants(policy map[string]Queue, root string) []string {
	children := Children(policy)
	seen := map[string]bool{}
	stack := append([]string(nil), children[root]...)
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[id] {
			continue
		}
		seen[id] = true
		stack = append(stack, children[id]...)
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// ComputeEffectiveRates calculates inherited rates in parent-before-child order.
func ComputeEffectiveRates(policy map[string]Queue) map[string]EffectiveRates {
	memo := make(map[string]EffectiveRates, len(policy))
	var compute func(string) EffectiveRates
	compute = func(id string) EffectiveRates {
		if got, ok := memo[id]; ok {
			return got
		}
		q := policy[id]
		r := EffectiveRates{
			QueueID: id, DeclaredCIRBPS: q.CIRBPS, EffectiveCIRBPS: q.CIRBPS,
			CIRSRC:         InheritanceSource{Kind: "declared", SelfValue: q.CIRBPS},
			DeclaredPIRBPS: q.PIRBPS, EffectivePIRBPS: q.PIRBPS,
			PIRSRC:   InheritanceSource{Kind: "declared", SelfValue: q.PIRBPS},
			ParentID: q.ParentID, ParentChain: []string{},
		}
		if q.ParentID != nil {
			p := compute(*q.ParentID)
			r.ParentChain = append(append([]string(nil), p.ParentChain...), p.QueueID)
			r.Depth = len(r.ParentChain)
			if q.CIRBPS > p.EffectiveCIRBPS {
				r.EffectiveCIRBPS = p.EffectiveCIRBPS
				r.CIRSRC = InheritanceSource{Kind: "parent_limit", SelfValue: q.CIRBPS, ParentQueueID: p.QueueID, ParentEffective: p.EffectiveCIRBPS}
			}
			if q.PIRBPS > p.EffectivePIRBPS {
				r.EffectivePIRBPS = p.EffectivePIRBPS
				r.PIRSRC = InheritanceSource{Kind: "parent_limit", SelfValue: q.PIRBPS, ParentQueueID: p.QueueID, ParentEffective: p.EffectivePIRBPS}
			}
		}
		memo[id] = r
		return r
	}
	for id := range policy {
		compute(id)
	}
	return memo
}

func EffectiveRatesFor(policy map[string]Queue, id string) EffectiveRates {
	return ComputeEffectiveRates(policy)[id]
}

func BuildTokenBucketView(q Queue, eff EffectiveRates, calculationVersion, validThrough int64) TokenBucketView {
	return TokenBucketView{
		QueueID: q.ID, EffectiveCIRBPS: eff.EffectiveCIRBPS, EffectivePIRBPS: eff.EffectivePIRBPS,
		CBSBytes: q.CBSBytes, PBSBytes: q.PBSBytes, Priority: q.Priority,
		PriorityLayer: fmt.Sprintf("P%d", q.Priority), ParentChain: append([]string(nil), eff.ParentChain...),
		TokenUnit:            "bits",
		InitialState:         "both buckets start full at the first packet timestamp",
		CommittedBucket:      fmt.Sprintf("capacity=%d bits; refill=effective CIR %d bps", q.CBSBytes*8, eff.EffectiveCIRBPS),
		PeakBucket:           fmt.Sprintf("capacity=%d bits; refill=effective PIR %d bps", q.PBSBytes*8, eff.EffectivePIRBPS),
		RefillContract:       "exact_millibits = rate_bps * delta_ms; whole_bits = floor((carry_millibits + exact_millibits)/1000); the remainder is carried to the next packet",
		RoundingContract:     "integer arithmetic only; each refill reports raw millibits, whole bits, carried remainder and cumulative error (<1 bit)",
		MsTruncationContract: "timestamp differences are integer milliseconds; any sub-millisecond component is discarded before refill (samples use integer ms, so this is zero)",
		CalculationVersion:   calculationVersion, ValidThroughVersion: validThrough,
	}
}

// BuildConsistencyScore evaluates own-field sanity and the complete parent chain.
// The penal scores are intentionally deterministic and inspectable.
func BuildConsistencyScore(q Queue, eff EffectiveRates, policy map[string]Queue, calculationVersion, validThrough int64) ConsistencyScore {
	checks := []ScoreCheck{
		{Name: "cbs_lte_pbs", Passed: q.CBSBytes <= q.PBSBytes, Penalty: 0},
		{Name: "cir_lte_pir", Passed: q.CIRBPS <= q.PIRBPS, Penalty: 0},
		{Name: "effective_cir_parent_bound", Passed: true, Penalty: 0},
		{Name: "effective_pir_parent_bound", Passed: true, Penalty: 0},
	}
	score := 100
	if eff.EffectiveCIRBPS < q.CIRBPS {
		checks = append(checks, ScoreCheck{Name: "declared_cir_inherited_clamp", Passed: true, Penalty: 10,
			Detail: fmt.Sprintf("effective CIR %d is inherited below declared CIR %d", eff.EffectiveCIRBPS, q.CIRBPS)})
		score -= 10
	}
	if eff.EffectivePIRBPS < q.PIRBPS {
		checks = append(checks, ScoreCheck{Name: "declared_pir_inherited_clamp", Passed: true, Penalty: 10,
			Detail: fmt.Sprintf("effective PIR %d is inherited below declared PIR %d", eff.EffectivePIRBPS, q.PIRBPS)})
		score -= 10
	}
	priorityInversions := 0
	refs := []DerivedReference{
		{Field: "own.cir_bps", QueueID: q.ID, Source: "declared", Value: q.CIRBPS},
		{Field: "own.pir_bps", QueueID: q.ID, Source: "declared", Value: q.PIRBPS},
		{Field: "own.cbs_bytes", QueueID: q.ID, Source: "declared", Value: q.CBSBytes},
		{Field: "own.pbs_bytes", QueueID: q.ID, Source: "declared", Value: q.PBSBytes},
		{Field: "own.priority", QueueID: q.ID, Source: "declared", Value: int64(q.Priority)},
	}
	for _, ancestorID := range eff.ParentChain {
		ancestor := policy[ancestorID]
		ancestorEff := EffectiveRatesFor(policy, ancestorID)
		refs = append(refs,
			DerivedReference{Field: "parent.effective_cir_bps", QueueID: ancestorID, Source: "parent_chain", Value: ancestorEff.EffectiveCIRBPS},
			DerivedReference{Field: "parent.effective_pir_bps", QueueID: ancestorID, Source: "parent_chain", Value: ancestorEff.EffectivePIRBPS},
			DerivedReference{Field: "parent.priority", QueueID: ancestorID, Source: "parent_chain", Value: int64(ancestor.Priority)},
		)
		if ancestorEff.EffectiveCIRBPS < eff.EffectiveCIRBPS || ancestorEff.EffectivePIRBPS < eff.EffectivePIRBPS {
			checks[2].Passed = false
			checks[3].Passed = false
			score -= 40
		}
		if q.Priority < ancestor.Priority {
			priorityInversions++
		}
	}
	if priorityInversions > 0 {
		penalty := priorityInversions * 5
		if penalty > 20 {
			penalty = 20
		}
		checks = append(checks, ScoreCheck{Name: "priority_monotonic_with_parent_chain", Passed: false, Penalty: penalty,
			Detail: fmt.Sprintf("child priority %d is below %d ancestor priorities", q.Priority, priorityInversions)})
		score -= penalty
	} else {
		checks = append(checks, ScoreCheck{Name: "priority_monotonic_with_parent_chain", Passed: true, Penalty: 0})
	}
	if score < 0 {
		score = 0
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Field == refs[j].Field {
			return refs[i].QueueID < refs[j].QueueID
		}
		return refs[i].Field < refs[j].Field
	})
	return ConsistencyScore{
		QueueID: q.ID, Score: score, Checks: checks, References: refs,
		ParentChain:        append([]string(nil), eff.ParentChain...),
		CalculationVersion: calculationVersion, ValidThroughVersion: validThrough,
	}
}

func cloneQueue(q Queue) Queue {
	out := q
	if q.ParentID != nil {
		p := *q.ParentID
		out.ParentID = &p
	}
	out.Match.DSCPValues = append([]int(nil), q.Match.DSCPValues...)
	return out
}

func ClonePolicy(in map[string]Queue) map[string]Queue {
	out := make(map[string]Queue, len(in))
	for id, q := range in {
		out[id] = cloneQueue(q)
	}
	return out
}

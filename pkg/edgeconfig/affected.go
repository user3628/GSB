package edgeconfig

import (
	"fmt"
	"sort"
)

type Graph struct {
	Keys           map[string]ConfigKey
	Nodes          map[string]Node
	Subscriptions  map[string]map[string]bool
	KeySubscribers map[string][]string
	RegionNodes    map[string][]string
	KeyRegions     map[string][]string
	Dependents     map[string][]string
	Policy         Policy
}

func NewGraph(keys []ConfigKey, nodes []Node, policy Policy) *Graph {
	g := &Graph{
		Keys:           make(map[string]ConfigKey, len(keys)),
		Nodes:          make(map[string]Node, len(nodes)),
		Subscriptions:  make(map[string]map[string]bool, len(nodes)),
		KeySubscribers: make(map[string][]string),
		RegionNodes:    make(map[string][]string),
		KeyRegions:     make(map[string][]string),
		Dependents:     make(map[string][]string),
		Policy:         withDefaultPolicy(policy),
	}
	for _, key := range keys {
		g.Keys[key.Path] = key
	}
	for _, node := range nodes {
		g.Nodes[node.ID] = node
		g.Subscriptions[node.ID] = make(map[string]bool, len(node.SubscribedKeys))
		for _, keyPath := range node.SubscribedKeys {
			if !g.Subscriptions[node.ID][keyPath] {
				g.Subscriptions[node.ID][keyPath] = true
				g.KeySubscribers[keyPath] = append(g.KeySubscribers[keyPath], node.ID)
			}
		}
		g.RegionNodes[node.Region] = append(g.RegionNodes[node.Region], node.ID)
		for _, dep := range node.DependsOn {
			g.Dependents[dep] = append(g.Dependents[dep], node.ID)
		}
	}
	for keyPath, ids := range g.KeySubscribers {
		sort.Strings(ids)
		g.KeySubscribers[keyPath] = ids
		regionSet := make(map[string]bool)
		for _, id := range ids {
			if node, ok := g.Nodes[id]; ok {
				regionSet[node.Region] = true
			}
		}
		regions := make([]string, 0, len(regionSet))
		for region := range regionSet {
			regions = append(regions, region)
		}
		sort.Strings(regions)
		g.KeyRegions[keyPath] = regions
	}
	for region := range g.RegionNodes {
		sort.Strings(g.RegionNodes[region])
	}
	for upstream := range g.Dependents {
		sort.Strings(g.Dependents[upstream])
	}
	return g
}

func DefaultPolicy() Policy {
	return Policy{
		Strategy: StrategyPolicy{
			PerKeyExactMaxChangedKeys:  4,
			RegionBucketMinChangedKeys: 5,
			LargeSubscriptionRatioPPM:  900_000,
			FullSubscriptionSwitchKeys: 2,
		},
		Probe: ProbePolicy{TimeoutMS: 1_000, Concurrency: 4},
		Numeric: NumericPolicy{
			Scale:                1_000_000,
			ThresholdUnits:       990_000,
			RelativeTolerancePPM: 1_000,
			DefaultWeight:        1,
			Rounding:             "round_half_up",
			TieBreak:             "region_then_node_lexicographic",
		},
	}
}

func withDefaultPolicy(policy Policy) Policy {
	def := DefaultPolicy()
	if policy.Strategy.PerKeyExactMaxChangedKeys == 0 {
		policy.Strategy.PerKeyExactMaxChangedKeys = def.Strategy.PerKeyExactMaxChangedKeys
	}
	if policy.Strategy.RegionBucketMinChangedKeys == 0 {
		policy.Strategy.RegionBucketMinChangedKeys = def.Strategy.RegionBucketMinChangedKeys
	}
	if policy.Strategy.LargeSubscriptionRatioPPM == 0 {
		policy.Strategy.LargeSubscriptionRatioPPM = def.Strategy.LargeSubscriptionRatioPPM
	}
	if policy.Strategy.FullSubscriptionSwitchKeys == 0 {
		policy.Strategy.FullSubscriptionSwitchKeys = def.Strategy.FullSubscriptionSwitchKeys
	}
	if policy.Probe.TimeoutMS == 0 {
		policy.Probe.TimeoutMS = def.Probe.TimeoutMS
	}
	if policy.Probe.Concurrency == 0 {
		policy.Probe.Concurrency = def.Probe.Concurrency
	}
	if policy.Numeric.Scale == 0 {
		policy.Numeric.Scale = def.Numeric.Scale
	}
	if policy.Numeric.ThresholdUnits == 0 {
		policy.Numeric.ThresholdUnits = def.Numeric.ThresholdUnits
	}
	if policy.Numeric.RelativeTolerancePPM == 0 {
		policy.Numeric.RelativeTolerancePPM = def.Numeric.RelativeTolerancePPM
	}
	if policy.Numeric.DefaultWeight == 0 {
		policy.Numeric.DefaultWeight = def.Numeric.DefaultWeight
	}
	if policy.Numeric.Rounding == "" {
		policy.Numeric.Rounding = def.Numeric.Rounding
	}
	if policy.Numeric.TieBreak == "" {
		policy.Numeric.TieBreak = def.Numeric.TieBreak
	}
	return policy
}

type ImpactInput struct {
	BaseVersion   int64
	TargetVersion int64
	ChangedKeys   []string
	// SeedTriggers maps nodes directly changed by a subscription mutation to trigger keys.
	SeedTriggers map[string][]string
}

func (g *Graph) AnalyzeImpact(input ImpactInput) *AffectedReport {
	changed := dedupSorted(input.ChangedKeys)
	changedSet := make(map[string]bool, len(changed))
	for _, key := range changed {
		changedSet[key] = true
	}

	metrics := StrategyMetrics{
		TotalNodes:       len(g.Nodes),
		ChangedKeys:      len(changed),
		NaiveScanProduct: len(changed) * len(g.Nodes),
	}
	declaredKeyCount := len(g.Keys)
	largeCount := 0
	for id := range g.Nodes {
		coverage := len(g.Subscriptions[id])
		if declaredKeyCount > 0 && coverage == declaredKeyCount {
			metrics.FullSubscriptionNodes++
		}
		if declaredKeyCount > 0 && int64(coverage)*1_000_000 >= g.Policy.Strategy.LargeSubscriptionRatioPPM*int64(declaredKeyCount) {
			largeCount++
		}
	}

	useRegionBucket := false
	basis := ""
	switch {
	case g.Policy.Strategy.RegionBucketMinChangedKeys > 0 && len(changed) >= g.Policy.Strategy.RegionBucketMinChangedKeys:
		useRegionBucket = true
		basis = fmt.Sprintf("changed_keys=%d meets region_bucket_min=%d", len(changed), g.Policy.Strategy.RegionBucketMinChangedKeys)
	case metrics.FullSubscriptionNodes > 0 && g.Policy.Strategy.FullSubscriptionSwitchKeys > 0 && len(changed) >= g.Policy.Strategy.FullSubscriptionSwitchKeys:
		useRegionBucket = true
		basis = fmt.Sprintf("changed_keys=%d meets full_subscription_switch=%d with %d full-subscription node(s)", len(changed), g.Policy.Strategy.FullSubscriptionSwitchKeys, metrics.FullSubscriptionNodes)
	case len(changed) <= g.Policy.Strategy.PerKeyExactMaxChangedKeys:
		basis = fmt.Sprintf("changed_keys=%d is within per_key_exact_max=%d", len(changed), g.Policy.Strategy.PerKeyExactMaxChangedKeys)
	default:
		useRegionBucket = true
		basis = fmt.Sprintf("changed_keys=%d exceeds per_key_exact_max=%d", len(changed), g.Policy.Strategy.PerKeyExactMaxChangedKeys)
	}
	_ = largeCount

	triggers := make(map[string]map[string]bool)
	addTrigger := func(nodeID, key string) {
		if triggers[nodeID] == nil {
			triggers[nodeID] = make(map[string]bool)
		}
		triggers[nodeID][key] = true
	}

	if useRegionBucket {
		regions := make(map[string]bool)
		for _, key := range changed {
			for _, region := range g.KeyRegions[key] {
				regions[region] = true
			}
		}
		for nodeID := range input.SeedTriggers {
			if node, ok := g.Nodes[nodeID]; ok {
				regions[node.Region] = true
			}
		}
		regionList := make([]string, 0, len(regions))
		for region := range regions {
			regionList = append(regionList, region)
		}
		sort.Strings(regionList)
		metrics.BucketRegions = len(regionList)
		for _, region := range regionList {
			for _, nodeID := range g.RegionNodes[region] {
				metrics.CandidateNodesScanned++
				for key := range g.Subscriptions[nodeID] {
					if changedSet[key] {
						addTrigger(nodeID, key)
						metrics.SubscriberVisits++
					}
				}
			}
		}
	} else {
		for _, key := range changed {
			metrics.IndexLookups++
			subscribers := g.KeySubscribers[key]
			metrics.SubscriberVisits += len(subscribers)
			for _, nodeID := range subscribers {
				addTrigger(nodeID, key)
			}
		}
	}

	seedSet := make(map[string]bool)
	for nodeID, keys := range input.SeedTriggers {
		if _, exists := g.Nodes[nodeID]; !exists {
			continue
		}
		seedSet[nodeID] = true
		for _, key := range keys {
			addTrigger(nodeID, key)
		}
	}

	hop := make(map[string]int)
	for nodeID := range triggers {
		hop[nodeID] = 1
	}
	for nodeID := range seedSet {
		hop[nodeID] = 1
	}

	order := g.topologicalOrder()
	for _, upstreamID := range order {
		currentHop, affected := hop[upstreamID]
		if !affected {
			continue
		}
		for _, downstreamID := range g.Dependents[upstreamID] {
			if existing, ok := hop[downstreamID]; !ok || currentHop+1 < existing {
				hop[downstreamID] = currentHop + 1
			}
			if triggers[downstreamID] == nil {
				triggers[downstreamID] = make(map[string]bool)
			}
			for key := range triggers[upstreamID] {
				triggers[downstreamID][key] = true
			}
		}
	}

	impacted := make([]ImpactedNode, 0, len(hop))
	for nodeID, nodeHop := range hop {
		node := g.Nodes[nodeID]
		keyList := make([]string, 0, len(triggers[nodeID]))
		for key := range triggers[nodeID] {
			keyList = append(keyList, key)
		}
		sort.Strings(keyList)
		impacted = append(impacted, ImpactedNode{
			NodeID:      nodeID,
			Region:      node.Region,
			Hop:         nodeHop,
			TriggerKeys: keyList,
			Seeds:       seedSet[nodeID] || containsAnyKeySubscription(nodeID, changedSet, g),
		})
	}
	sort.Slice(impacted, func(i, j int) bool {
		if impacted[i].Hop != impacted[j].Hop {
			return impacted[i].Hop < impacted[j].Hop
		}
		if impacted[i].Region != impacted[j].Region {
			return impacted[i].Region < impacted[j].Region
		}
		return impacted[i].NodeID < impacted[j].NodeID
	})

	path := "per_key_exact"
	if useRegionBucket {
		path = "region_bucket"
	}
	return &AffectedReport{
		BaseVersion:    input.BaseVersion,
		TargetVersion:  input.TargetVersion,
		ChangedKeys:    changed,
		SelectedPath:   path,
		SelectionBasis: basis,
		Metrics:        metrics,
		Nodes:          impacted,
	}
}

func containsAnyKeySubscription(nodeID string, changed map[string]bool, g *Graph) bool {
	for key := range g.Subscriptions[nodeID] {
		if changed[key] {
			return true
		}
	}
	return false
}

func (g *Graph) topologicalOrder() []string {
	indegree := make(map[string]int, len(g.Nodes))
	edges := make(map[string][]string, len(g.Nodes))
	for id, node := range g.Nodes {
		indegree[id] = len(node.DependsOn)
		for _, dep := range node.DependsOn {
			edges[dep] = append(edges[dep], id)
		}
	}
	ready := make([]string, 0)
	for id, degree := range indegree {
		if degree == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	order := make([]string, 0, len(g.Nodes))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		order = append(order, id)
		next := append([]string(nil), edges[id]...)
		sort.Strings(next)
		for _, downstream := range next {
			indegree[downstream]--
			if indegree[downstream] == 0 {
				ready = append(ready, downstream)
			}
		}
		sort.Strings(ready)
	}
	if len(order) != len(g.Nodes) {
		// Validation rejects cycles. Keep a stable fallback for defensive use.
		all := make([]string, 0, len(g.Nodes))
		for id := range g.Nodes {
			all = append(all, id)
		}
		sort.Strings(all)
		return all
	}
	return order
}

func BuildExpectedViews(commitVersion int64, g *Graph, previous map[string]ExpectedView) ([]ExpectedView, error) {
	views := make([]ExpectedView, 0, len(g.Nodes))
	digests := make(map[string]string, len(g.Nodes))
	for _, id := range g.topologicalOrder() {
		node := g.Nodes[id]
		subscribed := make([]string, 0, len(g.Subscriptions[id]))
		for keyPath := range g.Subscriptions[id] {
			subscribed = append(subscribed, keyPath)
		}
		sort.Strings(subscribed)
		keys := make([]ConfigKey, 0, len(subscribed))
		deps := make(map[string]string, len(node.DependsOn))
		for _, keyPath := range subscribed {
			keys = append(keys, g.Keys[keyPath])
		}
		for _, dep := range node.DependsOn {
			deps[dep] = digests[dep]
		}
		digest, canonical, err := ExpectedDigest(keys, deps)
		if err != nil {
			return nil, err
		}
		digests[id] = digest
		expectedVersion := commitVersion
		if prior, ok := previous[id]; ok && prior.ExpectedDigest == digest {
			expectedVersion = prior.ExpectedVersion
		}
		views = append(views, ExpectedView{
			CommitVersion:   commitVersion,
			NodeID:          id,
			Region:          node.Region,
			ExpectedVersion: expectedVersion,
			ExpectedDigest:  digest,
			KeyCount:        len(keys),
			DigestJSON:      canonical,
		})
	}
	sort.Slice(views, func(i, j int) bool { return views[i].NodeID < views[j].NodeID })
	return views, nil
}

func dedupSorted(values []string) []string {
	seen := make(map[string]bool, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

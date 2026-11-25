package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"elastic-scale/internal/model"
)

const (
	MetricRequests = "http.requests"
	MetricLatency  = "http.latency_ms"

	RefMetric   = "synthetic_metric"
	RefImpact   = "impact_set"
	RefDecision = "scaling_decision"
)

type State struct {
	Version  int64
	Services []model.Service
	Samples  []model.Sample

	serviceMap map[string]model.Service
	sampleMap  map[string][]model.Sample
}

func NewState(version int64, services []model.Service, samples []model.Sample) *State {
	for i := range services {
		services[i].Normalize()
	}
	samples = model.NormalizeSamples(samples)
	st := &State{Version: version, Services: services, Samples: samples, serviceMap: map[string]model.Service{}, sampleMap: map[string][]model.Sample{}}
	for _, svc := range services {
		st.serviceMap[svc.ID] = svc
	}
	for _, sample := range samples {
		st.sampleMap[sample.ServiceID] = append(st.sampleMap[sample.ServiceID], sample)
	}
	return st
}

func (s *State) Service(id string) (model.Service, bool) {
	v, ok := s.serviceMap[id]
	return v, ok
}

func (s *State) SamplesFor(serviceID string) []model.Sample {
	return s.sampleMap[serviceID]
}

func Checksum(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		sum := sha256.Sum256([]byte(fmt.Sprint(v)))
		return "sha256:" + hex.EncodeToString(sum[:])
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func ValidateCatalog(services []model.Service) error {
	seen := map[string]struct{}{}
	for i := range services {
		svc := &services[i]
		svc.Normalize()
		if svc.ID == "" {
			return model.NewError(model.CodeModelInvalid, "service id is required", map[string]any{"index": i})
		}
		if _, ok := seen[svc.ID]; ok {
			return model.NewError(model.CodeModelInvalid, "duplicate service id", map[string]any{"service_id": svc.ID})
		}
		seen[svc.ID] = struct{}{}
		if svc.Component == "" || svc.ScalingGroup == "" {
			return model.NewError(model.CodeModelInvalid, "component and scaling_group are required", map[string]any{"service_id": svc.ID})
		}
		if svc.Policy.MinReplicas > svc.Policy.MaxReplicas || svc.Policy.TargetConcurrency <= 0 {
			return model.NewError(model.CodeModelInvalid, "invalid scaling policy", map[string]any{"service_id": svc.ID})
		}
		subs := map[string]struct{}{}
		for _, sub := range svc.Subscriptions {
			if sub.Metric == "" || sub.Weight <= 0 {
				return model.NewError(model.CodeModelInvalid, "invalid subscription", map[string]any{"service_id": svc.ID, "metric": sub.Metric})
			}
			if _, ok := subs[sub.Metric]; ok {
				return model.NewError(model.CodeModelInvalid, "duplicate subscription metric", map[string]any{"service_id": svc.ID, "metric": sub.Metric})
			}
			subs[sub.Metric] = struct{}{}
		}
	}
	for _, svc := range services {
		for _, dep := range svc.Dependencies {
			if dep == svc.ID {
				return model.NewError(model.CodeModelInvalid, "service cannot depend on itself", map[string]any{"service_id": svc.ID})
			}
			if _, ok := seen[dep]; !ok {
				return model.NewError(model.CodeModelInvalid, "dependency does not exist", map[string]any{"service_id": svc.ID, "dependency": dep})
			}
		}
	}
	if cycle := FindCycle(services); cycle != nil {
		return model.NewError(model.CodeModelInvalid, "dependency cycle is not allowed", map[string]any{"cycle": cycle})
	}
	return nil
}

func FindCycle(services []model.Service) []string {
	graph := map[string][]string{}
	for _, svc := range services {
		graph[svc.ID] = svc.Dependencies
	}
	state := map[string]int{}
	var stack []string
	var dfs func(string) []string
	dfs = func(id string) []string {
		state[id] = 1
		stack = append(stack, id)
		deps := append([]string(nil), graph[id]...)
		sort.Strings(deps)
		for _, dep := range deps {
			if state[dep] == 1 {
				start := 0
				for i, v := range stack {
					if v == dep {
						start = i
						break
					}
				}
				return append(append([]string(nil), stack[start:]...), dep)
			}
			if state[dep] == 0 {
				if cycle := dfs(dep); cycle != nil {
					return cycle
				}
			}
		}
		stack = stack[:len(stack)-1]
		state[id] = 2
		return nil
	}
	ids := make([]string, 0, len(graph))
	for id := range graph {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if state[id] == 0 {
			if cycle := dfs(id); cycle != nil {
				return cycle
			}
		}
	}
	return nil
}

func TopologicalOrder(services []model.Service) ([]string, error) {
	if err := ValidateCatalog(services); err != nil {
		return nil, err
	}
	byID := map[string]model.Service{}
	consumers := map[string][]string{}
	remaining := map[string]int{}
	for _, svc := range services {
		byID[svc.ID] = svc
		remaining[svc.ID] = len(svc.Dependencies)
		for _, dep := range svc.Dependencies {
			consumers[dep] = append(consumers[dep], svc.ID)
		}
	}
	var ready []string
	for id, n := range remaining {
		if n == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	var order []string
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		order = append(order, id)
		next := append([]string(nil), consumers[id]...)
		sort.Strings(next)
		for _, consumer := range next {
			remaining[consumer]--
			if remaining[consumer] == 0 {
				ready = append(ready, consumer)
				sort.Strings(ready)
			}
		}
	}
	if len(order) != len(services) {
		return nil, model.NewError(model.CodeModelInvalid, "dependency cycle prevents topological order", nil)
	}
	return order, nil
}

func sortedServiceIDs(services []model.Service) []string {
	ids := make([]string, 0, len(services))
	for _, svc := range services {
		ids = append(ids, svc.ID)
	}
	sort.Strings(ids)
	return ids
}

func closureBFS(start string, neighbors map[string][]string) []model.ImpactNode {
	type queueItem struct {
		id   string
		hops int
		path []string
	}
	visited := map[string]int{start: 0}
	var out []model.ImpactNode
	q := []queueItem{{id: start, hops: 0, path: []string{start}}}
	for len(q) > 0 {
		cur := q[0]
		q = q[1:]
		next := append([]string(nil), neighbors[cur.id]...)
		sort.Strings(next)
		for _, id := range next {
			hops := cur.hops + 1
			if oldHops, ok := visited[id]; ok && oldHops <= hops {
				continue
			}
			visited[id] = hops
			path := append(append([]string(nil), cur.path...), id)
			out = append(out, model.ImpactNode{ServiceID: id, Hops: hops, Path: path})
			q = append(q, queueItem{id: id, hops: hops, path: path})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Hops != out[j].Hops {
			return out[i].Hops < out[j].Hops
		}
		return out[i].ServiceID < out[j].ServiceID
	})
	return out
}

func ImpactSet(state *State, trigger string) []model.ImpactNode {
	reverse := map[string][]string{}
	for _, svc := range state.Services {
		for _, dep := range svc.Dependencies {
			reverse[dep] = append(reverse[dep], svc.ID)
		}
	}
	nodes := closureBFS(trigger, reverse)
	availableMetrics := map[string]map[string]struct{}{}
	for _, svc := range state.Services {
		set := map[string]struct{}{}
		for _, sub := range svc.Subscriptions {
			set[sub.Metric] = struct{}{}
		}
		for _, sample := range state.SamplesFor(svc.ID) {
			set[sample.Metric] = struct{}{}
		}
		availableMetrics[svc.ID] = set
	}
	for i := range nodes {
		consumer, ok := state.Service(nodes[i].ServiceID)
		if !ok {
			continue
		}
		producer := trigger
		if nodes[i].Hops > 0 && len(nodes[i].Path) >= 2 {
			producer = nodes[i].Path[len(nodes[i].Path)-2]
		}
		producerMetrics := availableMetrics[producer]
		var metrics []string
		for _, sub := range consumer.Subscriptions {
			if producerMetrics != nil {
				if _, ok := producerMetrics[sub.Metric]; ok {
					metrics = append(metrics, sub.Metric)
				}
			}
		}
		sort.Strings(metrics)
		nodes[i].Metrics = metrics
	}
	return nodes
}

func UpstreamSet(state *State, serviceID string) []model.ImpactNode {
	forward := map[string][]string{}
	for _, svc := range state.Services {
		forward[svc.ID] = append([]string(nil), svc.Dependencies...)
	}
	nodes := closureBFS(serviceID, forward)
	availableMetrics := map[string]map[string]struct{}{}
	for _, svc := range state.Services {
		set := map[string]struct{}{}
		for _, sub := range svc.Subscriptions {
			set[sub.Metric] = struct{}{}
		}
		for _, sample := range state.SamplesFor(svc.ID) {
			set[sample.Metric] = struct{}{}
		}
		availableMetrics[svc.ID] = set
	}
	svc, ok := state.Service(serviceID)
	if !ok {
		return nodes
	}
	for i := range nodes {
		producerMetrics := availableMetrics[nodes[i].ServiceID]
		var metrics []string
		for _, sub := range svc.Subscriptions {
			if producerMetrics != nil {
				if _, ok := producerMetrics[sub.Metric]; ok {
					metrics = append(metrics, sub.Metric)
				}
			}
		}
		sort.Strings(metrics)
		nodes[i].Metrics = metrics
	}
	return nodes
}

func BuildImpactResponse(state *State, serviceID string) (*model.ImpactResponse, error) {
	if _, ok := state.Service(serviceID); !ok {
		return nil, model.NewError(model.CodeNotFound, "service not found", map[string]any{"service_id": serviceID})
	}
	resp := &model.ImpactResponse{
		TriggerServiceID:     serviceID,
		CurrentVersion:       state.Version,
		ImpactSet:            ImpactSet(state, serviceID),
		UpstreamDependencies: UpstreamSet(state, serviceID),
	}
	if resp.ImpactSet == nil {
		resp.ImpactSet = []model.ImpactNode{}
	}
	if resp.UpstreamDependencies == nil {
		resp.UpstreamDependencies = []model.ImpactNode{}
	}
	resp.Checksum = Checksum(struct {
		Trigger string             `json:"trigger"`
		Impact  []model.ImpactNode `json:"impact"`
		Up      []model.ImpactNode `json:"up"`
	}{resp.TriggerServiceID, resp.ImpactSet, resp.UpstreamDependencies})
	return resp, nil
}

type rawAggregate struct {
	valueSumMicroNumerator int64
	weightSum              int64
	count                  int
	valueMicro             int64
}

func aggregateOwn(samples []model.Sample) map[string]rawAggregate {
	type acc struct {
		weightedValue int64
		weight        int64
		count         int
	}
	byMetric := map[string]*acc{}
	for _, sample := range samples {
		a := byMetric[sample.Metric]
		if a == nil {
			a = &acc{}
			byMetric[sample.Metric] = a
		}
		w := sample.CollectionWeight
		if w <= 0 {
			w = 1
		}
		a.weightedValue += sample.Value * w
		a.weight += w
		a.count++
	}
	out := map[string]rawAggregate{}
	for metric, a := range byMetric {
		agg := rawAggregate{weightSum: a.weight, count: a.count}
		if a.weight > 0 {
			agg.valueMicro = a.weightedValue * model.Scale / a.weight
		}
		out[metric] = agg
	}
	return out
}

type Ref struct {
	Kind            string `json:"kind"`
	TargetServiceID string `json:"target_service_id"`
	TargetVersion   int64  `json:"target_version"`
	TargetChecksum  string `json:"target_checksum"`
}

type Derived struct {
	Service          model.Service
	Metrics          []model.MetricValue
	MetricsChecksum  string
	Impact           model.ImpactResponse
	ImpactJSON       string
	ImpactChecksum   string
	Decision         *model.DecisionResponse
	DecisionError    *model.BusinessError
	DecisionChecksum string
	Refs             []Ref
}

func metricChecksum(m model.MetricValue) string {
	type core struct {
		Metric          string               `json:"metric"`
		ValueMicro      int64                `json:"value_micro"`
		SampleCount     int                  `json:"sample_count"`
		SourceBreakdown []model.MetricSource `json:"sources"`
	}
	return Checksum(core{m.Metric, m.ValueMicro, m.SampleCount, m.SourceBreakdown})
}

func BuildService(state *State, serviceID string, views map[string]*Derived) (*Derived, error) {
	svc, ok := state.Service(serviceID)
	if !ok {
		return nil, model.NewError(model.CodeNotFound, "service not found", map[string]any{"service_id": serviceID})
	}
	impact, err := BuildImpactResponse(state, serviceID)
	if err != nil {
		return nil, err
	}
	own := aggregateOwn(state.SamplesFor(serviceID))
	metrics := []model.MetricValue{}
	subs := append([]model.Subscription(nil), svc.Subscriptions...)
	sort.Slice(subs, func(i, j int) bool { return subs[i].Metric < subs[j].Metric })
	for _, sub := range subs {
		type source struct {
			id     string
			weight int64
			value  int64
			count  int
			own    bool
		}
		var sources []source
		if agg, ok := own[sub.Metric]; ok && agg.weightSum > 0 {
			sources = append(sources, source{id: serviceID, weight: agg.weightSum, value: agg.valueMicro, count: agg.count, own: true})
		}
		deps := append([]string(nil), svc.Dependencies...)
		sort.Strings(deps)
		for _, depID := range deps {
			dv := views[depID]
			if dv == nil {
				continue
			}
			for _, dm := range dv.Metrics {
				if dm.Metric == sub.Metric {
					sources = append(sources, source{id: depID, weight: sub.Weight, value: dm.ValueMicro, count: dm.SampleCount})
					break
				}
			}
		}
		if len(sources) == 0 {
			continue
		}
		var totalWeight, numerator int64
		for _, src := range sources {
			totalWeight += src.weight
			numerator += src.weight * src.value
		}
		valueMicro := numerator / totalWeight
		breakdown := make([]model.MetricSource, 0, len(sources))
		for _, src := range sources {
			breakdown = append(breakdown, model.MetricSource{
				ServiceID:   src.id,
				WeightMicro: src.weight * model.Scale / totalWeight,
				ValueMicro:  src.value,
			})
		}
		sort.Slice(breakdown, func(i, j int) bool { return breakdown[i].ServiceID < breakdown[j].ServiceID })
		mv := model.MetricValue{
			Metric:          sub.Metric,
			ValueMicro:      valueMicro,
			Scale:           model.Scale,
			SampleCount:     own[sub.Metric].count,
			SourceBreakdown: breakdown,
		}
		mv.Checksum = metricChecksum(mv)
		metrics = append(metrics, mv)
	}
	metricsChecksum := Checksum(metrics)

	decision, derr := BuildDecision(svc, state.SamplesFor(serviceID), state.Version)
	dd := &Derived{Service: svc, Metrics: metrics, MetricsChecksum: metricsChecksum, Impact: *impact, ImpactChecksum: impact.Checksum}
	if derr != nil {
		dd.DecisionError = derr
		dd.DecisionChecksum = Checksum(map[string]any{"error": derr.Code, "context": derr.Context})
	} else {
		core := *decision
		core.CurrentVersion = 0
		core.MaterializedVersion = 0
		core.ObservedVersion = nil
		core.Checksum = ""
		decision.Checksum = Checksum(core)
		dd.Decision = decision
		dd.DecisionChecksum = decision.Checksum
	}
	for _, depID := range svc.Dependencies {
		dv := views[depID]
		if dv == nil {
			continue
		}
		dd.Refs = append(dd.Refs,
			Ref{Kind: RefMetric, TargetServiceID: depID, TargetVersion: state.Version, TargetChecksum: dv.MetricsChecksum},
			Ref{Kind: RefImpact, TargetServiceID: depID, TargetVersion: state.Version, TargetChecksum: dv.ImpactChecksum},
			Ref{Kind: RefDecision, TargetServiceID: depID, TargetVersion: state.Version, TargetChecksum: dv.DecisionChecksum},
		)
	}
	sort.Slice(dd.Refs, func(i, j int) bool {
		if dd.Refs[i].Kind != dd.Refs[j].Kind {
			return dd.Refs[i].Kind < dd.Refs[j].Kind
		}
		return dd.Refs[i].TargetServiceID < dd.Refs[j].TargetServiceID
	})
	return dd, nil
}

func BuildAll(state *State) (map[string]*Derived, error) {
	order, err := TopologicalOrder(state.Services)
	if err != nil {
		return nil, err
	}
	views := map[string]*Derived{}
	for _, id := range order {
		dv, err := BuildService(state, id, views)
		if err != nil {
			return nil, err
		}
		views[id] = dv
	}
	return views, nil
}

func BuildScope(state *State, scope []string, views map[string]*Derived) error {
	order, err := TopologicalOrder(state.Services)
	if err != nil {
		return err
	}
	scopeSet := map[string]struct{}{}
	for _, id := range scope {
		scopeSet[id] = struct{}{}
	}
	for _, id := range order {
		if _, ok := scopeSet[id]; !ok {
			continue
		}
		dv, err := BuildService(state, id, views)
		if err != nil {
			return err
		}
		views[id] = dv
	}
	return nil
}

func NearestRank(sorted []int64, percent int) (rank int, value int64) {
	n := len(sorted)
	if n == 0 {
		return 0, 0
	}
	rank = (percent*n + 99) / 100
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return rank, sorted[rank-1]
}

type instanceSamples struct {
	requestCount int64
	latencies    []int64
	weight       int64
}

func BuildDecision(svc model.Service, samples []model.Sample, version int64) (*model.DecisionResponse, *model.BusinessError) {
	policy := svc.Policy.Normalize()
	byInstance := map[string]*instanceSamples{}
	for _, sample := range samples {
		a := byInstance[sample.InstanceID]
		if a == nil {
			a = &instanceSamples{}
			byInstance[sample.InstanceID] = a
		}
		w := sample.CollectionWeight
		if w <= 0 {
			w = 1
		}
		a.weight += w
		switch sample.Metric {
		case MetricRequests:
			a.requestCount += sample.Value
		case MetricLatency:
			a.latencies = append(a.latencies, sample.Value)
		}
	}
	instanceIDs := make([]string, 0, len(byInstance))
	for id := range byInstance {
		instanceIDs = append(instanceIDs, id)
	}
	sort.Strings(instanceIDs)
	if len(instanceIDs) == 0 {
		return nil, model.NewError(model.CodeInsufficientSamples, "no instance samples for scaling decision", map[string]any{
			"service_id": svc.ID, "required_latency_samples": policy.MinLatencySamples, "actual_latency_samples": 0,
		})
	}
	basis := make([]model.InstanceBasis, 0, len(instanceIDs))
	var totalWeight, riskNumerator, satNumerator, latNumerator int64
	var loadNumerator int64
	totalLatencySamples := 0
	for _, instanceID := range instanceIDs {
		a := byInstance[instanceID]
		sort.Slice(a.latencies, func(i, j int) bool { return a.latencies[i] < a.latencies[j] })
		n := len(a.latencies)
		totalLatencySamples += n
		if n < policy.MinLatencySamples {
			return nil, model.NewError(model.CodeInsufficientSamples, "not enough latency samples for deterministic nearest-rank quantile", map[string]any{
				"service_id":       svc.ID,
				"instance_id":      instanceID,
				"required_samples": policy.MinLatencySamples,
				"actual_samples":   n,
				"sample_size":      n,
				"quantile_percent": policy.QuantilePercent,
			})
		}
		rank, p90 := NearestRank(a.latencies, policy.QuantilePercent)
		concurrencyMicro := a.requestCount * p90 * model.Scale / policy.WindowMS
		saturationMicro := concurrencyMicro / policy.TargetConcurrency
		latencyRatioMicro := p90 * model.Scale / policy.LatencyBudgetMS
		riskMicro := (policy.SaturationWeightMicro*saturationMicro + policy.LatencyRiskWeightMicro*latencyRatioMicro) / model.Scale
		basis = append(basis, model.InstanceBasis{
			InstanceID:           instanceID,
			SampleCount:          n,
			QuantileRank:         rank,
			P90LatencyMS:         p90,
			RequestCount:         a.requestCount,
			CollectionWeight:     a.weight,
			EstConcurrencyMicro:  concurrencyMicro,
			SaturationRatioMicro: saturationMicro,
			LatencyRatioMicro:    latencyRatioMicro,
			RiskScoreMicro:       riskMicro,
		})
		totalWeight += a.weight
		riskNumerator += a.weight * riskMicro
		satNumerator += a.weight * saturationMicro
		latNumerator += a.weight * latencyRatioMicro
		loadNumerator += a.requestCount * p90
	}
	if totalWeight <= 0 {
		totalWeight = int64(len(instanceIDs))
	}
	riskScore := riskNumerator / totalWeight
	satScore := satNumerator / totalWeight
	latScore := latNumerator / totalWeight
	concurrencyDesired := ceilDiv(loadNumerator, policy.WindowMS*policy.TargetConcurrency)
	latencyDesired := ceilDiv(int64(len(instanceIDs))*latScore, model.Scale)
	raw := concurrencyDesired
	if latencyDesired > raw {
		raw = latencyDesired
	}
	if raw < 1 {
		raw = 1
	}
	desired := int(raw)
	clamped := false
	if desired < policy.MinReplicas {
		desired = policy.MinReplicas
		clamped = true
	}
	if desired > policy.MaxReplicas {
		desired = policy.MaxReplicas
		clamped = true
	}
	resp := &model.DecisionResponse{
		ServiceID:                  svc.ID,
		CurrentVersion:             version,
		MaterializedVersion:        version,
		DesiredReplicas:            desired,
		MinReplicas:                policy.MinReplicas,
		MaxReplicas:                policy.MaxReplicas,
		RiskScoreMicro:             riskScore,
		SaturationScoreMicro:       satScore,
		LatencyScoreMicro:          latScore,
		ConcurrencyDesiredReplicas: int(concurrencyDesired),
		LatencyDesiredReplicas:     int(latencyDesired),
		Scale:                      model.Scale,
		SampleSize:                 totalLatencySamples,
		Formula: fmt.Sprintf(
			"nearest_rank p%d rank=ceil(%d*n/100); concurrency_micro=requests*p90_ms*%d/window_ms; concurrency_replicas=ceil(sum(requests*p90_ms)/(window_ms*target_concurrency)); latency_replicas=ceil(instances*weighted_latency_ratio_micro/%d); raw=max(signals); clamp[%d,%d]",
			policy.QuantilePercent, policy.QuantilePercent, model.Scale, model.Scale, policy.MinReplicas, policy.MaxReplicas,
		),
		Basis:       basis,
		Clamped:     clamped,
		RawReplicas: int(raw),
	}
	resp.Checksum = Checksum(resp)
	return resp, nil
}

func ceilDiv(a, b int64) int64 {
	if b <= 0 {
		return math.MaxInt64
	}
	if a <= 0 {
		return 0
	}
	return (a + b - 1) / b
}

func GlobalMetricsChecksum(views map[string]*Derived) string {
	ids := make([]string, 0, len(views))
	for id := range views {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	parts := map[string]string{}
	for _, id := range ids {
		parts[id] = views[id].MetricsChecksum
	}
	return Checksum(parts)
}

func GlobalDecisionsChecksum(views map[string]*Derived) string {
	ids := make([]string, 0, len(views))
	for id := range views {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	parts := map[string]string{}
	for _, id := range ids {
		parts[id] = views[id].DecisionChecksum
	}
	return Checksum(parts)
}

func CanonicalMetricsJSON(views map[string]*Derived) ([]byte, error) {
	ids := make([]string, 0, len(views))
	for id := range views {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := map[string][]model.MetricValue{}
	for _, id := range ids {
		out[id] = views[id].Metrics
	}
	return json.Marshal(out)
}

func CanonicalDecisionsJSON(views map[string]*Derived) ([]byte, error) {
	ids := make([]string, 0, len(views))
	for id := range views {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := map[string]string{}
	for _, id := range ids {
		if views[id].Decision != nil {
			b, err := json.Marshal(views[id].Decision)
			if err != nil {
				return nil, err
			}
			var v any
			if err := json.Unmarshal(b, &v); err != nil {
				return nil, err
			}
			out[id] = string(b)
		} else if views[id].DecisionError != nil {
			out[id] = "ERROR:" + views[id].DecisionError.Code
		}
	}
	return json.Marshal(out)
}

func RatioMicro(numerator, denominator int) int64 {
	if denominator <= 0 {
		return model.Scale
	}
	return int64(numerator) * model.Scale / int64(denominator)
}

func MustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return `{"error":"marshal_failed"}`
	}
	return string(b)
}

var _ = strings.TrimSpace

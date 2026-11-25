package drift

import (
	"sort"

	"auditdrift/internal/model"
)

type ConclusionKey struct {
	Host       string
	BaselineID string
}

// ApplyPropagation amplifies dependent baselines. If B depends on A, an A
// drift propagates along the reverse edge A -> B. The BFS hop limit is the
// declared maximum propagation hop count.
func ApplyPropagation(items []model.Baseline, conclusions map[ConclusionKey]*model.Conclusion, maxHops int) {
	if maxHops <= 0 {
		return
	}
	reverseDeps := make(map[string][]string, len(items))
	for _, b := range items {
		for _, upstream := range b.DependsOn {
			reverseDeps[upstream] = append(reverseDeps[upstream], b.ID)
		}
	}
	for id := range reverseDeps {
		sort.Strings(reverseDeps[id])
	}

	hosts := make([]string, 0)
	hostSeen := make(map[string]struct{})
	for key := range conclusions {
		if _, ok := hostSeen[key.Host]; !ok {
			hostSeen[key.Host] = struct{}{}
			hosts = append(hosts, key.Host)
		}
	}
	sort.Strings(hosts)

	for _, host := range hosts {
		for _, source := range driftedSources(host, conclusions) {
			type queueItem struct {
				id   string
				hop  int
				path []string
			}
			queue := []queueItem{{id: source.BaselineID, hop: 0, path: []string{source.BaselineID}}}
			for len(queue) > 0 {
				current := queue[0]
				queue = queue[1:]
				if current.hop >= maxHops {
					continue
				}
				for _, nextID := range reverseDeps[current.id] {
					if containsPath(current.path, nextID) {
						continue
					}
					hop := current.hop + 1
					path := append(append([]string{}, current.path...), nextID)
					key := ConclusionKey{Host: host, BaselineID: nextID}
					target := conclusions[key]
					if target == nil || target.Status == model.TaskError {
						queue = append(queue, queueItem{id: nextID, hop: hop, path: path})
						continue
					}
					boost := source.OwnLevel - hop + 1
					if boost < 0 {
						boost = 0
					}
					if boost > 0 {
						newLevel := target.OwnLevel + boost
						if newLevel > 4 {
							newLevel = 4
						}
						if boost > propagationBoost(target) {
							target.DriftLevel = newLevel
							target.Propagation = []model.PropagationHop{{
								FromBaselineID: source.BaselineID,
								ToBaselineID:   nextID,
								Hop:            hop,
								SourceLevel:    source.OwnLevel,
								Boost:          boost,
								Path:           path,
							}}
							if target.DriftCategory != model.DriftNone && newLevel > target.OwnLevel {
								target.Details["propagation_amplified"] = true
							}
						}
					}
					if hop < maxHops {
						queue = append(queue, queueItem{id: nextID, hop: hop, path: path})
					}
				}
			}
		}
	}
}

func driftedSources(host string, conclusions map[ConclusionKey]*model.Conclusion) []*model.Conclusion {
	out := make([]*model.Conclusion, 0)
	for key, conclusion := range conclusions {
		if key.Host != host {
			continue
		}
		if conclusion.Status != model.TaskError && conclusion.OwnLevel > 0 {
			out = append(out, conclusion)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].BaselineID != out[j].BaselineID {
			return out[i].BaselineID < out[j].BaselineID
		}
		return out[i].Host < out[j].Host
	})
	return out
}

func propagationBoost(c *model.Conclusion) int {
	if len(c.Propagation) == 0 {
		return 0
	}
	return c.Propagation[0].Boost
}

func containsPath(path []string, id string) bool {
	for _, item := range path {
		if item == id {
			return true
		}
	}
	return false
}

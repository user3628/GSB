package model

import (
	"hash/fnv"
	"sort"
	"strconv"
)

// DeterministicHash 以客户端标识 + 请求序号计算 FNV-1a 64 位哈希。
// 相同输入在任何进程/任何时间得到相同结果。
func DeterministicHash(clientID string, requestSeq int) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(clientID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strconv.Itoa(requestSeq)))
	return h.Sum64()
}

// NormalizeZoneWeights 按可用区对可调度实例的原始权重做千分比归一化。
// 采用最大余数法保证合计严格为 1000 且确定性（同权重时按实例 ID 排序打破平手）。
func NormalizeZoneWeights(instances []Instance, phases map[string]Phase) map[string]map[string]int {
	byZone := make(map[string][]Instance)
	for _, ins := range instances {
		if !IsDispatchable(phases[ins.ID]) {
			continue
		}
		byZone[ins.Zone] = append(byZone[ins.Zone], ins)
	}
	out := make(map[string]map[string]int, len(byZone))
	for zone, list := range byZone {
		sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
		total := 0
		for _, ins := range list {
			total += ins.Weight
		}
		weights := make(map[string]int, len(list))
		type remainder struct {
			id        string
			remainder int
		}
		remainders := make([]remainder, 0, len(list))
		allocated := 0
		for _, ins := range list {
			scaled := ins.Weight * 1000
			base := scaled / total
			weights[ins.ID] = base
			allocated += base
			remainders = append(remainders, remainder{ins.ID, scaled % total})
		}
		sort.SliceStable(remainders, func(i, j int) bool {
			if remainders[i].remainder != remainders[j].remainder {
				return remainders[i].remainder > remainders[j].remainder
			}
			return remainders[i].id < remainders[j].id
		})
		for idx := 0; allocated < 1000; idx++ {
			weights[remainders[idx%len(remainders)].id]++
			allocated++
		}
		out[zone] = weights
	}
	return out
}

// PickZone 按亲和比例与哈希值选择首选可用区。
func PickZone(ratios map[string]int, hash uint64) string {
	zones := SortedZones(ratios)
	point := int(hash % 1000)
	cumulative := 0
	for _, zone := range zones {
		cumulative += ratios[zone]
		if point < cumulative {
			return zone
		}
	}
	return zones[len(zones)-1]
}

// PickInstance 在某可用区内按归一化千分比区间选择实例，返回实例 ID 与命中区间描述。
func PickInstance(zoneWeights map[string]int, hash uint64) (string, string) {
	ids := make([]string, 0, len(zoneWeights))
	for id := range zoneWeights {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	point := int(hash % 1000)
	cumulative := 0
	for _, id := range ids {
		weight := zoneWeights[id]
		low, high := cumulative, cumulative+weight-1
		if point >= low && point <= high {
			return id, "[" + strconv.Itoa(low) + "," + strconv.Itoa(high) + "]@" + strconv.Itoa(point)
		}
		cumulative += weight
	}
	// 理论不可达：归一化保证合计 1000。
	last := ids[len(ids)-1]
	return last, "fallback-tail@" + strconv.Itoa(point)
}

// ChooseInstance 在给定快照上执行完整的亲和 + 回退选择，产出可回溯决策。
func ChooseInstance(snap *SnapshotVersion, clientID string, requestSeq int) (*Decision, error) {
	hash := DeterministicHash(clientID, requestSeq)
	preferred := PickZone(snap.ZoneRatios, hash)
	decision := &Decision{
		RequestSeq:      requestSeq,
		ClientID:        clientID,
		SnapshotVersion: snap.Version,
		HashValue:       hash,
		PreferredZone:   preferred,
	}

	tryZone := func(zone string) (string, string, bool) {
		weights, ok := snap.ZoneWeights[zone]
		if !ok || len(weights) == 0 {
			return "", "", false
		}
		id, interval := PickInstance(weights, hash)
		return id, interval, true
	}

	if id, interval, ok := tryZone(preferred); ok {
		decision.SelectedZone = preferred
		decision.SelectedInstance = id
		decision.WeightInterval = interval
		return decision, nil
	}

	// 声明回退规则：首选区无可用实例时，按可用区名确定性顺序依次尝试其余区。
	decision.Fallback = true
	for _, zone := range SortedZones(snap.ZoneRatios) {
		if zone == preferred {
			continue
		}
		if id, interval, ok := tryZone(zone); ok {
			decision.SelectedZone = zone
			decision.SelectedInstance = id
			decision.WeightInterval = interval
			decision.FallbackReason = "no dispatchable instance in preferred zone " + preferred + ", fallback to " + zone
			return decision, nil
		}
	}
	return nil, NewBizError(CodeNoCandidate, "no dispatchable instance across all zones",
		"client="+clientID+" seq="+strconv.Itoa(requestSeq))
}

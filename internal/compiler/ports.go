package compiler

import (
	"fmt"
	"sort"

	"netpolcompiler/internal/model"
)

type PortInterval struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

type PortMergeEvent struct {
	Protocol   string       `json:"protocol"`
	RuleID     string       `json:"ruleId"`
	InputRange PortInterval `json:"inputRange"`
	MergedInto PortInterval `json:"mergedInto"`
	Reason     string       `json:"reason"`
}

type MergedInterval struct {
	PortInterval
	SourceRuleIDs []string `json:"sourceRuleIds"`
}

type ProtocolPortView struct {
	Protocol         string           `json:"protocol"`
	RawCount         int              `json:"rawCount"`
	MergedCount      int              `json:"mergedCount"`
	Intervals        []MergedInterval `json:"intervals"`
	MergeEvents      []PortMergeEvent `json:"mergeEvents"`
	ImplicitAnyRules []string         `json:"implicitAnyRules,omitempty"`
}

type PortView struct {
	Namespace        string             `json:"namespace"`
	MergeRule        string             `json:"mergeRule"`
	TotalRawCount    int                `json:"totalRawCount"`
	TotalMergedCount int                `json:"totalMergedCount"`
	Protocols        []ProtocolPortView `json:"protocols"`
}

type rawPortRange struct {
	interval PortInterval
	ruleID   string
	implicit bool
}

func CompilePorts(namespace string, rules []model.Rule) PortView {
	byProtocol := map[model.Protocol][]rawPortRange{}
	for _, rule := range rules {
		if len(rule.Ports) == 0 {
			byProtocol[rule.Protocol] = append(byProtocol[rule.Protocol], rawPortRange{
				interval: PortInterval{Start: model.MinPort, End: model.MaxPort},
				ruleID:   rule.ID,
				implicit: true,
			})
			continue
		}
		for _, portRange := range rule.Ports {
			byProtocol[rule.Protocol] = append(byProtocol[rule.Protocol], rawPortRange{
				interval: PortInterval{Start: portRange.Start, End: portRange.End},
				ruleID:   rule.ID,
			})
		}
	}

	protocols := make([]string, 0, len(byProtocol))
	for protocol := range byProtocol {
		protocols = append(protocols, string(protocol))
	}
	sort.Strings(protocols)

	view := PortView{
		Namespace: namespace,
		MergeRule: "merge sorted integer intervals when next.start <= current.end + 1; next.start <= current.end is overlap, next.start == current.end+1 is adjacency",
	}
	for _, protocolName := range protocols {
		protocol := model.Protocol(protocolName)
		ranges := byProtocol[protocol]
		sort.SliceStable(ranges, func(i, j int) bool {
			if ranges[i].interval.Start != ranges[j].interval.Start {
				return ranges[i].interval.Start < ranges[j].interval.Start
			}
			if ranges[i].interval.End != ranges[j].interval.End {
				return ranges[i].interval.End < ranges[j].interval.End
			}
			return ranges[i].ruleID < ranges[j].ruleID
		})

		protocolView := ProtocolPortView{Protocol: protocolName, RawCount: len(ranges)}
		var current *MergedInterval
		flush := func() {
			if current != nil {
				sort.Strings(current.SourceRuleIDs)
				current.SourceRuleIDs = uniqueStrings(current.SourceRuleIDs)
				protocolView.Intervals = append(protocolView.Intervals, *current)
			}
			current = nil
		}

		for _, raw := range ranges {
			if raw.implicit {
				protocolView.ImplicitAnyRules = append(protocolView.ImplicitAnyRules, raw.ruleID)
			}
			if current == nil {
				current = &MergedInterval{PortInterval: raw.interval, SourceRuleIDs: []string{raw.ruleID}}
				continue
			}
			if raw.interval.Start <= current.End+1 {
				reason := "adjacent"
				if raw.interval.Start <= current.End {
					reason = "overlap"
				}
				previous := current.PortInterval
				if raw.interval.End > current.End {
					current.End = raw.interval.End
				}
				current.SourceRuleIDs = append(current.SourceRuleIDs, raw.ruleID)
				protocolView.MergeEvents = append(protocolView.MergeEvents, PortMergeEvent{
					Protocol: protocolName, RuleID: raw.ruleID, InputRange: raw.interval,
					MergedInto: current.PortInterval, Reason: fmt.Sprintf("%s_merged_previous_%d-%d", reason, previous.Start, previous.End),
				})
			} else {
				flush()
				current = &MergedInterval{PortInterval: raw.interval, SourceRuleIDs: []string{raw.ruleID}}
			}
		}
		flush()
		sort.Strings(protocolView.ImplicitAnyRules)
		protocolView.MergedCount = len(protocolView.Intervals)
		view.Protocols = append(view.Protocols, protocolView)
		view.TotalRawCount += protocolView.RawCount
		view.TotalMergedCount += protocolView.MergedCount
	}
	return view
}

func PortInRanges(port int, ranges []model.PortRange) bool {
	if len(ranges) == 0 {
		return port >= model.MinPort && port <= model.MaxPort
	}
	for _, portRange := range ranges {
		if port >= portRange.Start && port <= portRange.End {
			return true
		}
	}
	return false
}

func IntersectPortRanges(left, right []model.PortRange) []PortInterval {
	var intersections []PortInterval
	leftRanges := left
	if len(leftRanges) == 0 {
		leftRanges = []model.PortRange{{Start: model.MinPort, End: model.MaxPort}}
	}
	rightRanges := right
	if len(rightRanges) == 0 {
		rightRanges = []model.PortRange{{Start: model.MinPort, End: model.MaxPort}}
	}
	for _, a := range leftRanges {
		for _, b := range rightRanges {
			start := maxInt(a.Start, b.Start)
			end := minInt(a.End, b.End)
			if start <= end {
				intersections = append(intersections, PortInterval{Start: start, End: end})
			}
		}
	}
	return MergeSimpleIntervals(intersections)
}

func MergeSimpleIntervals(input []PortInterval) []PortInterval {
	if len(input) == 0 {
		return nil
	}
	sorted := append([]PortInterval(nil), input...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Start != sorted[j].Start {
			return sorted[i].Start < sorted[j].Start
		}
		return sorted[i].End < sorted[j].End
	})
	merged := []PortInterval{sorted[0]}
	for _, interval := range sorted[1:] {
		last := &merged[len(merged)-1]
		if interval.Start <= last.End+1 {
			if interval.End > last.End {
				last.End = interval.End
			}
		} else {
			merged = append(merged, interval)
		}
	}
	return merged
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	result := values[:0]
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}

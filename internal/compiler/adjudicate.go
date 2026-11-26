package compiler

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	"netpolcompiler/internal/model"
)

type EvaluationRequest struct {
	Namespace          string            `json:"namespace"`
	ObservationVersion int64             `json:"observationVersion,omitempty"`
	SourceLabels       map[string]string `json:"sourceLabels"`
	DestinationLabels  map[string]string `json:"destinationLabels"`
	Protocol           model.Protocol    `json:"protocol"`
	Ports              []int             `json:"ports,omitempty"`
	PortRange          *model.PortRange  `json:"portRange,omitempty"`
}

type RuleBasis struct {
	Rank             int          `json:"rank"`
	RuleID           string       `json:"ruleId"`
	Action           model.Action `json:"action"`
	Priority         int          `json:"priority"`
	DeclarationBasis string       `json:"declarationBasis"`
	SelectorMatch    bool         `json:"selectorMatch"`
	ProtocolMatch    bool         `json:"protocolMatch"`
	PortMatch        bool         `json:"portMatch"`
	Reason           string       `json:"reason"`
}

type PortDecision struct {
	Port           int         `json:"port"`
	MatchedRuleIDs []string    `json:"matchedRuleIds"`
	WinnerRuleID   string      `json:"winnerRuleId,omitempty"`
	Action         string      `json:"action"`
	Basis          []RuleBasis `json:"basis"`
}

type EvaluationResult struct {
	Namespace         string         `json:"namespace"`
	UsedVersion       int64          `json:"usedVersion"`
	Protocol          string         `json:"protocol"`
	EvaluatedPorts    []int          `json:"evaluatedPorts"`
	PortDecisions     []PortDecision `json:"portDecisions"`
	AllMatchedRuleIDs []string       `json:"allMatchedRuleIds"`
	FinalAction       string         `json:"finalAction"`
	DecisionDigest    string         `json:"decisionDigest"`
}

func Evaluate(compiled *CompiledNamespace, request EvaluationRequest) *EvaluationResult {
	protocol := model.Protocol(strings.ToUpper(string(request.Protocol)))
	ports := requestedPorts(request, compiled, protocol)
	result := &EvaluationResult{
		Namespace: compiled.Namespace, UsedVersion: compiled.Version,
		Protocol: string(protocol), EvaluatedPorts: ports, PortDecisions: []PortDecision{},
		AllMatchedRuleIDs: []string{}, FinalAction: "no_match",
	}
	allMatched := map[string]bool{}
	actions := map[string]bool{}

	for _, port := range ports {
		type candidate struct {
			rule  model.Rule
			basis RuleBasis
		}
		candidates := []candidate{}
		for _, rule := range compiled.Rules {
			sourceMatch := SelectorMatches(rule.Source, request.SourceLabels)
			destinationMatch := SelectorMatches(rule.Destination, request.DestinationLabels)
			protocolMatch := rule.Protocol == protocol
			portMatch := protocolMatch && PortInRanges(port, rule.Ports)
			selectorMatch := sourceMatch && destinationMatch
			if selectorMatch && protocolMatch && portMatch {
				candidates = append(candidates, candidate{rule: rule})
			}
		}
		sort.SliceStable(candidates, func(i, j int) bool {
			if candidates[i].rule.Priority != candidates[j].rule.Priority {
				return candidates[i].rule.Priority > candidates[j].rule.Priority
			}
			return i < j
		})

		decision := PortDecision{Port: port, MatchedRuleIDs: []string{}, Basis: []RuleBasis{}, Action: "no_match"}
		for rank, candidate := range candidates {
			rule := candidate.rule
			decision.MatchedRuleIDs = append(decision.MatchedRuleIDs, rule.ID)
			allMatched[rule.ID] = true
			reason := "highest_priority_wins"
			if rank > 0 && rule.Priority == candidates[0].rule.Priority {
				reason = "same_priority_canonical_declaration_order"
			}
			if rank == 0 {
				reason = "decisive:" + reason
			}
			decision.Basis = append(decision.Basis, RuleBasis{
				Rank: rank + 1, RuleID: rule.ID, Action: rule.Action, Priority: rule.Priority,
				DeclarationBasis: declarationOrderLabel(rule), SelectorMatch: true, ProtocolMatch: true,
				PortMatch: true, Reason: reason,
			})
		}
		if len(candidates) > 0 {
			decision.WinnerRuleID = candidates[0].rule.ID
			decision.Action = string(candidates[0].rule.Action)
			actions[decision.Action] = true
		}
		result.PortDecisions = append(result.PortDecisions, decision)
	}

	for ruleID := range allMatched {
		result.AllMatchedRuleIDs = append(result.AllMatchedRuleIDs, ruleID)
	}
	sort.Strings(result.AllMatchedRuleIDs)
	if len(actions) == 1 {
		for action := range actions {
			result.FinalAction = action
		}
	} else if len(actions) > 1 {
		result.FinalAction = "mixed_by_port"
	}
	data, _ := JSONDeterministic(result)
	sum := sha256.Sum256(data)
	result.DecisionDigest = "sha256:" + hex.EncodeToString(sum[:])
	return result
}

func requestedPorts(request EvaluationRequest, compiled *CompiledNamespace, protocol model.Protocol) []int {
	seen := map[int]bool{}
	ports := []int{}
	add := func(port int) {
		if port >= model.MinPort && port <= model.MaxPort && !seen[port] {
			seen[port] = true
			ports = append(ports, port)
		}
	}
	for _, port := range request.Ports {
		add(port)
	}
	if request.PortRange != nil {
		for port := request.PortRange.Start; port <= request.PortRange.End; port++ {
			add(port)
		}
	}
	if len(ports) > 0 {
		sort.Ints(ports)
		return ports
	}

	intervals := []PortInterval{}
	for _, rule := range compiled.Rules {
		if rule.Protocol != protocol {
			continue
		}
		if !SelectorMatches(rule.Source, request.SourceLabels) || !SelectorMatches(rule.Destination, request.DestinationLabels) {
			continue
		}
		if len(rule.Ports) == 0 {
			intervals = append(intervals, PortInterval{Start: model.MinPort, End: model.MaxPort})
			continue
		}
		for _, portRange := range rule.Ports {
			intervals = append(intervals, PortInterval{Start: portRange.Start, End: portRange.End})
		}
	}
	for _, interval := range MergeSimpleIntervals(intervals) {
		for port := interval.Start; port <= interval.End; port++ {
			add(port)
		}
	}
	sort.Ints(ports)
	return ports
}

func JSONDeterministic(value any) ([]byte, error) {
	return stableMarshal(value)
}

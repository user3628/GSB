package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	MinPriority = 0
	MaxPriority = 1000
	MinPort     = 1
	MaxPort     = 65535
)

type Action string

const (
	ActionAllow Action = "allow"
	ActionDeny  Action = "deny"
)

type Protocol string

const (
	ProtocolTCP Protocol = "TCP"
	ProtocolUDP Protocol = "UDP"
)

type LabelSelector struct {
	MatchLabels map[string]string  `json:"matchLabels,omitempty"`
	Exists      []string           `json:"exists,omitempty"`
	NotIn       []NotInRequirement `json:"notIn,omitempty"`
}

type NotInRequirement struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

type PortRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

type Rule struct {
	Namespace        string        `json:"namespace"`
	ID               string        `json:"id"`
	Action           Action        `json:"action"`
	Source           LabelSelector `json:"source"`
	Destination      LabelSelector `json:"destination"`
	Ports            []PortRange   `json:"ports,omitempty"`
	Protocol         Protocol      `json:"protocol"`
	Priority         int           `json:"priority"`
	DeclarationOrder *int          `json:"declarationOrder,omitempty"`
}

type Snapshot struct {
	RequestID   string `json:"requestId,omitempty"`
	Version     int64  `json:"version"`
	GeneratedAt string `json:"generatedAt"`
	Digest      string `json:"digest"`
	Rules       []Rule `json:"rules"`
}

type RuleChange struct {
	Op        string `json:"op"`
	Rule      *Rule  `json:"rule,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	RuleID    string `json:"ruleId,omitempty"`
	Rules     []Rule `json:"rules,omitempty"`
}

type Increment struct {
	RequestID   string       `json:"requestId,omitempty"`
	BaseVersion int64        `json:"baseVersion"`
	GeneratedAt string       `json:"generatedAt,omitempty"`
	Changes     []RuleChange `json:"changes"`
}

type ValidationError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("E_RULE_INVALID field=%s: %s", e.Field, e.Message)
}

func RuleError(field, format string, args ...any) error {
	return &ValidationError{Field: field, Message: fmt.Sprintf(format, args...)}
}

func ValidateSnapshot(snapshot *Snapshot) error {
	if snapshot == nil {
		return RuleError("snapshot", "snapshot is required")
	}
	if snapshot.Version <= 0 {
		return RuleError("version", "must be a positive monotonic integer")
	}
	if strings.TrimSpace(snapshot.GeneratedAt) == "" {
		return RuleError("generatedAt", "must be an RFC3339 timestamp")
	}
	if _, err := time.Parse(time.RFC3339, snapshot.GeneratedAt); err != nil {
		return RuleError("generatedAt", "must be RFC3339: %v", err)
	}
	if strings.TrimSpace(snapshot.Digest) == "" {
		return RuleError("digest", "must not be empty")
	}

	seen := make(map[string]map[string]int)
	for i := range snapshot.Rules {
		rule := &snapshot.Rules[i]
		prefix := fmt.Sprintf("rules[%d]", i)
		if err := ValidateRule(rule, prefix); err != nil {
			return err
		}
		if seen[rule.Namespace] == nil {
			seen[rule.Namespace] = make(map[string]int)
		}
		if first, exists := seen[rule.Namespace][rule.ID]; exists {
			return RuleError(prefix+".id", "rule id %q duplicates rules[%d].id in namespace %q", rule.ID, first, rule.Namespace)
		}
		seen[rule.Namespace][rule.ID] = i
	}
	return nil
}

func ValidateRule(rule *Rule, prefix string) error {
	if rule == nil {
		return RuleError(prefix, "rule is required")
	}
	if strings.TrimSpace(rule.Namespace) == "" {
		return RuleError(prefix+".namespace", "must not be empty")
	}
	if strings.TrimSpace(rule.ID) == "" {
		return RuleError(prefix+".id", "must not be empty")
	}
	action := Action(strings.ToLower(string(rule.Action)))
	switch action {
	case ActionAllow, ActionDeny:
	default:
		return RuleError(prefix+".action", "must be %q or %q", ActionAllow, ActionDeny)
	}
	if err := ValidateSelector(rule.Source, prefix+".source"); err != nil {
		return err
	}
	if err := ValidateSelector(rule.Destination, prefix+".destination"); err != nil {
		return err
	}
	if err := ValidatePortRanges(rule.Ports, prefix+".ports"); err != nil {
		return err
	}
	protocol := Protocol(strings.ToUpper(string(rule.Protocol)))
	switch protocol {
	case ProtocolTCP, ProtocolUDP:
	default:
		return RuleError(prefix+".protocol", "must be %q or %q", ProtocolTCP, ProtocolUDP)
	}
	if rule.Priority < MinPriority || rule.Priority > MaxPriority {
		return RuleError(prefix+".priority", "must be within [%d,%d]", MinPriority, MaxPriority)
	}
	if rule.DeclarationOrder != nil && *rule.DeclarationOrder < 0 {
		return RuleError(prefix+".declarationOrder", "must not be negative")
	}
	return nil
}

func ValidateSelector(selector LabelSelector, prefix string) error {
	for key, value := range selector.MatchLabels {
		if strings.TrimSpace(key) == "" {
			return RuleError(prefix+".matchLabels", "label key must not be empty")
		}
		_ = value
	}
	for i, key := range selector.Exists {
		if strings.TrimSpace(key) == "" {
			return RuleError(fmt.Sprintf("%s.exists[%d]", prefix, i), "key must not be empty")
		}
	}
	for i, req := range selector.NotIn {
		reqPrefix := fmt.Sprintf("%s.notIn[%d]", prefix, i)
		if strings.TrimSpace(req.Key) == "" {
			return RuleError(reqPrefix+".key", "must not be empty")
		}
		if len(req.Values) == 0 {
			return RuleError(reqPrefix+".values", "must contain at least one value")
		}
		for j, value := range req.Values {
			if value == "" {
				return RuleError(fmt.Sprintf("%s.values[%d]", reqPrefix, j), "must not be empty")
			}
		}
	}
	return nil
}

func ValidatePortRanges(ranges []PortRange, prefix string) error {
	for i, portRange := range ranges {
		field := fmt.Sprintf("%s[%d]", prefix, i)
		if portRange.Start < MinPort || portRange.Start > MaxPort {
			return RuleError(field+".start", "must be within [%d,%d]", MinPort, MaxPort)
		}
		if portRange.End < MinPort || portRange.End > MaxPort {
			return RuleError(field+".end", "must be within [%d,%d]", MinPort, MaxPort)
		}
		if portRange.Start > portRange.End {
			return RuleError(field+".start", "start %d must not exceed end %d", portRange.Start, portRange.End)
		}
	}
	return nil
}

func ValidateIncrement(increment *Increment, currentVersion int64) error {
	if increment == nil {
		return RuleError("increment", "increment is required")
	}
	if increment.BaseVersion != currentVersion {
		return nil
	}
	if len(increment.Changes) == 0 {
		return RuleError("changes", "must contain at least one rule change")
	}
	for i := range increment.Changes {
		change := &increment.Changes[i]
		prefix := fmt.Sprintf("changes[%d]", i)
		switch change.Op {
		case "upsert", "":
			if change.Rule == nil {
				return RuleError(prefix+".rule", "must be present for upsert")
			}
			if err := ValidateRule(change.Rule, prefix+".rule"); err != nil {
				return err
			}
		case "delete":
			if strings.TrimSpace(change.Namespace) == "" {
				return RuleError(prefix+".namespace", "must not be empty for delete")
			}
			if strings.TrimSpace(change.RuleID) == "" {
				return RuleError(prefix+".ruleId", "must not be empty for delete")
			}
		case "replace_namespace":
			if strings.TrimSpace(change.Namespace) == "" {
				return RuleError(prefix+".namespace", "must not be empty for replace_namespace")
			}
			seenRuleIDs := map[string]int{}
			for ruleIndex := range change.Rules {
				rule := &change.Rules[ruleIndex]
				if rule.Namespace == "" {
					rule.Namespace = change.Namespace
				}
				if rule.Namespace != change.Namespace {
					return RuleError(fmt.Sprintf("%s.rules[%d].namespace", prefix, ruleIndex), "must match change.namespace %q", change.Namespace)
				}
				if err := ValidateRule(rule, fmt.Sprintf("%s.rules[%d]", prefix, ruleIndex)); err != nil {
					return err
				}
				if first, exists := seenRuleIDs[rule.ID]; exists {
					return RuleError(fmt.Sprintf("%s.rules[%d].id", prefix, ruleIndex), "duplicates %s.rules[%d].id", prefix, first)
				}
				seenRuleIDs[rule.ID] = ruleIndex
			}
		default:
			return RuleError(prefix+".op", "must be upsert, delete, or replace_namespace")
		}
	}
	return nil
}

// CanonicalRules returns a stable copy. Rule IDs are the final order-independent
// tie breaker; an explicit declarationOrder preserves a controller-authored order.
func CanonicalRules(rules []Rule) []Rule {
	cloned := make([]Rule, len(rules))
	copy(cloned, rules)
	for i := range cloned {
		cloned[i] = NormalizeRule(cloned[i])
	}
	sort.SliceStable(cloned, func(i, j int) bool {
		if cloned[i].Namespace != cloned[j].Namespace {
			return cloned[i].Namespace < cloned[j].Namespace
		}
		leftOrder, leftExplicit := declarationRank(cloned[i])
		rightOrder, rightExplicit := declarationRank(cloned[j])
		if leftExplicit != rightExplicit {
			return leftExplicit
		}
		if leftOrder != rightOrder {
			return leftOrder < rightOrder
		}
		return cloned[i].ID < cloned[j].ID
	})
	return cloned
}

func NormalizeRule(rule Rule) Rule {
	rule.Action = Action(strings.ToLower(string(rule.Action)))
	rule.Protocol = Protocol(strings.ToUpper(string(rule.Protocol)))
	return rule
}

func declarationRank(rule Rule) (int, bool) {
	if rule.DeclarationOrder != nil {
		return *rule.DeclarationOrder, true
	}
	return 0, false
}

type digestDocument struct {
	Version     int64  `json:"version"`
	GeneratedAt string `json:"generatedAt"`
	Rules       []Rule `json:"rules"`
}

func DigestSnapshot(snapshot Snapshot) string {
	canonical := CanonicalRules(snapshot.Rules)
	data, _ := json.Marshal(digestDocument{
		Version:     snapshot.Version,
		GeneratedAt: snapshot.GeneratedAt,
		Rules:       canonical,
	})
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func VerifyDigest(snapshot Snapshot) bool {
	return strings.EqualFold(strings.TrimSpace(snapshot.Digest), DigestSnapshot(snapshot))
}

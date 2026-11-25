package edgeconfig

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

var keyPathPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_-]*)*$`)

var allowedKeyTypes = map[string]bool{
	"string": true,
	"number": true,
	"bool":   true,
	"json":   true,
}

type ValidationIssue struct {
	Code    string `json:"code"`
	Field   string `json:"field"`
	Message string `json:"message"`
}

func ValidKeyPath(path string) bool {
	return keyPathPattern.MatchString(path)
}

func ValidateTopology(topology Topology) error {
	issues := make([]ValidationIssue, 0)
	keySet := make(map[string]ConfigKey, len(topology.Keys))

	for i, key := range topology.Keys {
		field := fmt.Sprintf("keys[%d].path", i)
		if !ValidKeyPath(key.Path) {
			issues = append(issues, ValidationIssue{Code: CodeInvalidKeyPath, Field: field, Message: "key path must be dotted lowercase segments"})
			continue
		}
		if _, exists := keySet[key.Path]; exists {
			issues = append(issues, ValidationIssue{Code: CodeValidation, Field: field, Message: "duplicate config key"})
			continue
		}
		if !allowedKeyTypes[key.Type] {
			issues = append(issues, ValidationIssue{Code: CodeValidation, Field: fmt.Sprintf("keys[%d].type", i), Message: "type must be string, number, bool, or json"})
		}
		if key.OrderNo < 0 {
			issues = append(issues, ValidationIssue{Code: CodeValidation, Field: fmt.Sprintf("keys[%d].order_no", i), Message: "order_no must be non-negative"})
		}
		keySet[key.Path] = key
	}

	nodeSet := make(map[string]Node, len(topology.Nodes))
	for i, node := range topology.Nodes {
		field := fmt.Sprintf("nodes[%d].id", i)
		if strings.TrimSpace(node.ID) == "" {
			issues = append(issues, ValidationIssue{Code: CodeValidation, Field: field, Message: "node id is required"})
		} else if _, exists := nodeSet[node.ID]; exists {
			issues = append(issues, ValidationIssue{Code: CodeDuplicateNode, Field: field, Message: "node id must be unique: " + node.ID})
		}
		if strings.TrimSpace(node.Region) == "" {
			issues = append(issues, ValidationIssue{Code: CodeValidation, Field: fmt.Sprintf("nodes[%d].region", i), Message: "region is required"})
		}
		if err := validateEndpoint(node.Endpoint); err != nil {
			issues = append(issues, ValidationIssue{Code: CodeValidation, Field: fmt.Sprintf("nodes[%d].endpoint", i), Message: err.Error()})
		}
		for _, keyPath := range node.SubscribedKeys {
			if _, declared := keySet[keyPath]; !declared {
				issues = append(issues, ValidationIssue{Code: CodeUndeclaredKey, Field: fmt.Sprintf("nodes[%d].subscribed_keys", i), Message: "subscription references undeclared key: " + keyPath})
			}
		}
		nodeSet[node.ID] = node
	}

	for _, node := range topology.Nodes {
		for _, dep := range node.DependsOn {
			if _, exists := nodeSet[dep]; !exists {
				issues = append(issues, ValidationIssue{Code: CodeNotFound, Field: "nodes.depends_on", Message: node.ID + " depends on undeclared node: " + dep})
			}
			if dep == node.ID {
				issues = append(issues, ValidationIssue{Code: CodeDependencyCycle, Field: "nodes.depends_on", Message: "node cannot depend on itself: " + node.ID})
			}
		}
	}
	if cycle := dependencyCycle(topology.Nodes); cycle != "" {
		issues = append(issues, ValidationIssue{Code: CodeDependencyCycle, Field: "nodes.depends_on", Message: cycle})
	}
	issues = append(issues, validatePolicy(topology.Policy)...)

	if len(issues) > 0 {
		code := CodeValidation
		for _, issue := range issues {
			if issue.Code == CodeDuplicateNode || issue.Code == CodeInvalidKeyPath || issue.Code == CodeUndeclaredKey {
				code = issue.Code
				break
			}
		}
		return NewAPIError(400, code, "topology validation failed", issues)
	}
	return nil
}

func validateEndpoint(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return fmt.Errorf("endpoint is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("endpoint must be a valid URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("endpoint scheme must be http or https")
	}
	if parsed.Host == "" {
		return fmt.Errorf("endpoint host is required")
	}
	return nil
}

func validatePolicy(policy Policy) []ValidationIssue {
	issues := make([]ValidationIssue, 0)
	if policy.Strategy.PerKeyExactMaxChangedKeys < 0 || policy.Strategy.RegionBucketMinChangedKeys < 0 {
		issues = append(issues, ValidationIssue{Code: CodeValidation, Field: "policy.strategy", Message: "strategy thresholds must be non-negative"})
	}
	if policy.Probe.TimeoutMS < 0 || policy.Probe.Concurrency < 0 {
		issues = append(issues, ValidationIssue{Code: CodeValidation, Field: "policy.probe", Message: "probe values must be non-negative"})
	}
	if policy.Numeric.Scale < 0 {
		issues = append(issues, ValidationIssue{Code: CodeValidation, Field: "policy.numeric.scale", Message: "numeric scale must be positive"})
	}
	if policy.Numeric.RelativeTolerancePPM < 0 {
		issues = append(issues, ValidationIssue{Code: CodeValidation, Field: "policy.numeric.relative_tolerance_ppm", Message: "tolerance must be non-negative"})
	}
	for nodeID, weight := range policy.Numeric.NodeWeights {
		if weight <= 0 {
			issues = append(issues, ValidationIssue{Code: CodeValidation, Field: "policy.numeric.node_weights." + nodeID, Message: "weight must be positive"})
		}
	}
	return issues
}

func dependencyCycle(nodes []Node) string {
	deps := make(map[string][]string, len(nodes))
	for _, node := range nodes {
		deps[node.ID] = node.DependsOn
	}
	state := make(map[string]int, len(nodes))
	var visit func(string, []string) string
	visit = func(id string, stack []string) string {
		state[id] = 1
		for _, next := range deps[id] {
			if state[next] == 1 {
				return "dependency cycle: " + strings.Join(append(stack, id, next), " -> ")
			}
			if state[next] == 0 {
				if msg := visit(next, append(stack, id)); msg != "" {
					return msg
				}
			}
		}
		state[id] = 2
		return ""
	}
	for _, node := range nodes {
		if state[node.ID] == 0 {
			if msg := visit(node.ID, nil); msg != "" {
				return msg
			}
		}
	}
	return ""
}

func normalizeSubscriptions(values []string, validKeys map[string]bool) ([]string, error) {
	seen := make(map[string]bool, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if !validKeys[value] {
			return nil, NewAPIError(400, CodeUndeclaredKey, "undeclared subscription key: "+value, nil)
		}
		if !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	return SortedStrings(out), nil
}

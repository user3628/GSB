package model

import (
	"regexp"

	"configaudit/internal/fixedpoint"
)

// keyPathRE allows dotted segments of [A-Za-z0-9_-], each non-empty.
var keyPathRE = regexp.MustCompile(`^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$`)

// ValidKeyPath reports whether a key path is well formed.
func ValidKeyPath(p string) bool { return keyPathRE.MatchString(p) }

// Spec is a declared configuration item.
type Spec struct {
	KeyPath       string                 `json:"key_path"`
	Type          string                 `json:"type"` // integer | decimal
	Scale         int                    `json:"scale"`
	Chain         []fixedpoint.ChainStep `json:"chain"`
	Priority      int                    `json:"priority"`
	Checksum      string                 `json:"checksum"`
	DeclaredBound string                 `json:"declared_bound"` // decimal string, same units as final
}

// Channel is one source channel on a node.
type Channel struct {
	Name      string `json:"name"` // file | env | template | script
	Priority  int    `json:"priority"`
	Order     int    `json:"order"`
	Exclusive bool   `json:"exclusive"` // when true the channel shadows later channels
}

// Node is a node declaration with its source channel set.
type Node struct {
	ID       string    `json:"id"`
	Channels []Channel `json:"channels"`
}

// SourceLine is one raw source description line from a node shard.
type SourceLine struct {
	NodeID  string `json:"node_id"`
	KeyPath string `json:"key_path"`
	Channel string `json:"channel"`
	Format  string `json:"format"` // v1 | v2
	Text    string `json:"text"`   // raw text for both generations
	// Structured fields, populated only for v2 lines.
	Value    string `json:"value,omitempty"`
	Unit     string `json:"unit,omitempty"`
	Priority int    `json:"priority,omitempty"`
	Order    int    `json:"order,omitempty"`
}

// Reference is the high-precision rational reference value for one combination.
type Reference struct {
	NodeID  string `json:"node_id"`
	KeyPath string `json:"key_path"`
	Channel string `json:"channel"`
	// Exact is a rational string such as "1610612736/1000".
	Exact string `json:"exact"`
}

// ValidateSpec enforces the spec rules and returns E_SPEC_INVALID with field and
// line information on failure.
func ValidateSpec(s Spec, line int) error {
	field := func(name string) *fixedpoint.Error {
		return fixedpoint.NewError(fixedpoint.ESpecInvalid, "invalid spec on line %d", line).WithField(name).WithLine(line)
	}
	if !ValidKeyPath(s.KeyPath) {
		return field("key_path")
	}
	if s.Type != "integer" && s.Type != "decimal" {
		return field("type")
	}
	if !fixedpoint.AllowedScales[s.Scale] {
		return field("scale")
	}
	if s.Type == "integer" && s.Scale != 0 {
		return field("scale")
	}
	for i, st := range s.Chain {
		if st.Op != "mul" && st.Op != "div" {
			return field("chain.op")
		}
		if st.Factor <= 0 {
			return field("chain.factor")
		}
		_ = i
	}
	if s.DeclaredBound == "" {
		return field("declared_bound")
	}
	return nil
}

// ValidateNode enforces node/channel rules.
func ValidateNode(n Node, line int) error {
	field := func(name string) *fixedpoint.Error {
		return fixedpoint.NewError(fixedpoint.ESpecInvalid, "invalid node on line %d", line).WithField(name).WithLine(line)
	}
	if n.ID == "" {
		return field("id")
	}
	seen := map[string]bool{}
	for _, ch := range n.Channels {
		switch ch.Name {
		case "file", "env", "template", "script":
		default:
			return field("channels.name")
		}
		if seen[ch.Name] {
			return field("channels.name")
		}
		seen[ch.Name] = true
	}
	return nil
}

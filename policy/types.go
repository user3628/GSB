// Package policy implements the layered sandbox policy model, deterministic
// normalization, strict parsing, change-set application and dependency impact
// analysis.
package policy

import "encoding/json"

const SchemaVersion = 1

// Policy is a complete policy snapshot. Marshal order follows the struct field
// order; all slices are sorted by Normalize before persistence or display.
type Policy struct {
	SchemaVersion int            `json:"schema_version"`
	Namespace     string         `json:"namespace"`
	Directories   []Directory    `json:"directories"`
	ProcessGroups []ProcessGroup `json:"process_groups"`
	RegistryPaths []RegistryPath `json:"registry_paths"`
	Rules         []Rule         `json:"rules"`
}

type Directory struct {
	ID       string   `json:"id"`
	Path     string   `json:"path"`
	ParentID string   `json:"parent_id,omitempty"`
	Access   []string `json:"access,omitempty"`
}

type ProcessGroup struct {
	ID          string   `json:"id"`
	ParentID    string   `json:"parent_id,omitempty"`
	Executables []string `json:"executables"`
}

type RegistryPath struct {
	ID       string   `json:"id"`
	Path     string   `json:"path"`
	ParentID string   `json:"parent_id,omitempty"`
	Access   []string `json:"access,omitempty"`
}

type Rule struct {
	ID        string   `json:"id"`
	Effect    string   `json:"effect"`
	Action    string   `json:"action"`
	Ref       string   `json:"ref"`
	DependsOn []string `json:"depends_on,omitempty"`
}

// ChangeSet is applied atomically to a baseline snapshot. An operation carries
// one typed entity in Entity. Delete operations may set ID instead of Entity.
type ChangeSet struct {
	SchemaVersion int         `json:"schema_version,omitempty"`
	Namespace     string      `json:"namespace"`
	BaseVersion   int         `json:"base_version,omitempty"`
	Operations    []Operation `json:"operations"`
}

type Operation struct {
	Op     string          `json:"op"`
	Kind   string          `json:"kind"`
	ID     string          `json:"id,omitempty"`
	Entity json.RawMessage `json:"entity,omitempty"`
}

// Clone returns a deep copy so concurrent callers can normalize independently.
func (c *ChangeSet) Clone() *ChangeSet {
	if c == nil {
		return nil
	}
	out := *c
	out.Operations = make([]Operation, len(c.Operations))
	copy(out.Operations, c.Operations)
	for i := range out.Operations {
		if len(c.Operations[i].Entity) > 0 {
			out.Operations[i].Entity = append(json.RawMessage(nil), c.Operations[i].Entity...)
		}
	}
	return &out
}

// Clone returns a deep copy so change-set application never mutates the
// committed snapshot held in memory.
func (p *Policy) Clone() *Policy {
	if p == nil {
		return nil
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return &Policy{}
	}
	var out Policy
	if err := json.Unmarshal(raw, &out); err != nil {
		return &Policy{}
	}
	return &out
}

func (p *Policy) normalizeSlices() {
	if p.Directories == nil {
		p.Directories = []Directory{}
	}
	if p.ProcessGroups == nil {
		p.ProcessGroups = []ProcessGroup{}
	}
	if p.RegistryPaths == nil {
		p.RegistryPaths = []RegistryPath{}
	}
	if p.Rules == nil {
		p.Rules = []Rule{}
	}
}

func (p *Policy) entityByID(id string) (kind string, value any, ok bool) {
	for i := range p.Directories {
		if p.Directories[i].ID == id {
			return KindDirectory, p.Directories[i], true
		}
	}
	for i := range p.ProcessGroups {
		if p.ProcessGroups[i].ID == id {
			return KindProcessGroup, p.ProcessGroups[i], true
		}
	}
	for i := range p.RegistryPaths {
		if p.RegistryPaths[i].ID == id {
			return KindRegistryPath, p.RegistryPaths[i], true
		}
	}
	return "", nil, false
}

func (p *Policy) ruleByID(id string) (Rule, bool) {
	for i := range p.Rules {
		if p.Rules[i].ID == id {
			return p.Rules[i], true
		}
	}
	return Rule{}, false
}

func (p *Policy) parentID(kind, id string) string {
	switch kind {
	case KindDirectory:
		for i := range p.Directories {
			if p.Directories[i].ID == id {
				return p.Directories[i].ParentID
			}
		}
	case KindProcessGroup:
		for i := range p.ProcessGroups {
			if p.ProcessGroups[i].ID == id {
				return p.ProcessGroups[i].ParentID
			}
		}
	case KindRegistryPath:
		for i := range p.RegistryPaths {
			if p.RegistryPaths[i].ID == id {
				return p.RegistryPaths[i].ParentID
			}
		}
	}
	return ""
}

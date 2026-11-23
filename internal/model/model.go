package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"protocolfuzzlab/internal/protocol"
)

const (
	EntitySeed      = "seed"
	EntityOperator  = "operator"
	EntityAssertion = "assertion"
	EntityCase      = "case"

	ActionAdd    = "add"
	ActionModify = "modify"
	ActionDelete = "delete"
)

type Seed struct {
	ID          string               `json:"id"`
	Type        protocol.MessageType `json:"type"`
	Payload     []byte               `json:"payload"`
	Description string               `json:"description,omitempty"`
}

type MutationOperator struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	SeedID    string   `json:"seed_id"`
	Kind      string   `json:"kind"`
	Arg       string   `json:"arg,omitempty"`
	DependsOn []string `json:"depends_on,omitempty"`
}

type PathAssertion struct {
	ID         string         `json:"id"`
	CaseID     string         `json:"case_id"`
	TemplateID string         `json:"template_id"`
	From       protocol.State `json:"from"`
	To         protocol.State `json:"to"`
}

type CaseStep struct {
	SeedID      string   `json:"seed_id"`
	OperatorIDs []string `json:"operator_ids,omitempty"`
}

type TestCase struct {
	ID    string     `json:"id"`
	Name  string     `json:"name"`
	Steps []CaseStep `json:"steps"`
}

type CaseSet struct {
	Seeds         []Seed             `json:"seeds"`
	Operators     []MutationOperator `json:"operators"`
	Assertions    []PathAssertion    `json:"assertions"`
	Cases         []TestCase         `json:"cases"`
	Invalidations []Diagnostic       `json:"invalidations,omitempty"`
}

type ChangeSet struct {
	Description string   `json:"description,omitempty"`
	Operations  []Change `json:"operations"`
}

type Change struct {
	Entity string          `json:"entity"`
	Action string          `json:"action"`
	ID     string          `json:"id"`
	Value  json.RawMessage `json:"value,omitempty"`
}

type Diagnostic struct {
	Entity    string   `json:"entity_type"`
	EntityID  string   `json:"entity_id"`
	Code      string   `json:"code"`
	RootChain []string `json:"root_chain"`
	Version   int      `json:"caseset_version"`
}

type DiffSummary struct {
	BeforeVersion     int          `json:"before_version"`
	AfterVersion      int          `json:"after_version,omitempty"`
	Description       string       `json:"description,omitempty"`
	Added             []EntityRef  `json:"added"`
	Modified          []EntityRef  `json:"modified"`
	Deleted           []EntityRef  `json:"deleted"`
	AffectedCases     []string     `json:"affected_cases"`
	AffectedOperators []string     `json:"affected_operators"`
	Diagnostics       []Diagnostic `json:"diagnostics"`
}

type EntityRef struct {
	Entity string `json:"entity"`
	ID     string `json:"id"`
}

type TransitionKey struct {
	From protocol.State `json:"from"`
	To   protocol.State `json:"to"`
}

func (k TransitionKey) String() string { return string(k.From) + "->" + string(k.To) }

func NewCaseSet(seeds []Seed, operators []MutationOperator, assertions []PathAssertion, cases []TestCase) CaseSet {
	cs := CaseSet{Seeds: seeds, Operators: operators, Assertions: assertions, Cases: cases}
	cs.Normalize()
	return cs
}

// Normalize puts collections in a stable order and canonicalizes set-like values.
func (cs *CaseSet) Normalize() {
	sort.Slice(cs.Seeds, func(i, j int) bool { return cs.Seeds[i].ID < cs.Seeds[j].ID })
	sort.Slice(cs.Operators, func(i, j int) bool { return cs.Operators[i].ID < cs.Operators[j].ID })
	sort.Slice(cs.Assertions, func(i, j int) bool { return cs.Assertions[i].ID < cs.Assertions[j].ID })
	sort.Slice(cs.Cases, func(i, j int) bool { return cs.Cases[i].ID < cs.Cases[j].ID })
	sort.Slice(cs.Invalidations, func(i, j int) bool {
		a, b := cs.Invalidations[i], cs.Invalidations[j]
		if a.Entity != b.Entity {
			return a.Entity < b.Entity
		}
		if a.EntityID != b.EntityID {
			return a.EntityID < b.EntityID
		}
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		return fmt.Sprint(a.RootChain) < fmt.Sprint(b.RootChain)
	})
	for i := range cs.Operators {
		sort.Strings(cs.Operators[i].DependsOn)
	}
	for i := range cs.Cases {
		for j := range cs.Cases[i].Steps {
			// Operator order is part of deterministic mutation semantics.
			_ = j
		}
	}
}

func (cs CaseSet) JSON() ([]byte, error) {
	cs.Normalize()
	return json.Marshal(cs)
}

func (cs CaseSet) Digest() string {
	b, err := cs.JSON()
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func DigestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (cs CaseSet) Seed(id string) (Seed, bool) {
	for _, s := range cs.Seeds {
		if s.ID == id {
			return s, true
		}
	}
	return Seed{}, false
}

func (cs CaseSet) Operator(id string) (MutationOperator, bool) {
	for _, op := range cs.Operators {
		if op.ID == id {
			return op, true
		}
	}
	return MutationOperator{}, false
}

func (cs CaseSet) Assertion(id string) (PathAssertion, bool) {
	for _, a := range cs.Assertions {
		if a.ID == id {
			return a, true
		}
	}
	return PathAssertion{}, false
}

func (cs CaseSet) Case(id string) (TestCase, bool) {
	for _, c := range cs.Cases {
		if c.ID == id {
			return c, true
		}
	}
	return TestCase{}, false
}

func (cs CaseSet) CaseIDs() []string {
	out := make([]string, 0, len(cs.Cases))
	for _, c := range cs.Cases {
		out = append(out, c.ID)
	}
	sort.Strings(out)
	return out
}

func (cs CaseSet) OperatorIDs() []string {
	out := make([]string, 0, len(cs.Operators))
	for _, op := range cs.Operators {
		out = append(out, op.ID)
	}
	sort.Strings(out)
	return out
}

func (cs CaseSet) AssertionsForCase(id string) []PathAssertion {
	var out []PathAssertion
	for _, a := range cs.Assertions {
		if a.CaseID == id {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ValidateShape checks ID uniqueness and closed enum values. Dangling references
// are intentionally handled by the dependency analyzer so delete operations can
// produce an invalidation closure instead of being silently rejected.
func (cs CaseSet) ValidateShape() error {
	seen := make(map[string]bool)
	check := func(kind, id string) error {
		if strings.TrimSpace(id) == "" {
			return fmt.Errorf("%s id is empty", kind)
		}
		key := kind + ":" + id
		if seen[key] {
			return fmt.Errorf("duplicate %s id %q", kind, id)
		}
		seen[key] = true
		return nil
	}
	for _, s := range cs.Seeds {
		if err := check(EntitySeed, s.ID); err != nil {
			return err
		}
		if !s.Type.Valid() {
			return fmt.Errorf("seed %q has unknown message type %d", s.ID, s.Type)
		}
		if len(s.Payload) > protocol.MaxWirePayload {
			return fmt.Errorf("seed %q payload exceeds wire limit", s.ID)
		}
	}
	for _, op := range cs.Operators {
		if err := check(EntityOperator, op.ID); err != nil {
			return err
		}
		switch op.Kind {
		case "append", "prepend", "xor", "truncate", "duplicate":
		default:
			return fmt.Errorf("operator %q has unknown kind %q", op.ID, op.Kind)
		}
	}
	for _, a := range cs.Assertions {
		if err := check(EntityAssertion, a.ID); err != nil {
			return err
		}
		if !a.From.Valid() || !a.To.Valid() {
			return fmt.Errorf("assertion %q has invalid state", a.ID)
		}
	}
	for _, c := range cs.Cases {
		if err := check(EntityCase, c.ID); err != nil {
			return err
		}
		if len(c.Steps) == 0 {
			return fmt.Errorf("case %q has no steps", c.ID)
		}
	}
	return nil
}

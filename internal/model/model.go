package model

import (
	"sort"
	"strings"
	"time"
)

type OperatorType string

const (
	TypeScan      OperatorType = "Scan"
	TypeFilter    OperatorType = "Filter"
	TypeProject   OperatorType = "Project"
	TypeJoin      OperatorType = "Join"
	TypeAggregate OperatorType = "Aggregate"
	TypeSort      OperatorType = "Sort"
)

var operatorTypes = map[OperatorType]struct{}{
	TypeScan: {}, TypeFilter: {}, TypeProject: {}, TypeJoin: {}, TypeAggregate: {}, TypeSort: {},
}

func (t OperatorType) Valid() bool {
	_, ok := operatorTypes[t]
	return ok
}

type ColumnRef struct {
	Qualifier string `json:"qualifier,omitempty"`
	Name      string `json:"name"`
}

func (c ColumnRef) Key() string {
	if c.Qualifier == "" {
		return c.Name
	}
	return c.Qualifier + "." + c.Name
}

func (c ColumnRef) Display() string { return c.Key() }

type Predicate struct {
	Expr    string      `json:"expr"`
	Columns []ColumnRef `json:"columns,omitempty"`
}

func (p *Predicate) Clone() *Predicate {
	if p == nil {
		return nil
	}
	out := &Predicate{Expr: p.Expr, Columns: append([]ColumnRef(nil), p.Columns...)}
	return out
}

type StatsBinding struct {
	OperatorID string           `json:"operator_id"`
	RowCount   int64            `json:"row_count"`
	Distinct   map[string]int64 `json:"distinct,omitempty"`
}

func (s *StatsBinding) Clone() *StatsBinding {
	if s == nil {
		return nil
	}
	out := &StatsBinding{OperatorID: s.OperatorID, RowCount: s.RowCount}
	if s.Distinct != nil {
		out.Distinct = make(map[string]int64, len(s.Distinct))
		for k, v := range s.Distinct {
			out.Distinct[k] = v
		}
	}
	return out
}

type Node struct {
	ID        string        `json:"id"`
	Type      OperatorType  `json:"type"`
	Children  []string      `json:"children"`
	Columns   []ColumnRef   `json:"columns"`
	Predicate *Predicate    `json:"predicate,omitempty"`
	Stats     *StatsBinding `json:"stats,omitempty"`
}

func (n *Node) Clone() *Node {
	if n == nil {
		return nil
	}
	return &Node{
		ID:        n.ID,
		Type:      n.Type,
		Children:  append([]string(nil), n.Children...),
		Columns:   append([]ColumnRef(nil), n.Columns...),
		Predicate: n.Predicate.Clone(),
		Stats:     n.Stats.Clone(),
	}
}

type Degradation struct {
	Record     int    `json:"record"`
	OperatorID string `json:"operator_id,omitempty"`
	Field      string `json:"field"`
	Action     string `json:"action"`
	Default    any    `json:"default,omitempty"`
	Reason     string `json:"reason"`
}

type Snapshot struct {
	FormatVersion int
	SnapshotSeq   uint64
	GeneratedAt   time.Time
	Nodes         []*Node
	Degradations  []Degradation
}

// Plan is a validated, immutable view of an operator tree. Outputs contains the
// columns produced by every node, in tree order.
type Plan struct {
	Nodes   map[string]*Node
	Root    string
	Outputs map[string][]ColumnRef
}

func SortColumnRefs(cols []ColumnRef) []ColumnRef {
	out := append([]ColumnRef(nil), cols...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Qualifier == out[j].Qualifier {
			return out[i].Name < out[j].Name
		}
		return out[i].Qualifier < out[j].Qualifier
	})
	return out
}

func SortedUniqueStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func SortedColumns(cols []ColumnRef) []ColumnRef {
	out := SortColumnRefs(cols)
	seen := make(map[string]struct{}, len(out))
	deduped := out[:0]
	for _, c := range out {
		k := c.Key()
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		deduped = append(deduped, c)
	}
	return deduped
}

func NormalizeExpression(expr string) string {
	return strings.Join(strings.Fields(expr), " ")
}

package planstate

import (
	"time"

	"example.com/logicplan/editor/internal/model"
)

type ColumnIndex struct {
	References map[string][]string `json:"references"`
	Outputs    map[string][]string `json:"outputs"`
}

type PredicateIndex struct {
	ByOperator map[string][]string `json:"by_operator"`
	ByColumn   map[string][]string `json:"by_column"`
}

type NodeCost struct {
	Cost float64 `json:"cost"`
	Rows float64 `json:"rows"`
}

type CostEstimate struct {
	ByNode map[string]NodeCost `json:"by_node"`
	Total  float64             `json:"total"`
	Rows   float64             `json:"rows"`
}

type Derived struct {
	TreeSummary    map[string]string `json:"tree_summary"`
	RootHash       string            `json:"root_hash"`
	ColumnIndex    ColumnIndex       `json:"column_index"`
	PredicateIndex PredicateIndex    `json:"predicate_index"`
	CostEstimate   CostEstimate      `json:"cost_estimate"`
}

// State is an immutable committed snapshot. A new State object is published by
// one atomic pointer swap only after SQLite has committed the matching version.
type State struct {
	Version       uint64                       `json:"version"`
	Format        string                       `json:"format"`
	FormatVersion int                          `json:"format_version"`
	Decision      string                       `json:"decision"`
	GeneratedAt   time.Time                    `json:"generated_at"`
	CommittedAt   time.Time                    `json:"committed_at"`
	Root          string                       `json:"root"`
	Nodes         map[string]*model.Node       `json:"-"`
	Outputs       map[string][]model.ColumnRef `json:"-"`
	Derived       Derived                      `json:"derived"`
	Degradations  []model.Degradation          `json:"degradations"`
}

type Summary struct {
	Version        uint64    `json:"version"`
	Format         string    `json:"format"`
	FormatVersion  int       `json:"format_version"`
	Decision       string    `json:"decision"`
	GeneratedAt    time.Time `json:"generated_at"`
	CommittedAt    time.Time `json:"committed_at"`
	Root           string    `json:"root"`
	NodeCount      int       `json:"node_count"`
	TreeHash       string    `json:"tree_hash"`
	DegradationCnt int       `json:"degradation_count"`
}

func (s *State) Summary() Summary {
	if s == nil {
		return Summary{}
	}
	return Summary{
		Version: s.Version, Format: s.Format, FormatVersion: s.FormatVersion,
		Decision: s.Decision, GeneratedAt: s.GeneratedAt, CommittedAt: s.CommittedAt,
		Root: s.Root, NodeCount: len(s.Nodes), TreeHash: s.Derived.RootHash,
		DegradationCnt: len(s.Degradations),
	}
}

func NewFromSnapshot(snapshot *model.Snapshot, format, decision string, committedAt time.Time) (*State, error) {
	plan, err := model.Analyze(snapshot.Nodes)
	if err != nil {
		return nil, err
	}
	derived := BuildFull(plan)
	return &State{
		Version: snapshot.SnapshotSeq, Format: format, FormatVersion: snapshot.FormatVersion,
		Decision: decision, GeneratedAt: snapshot.GeneratedAt, CommittedAt: committedAt,
		Root: plan.Root, Nodes: plan.Nodes, Outputs: plan.Outputs,
		Derived: derived, Degradations: append([]model.Degradation(nil), snapshot.Degradations...),
	}, nil
}

func (s *State) NodeList() []*model.Node {
	ids := make([]string, 0, len(s.Nodes))
	for id := range s.Nodes {
		ids = append(ids, id)
	}
	// Keep the wire representation deterministic without implying tree order.
	sortStrings(ids)
	out := make([]*model.Node, 0, len(ids))
	for _, id := range ids {
		out = append(out, s.Nodes[id])
	}
	return out
}

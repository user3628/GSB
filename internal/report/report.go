package report

import (
	"example.com/ntfsacl/internal/acl"
	"example.com/ntfsacl/internal/domain"
	"example.com/ntfsacl/internal/store"
)

const Schema = "ntfsacl/report/v1"

type SnapshotRef struct {
	Version         int64  `json:"version"`
	Kind            string `json:"kind"`
	ParentVersion   int64  `json:"parent_version"`
	ParentChainHash string `json:"parent_chain_hash"`
}

type SessionReport struct {
	ID           string `json:"id"`
	Subtree      string `json:"subtree"`
	Status       string `json:"status"`
	Code         string `json:"code,omitempty"`
	NodesAudited int    `json:"nodes_audited"`
	NodeBudget   int    `json:"node_budget,omitempty"`
	Partial      bool   `json:"partial"`
}

type Summary struct {
	NodeCount     int                      `json:"node_count"`
	DecisionCount int                      `json:"decision_count"`
	MismatchCount int                      `json:"mismatch_count"`
	ConflictCount int                      `json:"conflict_count"`
	Conflicts     []domain.ConflictSummary `json:"conflicts"`
}

type Report struct {
	Schema        string               `json:"schema"`
	FixtureID     string               `json:"fixture_id"`
	Code          string               `json:"code,omitempty"`
	Snapshot      SnapshotRef          `json:"snapshot"`
	TreeHash      string               `json:"tree_hash"`
	Sessions      []SessionReport      `json:"sessions"`
	Summary       Summary              `json:"summary"`
	Nodes         []domain.NodeReport  `json:"nodes"`
	Conflicts     []domain.Conflict    `json:"conflicts"`
	SnapshotChain []store.SnapshotInfo `json:"snapshot_chain,omitempty"`
}

func (r *Report) Marshal() ([]byte, error) {
	if r.Nodes == nil {
		r.Nodes = []domain.NodeReport{}
	}
	if r.Conflicts == nil {
		r.Conflicts = []domain.Conflict{}
	}
	if r.Summary.Conflicts == nil {
		r.Summary.Conflicts = []domain.ConflictSummary{}
	}
	if r.Sessions == nil {
		r.Sessions = []SessionReport{}
	}
	return domain.CanonicalJSON(r)
}

func New(snapshot SnapshotRef, fixtureID, treeHash, code string, sessions []SessionReport, rebuilt acl.Rebuild) Report {
	summary := Summary{
		NodeCount:     len(rebuilt.Nodes),
		ConflictCount: len(rebuilt.Conflicts),
		Conflicts:     rebuilt.Summary,
	}
	for _, n := range rebuilt.Nodes {
		summary.DecisionCount += len(n.Decisions)
		for _, d := range n.Decisions {
			if !d.Matched {
				summary.MismatchCount++
			}
		}
	}
	return Report{
		Schema: Schema, FixtureID: fixtureID, Code: code, Snapshot: snapshot, TreeHash: treeHash,
		Sessions: sessions, Summary: summary, Nodes: rebuilt.Nodes, Conflicts: rebuilt.Conflicts,
	}
}

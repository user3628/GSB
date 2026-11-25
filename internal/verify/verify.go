// Package verify implements leaf-level and intermediate-level digest
// verification, multi-replica divergence location with quorum consensus,
// and incremental re-verification from a previous checkpoint.
package verify

import (
	"sort"

	"blockinspect/internal/cerr"
	"blockinspect/internal/manifest"
	"blockinspect/internal/trust"
)

// PageReader supplies the encoded bytes of one page. It must return
// os.ErrNotExist (wrapped) when the page data is missing.
type PageReader func(page int) ([]byte, error)

// Side describes one side of a divergence point: the left/right child
// digests and the replicas whose trees contain exactly those digests at
// the divergence node.
type Side struct {
	LeftDigest  string   `json:"left_digest"`
	RightDigest string   `json:"right_digest"`
	Replicas    []string `json:"replicas"`
}

// Divergence pinpoints where two digest trees first disagree.
type Divergence struct {
	FirstPage  int  `json:"first_page"`  // first inconsistent page, -1 when none
	FirstLevel int  `json:"first_level"` // first tree level that diverges (0 = leaves)
	NodeIndex  int  `json:"node_index"`  // node index inside FirstLevel
	Registered Side `json:"registered"`  // registered / consensus side
	Recomputed Side `json:"recomputed"`  // freshly computed / replica side
}

// FullResult is the outcome of a full (non-incremental) verification.
type FullResult struct {
	BlockID           string         `json:"block_id"`
	PagesVerified     int            `json:"pages_verified"`
	MismatchedPages   []int          `json:"mismatched_pages"`
	FirstMismatchPage int            `json:"first_mismatch_page"` // -1 when none
	Divergence        *Divergence    `json:"divergence,omitempty"`
	RootDigest        string         `json:"root_digest"`
	RootMatches       bool           `json:"root_matches"`
	RecomputedTree    *manifest.Tree `json:"-"`
}

// Full re-computes every leaf digest from page data, then rebuilds the
// tree bottom-up and compares it level by level against the registered
// tree. A missing page or a page whose length differs from the manifest
// aborts the run with E_PAGE_MISMATCH; verification never skips ahead and
// aggregates wrong digests upward.
func Full(m *manifest.Manifest, read PageReader, registered *manifest.Tree) (*FullResult, error) {
	leaves, err := recomputeLeaves(m, read, nil)
	if err != nil {
		return nil, err
	}
	return finishFull(m, leaves, registered)
}

// recomputeLeaves recomputes leaf digests. When onlyChanged is non-nil,
// only those pages are read and returned in the same order.
func recomputeLeaves(m *manifest.Manifest, read PageReader, onlyChanged map[int]bool) ([]manifest.Digest, error) {
	var leaves []manifest.Digest
	for p := 0; p < m.PageCount; p++ {
		if onlyChanged != nil && !onlyChanged[p] {
			continue
		}
		data, err := read(p)
		if err != nil || data == nil {
			return nil, cerr.New(cerr.PageMismatch, "page data missing",
				"page", p, "expected_length", m.PageLengths[p], "actual_length", -1)
		}
		if int64(len(data)) != m.PageLengths[p] {
			return nil, cerr.New(cerr.PageMismatch, "page length differs from manifest",
				"page", p, "expected_length", m.PageLengths[p], "actual_length", len(data))
		}
		leaves = append(leaves, manifest.LeafDigest(data))
	}
	return leaves, nil
}

func finishFull(m *manifest.Manifest, leaves []manifest.Digest, registered *manifest.Tree) (*FullResult, error) {
	res := &FullResult{BlockID: m.BlockID, FirstMismatchPage: -1}
	recomputed := manifest.BuildTree(leaves)
	res.RecomputedTree = recomputed
	res.PagesVerified = len(leaves)
	for p := 0; p < m.PageCount; p++ {
		if leaves[p] != m.LeafDigests[p] {
			res.MismatchedPages = append(res.MismatchedPages, p)
			if res.FirstMismatchPage == -1 {
				res.FirstMismatchPage = p
			}
		}
	}
	res.RootDigest = recomputed.Root().Hex()
	res.RootMatches = recomputed.Root() == m.RootDigest
	if registered != nil {
		res.Divergence = firstDivergence(registered, recomputed, nil, nil)
	}
	return res, nil
}

// childDigests returns the left/right child digests of node (level, idx).
// An odd node carried up unchanged has identical left/right children.
func childDigests(t *manifest.Tree, level, idx int) (manifest.Digest, manifest.Digest) {
	if level == 0 {
		d := t.Levels[0][idx]
		return d, d
	}
	below := t.Levels[level-1]
	left := below[2*idx]
	right := left
	if 2*idx+1 < len(below) {
		right = below[2*idx+1]
	}
	return left, right
}

// firstDivergence finds the first page and the first tree level where two
// trees disagree, plus the left/right child digests on both sides at the
// divergence node. regReplicas/recReplicas optionally name the owners of
// each side's digests.
func firstDivergence(registered, recomputed *manifest.Tree, regReplicas, recReplicas []string) *Divergence {
	if len(registered.Levels) == 0 || len(recomputed.Levels) == 0 {
		return nil
	}
	d := &Divergence{FirstPage: -1, FirstLevel: -1}
	leaves := registered.Levels[0]
	for p := 0; p < len(leaves) && p < len(recomputed.Levels[0]); p++ {
		if leaves[p] != recomputed.Levels[0][p] {
			d.FirstPage = p
			break
		}
	}
	maxLevel := registered.MaxLevel()
	if recomputed.MaxLevel() > maxLevel {
		maxLevel = recomputed.MaxLevel()
	}
	for level := 0; level <= maxLevel; level++ {
		if level > registered.MaxLevel() || level > recomputed.MaxLevel() {
			break
		}
		n := len(registered.Levels[level])
		if len(recomputed.Levels[level]) < n {
			n = len(recomputed.Levels[level])
		}
		found := -1
		for i := 0; i < n; i++ {
			if registered.Levels[level][i] != recomputed.Levels[level][i] {
				found = i
				break
			}
		}
		if found >= 0 {
			d.FirstLevel = level
			d.NodeIndex = found
			rl, rr := childDigests(registered, level, found)
			d.Registered = Side{LeftDigest: rl.Hex(), RightDigest: rr.Hex(), Replicas: regReplicas}
			cl, cr := childDigests(recomputed, level, found)
			d.Recomputed = Side{LeftDigest: cl.Hex(), RightDigest: cr.Hex(), Replicas: recReplicas}
			break
		}
	}
	if d.FirstPage == -1 && d.FirstLevel == -1 {
		return nil
	}
	return d
}

// Report is one replica's inspection report.
type Report struct {
	ReplicaID   string
	PageDigests []manifest.Digest
	RootDigest  manifest.Digest
}

// ReplicaDivergence describes how one replica diverges from consensus.
type ReplicaDivergence struct {
	ReplicaID      string `json:"replica_id"`
	InQuorum       bool   `json:"in_quorum"`
	DivergentPages []int  `json:"divergent_pages"`
	FirstPage      int    `json:"first_page"`  // -1 when fully consistent
	FirstLevel     int    `json:"first_level"` // -1 when fully consistent
}

// ConsensusResult is the outcome of multi-replica divergence location.
type ConsensusResult struct {
	Quorum             bool                `json:"quorum"`
	ConsensusRoot      string              `json:"consensus_root"`
	QuorumReplicas     []string            `json:"quorum_replicas"`
	Votes              map[string]int      `json:"votes"` // candidate root digest -> votes
	FirstDivergentPage int                 `json:"first_divergent_page"`
	FirstDivergeLevel  int                 `json:"first_diverge_level"`
	Divergence         *Divergence         `json:"divergence,omitempty"`
	PerReplica         []ReplicaDivergence `json:"per_replica"`
	ConsensusTree      *manifest.Tree      `json:"-"`
}

// Consensus locates the first divergent page/level across replicas and
// derives the quorum-backed consensus root and page digest set. When no
// digest set wins a strict majority it fails with E_NO_QUORUM and the
// per-candidate vote counts; no replica is picked by default.
func Consensus(reports []Report, pageCount int) (*ConsensusResult, error) {
	if len(reports) == 0 {
		return nil, cerr.New(cerr.NoQuorum, "no replica reports supplied", "votes", map[string]int{})
	}
	for _, r := range reports {
		if len(r.PageDigests) != pageCount {
			return nil, cerr.New(cerr.PageMismatch,
				"replica report page digest count differs from manifest",
				"replica", r.ReplicaID,
				"expected_length", pageCount, "actual_length", len(r.PageDigests))
		}
	}
	// group replicas by reported root digest
	type group struct {
		root     manifest.Digest
		replicas []string
	}
	var groups []*group
	byRoot := map[manifest.Digest]*group{}
	for _, r := range reports {
		g, ok := byRoot[r.RootDigest]
		if !ok {
			g = &group{root: r.RootDigest}
			byRoot[r.RootDigest] = g
			groups = append(groups, g)
		}
		g.replicas = append(g.replicas, r.ReplicaID)
	}
	votes := map[string]int{}
	for _, g := range groups {
		sort.Strings(g.replicas)
		votes[g.root.Hex()] = len(g.replicas)
	}
	res := &ConsensusResult{Votes: votes, FirstDivergentPage: -1, FirstDivergeLevel: -1}
	var winner *group
	for _, g := range groups {
		if len(g.replicas)*2 > len(reports) {
			winner = g
			break
		}
	}
	if winner == nil {
		return nil, cerr.New(cerr.NoQuorum, "no digest set reached a strict majority",
			"votes", votes, "replicas", len(reports))
	}
	res.Quorum = true
	res.ConsensusRoot = winner.root.Hex()
	res.QuorumReplicas = winner.replicas

	byID := map[string]Report{}
	for _, r := range reports {
		byID[r.ReplicaID] = r
	}
	// consensus page digests come from the lexicographically first quorum
	// replica, keeping the choice deterministic
	consensusRep := byID[winner.replicas[0]]
	res.ConsensusTree = manifest.BuildTree(consensusRep.PageDigests)

	// per-replica divergence against consensus
	for _, r := range reports {
		rd := ReplicaDivergence{ReplicaID: r.ReplicaID, FirstPage: -1, FirstLevel: -1}
		rd.InQuorum = contains(winner.replicas, r.ReplicaID)
		rt := manifest.BuildTree(r.PageDigests)
		for p := 0; p < pageCount; p++ {
			if r.PageDigests[p] != consensusRep.PageDigests[p] {
				rd.DivergentPages = append(rd.DivergentPages, p)
				if rd.FirstPage == -1 {
					rd.FirstPage = p
				}
			}
		}
		if len(rd.DivergentPages) > 0 {
			div := firstDivergence(res.ConsensusTree, rt, winner.replicas, []string{r.ReplicaID})
			if div != nil {
				rd.FirstLevel = div.FirstLevel
				if res.FirstDivergentPage == -1 || div.FirstPage < res.FirstDivergentPage {
					res.FirstDivergentPage = div.FirstPage
				}
				if res.FirstDivergeLevel == -1 || div.FirstLevel < res.FirstDivergeLevel {
					res.FirstDivergeLevel = div.FirstLevel
					res.Divergence = div
				}
			}
		}
		res.PerReplica = append(res.PerReplica, rd)
	}
	sort.Slice(res.PerReplica, func(i, j int) bool {
		return res.PerReplica[i].ReplicaID < res.PerReplica[j].ReplicaID
	})
	return res, nil
}

// TrustInputs converts the consensus result into trust-score inputs.
func (c *ConsensusResult) TrustInputs() []trust.ReplicaInput {
	var out []trust.ReplicaInput
	for _, rd := range c.PerReplica {
		lvl := rd.FirstLevel
		if lvl < 0 {
			lvl = 0
		}
		out = append(out, trust.ReplicaInput{
			ReplicaID:         rd.ReplicaID,
			DivergentPages:    len(rd.DivergentPages),
			FirstDivergeLevel: lvl,
			InQuorum:          rd.InQuorum,
		})
	}
	return out
}

// DivergentPageSet returns the sorted union of pages diverging from
// consensus across all replicas.
func (c *ConsensusResult) DivergentPageSet() []int {
	seen := map[int]bool{}
	var out []int
	for _, rd := range c.PerReplica {
		for _, p := range rd.DivergentPages {
			if !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
	}
	sort.Ints(out)
	return out
}

// IncrementalResult reports an incremental re-verification.
type IncrementalResult struct {
	CheckpointID    int            `json:"checkpoint_id"`
	ChangedPages    []int          `json:"changed_pages"`
	RecomputedNodes int            `json:"recomputed_nodes"`
	ReusedNodes     int            `json:"reused_nodes"`
	TotalNodes      int            `json:"total_nodes"`
	SavingsRatio    int64          `json:"savings_ratio"` // fixed-point, trust.Scale == 1.0
	SavingsText     string         `json:"savings_text"`
	NewRootDigest   string         `json:"new_root_digest"`
	NewTree         *manifest.Tree `json:"-"`
}

// Incremental re-verifies a block from a previous checkpoint tree: only
// the declared changed pages have their leaf digests recomputed, plus the
// intermediate nodes on the affected paths. Every untouched subtree
// reuses its registered digest. The result is a pure function of the
// inputs, so repeating it for the same block and checkpoint yields
// bit-identical output.
func Incremental(m *manifest.Manifest, read PageReader, prev *manifest.Tree,
	checkpointID int, changedPages []int) (*IncrementalResult, error) {
	changed := map[int]bool{}
	var sortedChanged []int
	for _, p := range changedPages {
		if p < 0 || p >= m.PageCount {
			return nil, cerr.New(cerr.BadRequest, "changed page out of range",
				"page", p, "page_count", m.PageCount)
		}
		if !changed[p] {
			changed[p] = true
			sortedChanged = append(sortedChanged, p)
		}
	}
	sort.Ints(sortedChanged)

	newLeaves, err := recomputeLeaves(m, read, changed)
	if err != nil {
		return nil, err
	}
	// start from the previous tree and splice in recomputed nodes
	levels := make([][]manifest.Digest, len(prev.Levels))
	for i, l := range prev.Levels {
		levels[i] = append([]manifest.Digest(nil), l...)
	}
	li := 0
	for _, p := range sortedChanged {
		levels[0][p] = newLeaves[li]
		li++
	}
	affected := manifest.AffectedNodes(m.PageCount, sortedChanged)
	for level := 1; level < len(levels); level++ {
		for id := range affected {
			if id.Level != level {
				continue
			}
			left, right := childDigests(&manifest.Tree{Levels: levels}, level, id.Index)
			if left == right && 2*id.Index+1 >= len(levels[level-1]) {
				levels[level][id.Index] = left // carried-up odd node
			} else {
				levels[level][id.Index] = manifest.NodeDigest(left, right)
			}
		}
	}
	newTree := &manifest.Tree{Levels: levels}
	total := newTree.NodeCount()
	recomputed := len(affected)
	reused := total - recomputed
	ledger := trust.NewLedger()
	savings := ledger.Div("savings_ratio", int64(reused), int64(total))
	return &IncrementalResult{
		CheckpointID:    checkpointID,
		ChangedPages:    sortedChanged,
		RecomputedNodes: recomputed,
		ReusedNodes:     reused,
		TotalNodes:      total,
		SavingsRatio:    savings,
		SavingsText:     trust.ScoreText(savings),
		NewRootDigest:   newTree.Root().Hex(),
		NewTree:         newTree,
	}, nil
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

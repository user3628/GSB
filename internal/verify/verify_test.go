package verify

import (
	"testing"

	"blockinspect/internal/cerr"
	"blockinspect/internal/manifest"
)

func buildManifest(t *testing.T, pages [][]byte) *manifest.Manifest {
	t.Helper()
	m := &manifest.Manifest{BlockID: "blk-v", PageCount: len(pages), ColumnsPerPage: 2}
	for _, p := range pages {
		m.PageLengths = append(m.PageLengths, int64(len(p)))
		m.LeafDigests = append(m.LeafDigests, manifest.LeafDigest(p))
	}
	m.RootDigest = manifest.BuildTree(m.LeafDigests).Root()
	return m
}

func readerFor(pages [][]byte) PageReader {
	return func(p int) ([]byte, error) {
		if p < 0 || p >= len(pages) {
			return nil, nil
		}
		return pages[p], nil
	}
}

func samplePages(n int) [][]byte {
	pages := make([][]byte, n)
	for i := range pages {
		p := make([]byte, 64+i)
		for j := range p {
			p[j] = byte(i*31 + j)
		}
		pages[i] = p
	}
	return pages
}

func TestFullVerifyClean(t *testing.T) {
	pages := samplePages(8)
	m := buildManifest(t, pages)
	res, err := Full(m, readerFor(pages), manifest.BuildTree(m.LeafDigests))
	if err != nil {
		t.Fatal(err)
	}
	if res.FirstMismatchPage != -1 || !res.RootMatches || res.Divergence != nil {
		t.Fatalf("clean verify reported divergence: %+v", res)
	}
}

func TestFullVerifyLocatesCorruption(t *testing.T) {
	pages := samplePages(8)
	m := buildManifest(t, pages)
	pages[5][0] ^= 0xFF // corrupt page 5 on disk
	res, err := Full(m, readerFor(pages), manifest.BuildTree(m.LeafDigests))
	if err != nil {
		t.Fatal(err)
	}
	if res.FirstMismatchPage != 5 {
		t.Fatalf("want first mismatch page 5, got %d", res.FirstMismatchPage)
	}
	if res.Divergence == nil || res.Divergence.FirstPage != 5 || res.Divergence.FirstLevel != 0 {
		t.Fatalf("bad divergence: %+v", res.Divergence)
	}
	if res.RootMatches {
		t.Fatal("root must not match after corruption")
	}
}

func TestFullVerifyPageMismatch(t *testing.T) {
	pages := samplePages(4)
	m := buildManifest(t, pages)
	pages[2] = pages[2][:len(pages[2])-1] // truncate page 2
	_, err := Full(m, readerFor(pages), manifest.BuildTree(m.LeafDigests))
	ce, ok := err.(*cerr.Error)
	if !ok || ce.Code != cerr.PageMismatch {
		t.Fatalf("want E_PAGE_MISMATCH, got %v", err)
	}
	if ce.Context["page"] != 2 {
		t.Fatalf("want page 2 in context, got %v", ce.Context)
	}
}

func TestFullVerifyMissingPage(t *testing.T) {
	pages := samplePages(4)
	m := buildManifest(t, pages)
	reader := func(p int) ([]byte, error) {
		if p == 1 {
			return nil, nil
		}
		return pages[p], nil
	}
	_, err := Full(m, reader, nil)
	if cerr.CodeOf(err) != cerr.PageMismatch {
		t.Fatalf("want E_PAGE_MISMATCH, got %v", err)
	}
}

func reportsFrom(t *testing.T, leaves []manifest.Digest, ids ...string) []Report {
	t.Helper()
	var out []Report
	for _, id := range ids {
		out = append(out, Report{
			ReplicaID:   id,
			PageDigests: append([]manifest.Digest(nil), leaves...),
			RootDigest:  manifest.BuildTree(leaves).Root(),
		})
	}
	return out
}

func TestConsensusQuorumAndDivergence(t *testing.T) {
	pages := samplePages(8)
	m := buildManifest(t, pages)
	good := m.LeafDigests
	bad := append([]manifest.Digest(nil), good...)
	bad[3][0] ^= 0xFF
	reps := reportsFrom(t, good, "a", "b")
	reps = append(reps, Report{ReplicaID: "c", PageDigests: bad,
		RootDigest: manifest.BuildTree(bad).Root()})
	res, err := Consensus(reps, m.PageCount)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Quorum || res.FirstDivergentPage != 3 {
		t.Fatalf("bad consensus: %+v", res)
	}
	if len(res.QuorumReplicas) != 2 {
		t.Fatalf("want 2 quorum replicas, got %v", res.QuorumReplicas)
	}
	if res.Divergence == nil || len(res.Divergence.Registered.Replicas) != 2 ||
		len(res.Divergence.Recomputed.Replicas) != 1 {
		t.Fatalf("bad divergence sides: %+v", res.Divergence)
	}
}

func TestConsensusNoQuorum(t *testing.T) {
	pages := samplePages(4)
	m := buildManifest(t, pages)
	a := m.LeafDigests
	b := append([]manifest.Digest(nil), a...)
	b[0][0] ^= 1
	c := append([]manifest.Digest(nil), a...)
	c[1][0] ^= 1
	reps := reportsFrom(t, a, "r1")
	reps = append(reps, Report{ReplicaID: "r2", PageDigests: b, RootDigest: manifest.BuildTree(b).Root()})
	reps = append(reps, Report{ReplicaID: "r3", PageDigests: c, RootDigest: manifest.BuildTree(c).Root()})
	_, err := Consensus(reps, m.PageCount)
	ce, ok := err.(*cerr.Error)
	if !ok || ce.Code != cerr.NoQuorum {
		t.Fatalf("want E_NO_QUORUM, got %v", err)
	}
	votes, ok := ce.Context["votes"].(map[string]int)
	if !ok || len(votes) != 3 {
		t.Fatalf("want 3 vote entries, got %v", ce.Context["votes"])
	}
}

func TestIncrementalReusesUnchanged(t *testing.T) {
	pages := samplePages(16)
	m := buildManifest(t, pages)
	prev := manifest.BuildTree(m.LeafDigests)
	// mutate page data for pages 2 and 9
	pages[2][0] ^= 0xAA
	pages[9][3] ^= 0x55
	res, err := Incremental(m, readerFor(pages), prev, 0, []int{2, 9})
	if err != nil {
		t.Fatal(err)
	}
	if res.RecomputedNodes <= 0 || res.ReusedNodes <= 0 ||
		res.RecomputedNodes+res.ReusedNodes != res.TotalNodes {
		t.Fatalf("bad counts: %+v", res)
	}
	// full recompute over the mutated data must yield the same new tree
	full := manifest.BuildTree(nil)
	var leaves []manifest.Digest
	for _, p := range pages {
		leaves = append(leaves, manifest.LeafDigest(p))
	}
	full = manifest.BuildTree(leaves)
	if res.NewTree.Root() != full.Root() {
		t.Fatal("incremental root differs from full recompute root")
	}
	for lvl := range full.Levels {
		for i := range full.Levels[lvl] {
			if res.NewTree.Levels[lvl][i] != full.Levels[lvl][i] {
				t.Fatalf("node mismatch at level %d idx %d", lvl, i)
			}
		}
	}
	// determinism: same inputs -> identical result
	res2, err := Incremental(m, readerFor(pages), prev, 0, []int{2, 9})
	if err != nil {
		t.Fatal(err)
	}
	if res.RecomputedNodes != res2.RecomputedNodes || res.NewRootDigest != res2.NewRootDigest ||
		res.SavingsRatio != res2.SavingsRatio {
		t.Fatal("incremental verification is not deterministic")
	}
}

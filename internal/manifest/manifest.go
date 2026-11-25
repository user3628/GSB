// Package manifest models a columnar block manifest and its Merkle-style
// chunked digest tree. Leaves are per-page content digests; intermediate
// nodes are the digest of the concatenation of their children digests; the
// root digest is registered in the block manifest.
package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"blockinspect/internal/cerr"
)

// DigestLen is the fixed byte length of every digest in the system.
const DigestLen = 32

// Digest is a 32-byte fixed-size content digest.
type Digest [DigestLen]byte

// ParseDigest decodes a hex string into a Digest, enforcing the 32-byte
// fixed length.
func ParseDigest(s string) (Digest, error) {
	var d Digest
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != DigestLen {
		return d, fmt.Errorf("digest must be %d bytes hex-encoded, got %d decoded bytes", DigestLen, len(b))
	}
	copy(d[:], b)
	return d, nil
}

// Hex renders the digest as lowercase hex.
func (d Digest) Hex() string { return hex.EncodeToString(d[:]) }

// IsZero reports whether the digest is all zeroes.
func (d Digest) IsZero() bool { return d == Digest{} }

// LeafDigest computes the content digest of one encoded page.
func LeafDigest(pageBytes []byte) Digest { return Digest(sha256.Sum256(pageBytes)) }

// NodeDigest computes an intermediate node digest from its left and right
// child digests: sha256(left || right).
func NodeDigest(left, right Digest) Digest {
	return Digest(sha256.Sum256(append(left[:], right[:]...)))
}

// Manifest describes one columnar block.
type Manifest struct {
	BlockID        string   `json:"block_id"`
	Rows           int64    `json:"rows"`
	PageCount      int      `json:"page_count"`
	ColumnsPerPage int      `json:"columns_per_page"`
	PageLengths    []int64  `json:"page_lengths"`
	LeafDigests    []Digest `json:"leaf_digests"`
	RootDigest     Digest   `json:"root_digest"`
	Seed           int64    `json:"seed"`
}

// manifestJSON is the on-disk wire form (digests as hex strings).
type manifestJSON struct {
	BlockID        string   `json:"block_id"`
	Rows           int64    `json:"rows"`
	PageCount      int      `json:"page_count"`
	ColumnsPerPage int      `json:"columns_per_page"`
	PageLengths    []int64  `json:"page_lengths"`
	LeafDigests    []string `json:"leaf_digests"`
	RootDigest     string   `json:"root_digest"`
	Seed           int64    `json:"seed"`
}

// MarshalJSON implements json.Marshaler.
func (m *Manifest) MarshalJSON() ([]byte, error) {
	w := manifestJSON{
		BlockID:        m.BlockID,
		Rows:           m.Rows,
		PageCount:      m.PageCount,
		ColumnsPerPage: m.ColumnsPerPage,
		PageLengths:    m.PageLengths,
		RootDigest:     m.RootDigest.Hex(),
		Seed:           m.Seed,
	}
	for _, d := range m.LeafDigests {
		w.LeafDigests = append(w.LeafDigests, d.Hex())
	}
	return json.Marshal(w)
}

// UnmarshalJSON implements json.Unmarshaler with full validation.
func (m *Manifest) UnmarshalJSON(b []byte) error {
	var w manifestJSON
	if err := json.Unmarshal(b, &w); err != nil {
		return cerr.New(cerr.ManifestInvalid, "manifest is not valid JSON", "detail", err.Error())
	}
	parsed, err := parseManifest(&w)
	if err != nil {
		return err
	}
	*m = *parsed
	return nil
}

func parseManifest(w *manifestJSON) (*Manifest, *cerr.Error) {
	m := &Manifest{
		BlockID:        w.BlockID,
		Rows:           w.Rows,
		PageCount:      w.PageCount,
		ColumnsPerPage: w.ColumnsPerPage,
		PageLengths:    w.PageLengths,
		Seed:           w.Seed,
	}
	if m.BlockID == "" {
		return nil, cerr.New(cerr.ManifestInvalid, "block_id must not be empty")
	}
	if m.PageCount <= 0 {
		return nil, cerr.New(cerr.ManifestInvalid, "page_count must be a positive integer",
			"page_count", m.PageCount)
	}
	if m.ColumnsPerPage <= 0 {
		return nil, cerr.New(cerr.ManifestInvalid, "columns_per_page must be a positive integer",
			"columns_per_page", m.ColumnsPerPage)
	}
	if len(m.PageLengths) != m.PageCount {
		return nil, cerr.New(cerr.ManifestInvalid, "page_lengths length must equal page_count",
			"page_count", m.PageCount, "page_lengths", len(m.PageLengths))
	}
	for i, l := range m.PageLengths {
		if l <= 0 {
			return nil, cerr.New(cerr.ManifestInvalid, "page length must be positive",
				"page", i, "length", l)
		}
	}
	if len(w.LeafDigests) != m.PageCount {
		return nil, cerr.New(cerr.ManifestInvalid, "leaf digest count must equal page_count",
			"page_count", m.PageCount, "leaf_digests", len(w.LeafDigests))
	}
	for i, s := range w.LeafDigests {
		d, err := ParseDigest(s)
		if err != nil {
			return nil, cerr.New(cerr.ManifestInvalid, "leaf digest is not a 32-byte value",
				"page", i, "detail", err.Error())
		}
		m.LeafDigests = append(m.LeafDigests, d)
	}
	root, err := ParseDigest(w.RootDigest)
	if err != nil {
		return nil, cerr.New(cerr.ManifestInvalid, "root digest is not a 32-byte value",
			"detail", err.Error())
	}
	m.RootDigest = root
	// The registered root digest must correspond to the bottom-level leaf
	// set: rebuild the tree from the leaves and compare roots.
	if got := BuildTree(m.LeafDigests).Root(); got != m.RootDigest {
		return nil, cerr.New(cerr.ManifestInvalid,
			"root digest does not correspond to the leaf digests",
			"expected_root", m.RootDigest.Hex(), "computed_root", got.Hex(),
			"leaf_count", len(m.LeafDigests))
	}
	return m, nil
}

// LoadFile reads and validates a manifest from disk. On validation failure
// it returns E_MANIFEST_INVALID and a nil manifest, so callers keep using
// any previously loaded manifest.
func LoadFile(path string) (*Manifest, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, cerr.New(cerr.ManifestInvalid, "cannot read manifest file",
			"path", path, "detail", err.Error())
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// Tree is a Merkle-style chunked digest tree. Levels[0] holds the leaf
// (per-page) digests; each higher level pairs children digests. An odd
// node at the end of a level is carried up unchanged.
type Tree struct {
	Levels [][]Digest `json:"levels"`
}

// BuildTree constructs the full digest tree above the given leaves.
func BuildTree(leaves []Digest) *Tree {
	t := &Tree{}
	if len(leaves) == 0 {
		return t
	}
	cur := append([]Digest(nil), leaves...)
	t.Levels = append(t.Levels, cur)
	for len(cur) > 1 {
		next := make([]Digest, 0, (len(cur)+1)/2)
		for i := 0; i < len(cur); i += 2 {
			if i+1 < len(cur) {
				next = append(next, NodeDigest(cur[i], cur[i+1]))
			} else {
				next = append(next, cur[i]) // odd node carried up unchanged
			}
		}
		t.Levels = append(t.Levels, next)
		cur = next
	}
	return t
}

// Root returns the root digest of the tree (zero digest for an empty tree).
func (t *Tree) Root() Digest {
	if len(t.Levels) == 0 {
		return Digest{}
	}
	top := t.Levels[len(t.Levels)-1]
	return top[0]
}

// MaxLevel returns the highest level index (leaves are level 0).
func (t *Tree) MaxLevel() int { return len(t.Levels) - 1 }

// NodeCount returns the total number of nodes across all levels.
func (t *Tree) NodeCount() int {
	n := 0
	for _, l := range t.Levels {
		n += len(l)
	}
	return n
}

// NodeID identifies one node inside the tree.
type NodeID struct {
	Level int `json:"level"`
	Index int `json:"index"`
}

// AffectedNodes computes the set of nodes that must be recomputed when the
// given leaf pages change: the changed leaves themselves plus every
// ancestor up to the root. Everything else can be reused.
func AffectedNodes(leafCount int, changedPages []int) map[NodeID]struct{} {
	out := map[NodeID]struct{}{}
	if leafCount == 0 {
		return out
	}
	// nodes per level, mirroring BuildTree's carry-up rule
	widths := []int{leafCount}
	for widths[len(widths)-1] > 1 {
		widths = append(widths, (widths[len(widths)-1]+1)/2)
	}
	for _, p := range changedPages {
		if p < 0 || p >= leafCount {
			continue
		}
		idx := p
		for level := 0; level < len(widths); level++ {
			out[NodeID{Level: level, Index: idx}] = struct{}{}
			idx /= 2
		}
	}
	return out
}

package manifest

import (
	"encoding/json"
	"testing"

	"blockinspect/internal/cerr"
)

func validManifestJSON(t *testing.T) []byte {
	t.Helper()
	leaves := []Digest{LeafDigest([]byte("p0")), LeafDigest([]byte("p1")), LeafDigest([]byte("p2"))}
	m := &Manifest{
		BlockID: "blk-t", Rows: 100, PageCount: 3, ColumnsPerPage: 2,
		PageLengths: []int64{2, 2, 2}, LeafDigests: leaves,
		RootDigest: BuildTree(leaves).Root(), Seed: 7,
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestManifestRoundTrip(t *testing.T) {
	var m Manifest
	if err := json.Unmarshal(validManifestJSON(t), &m); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}
	if m.PageCount != 3 || m.RootDigest.IsZero() {
		t.Fatalf("unexpected manifest: %+v", m)
	}
}

func TestManifestRejectsNonPositivePageCount(t *testing.T) {
	b := validManifestJSON(t)
	var raw map[string]any
	json.Unmarshal(b, &raw)
	raw["page_count"] = 0
	b, _ = json.Marshal(raw)
	var m Manifest
	err := json.Unmarshal(b, &m)
	if cerr.CodeOf(err) != cerr.ManifestInvalid {
		t.Fatalf("want E_MANIFEST_INVALID, got %v", err)
	}
}

func TestManifestRejectsShortDigest(t *testing.T) {
	b := validManifestJSON(t)
	var raw map[string]any
	json.Unmarshal(b, &raw)
	raw["leaf_digests"].([]any)[0] = "abcd"
	b, _ = json.Marshal(raw)
	var m Manifest
	err := json.Unmarshal(b, &m)
	if cerr.CodeOf(err) != cerr.ManifestInvalid {
		t.Fatalf("want E_MANIFEST_INVALID, got %v", err)
	}
}

func TestManifestRejectsRootLeafMismatch(t *testing.T) {
	b := validManifestJSON(t)
	var raw map[string]any
	json.Unmarshal(b, &raw)
	raw["root_digest"] = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	b, _ = json.Marshal(raw)
	var m Manifest
	err := json.Unmarshal(b, &m)
	if cerr.CodeOf(err) != cerr.ManifestInvalid {
		t.Fatalf("want E_MANIFEST_INVALID, got %v", err)
	}
}

func TestAffectedNodes(t *testing.T) {
	// 8 leaves -> levels of width 8,4,2,1 = 15 nodes; one changed page
	// touches 1 node per level = 4 nodes.
	got := AffectedNodes(8, []int{3})
	if len(got) != 4 {
		t.Fatalf("want 4 affected nodes, got %d", len(got))
	}
	// odd leaf count: 5 leaves -> widths 5,3,2,1
	got = AffectedNodes(5, []int{4})
	if len(got) != 4 {
		t.Fatalf("want 4 affected nodes for odd tree, got %d", len(got))
	}
}

// Package pathtree implements a dot-segmented key path trie used for
// longest-prefix-first subscription matching.
package pathtree

import (
	"strings"

	"configtree/internal/spec"
)

// Node is one segment of the key path trie. Terminal nodes carry the prefix
// that ends at this node and the owning component (if any).
type Node struct {
	Segment  string
	Children map[string]*Node
	Terminal bool
	Prefix   string
}

// Trie is a key path prefix trie.
type Trie struct {
	root *Node
}

func New() *Trie {
	return &Trie{root: &Node{Children: map[string]*Node{}}}
}

// Insert adds a prefix path. Empty segments are rejected by the spec layer.
func (t *Trie) Insert(prefix string) {
	segments := strings.Split(prefix, ".")
	cur := t.root
	built := ""
	for i, seg := range segments {
		child, ok := cur.Children[seg]
		if !ok {
			child = &Node{Segment: seg, Children: map[string]*Node{}}
			cur.Children[seg] = child
		}
		cur = child
		if i > 0 {
			built += "."
		}
		built += seg
	}
	cur.Terminal = true
	cur.Prefix = prefix
}

// Prefixes returns all terminal prefixes that are path-wise prefixes of key,
// ordered longest first.
func (t *Trie) Prefixes(key string) []string {
	segments := strings.Split(key, ".")
	cur := t.root
	var matched []string
	for _, seg := range segments {
		child, ok := cur.Children[seg]
		if !ok {
			break
		}
		cur = child
		if cur.Terminal {
			matched = append(matched, cur.Prefix)
		}
	}
	// Reverse: deepest (longest) prefix first.
	for i, j := 0, len(matched)-1; i < j; i, j = i+1, j-1 {
		matched[i], matched[j] = matched[j], matched[i]
	}
	return matched
}

// Match returns the longest matching prefix for key and the hop distance
// between them. Empty prefix means no match (unless the subscription set is
// wildcard, which the caller handles separately).
func (t *Trie) Match(key string) (prefix string, hops int, matched bool) {
	prefixes := t.Prefixes(key)
	if len(prefixes) == 0 {
		return "", 0, false
	}
	return prefixes[0], spec.HopDistance(prefixes[0], key), true
}

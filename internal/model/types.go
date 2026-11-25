package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
)

// HeadID identifies a single attention head inside the small MLM.
type HeadID struct {
	Layer int `json:"layer"`
	Head  int `json:"head"`
}

func (h HeadID) String() string { return fmt.Sprintf("L%dH%d", h.Layer, h.Head) }

func (h HeadID) Less(o HeadID) bool {
	if h.Layer != o.Layer {
		return h.Layer < o.Layer
	}
	return h.Head < o.Head
}

// Head is one attention-head weight shard. Activations are deterministic
// per-probe activation samples produced by the built-in generator.
type Head struct {
	Layer         int       `json:"layer"`
	Head          int       `json:"head"`
	Dim           int       `json:"dim"`
	Weight        []float64 `json:"weight"`
	SynergyWeight []float64 `json:"synergy_weight,omitempty"`
	Activations   []float64 `json:"activations"`
}

func (h Head) ID() HeadID { return HeadID{Layer: h.Layer, Head: h.Head} }

// Candidate is one ground-truth candidate for a masked position.
type Candidate struct {
	Token  string    `json:"token"`
	Feat   []float64 `json:"feat"`
	IsTrue bool      `json:"is_true"`
}

// Sentence is a masked annotation pair with token, candidates and features.
type Sentence struct {
	ID         string      `json:"id"`
	MaskedTok  string      `json:"masked_token"`
	Candidates []Candidate `json:"candidates"`
}

// Policy is the pruning strategy snapshot with a monotonic sequence number.
type Policy struct {
	Version             int64     `json:"version"`
	CandidateRatios     []float64 `json:"candidate_ratios"`
	QuantBits           int       `json:"quant_bits"`
	Tolerance           float64   `json:"reconstruction_tolerance"`
	ProbeLimit          int       `json:"probe_sample_limit"`
	MinProbes           int       `json:"min_probes"`
	MaxPruneHeads       int       `json:"max_prune_heads"`
	CouplingThreshold   float64   `json:"coupling_deviation_threshold"`
	RollbackGenerations int       `json:"rollback_keep_generations"`
	Seed                int64     `json:"seed"`
}

// Dataset bundles the three replaceable inputs of the workbench.
type Dataset struct {
	Heads     []Head     `json:"heads"`
	Sentences []Sentence `json:"sentences"`
	Policy    Policy     `json:"policy"`
}

// HeadSet is a canonical, sorted head collection.
type HeadSet []HeadID

func (s HeadSet) Sorted() HeadSet {
	out := append(HeadSet(nil), s...)
	sort.Slice(out, func(i, j int) bool { return out[i].Less(out[j]) })
	return out
}

func (s HeadSet) Contains(id HeadID) bool {
	for _, x := range s {
		if x == id {
			return true
		}
	}
	return false
}

func (s HeadSet) Fingerprint() string {
	cp := s.Sorted()
	b, _ := CanonicalJSON(cp)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// CanonicalJSON serializes any value with sorted object keys and fixed
// settings, so identical inputs always yield byte-identical output.
func CanonicalJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Digest returns the hex SHA-256 of the canonical JSON encoding.
func Digest(v any) (string, error) {
	b, err := CanonicalJSON(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// WriteJSON writes canonical JSON with a trailing newline.
func WriteJSON(path string, v any) error {
	b, err := CanonicalJSON(v)
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// ReadJSON loads a JSON document.
func ReadJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// Finite reports whether the value is a normal finite number.
func Finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }

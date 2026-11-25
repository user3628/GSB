package model

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

func SHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func HashJSON(v any) (string, error) {
	data, err := CanonicalJSON(v)
	if err != nil {
		return "", err
	}
	return SHA256(data), nil
}

func HashParts(label string, parts ...string) string {
	h := sha256.New()
	h.Write([]byte(label))
	for _, part := range parts {
		h.Write([]byte{0})
		h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func NormalizeItems(items []Item) []Item {
	out := append([]Item(nil), items...)
	sort.Slice(out, func(i, j int) bool { return out[i].ItemID < out[j].ItemID })
	return out
}

func NormalizeQueries(queries []Query) []Query {
	out := append([]Query(nil), queries...)
	for i := range out {
		if out[i].ClickedCategories == nil {
			out[i].ClickedCategories = []string{}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].QueryID < out[j].QueryID })
	return out
}

func NormalizeJudgments(judgments []Judgment) []Judgment {
	out := append([]Judgment(nil), judgments...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].QueryID != out[j].QueryID {
			return out[i].QueryID < out[j].QueryID
		}
		if out[i].ReferenceRank != out[j].ReferenceRank {
			return out[i].ReferenceRank < out[j].ReferenceRank
		}
		return out[i].ItemID < out[j].ItemID
	})
	return out
}

func ItemsDigest(items []Item) (string, error) {
	return HashJSON(NormalizeItems(items))
}

func QueriesDigest(queries []Query) (string, error) {
	return HashJSON(NormalizeQueries(queries))
}

func JudgmentsDigest(judgments []Judgment) (string, error) {
	return HashJSON(NormalizeJudgments(judgments))
}

func ConfigContentFingerprint(c Config) (string, error) {
	normalized := c.Normalize()
	return HashJSON(struct {
		Version              string           `json:"version"`
		Seed                 int64            `json:"seed"`
		VectorDimension      int              `json:"vector_dim"`
		Routes               RoutesConfig     `json:"routes"`
		Fusion               FusionConfig     `json:"fusion"`
		Budget               BudgetConfig     `json:"budget"`
		Evaluation           EvaluationConfig `json:"evaluation"`
		MaxConcurrentQueries int              `json:"max_concurrent_queries"`
	}{
		Version:              normalized.Version,
		Seed:                 normalized.Seed,
		VectorDimension:      normalized.VectorDimension,
		Routes:               normalized.Routes,
		Fusion:               normalized.Fusion,
		Budget:               normalized.Budget,
		Evaluation:           normalized.Evaluation,
		MaxConcurrentQueries: normalized.Execution.MaxConcurrentQueries,
	})
}

func RecallConfigDigest(c Config) (string, error) {
	return HashJSON(struct {
		Routes RoutesConfig `json:"routes"`
		Budget BudgetConfig `json:"budget"`
		Vector int          `json:"vector_dim"`
		Seed   int64        `json:"seed"`
	}{
		Routes: c.Routes,
		Budget: c.Budget,
		Vector: c.VectorDimension,
		Seed:   c.Seed,
	})
}

func FusionConfigDigest(c Config) (string, error) {
	return HashJSON(c.Fusion)
}

func EvaluationConfigDigest(c Config) (string, error) {
	return HashJSON(struct {
		Evaluation EvaluationConfig `json:"evaluation"`
		FusionTopK int              `json:"fusion_top_k"`
	}{
		Evaluation: c.Evaluation,
		FusionTopK: c.Fusion.TopK,
	})
}

func DataFingerprint(itemsDigest, queriesDigest, judgmentsDigest string) string {
	return HashParts("data-v1", itemsDigest, queriesDigest, judgmentsDigest)
}

func RecallInputFingerprint(itemsDigest, queryDigest, recallConfigDigest string) string {
	return HashParts("recall-input-v1", itemsDigest, queryDigest, recallConfigDigest)
}

func FusionInputFingerprint(recallDigest, fusionConfigDigest string) string {
	return HashParts("fusion-input-v1", recallDigest, fusionConfigDigest)
}

func EvaluationInputFingerprint(fusionDigest, judgmentsDigest, evalConfigDigest string) string {
	return HashParts("evaluation-input-v1", fusionDigest, judgmentsDigest, evalConfigDigest)
}

func QueryDigest(q Query) (string, error) {
	return HashJSON(q)
}

func DigestWire(v any) (string, []byte, error) {
	data, err := CanonicalJSON(v)
	if err != nil {
		return "", nil, err
	}
	return SHA256(data), data, nil
}

func FirstDifferentQuery(got, want []QueryResult) string {
	n := len(got)
	if len(want) < n {
		n = len(want)
	}
	for i := 0; i < n; i++ {
		gotData, _ := CanonicalJSON(got[i])
		wantData, _ := CanonicalJSON(want[i])
		if string(gotData) != string(wantData) {
			if got[i].QueryID < want[i].QueryID {
				return got[i].QueryID
			}
			return want[i].QueryID
		}
	}
	if len(got) > n {
		return got[n].QueryID
	}
	if len(want) > n {
		return want[n].QueryID
	}
	return ""
}

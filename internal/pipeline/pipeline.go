package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"

	"local/configmiddleware/internal/core"
)

const (
	WeightX            = "weight_x"
	WeightY            = "weight_y"
	Bias               = "bias"
	NormalizationRatio = "normalization_ratio"
	DecisionThreshold  = "decision_threshold"
	Algorithm          = "weighted_sum_normalized_threshold.v1"
)

var RequiredKeys = []string{WeightX, WeightY, Bias, NormalizationRatio, DecisionThreshold}

type Vector struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type Document struct {
	Seed      int64    `json:"seed"`
	SampleID  string   `json:"sample_id"`
	Generator string   `json:"generator,omitempty"`
	Digest    string   `json:"digest"`
	Vectors   []Vector `json:"vectors"`
}

type Summary struct {
	Seed         int64   `json:"seed"`
	SampleID     string  `json:"sample_id"`
	SampleDigest string  `json:"sample_digest"`
	WeightedSum  float64 `json:"weighted_sum"`
	Normalized   float64 `json:"normalized"`
	Decision     bool    `json:"decision"`
	ResultDigest string  `json:"result_digest"`
}

func Load(path string) (*Document, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc Document
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, err
	}
	if len(doc.Vectors) == 0 || doc.SampleID == "" {
		return nil, fmt.Errorf("sample document is empty")
	}
	want := DocumentDigest(doc.Seed, doc.SampleID, doc.Vectors)
	if doc.Digest == "" || !strings.EqualFold(doc.Digest, want) {
		return nil, fmt.Errorf("sample digest mismatch: have %s want %s", doc.Digest, want)
	}
	return &doc, nil
}

func DocumentDigest(seed int64, sampleID string, vectors []Vector) string {
	var b strings.Builder
	fmt.Fprintf(&b, "samples.v1\n%d\n%s\n", seed, sampleID)
	for _, v := range vectors {
		b.WriteString(core.FloatString(v.X))
		b.WriteByte(',')
		b.WriteString(core.FloatString(v.Y))
		b.WriteByte('\n')
	}
	sum := sha256.Sum256([]byte(b.String()))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func ValidateConfig(config map[string]float64) error {
	missing := make([]string, 0)
	for _, key := range RequiredKeys {
		if _, ok := config[key]; !ok {
			missing = append(missing, key)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		return core.NewBizError("E_CONFIG_INVALID", "missing pipeline config keys", map[string]any{"missing": missing})
	}
	return nil
}

func Compute(clientID string, version int64, config map[string]float64, doc *Document) (Summary, error) {
	if err := ValidateConfig(config); err != nil {
		return Summary{}, err
	}
	weighted := 0.0
	for _, v := range doc.Vectors {
		weighted += v.X*config[WeightX] + v.Y*config[WeightY]
	}
	weighted += config[Bias] * float64(len(doc.Vectors))
	normalized := weighted * config[NormalizationRatio]
	decision := normalized >= config[DecisionThreshold]
	result := core.Result{
		ClientID:      clientID,
		PickedVersion: version,
		Seed:          doc.Seed,
		SampleID:      doc.SampleID,
		SampleDigest:  doc.Digest,
		WeightedSum:   weighted,
		Normalized:    normalized,
		Decision:      decision,
	}
	return Summary{
		Seed:         doc.Seed,
		SampleID:     doc.SampleID,
		SampleDigest: doc.Digest,
		WeightedSum:  weighted,
		Normalized:   normalized,
		Decision:     decision,
		ResultDigest: core.ResultDigest(result),
	}, nil
}

func GenerateVectors(seed int64, count int) []Vector {
	// The versioned generator is described in samples.json. Runtime clients always
	// consume the committed tensor; this helper only regenerates the same fixture.
	state := uint64(seed)
	vectors := make([]Vector, count)
	for i := range vectors {
		state = state*6364136223846793005 + 1442695040888963407
		x := normalizeUint(state>>11) * 2
		state = state*6364136223846793005 + 1442695040888963407
		y := normalizeUint(state>>11) * 2
		vectors[i] = Vector{X: round6(x - 1), Y: round6(y - 1)}
	}
	return vectors
}

func normalizeUint(v uint64) float64 { return float64(v) / float64(uint64(1)<<53) }

func round6(v float64) float64 {
	return math.Round(v*1e6) / 1e6
}

func (s Summary) AsMap() map[string]any {
	return map[string]any{
		"seed": s.Seed, "sample_id": s.SampleID, "sample_digest": s.SampleDigest,
		"weighted_sum": s.WeightedSum, "normalized": s.Normalized,
		"decision": s.Decision, "result_digest": s.ResultDigest,
	}
}

package grayrouter

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"
)

const FixedScale uint64 = 1_000_000

type KahanSum struct {
	Sum          float64 `json:"sum"`
	Compensation float64 `json:"compensation"`
}

func (k *KahanSum) Add(x float64) {
	y := x - k.Compensation
	t := k.Sum + y
	k.Compensation = (t - k.Sum) - y
	k.Sum = t
}

type NormalizedWeight struct {
	Target        string  `json:"target"`
	InputWeight   float64 `json:"input_weight"`
	ExactRatio    float64 `json:"exact_ratio"`
	RawFixed      uint64  `json:"raw_fixed"`
	Fixed         uint64  `json:"fixed"`
	RoundingError float64 `json:"rounding_error"`
}

type NormalizedWeights struct {
	Scale        uint64             `json:"scale"`
	RoundingMode string             `json:"rounding_mode"`
	Weights      []NormalizedWeight `json:"weights"`
	InputKahan   KahanSum           `json:"input_kahan"`
	ErrorKahan   KahanSum           `json:"error_kahan"`
	TotalFixed   uint64             `json:"total_fixed"`
	MaxAbsError  float64            `json:"max_abs_error"`
}

func NormalizeWeights(weights map[string]float64) (*NormalizedWeights, error) {
	if len(weights) == 0 {
		return nil, NewAPIError(CodeInvalidRequest, "at least one gray weight is required", nil)
	}
	targets := make([]string, 0, len(weights))
	var inputSum KahanSum
	for target, weight := range weights {
		if strings.TrimSpace(target) == "" {
			return nil, NewAPIError(CodeInvalidRequest, "gray target must not be empty", nil)
		}
		if math.IsNaN(weight) || math.IsInf(weight, 0) || weight < 0 {
			return nil, NewAPIError(CodeInvalidRequest, "gray weights must be finite and non-negative", map[string]any{"target": target, "weight": weight})
		}
		targets = append(targets, target)
		inputSum.Add(weight)
	}
	if inputSum.Sum <= 0 {
		return nil, NewAPIError(CodeInvalidRequest, "gray weights must have a positive total", map[string]any{"sum": inputSum.Sum})
	}
	sort.Strings(targets)
	result := &NormalizedWeights{Scale: FixedScale, RoundingMode: "floor-plus-largest-remainder", InputKahan: inputSum}
	type remainder struct {
		index int
		frac  float64
	}
	remainders := make([]remainder, 0, len(targets))
	var floorTotal uint64
	for i, target := range targets {
		weight := weights[target]
		exact := weight / inputSum.Sum
		scaled := exact * float64(FixedScale)
		rounded := uint64(math.Round(scaled))
		floored := uint64(math.Floor(scaled))
		item := NormalizedWeight{Target: target, InputWeight: weight, ExactRatio: exact, RawFixed: rounded, Fixed: floored}
		result.Weights = append(result.Weights, item)
		floorTotal += floored
		remainders = append(remainders, remainder{index: i, frac: scaled - math.Floor(scaled)})
	}
	sort.SliceStable(remainders, func(i, j int) bool {
		if remainders[i].frac != remainders[j].frac {
			return remainders[i].frac > remainders[j].frac
		}
		return targets[remainders[i].index] < targets[remainders[j].index]
	})
	deficit := int64(FixedScale) - int64(floorTotal)
	for i := 0; deficit > 0 && i < len(remainders); i++ {
		idx := remainders[i].index
		result.Weights[idx].Fixed++
		deficit--
	}
	for i := len(remainders) - 1; deficit < 0 && i >= 0; i-- {
		idx := remainders[i].index
		if result.Weights[idx].Fixed > 0 {
			result.Weights[idx].Fixed--
			deficit++
		}
	}
	for i := range result.Weights {
		item := &result.Weights[i]
		item.RoundingError = float64(item.Fixed)/float64(FixedScale) - item.ExactRatio
		result.ErrorKahan.Add(item.RoundingError)
		result.TotalFixed += item.Fixed
		result.MaxAbsError = math.Max(result.MaxAbsError, math.Abs(item.RoundingError))
	}
	return result, nil
}

type GrayDecisionRequest struct {
	RequestID       string             `json:"request_id"`
	RouteKey        string             `json:"route_key"`
	SnapshotVersion int64              `json:"snapshot_version"`
	Baseline        string             `json:"baseline"`
	Canary          string             `json:"canary"`
	CanaryRate      float64            `json:"canary_rate"`
	Weights         map[string]float64 `json:"weights,omitempty"`
	Salt            string             `json:"salt,omitempty"`
}

type GrayDecision struct {
	RequestID       string             `json:"request_id"`
	RouteKey        string             `json:"route_key"`
	SnapshotVersion int64              `json:"snapshot_version"`
	StableKey       string             `json:"stable_key"`
	Digest          string             `json:"digest"`
	Bucket          uint64             `json:"bucket"`
	Scale           uint64             `json:"scale"`
	Selected        string             `json:"selected"`
	Strategy        string             `json:"strategy"`
	Normalization   *NormalizedWeights `json:"normalization"`
	BoundModelKey   string             `json:"bound_model_key"`
}

func DecideGray(req GrayDecisionRequest) (*GrayDecision, error) {
	if strings.TrimSpace(req.RequestID) == "" {
		return nil, NewAPIError(CodeInvalidRequest, "request_id is required for stable gray decision", nil)
	}
	weights := req.Weights
	if len(weights) == 0 {
		if req.Baseline == "" || req.Canary == "" {
			return nil, NewAPIError(CodeInvalidRequest, "baseline and canary model keys are required", nil)
		}
		rate := req.CanaryRate
		if rate < 0 || rate > 1 || math.IsNaN(rate) || math.IsInf(rate, 0) {
			return nil, NewAPIError(CodeInvalidRequest, "canary_rate must be in [0,1]", map[string]any{"canary_rate": rate})
		}
		weights = map[string]float64{req.Baseline: 1 - rate, req.Canary: rate}
	}
	normalized, err := NormalizeWeights(weights)
	if err != nil {
		return nil, err
	}
	stableKey := fmt.Sprintf("grayrouter-gray-v1|snapshot=%d|route=%s|salt=%s|request=%s", req.SnapshotVersion, req.RouteKey, req.Salt, req.RequestID)
	sum := sha256Hex([]byte(stableKey))
	firstBytes := decodeHexFirst8(sum)
	bucket := binary.BigEndian.Uint64(firstBytes[:]) % FixedScale
	boundary := uint64(0)
	selected := ""
	for _, weight := range normalized.Weights {
		boundary += weight.Fixed
		if bucket < boundary && selected == "" {
			selected = weight.Target
		}
	}
	if selected == "" {
		selected = normalized.Weights[len(normalized.Weights)-1].Target
	}
	return &GrayDecision{
		RequestID: req.RequestID, RouteKey: req.RouteKey, SnapshotVersion: req.SnapshotVersion,
		StableKey: stableKey, Digest: sum, Bucket: bucket, Scale: FixedScale,
		Selected: selected, Strategy: "sha256-bigendian-mod-1e6", Normalization: normalized, BoundModelKey: selected,
	}, nil
}

func decodeHexFirst8(hexDigest string) [8]byte {
	var out [8]byte
	for i := 0; i < 8; i++ {
		var value byte
		for j := 0; j < 2; j++ {
			c := hexDigest[i*2+j]
			value <<= 4
			switch {
			case c >= '0' && c <= '9':
				value |= c - '0'
			case c >= 'a' && c <= 'f':
				value |= c - 'a' + 10
			}
		}
		out[i] = value
	}
	return out
}

func stableUint64(text string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(text))
	return h.Sum64()
}

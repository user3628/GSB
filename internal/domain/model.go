package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
)

var identifierRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

type ModelSpec struct {
	ModelID        string `json:"model_id"`
	VRAMPerSample  int64  `json:"vram_per_sample_bytes"`
	CostPerSample  int64  `json:"cost_per_sample_us"`
	MaxBatchSample int64  `json:"max_batch_samples"`
}

type ModelCatalog struct {
	Version string               `json:"version"`
	Models  []ModelSpec          `json:"models"`
	ByID    map[string]ModelSpec `json:"-"`
	Hash    string               `json:"catalog_hash"`
}

type rawModelFile struct {
	Version string            `json:"version"`
	Models  []json.RawMessage `json:"models"`
}

type rawModel struct {
	ModelID             string          `json:"model_id"`
	VRAMPerSample       json.RawMessage `json:"vram_per_sample"`
	VRAMPerSampleBytes  json.RawMessage `json:"vram_per_sample_bytes"`
	CostPerSample       json.RawMessage `json:"cost_per_sample"`
	CostPerSampleMicros json.RawMessage `json:"cost_per_sample_us"`
	MaxBatchSamples     json.RawMessage `json:"max_batch_samples"`
}

func ParseModelCatalog(data []byte) (*ModelCatalog, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil, specError("empty model catalog", nil)
	}
	var envelope rawModelFile
	if strings.HasPrefix(trimmed, "[") {
		var models []json.RawMessage
		if err := json.Unmarshal(data, &models); err != nil {
			return nil, specError("model catalog is not valid JSON", map[string]any{"reason": err.Error()})
		}
		envelope.Models = models
		envelope.Version = "inline"
	} else if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, specError("model catalog is not valid JSON", map[string]any{"reason": err.Error()})
	}
	if strings.TrimSpace(envelope.Version) == "" {
		envelope.Version = "catalog"
	}
	if len(envelope.Models) == 0 {
		return nil, specError("model catalog must contain at least one model", nil)
	}

	byID := make(map[string]ModelSpec, len(envelope.Models))
	models := make([]ModelSpec, 0, len(envelope.Models))
	for i, raw := range envelope.Models {
		var m rawModel
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, specError("model entry is not valid JSON", map[string]any{"index": i, "reason": err.Error()})
		}
		if !identifierRE.MatchString(m.ModelID) {
			return nil, specError("model_id is missing or invalid", map[string]any{"index": i, "model_id": m.ModelID})
		}
		vramRaw := firstRaw(m.VRAMPerSample, m.VRAMPerSampleBytes)
		costRaw := firstRaw(m.CostPerSample, m.CostPerSampleMicros)
		vram, err := ParseByteQuantityRaw(vramRaw)
		if err != nil {
			return nil, specError("invalid vram_per_sample", map[string]any{"index": i, "model_id": m.ModelID, "reason": err.Error()})
		}
		cost, err := ParseDurationUSRaw(costRaw)
		if err != nil {
			return nil, specError("invalid cost_per_sample", map[string]any{"index": i, "model_id": m.ModelID, "reason": err.Error()})
		}
		maxSamples, err := ParsePositiveIntRaw(m.MaxBatchSamples, "max_batch_samples")
		if err != nil {
			return nil, specError("invalid max_batch_samples", map[string]any{"index": i, "model_id": m.ModelID, "reason": err.Error()})
		}
		if vram <= 0 || cost <= 0 {
			return nil, specError("model quantities must be positive", map[string]any{"index": i, "model_id": m.ModelID})
		}
		if maxSamples > math.MaxInt32 {
			return nil, specError("max_batch_samples is too large", map[string]any{"model_id": m.ModelID, "value": maxSamples})
		}
		if vram > math.MaxInt64/maxSamples || cost > math.MaxInt64/maxSamples {
			return nil, specError("model quantity is too large for deterministic sample multiplication", map[string]any{"model_id": m.ModelID})
		}
		if _, exists := byID[m.ModelID]; exists {
			return nil, specError("duplicate model_id", map[string]any{"model_id": m.ModelID})
		}
		spec := ModelSpec{ModelID: m.ModelID, VRAMPerSample: vram, CostPerSample: cost, MaxBatchSample: maxSamples}
		byID[m.ModelID] = spec
		models = append(models, spec)
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ModelID < models[j].ModelID })
	canonical := struct {
		Version string      `json:"version"`
		Models  []ModelSpec `json:"models"`
	}{Version: envelope.Version, Models: models}
	b, _ := json.Marshal(canonical)
	sum := sha256.Sum256(b)
	return &ModelCatalog{Version: envelope.Version, Models: models, ByID: byID, Hash: hex.EncodeToString(sum[:])}, nil
}

func specError(message string, context map[string]any) *BizError {
	return NewBizError(CodeSpecInvalid, message, context)
}

func firstRaw(values ...json.RawMessage) json.RawMessage {
	for _, raw := range values {
		if len(raw) > 0 && string(raw) != "null" {
			return raw
		}
	}
	return nil
}

func validateIdentifier(kind, value string) error {
	if !identifierRE.MatchString(value) {
		return fmt.Errorf("%s must match %s", kind, identifierRE.String())
	}
	return nil
}

func MustCatalogFromModels(version string, models []ModelSpec) *ModelCatalog {
	byID := make(map[string]ModelSpec, len(models))
	for _, m := range models {
		byID[m.ModelID] = m
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ModelID < models[j].ModelID })
	canonical := struct {
		Version string      `json:"version"`
		Models  []ModelSpec `json:"models"`
	}{Version: version, Models: models}
	b, _ := json.Marshal(canonical)
	sum := sha256.Sum256(b)
	return &ModelCatalog{Version: version, Models: models, ByID: byID, Hash: hex.EncodeToString(sum[:])}
}

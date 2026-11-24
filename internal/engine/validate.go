package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"checkpointengine/internal/model"
)

func prepareInputShards(inputs []model.ShardInput, nowMS int64) ([]model.Shard, error) {
	if len(inputs) == 0 {
		return nil, model.NewAPIError(model.CodeBadRequest, "increment must contain at least one shard")
	}
	seen := make(map[string]bool, len(inputs))
	out := make([]model.Shard, 0, len(inputs))
	for i := range inputs {
		in := &inputs[i]
		in.ShardID = strings.TrimSpace(in.ShardID)
		in.ParameterID = strings.TrimSpace(in.ParameterID)
		in.Precision = model.NormalizePrecision(in.Precision)
		in.TargetPool = model.NormalizePool(in.TargetPool)
		in.Digest = strings.TrimSpace(in.Digest)
		in.MomentumDigest = strings.TrimSpace(in.MomentumDigest)
		in.RestoredFromDigest = strings.TrimSpace(in.RestoredFromDigest)
		in.Quantization = strings.TrimSpace(in.Quantization)
		in.PayloadHex = strings.TrimSpace(in.PayloadHex)
		if in.Round <= 0 {
			return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "round must be positive"), map[string]any{"index": i})
		}
		if in.ShardID == "" {
			return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "shard_id is required"), map[string]any{"index": i})
		}
		if len(in.ShardID) > 256 {
			return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "shard_id is too long"), map[string]any{"index": i})
		}
		key := shardKey(in.Round, in.ShardID)
		if seen[key] {
			return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "shard_id must be unique within a round in one submission"),
				map[string]any{"round": in.Round, "shard_id": in.ShardID})
		}
		seen[key] = true
		if in.ParameterID == "" {
			in.ParameterID = in.ShardID
		}
		if !model.ValidPrecision(in.Precision) {
			return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "precision must be FP16 or FP32"),
				map[string]any{"round": in.Round, "shard_id": in.ShardID, "precision": in.Precision})
		}
		if in.Bytes < 0 {
			return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "bytes must be a non-negative integer"),
				map[string]any{"round": in.Round, "shard_id": in.ShardID, "bytes": in.Bytes})
		}
		if in.TargetPool != "" && !model.ValidPool(in.TargetPool) {
			return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "target_pool must be VRAM or PERSISTED"),
				map[string]any{"round": in.Round, "shard_id": in.ShardID, "target_pool": in.TargetPool})
		}
		if in.LastAccessAt != "" {
			ms, err := model.ParseTimeMS(in.LastAccessAt)
			if err != nil {
				return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "last_access_at must be RFC3339"),
					map[string]any{"round": in.Round, "shard_id": in.ShardID})
			}
			in.LastAccessMS = &ms
		}
		if in.LastAccessMS != nil && *in.LastAccessMS < 0 {
			return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "last_access_ms must be non-negative"),
				map[string]any{"round": in.Round, "shard_id": in.ShardID})
		}
		if in.WriteMS < 0 {
			return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "write_ms must be non-negative"),
				map[string]any{"round": in.Round, "shard_id": in.ShardID})
		}
		if in.Precision == model.PrecisionFP16 {
			if in.Quantization == "" {
				in.Quantization = model.RoundingRNE
			}
			if in.Quantization != model.RoundingRNE {
				return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "unsupported FP16 quantization contract"),
					map[string]any{"round": in.Round, "shard_id": in.ShardID, "quantization": in.Quantization})
			}
			if len(in.FP32Nanos) > 0 {
				return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "FP16 shard cannot carry fp32_nanos"),
					map[string]any{"round": in.Round, "shard_id": in.ShardID})
			}
		} else {
			if len(in.FP16Bits) > 0 {
				return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "FP32 shard cannot carry fp16_bits"),
					map[string]any{"round": in.Round, "shard_id": in.ShardID})
			}
			if in.Quantization != "" {
				return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "quantization is only valid for FP16 shards"),
					map[string]any{"round": in.Round, "shard_id": in.ShardID})
			}
		}
		computed, hasPayload, err := contentDigest(*in)
		if err != nil {
			return nil, model.WithDetails(model.WrapAPIError(model.CodeShardInvalid, "invalid shard payload", err),
				map[string]any{"round": in.Round, "shard_id": in.ShardID})
		}
		if in.Digest == "" {
			if !hasPayload {
				return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "digest is required when no deterministic payload is supplied"),
					map[string]any{"round": in.Round, "shard_id": in.ShardID})
			}
			in.Digest = computed
		}
		if !validOpaqueDigest(in.Digest) {
			return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "digest must be non-empty printable text without path separators"),
				map[string]any{"round": in.Round, "shard_id": in.ShardID})
		}
		if in.MomentumDigest != "" && !validOpaqueDigest(in.MomentumDigest) {
			return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "momentum_digest is invalid"),
				map[string]any{"round": in.Round, "shard_id": in.ShardID})
		}
		if in.RestoredFromDigest != "" && !validOpaqueDigest(in.RestoredFromDigest) {
			return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "restored_from_digest is invalid"),
				map[string]any{"round": in.Round, "shard_id": in.ShardID})
		}
		if hasPayload && computed != in.Digest {
			return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "digest does not match the supplied deterministic payload"),
				map[string]any{"round": in.Round, "shard_id": in.ShardID, "expected_digest": computed, "submitted_digest": in.Digest})
		}
		access := nowMS
		if in.LastAccessMS != nil {
			access = *in.LastAccessMS
		}
		writeMS := in.WriteMS
		if writeMS == 0 {
			writeMS = 1 + in.Bytes/(1024*1024)
		}
		out = append(out, model.Shard{
			Round:              in.Round,
			ShardID:            in.ShardID,
			ParameterID:        in.ParameterID,
			Precision:          in.Precision,
			Bytes:              in.Bytes,
			Digest:             in.Digest,
			MomentumDigest:     in.MomentumDigest,
			RestoredFromDigest: in.RestoredFromDigest,
			DesiredPool:        in.TargetPool,
			WriteState:         model.WritePending,
			LastAccessMS:       access,
			Priority:           in.Priority,
			WriteMS:            writeMS,
			Pinned:             in.Pinned,
			Quantization:       in.Quantization,
			FP16Bits:           append([]uint16(nil), in.FP16Bits...),
			FP32Nanos:          append([]int64(nil), in.FP32Nanos...),
			PayloadHex:         in.PayloadHex,
		})
	}
	return out, nil
}

func contentDigest(in model.ShardInput) (string, bool, error) {
	forms := 0
	if in.PayloadHex != "" {
		forms++
	}
	if len(in.FP16Bits) > 0 {
		forms++
	}
	if len(in.FP32Nanos) > 0 {
		forms++
	}
	if forms > 1 {
		return "", false, fmt.Errorf("use exactly one of payload_hex, fp16_bits, or fp32_nanos")
	}
	switch {
	case in.PayloadHex != "":
		raw, err := hex.DecodeString(in.PayloadHex)
		if err != nil {
			return "", false, err
		}
		sum := sha256.Sum256(raw)
		return hex.EncodeToString(sum[:]), true, nil
	case len(in.FP16Bits) > 0:
		parts := make([]string, len(in.FP16Bits))
		for i, b := range in.FP16Bits {
			parts[i] = strconv.FormatUint(uint64(b), 10)
		}
		return model.NewDigest("FP16_BITS_V1", strings.Join(parts, ",")), true, nil
	case len(in.FP32Nanos) > 0:
		parts := make([]string, len(in.FP32Nanos))
		for i, n := range in.FP32Nanos {
			parts[i] = strconv.FormatInt(n, 10)
		}
		return model.NewDigest("FP32_NANOS_V1", strings.Join(parts, ",")), true, nil
	default:
		return "", false, nil
	}
}

func validOpaqueDigest(s string) bool {
	if s == "" || len(s) > 256 {
		return false
	}
	for _, r := range s {
		if r < 0x21 || r > 0x7e || r == '/' || r == '\\' {
			return false
		}
	}
	return true
}

func resolveRelationships(shards []model.Shard) ([]model.Shard, error) {
	type fp16Choice struct {
		digest string
		id     string
		shard  model.Shard
	}
	byParam := make(map[string][]model.Shard)
	for _, s := range shards {
		key := strconv.Itoa(s.Round) + "\x00" + s.ParameterID
		if s.Precision == model.PrecisionFP16 {
			byParam[key] = append(byParam[key], s)
		}
	}
	out := append([]model.Shard(nil), shards...)
	for i := range out {
		s := &out[i]
		if s.Precision != model.PrecisionFP32 {
			continue
		}
		key := strconv.Itoa(s.Round) + "\x00" + s.ParameterID
		choices := byParam[key]
		if s.RestoredFromDigest == "" {
			if len(choices) == 1 {
				s.RestoredFromDigest = choices[0].Digest
			} else if len(choices) == 0 {
				return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "FP32 master shard has no corresponding FP16 shard in its round"),
					map[string]any{"round": s.Round, "shard_id": s.ShardID, "parameter_id": s.ParameterID})
			} else {
				return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "FP32 master has multiple FP16 candidates; set restored_from_digest explicitly"),
					map[string]any{"round": s.Round, "shard_id": s.ShardID, "parameter_id": s.ParameterID})
			}
		}
		found := false
		for _, c := range choices {
			if c.Digest == s.RestoredFromDigest {
				found = true
				if len(s.FP32Nanos) > 0 && len(c.FP16Bits) > 0 && len(s.FP32Nanos) != len(c.FP16Bits) {
					return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "FP16 and FP32 element counts differ"),
						map[string]any{"round": s.Round, "shard_id": s.ShardID, "fp16_elements": len(c.FP16Bits), "fp32_elements": len(s.FP32Nanos)})
				}
				break
			}
		}
		if !found {
			return nil, model.WithDetails(model.NewAPIError(model.CodeShardInvalid, "FP32 master does not reference a matching FP16 shard digest"),
				map[string]any{"round": s.Round, "shard_id": s.ShardID, "parameter_id": s.ParameterID, "restored_from_digest": s.RestoredFromDigest})
		}
	}
	return out, nil
}

func requestHash(req model.IncrementRequest) (string, error) {
	type canonicalShard struct {
		Round              int      `json:"round"`
		ShardID            string   `json:"shard_id"`
		ParameterID        string   `json:"parameter_id"`
		Precision          string   `json:"precision"`
		Bytes              int64    `json:"bytes"`
		Digest             string   `json:"digest"`
		MomentumDigest     string   `json:"momentum_digest"`
		RestoredFromDigest string   `json:"restored_from_digest"`
		TargetPool         string   `json:"target_pool"`
		LastAccessMS       *int64   `json:"last_access_ms"`
		Priority           int      `json:"priority"`
		WriteMS            int64    `json:"write_ms"`
		Pinned             bool     `json:"pinned"`
		Quantization       string   `json:"quantization"`
		FP16Bits           []uint16 `json:"fp16_bits"`
		FP32Nanos          []int64  `json:"fp32_nanos"`
		PayloadHex         string   `json:"payload_hex"`
	}
	req.Normalize()
	shards := make([]canonicalShard, len(req.Shards))
	for i, s := range req.Shards {
		pool := model.NormalizePool(s.TargetPool)
		shards[i] = canonicalShard{
			Round: s.Round, ShardID: strings.TrimSpace(s.ShardID),
			ParameterID: strings.TrimSpace(s.ParameterID), Precision: model.NormalizePrecision(s.Precision),
			Bytes: s.Bytes, Digest: strings.TrimSpace(s.Digest), MomentumDigest: strings.TrimSpace(s.MomentumDigest),
			RestoredFromDigest: strings.TrimSpace(s.RestoredFromDigest), TargetPool: pool,
			LastAccessMS: s.LastAccessMS, Priority: s.Priority, WriteMS: s.WriteMS, Pinned: s.Pinned,
			Quantization: strings.TrimSpace(s.Quantization), FP16Bits: s.FP16Bits, FP32Nanos: s.FP32Nanos,
			PayloadHex: strings.TrimSpace(s.PayloadHex),
		}
	}
	sort.Slice(shards, func(i, j int) bool {
		if shards[i].Round != shards[j].Round {
			return shards[i].Round < shards[j].Round
		}
		return shards[i].ShardID < shards[j].ShardID
	})
	numeric := func(raw json.RawMessage) string {
		if len(strings.TrimSpace(string(raw))) == 0 {
			return ""
		}
		v, err := model.DecimalValue(raw)
		if err != nil {
			return strings.TrimSpace(string(raw))
		}
		return v
	}
	obj := struct {
		SubmissionID    string           `json:"submission_id"`
		BaseVersion     int64            `json:"base_version"`
		Round           int              `json:"round"`
		LossScale       string           `json:"loss_scale"`
		LossScaleGrowth string           `json:"loss_scale_growth"`
		Shards          []canonicalShard `json:"shards"`
	}{
		SubmissionID: strings.TrimSpace(req.SubmissionID), BaseVersion: req.BaseVersion,
		Round: req.Round, LossScale: numeric(req.LossScale), LossScaleGrowth: numeric(req.LossScaleGrowth), Shards: shards,
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

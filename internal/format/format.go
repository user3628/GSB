package format

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"

	"mpckpt/internal/model"
)

const (
	FormatBinary = "binary"
	FormatJSONL  = "jsonl"

	MaxVersion        = 2
	knownRequiredMask = uint16(0x0003)
	knownPresenceMask = uint16(0x003f)
	binaryHeaderSize  = 28
)

var binaryMagic = [4]byte{'M', 'P', 'C', 'K'}

type Manifest struct {
	Format             string
	Version            int
	RequiredMask       uint16
	LossScaleFixed     int64
	LossScalePrecision string
	Shards             []model.Shard
}

func Inspect(data []byte, formatHint string) (string, int, *model.BizError) {
	formatHint = model.NormalizeFormat(formatHint)
	if formatHint == "" {
		if len(data) >= 4 && bytes.Equal(data[:4], binaryMagic[:]) {
			formatHint = FormatBinary
		} else {
			formatHint = FormatJSONL
		}
	}
	switch formatHint {
	case FormatBinary:
		if len(data) < 6 {
			return "", 0, model.NewError(model.CodeRecordCorrupt, "binary header is truncated", map[string]any{
				"record_index": 0,
			})
		}
		if !bytes.Equal(data[:4], binaryMagic[:]) {
			return "", 0, model.NewError(model.CodeFormatUnsupported, "binary magic is not MPCK", map[string]any{
				"format": FormatBinary,
			})
		}
		return FormatBinary, int(data[4]), nil
	case FormatJSONL:
		meta, _, _, err := decodeJSONLHeader(data)
		if err != nil {
			return "", 0, err
		}
		return FormatJSONL, meta.Version, nil
	default:
		return "", 0, model.NewError(model.CodeFormatUnsupported, "unknown interchange format", map[string]any{
			"format":  formatHint,
			"allowed": []string{FormatBinary, FormatJSONL},
		})
	}
}

func Encode(manifest Manifest) ([]byte, *model.BizError) {
	manifest.Format = model.NormalizeFormat(manifest.Format)
	if err := validateManifestForEncode(manifest); err != nil {
		return nil, err
	}
	switch manifest.Format {
	case FormatBinary:
		return encodeBinary(manifest)
	case FormatJSONL:
		return encodeJSONL(manifest)
	default:
		return nil, model.NewError(model.CodeFormatUnsupported, "unknown interchange format", map[string]any{
			"format": manifest.Format,
		})
	}
}

func Decode(data []byte, formatHint string) (Manifest, *model.BizError) {
	formatHint = model.NormalizeFormat(formatHint)
	if formatHint == "" {
		if len(data) >= 4 && bytes.Equal(data[:4], binaryMagic[:]) {
			formatHint = FormatBinary
		} else {
			formatHint = FormatJSONL
		}
	}
	switch formatHint {
	case FormatBinary:
		return decodeBinary(data)
	case FormatJSONL:
		return decodeJSONL(data)
	default:
		return Manifest{}, model.NewError(model.CodeFormatUnsupported, "unknown interchange format", map[string]any{
			"format": formatHint,
		})
	}
}

func validateManifestForEncode(manifest Manifest) *model.BizError {
	if manifest.Version < 1 || manifest.Version > MaxVersion {
		return unsupportedVersion("", manifest.Version)
	}
	if manifest.RequiredMask&^knownRequiredMask != 0 {
		return model.NewError(model.CodeFormatUnsupported, "manifest requires unknown capabilities", map[string]any{
			"required_mask": manifest.RequiredMask,
			"known_mask":    knownRequiredMask,
		})
	}
	if manifest.LossScaleFixed <= 0 {
		return model.NewError(model.CodeSpecInvalid, "loss_scale_fixed must be positive", map[string]any{
			"field": "loss_scale_fixed",
		})
	}
	if manifest.LossScalePrecision == "" {
		manifest.LossScalePrecision = model.LossScalePrecisionFixed64
	}
	if manifest.LossScalePrecision != model.LossScalePrecisionFloat32 && manifest.LossScalePrecision != model.LossScalePrecisionFixed64 {
		return model.NewError(model.CodeSpecInvalid, "unknown loss scale precision", map[string]any{
			"field": "loss_scale_precision",
			"value": manifest.LossScalePrecision,
		})
	}
	seen := make(map[string]struct{}, len(manifest.Shards))
	for index, shard := range manifest.Shards {
		if strings.TrimSpace(shard.ID) == "" {
			return model.NewError(model.CodeSpecInvalid, "shard id must not be empty", map[string]any{
				"record_index": index + 1, "field": "id",
			})
		}
		if shard.Precision != model.PrecisionFP16 && shard.Precision != model.PrecisionFP32 {
			return model.NewError(model.CodeSpecInvalid, "precision is not in the allowed set", map[string]any{
				"record_index": index + 1, "shard_id": shard.ID, "field": "precision", "value": shard.Precision,
			})
		}
		if shard.SizeBytes <= 0 {
			return model.NewError(model.CodeSpecInvalid, "size_bytes must be positive", map[string]any{
				"record_index": index + 1, "shard_id": shard.ID, "field": "size_bytes",
			})
		}
		if err := model.ValidateDigest(shard.Digest, "digest", shard.ID); err != nil {
			err.Context["record_index"] = index + 1
			return err
		}
		if shard.Pool != "" && shard.Pool != model.PoolVRAM && shard.Pool != model.PoolPersistent {
			return model.NewError(model.CodeSpecInvalid, "pool is not in the allowed set", map[string]any{
				"record_index": index + 1, "shard_id": shard.ID, "field": "pool", "value": shard.Pool,
			})
		}
		if manifest.Version >= 2 && shard.Pool == "" {
			return model.NewError(model.CodeSpecInvalid, "version 2 manifest requires pool", map[string]any{
				"record_index": index + 1, "shard_id": shard.ID,
			})
		}
		if shard.Precision == model.PrecisionFP16 {
			if err := model.ValidateDigest(shard.SourceDigest, "source_digest", shard.ID); err != nil {
				err.Context["record_index"] = index + 1
				return err
			}
		}
		if shard.Precision == model.PrecisionFP32 && shard.Pool == model.PoolVRAM {
			return model.NewError(model.CodeSpecInvalid, "FP32 master shard must be persistent", map[string]any{
				"record_index": index + 1, "shard_id": shard.ID,
			})
		}
		if _, exists := seen[shard.ID]; exists {
			return model.NewError(model.CodeSpecInvalid, "duplicate shard id", map[string]any{
				"record_index": index + 1, "shard_id": shard.ID,
			})
		}
		seen[shard.ID] = struct{}{}
	}
	return nil
}

func unsupportedVersion(format string, version int) *model.BizError {
	ctx := map[string]any{"format_version": version, "max_supported_version": MaxVersion}
	if format != "" {
		ctx["format"] = format
	}
	return model.NewError(model.CodeFormatUnsupported, "format version is not supported", ctx)
}

func recordCorrupt(index int, message string, extra map[string]any) *model.BizError {
	ctx := map[string]any{"record_index": index, "reason": message}
	for key, value := range extra {
		ctx[key] = value
	}
	return model.NewError(model.CodeRecordCorrupt, message, ctx)
}

func precisionCode(precision model.Precision) (byte, bool) {
	switch precision {
	case model.PrecisionFP16:
		return 1, true
	case model.PrecisionFP32:
		return 2, true
	default:
		return 0, false
	}
}

func precisionFromCode(code byte) (model.Precision, bool) {
	switch code {
	case 1:
		return model.PrecisionFP16, true
	case 2:
		return model.PrecisionFP32, true
	default:
		return "", false
	}
}

func poolCode(pool model.Pool) (byte, bool) {
	switch pool {
	case model.PoolVRAM:
		return 1, true
	case model.PoolPersistent:
		return 2, true
	default:
		return 0, false
	}
}

func poolFromCode(code byte) (model.Pool, bool) {
	switch code {
	case 1:
		return model.PoolVRAM, true
	case 2:
		return model.PoolPersistent, true
	default:
		return "", false
	}
}

func lossPrecisionCode(value string) byte {
	switch value {
	case model.LossScalePrecisionFloat32:
		return 1
	case model.LossScalePrecisionFixed64:
		return 2
	default:
		return 0
	}
}

func lossPrecisionFromCode(code byte) (string, bool) {
	switch code {
	case 1:
		return model.LossScalePrecisionFloat32, true
	case 2:
		return model.LossScalePrecisionFixed64, true
	default:
		return "", false
	}
}

func normalizePrecisionString(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "fp16", "float16", "half":
		return string(model.PrecisionFP16)
	case "fp32", "float32", "single":
		return string(model.PrecisionFP32)
	default:
		return value
	}
}

func normalizePoolString(value string) model.Pool {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "VRAM", "GPU", "RESIDENT":
		return model.PoolVRAM
	case "PERSISTED", "PERSISTENT", "CPU", "DISK":
		return model.PoolPersistent
	default:
		return model.Pool(value)
	}
}

func sortedShards(shards []model.Shard) []model.Shard {
	out := model.CopyShards(shards)
	sort.SliceStable(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func digestBytes(value string) ([]byte, *model.BizError) {
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) != 32 {
		return nil, model.NewError(model.CodeSpecInvalid, "digest must be 32-byte hexadecimal", map[string]any{
			"value": value,
		})
	}
	return raw, nil
}

func crc32IEEE(value []byte) uint32 {
	return crc32.ChecksumIEEE(value)
}

func canonicalJSON(value any) ([]byte, *model.BizError) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, model.NewError(model.CodeInternal, fmt.Sprintf("marshal failed: %v", err), nil)
	}
	return raw, nil
}

func byteOrder(marker byte) (binary.ByteOrder, bool) {
	switch marker {
	case 1:
		return binary.LittleEndian, true
	case 2:
		return binary.BigEndian, true
	default:
		return nil, false
	}
}

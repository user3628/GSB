package format

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"mpckpt/internal/model"
)

type jsonlMeta struct {
	Kind               string `json:"kind"`
	Format             string `json:"format"`
	Version            int    `json:"version"`
	RequiredMask       uint16 `json:"required_mask"`
	RecordCount        int    `json:"record_count"`
	LossScaleFixed     int64  `json:"loss_scale_fixed"`
	LossScalePrecision string `json:"loss_scale_precision"`
	CRC32              string `json:"crc32"`
}

type jsonlMetaPayload struct {
	Format             string `json:"format"`
	Version            int    `json:"version"`
	RequiredMask       uint16 `json:"required_mask"`
	RecordCount        int    `json:"record_count"`
	LossScaleFixed     int64  `json:"loss_scale_fixed"`
	LossScalePrecision string `json:"loss_scale_precision"`
}

type jsonlRecordPayload struct {
	ID             string          `json:"id"`
	Round          int64           `json:"round,omitempty"`
	Precision      model.Precision `json:"precision"`
	SizeBytes      int64           `json:"size_bytes"`
	Digest         string          `json:"digest"`
	Pool           model.Pool      `json:"pool,omitempty"`
	LastAccess     int64           `json:"last_access,omitempty"`
	SourceDigest   string          `json:"source_digest,omitempty"`
	MomentumDigest string          `json:"momentum_digest,omitempty"`
	Priority       int64           `json:"priority,omitempty"`
}

// Every non-empty JSONL line written by this version is a self-describing shard
// record. The trailing metadata fields are repeated so consumers can stream one
// record per line without a sidecar.
type jsonlRecordLine struct {
	Kind               string             `json:"kind"`
	FormatVersion      int                `json:"format_version"`
	RequiredMask       uint16             `json:"required_mask"`
	RecordCount        int                `json:"record_count"`
	LossScaleFixed     int64              `json:"loss_scale_fixed"`
	LossScalePrecision string             `json:"loss_scale_precision"`
	Presence           uint16             `json:"presence"`
	Payload            jsonlRecordPayload `json:"payload"`
	CRC32              string             `json:"crc32"`
}

type jsonlEnvelope struct {
	Kind               string          `json:"kind"`
	FormatVersion      int             `json:"format_version,omitempty"`
	RequiredMask       uint16          `json:"required_mask,omitempty"`
	RecordCount        int             `json:"record_count,omitempty"`
	LossScaleFixed     int64           `json:"loss_scale_fixed,omitempty"`
	LossScalePrecision string          `json:"loss_scale_precision,omitempty"`
	Presence           *uint16         `json:"presence,omitempty"`
	Payload            json.RawMessage `json:"payload,omitempty"`
	CRC32              string          `json:"crc32"`
}

func encodeJSONL(manifest Manifest) ([]byte, *model.BizError) {
	if manifest.LossScalePrecision == "" {
		manifest.LossScalePrecision = model.LossScalePrecisionFixed64
	}
	var out bytes.Buffer
	for index, shard := range sortedShards(manifest.Shards) {
		payload := payloadFromShard(shard)
		presence := presenceFromShard(shard)
		rawPayload, err := canonicalJSON(payload)
		if err != nil {
			return nil, err
		}
		crcMaterial := append([]byte{byte(presence >> 8), byte(presence)}, rawPayload...)
		line := jsonlRecordLine{
			Kind:               "record",
			FormatVersion:      manifest.Version,
			RequiredMask:       manifest.RequiredMask,
			RecordCount:        len(manifest.Shards),
			LossScaleFixed:     manifest.LossScaleFixed,
			LossScalePrecision: manifest.LossScalePrecision,
			Presence:           presence,
			Payload:            payload,
			CRC32:              fmt.Sprintf("%08x", crc32IEEE(crcMaterial)),
		}
		rawLine, jerr := json.Marshal(line)
		if jerr != nil {
			return nil, model.NewError(model.CodeInternal, jerr.Error(), map[string]any{"record_index": index + 1})
		}
		out.Write(rawLine)
		out.WriteByte('\n')
	}
	return out.Bytes(), nil
}

func payloadFromShard(shard model.Shard) jsonlRecordPayload {
	return jsonlRecordPayload{
		ID:             shard.ID,
		Round:          shard.Round,
		Precision:      shard.Precision,
		SizeBytes:      shard.SizeBytes,
		Digest:         shard.Digest,
		Pool:           shard.Pool,
		LastAccess:     shard.LastAccess,
		SourceDigest:   shard.SourceDigest,
		MomentumDigest: shard.MomentumDigest,
		Priority:       shard.Priority,
	}
}

func presenceFromShard(shard model.Shard) uint16 {
	var presence uint16
	if shard.Pool != "" {
		presence |= presencePool
	}
	if shard.SourceDigest != "" {
		presence |= presenceSourceDigest
	}
	if shard.Round != 0 {
		presence |= presenceRound
	}
	if shard.LastAccess != 0 {
		presence |= presenceLastAccess
	}
	if shard.MomentumDigest != "" {
		presence |= presenceMomentum
	}
	if shard.Priority != 0 {
		presence |= presencePriority
	}
	return presence
}

func decodeJSONL(data []byte) (Manifest, *model.BizError) {
	meta, lines, inlineMeta, err := decodeJSONLHeader(data)
	if err != nil {
		return Manifest{}, err
	}
	if meta.RequiredMask&^knownRequiredMask != 0 {
		return Manifest{}, model.NewError(model.CodeFormatUnsupported, "manifest requires unknown capabilities", map[string]any{
			"format": FormatJSONL, "format_version": meta.Version, "required_mask": meta.RequiredMask,
			"known_mask": knownRequiredMask,
		})
	}
	if meta.Version < 1 || meta.Version > MaxVersion {
		return Manifest{}, unsupportedVersion(FormatJSONL, meta.Version)
	}
	lossPrecision := strings.ToLower(strings.TrimSpace(meta.LossScalePrecision))
	if lossPrecision != model.LossScalePrecisionFloat32 && lossPrecision != model.LossScalePrecisionFixed64 {
		return Manifest{}, recordCorrupt(0, "invalid loss scale precision", map[string]any{"value": meta.LossScalePrecision})
	}
	manifest := Manifest{
		Format:             FormatJSONL,
		Version:            meta.Version,
		RequiredMask:       meta.RequiredMask,
		LossScaleFixed:     meta.LossScaleFixed,
		LossScalePrecision: lossPrecision,
		Shards:             make([]model.Shard, 0, meta.RecordCount),
	}
	for index, line := range lines {
		recordIndex := index + 1
		var envelope jsonlEnvelope
		if jerr := json.Unmarshal(line, &envelope); jerr != nil {
			return Manifest{}, recordCorrupt(recordIndex, "record line is not valid JSON", map[string]any{"detail": jerr.Error()})
		}
		if envelope.Kind != "record" {
			return Manifest{}, recordCorrupt(recordIndex, "expected a record line", map[string]any{"kind": envelope.Kind})
		}
		if inlineMeta {
			if envelope.FormatVersion != meta.Version || envelope.RequiredMask != meta.RequiredMask || envelope.RecordCount != meta.RecordCount ||
				envelope.LossScaleFixed != meta.LossScaleFixed || envelope.LossScalePrecision != meta.LossScalePrecision {
				return Manifest{}, recordCorrupt(recordIndex, "inline record metadata differs from the first record", nil)
			}
		}
		if envelope.Presence == nil {
			return Manifest{}, recordCorrupt(recordIndex, "presence bitmap is missing", nil)
		}
		presence := *envelope.Presence
		if presence&^knownPresenceMask != 0 {
			return Manifest{}, recordCorrupt(recordIndex, "unknown presence bitmap bits", map[string]any{
				"presence": presence, "known_mask": knownPresenceMask,
			})
		}
		crcMaterial := append([]byte{byte(presence >> 8), byte(presence)}, envelope.Payload...)
		expectedCRC := parseCRC(envelope.CRC32)
		if expectedCRC < 0 {
			return Manifest{}, recordCorrupt(recordIndex, "record CRC32 is not eight hexadecimal digits", map[string]any{"crc32": envelope.CRC32})
		}
		if actual := crc32IEEE(crcMaterial); uint32(expectedCRC) != actual {
			return Manifest{}, recordCorrupt(recordIndex, "record CRC32 mismatch", map[string]any{
				"expected_crc32": fmt.Sprintf("%08x", expectedCRC), "actual_crc32": fmt.Sprintf("%08x", actual),
			})
		}
		var payload jsonlRecordPayload
		if jerr := json.Unmarshal(envelope.Payload, &payload); jerr != nil {
			return Manifest{}, recordCorrupt(recordIndex, "record payload is not valid", map[string]any{"detail": jerr.Error()})
		}
		shard, err := shardFromJSONPayload(payload, presence, recordIndex, meta.Version)
		if err != nil {
			return Manifest{}, err
		}
		manifest.Shards = append(manifest.Shards, shard)
	}
	if len(manifest.Shards) != meta.RecordCount {
		return Manifest{}, recordCorrupt(len(manifest.Shards)+1, "record count does not match metadata", map[string]any{
			"expected": meta.RecordCount, "actual": len(manifest.Shards),
		})
	}
	return manifest, nil
}

func splitJSONLines(data []byte) [][]byte {
	var lines [][]byte
	for _, raw := range bytes.Split(data, []byte{'\n'}) {
		if len(bytes.TrimSpace(raw)) > 0 {
			lines = append(lines, append([]byte(nil), raw...))
		}
	}
	return lines
}

func decodeJSONLHeader(data []byte) (jsonlMeta, [][]byte, bool, *model.BizError) {
	lines := splitJSONLines(data)
	if len(lines) == 0 {
		return jsonlMeta{}, nil, false, recordCorrupt(0, "JSONL input is empty", nil)
	}
	var first jsonlEnvelope
	if err := json.Unmarshal(lines[0], &first); err != nil {
		return jsonlMeta{}, nil, false, recordCorrupt(0, "first JSONL line is not valid JSON", map[string]any{"detail": err.Error()})
	}
	if first.Kind == "meta" {
		meta, rest, err := decodeJSONLMetaLines(lines)
		return meta, rest, false, err
	}
	if first.Kind != "record" {
		return jsonlMeta{}, nil, false, recordCorrupt(0, "first JSONL line must be a record or metadata object", map[string]any{"kind": first.Kind})
	}
	if first.RecordCount < 0 || first.RecordCount > 1_000_000 {
		return jsonlMeta{}, nil, false, recordCorrupt(0, "record count is invalid", map[string]any{"record_count": first.RecordCount})
	}
	meta := jsonlMeta{
		Kind: "record", Format: FormatJSONL, Version: first.FormatVersion,
		RequiredMask: first.RequiredMask, RecordCount: first.RecordCount, LossScaleFixed: first.LossScaleFixed,
		LossScalePrecision: first.LossScalePrecision,
	}
	return meta, lines, true, nil
}

func decodeJSONLMetaLines(lines [][]byte) (jsonlMeta, [][]byte, *model.BizError) {
	var envelope jsonlEnvelope
	if err := json.Unmarshal(lines[0], &envelope); err != nil {
		return jsonlMeta{}, nil, recordCorrupt(0, "metadata line is not valid JSON", map[string]any{"detail": err.Error()})
	}
	if envelope.Kind != "meta" {
		return jsonlMeta{}, nil, recordCorrupt(0, "metadata line kind must be meta", map[string]any{"kind": envelope.Kind})
	}
	var meta jsonlMeta
	if err := json.Unmarshal(lines[0], &meta); err != nil {
		return jsonlMeta{}, nil, recordCorrupt(0, "metadata line is not valid", map[string]any{"detail": err.Error()})
	}
	metaPayload := jsonlMetaPayload{
		Format:             meta.Format,
		Version:            meta.Version,
		RequiredMask:       meta.RequiredMask,
		RecordCount:        meta.RecordCount,
		LossScaleFixed:     meta.LossScaleFixed,
		LossScalePrecision: meta.LossScalePrecision,
	}
	rawMeta, jerr := canonicalJSON(metaPayload)
	if jerr != nil {
		return jsonlMeta{}, nil, jerr
	}
	expectedCRC := parseCRC(meta.CRC32)
	if expectedCRC < 0 {
		return jsonlMeta{}, nil, recordCorrupt(0, "metadata CRC32 is not eight hexadecimal digits", map[string]any{"crc32": meta.CRC32})
	}
	if actual := crc32IEEE(rawMeta); uint32(expectedCRC) != actual {
		return jsonlMeta{}, nil, recordCorrupt(0, "metadata CRC32 mismatch", map[string]any{
			"expected_crc32": fmt.Sprintf("%08x", expectedCRC), "actual_crc32": fmt.Sprintf("%08x", actual),
		})
	}
	if normalizeFormat(meta.Format) != FormatJSONL {
		return jsonlMeta{}, nil, recordCorrupt(0, "metadata format is not jsonl", map[string]any{"format": meta.Format})
	}
	if meta.RecordCount < 0 || meta.RecordCount > 1_000_000 {
		return jsonlMeta{}, nil, recordCorrupt(0, "metadata record count is invalid", map[string]any{"record_count": meta.RecordCount})
	}
	return meta, lines[1:], nil
}

func shardFromJSONPayload(payload jsonlRecordPayload, presence uint16, index, version int) (model.Shard, *model.BizError) {
	if (presence&presencePool != 0) != (payload.Pool != "") {
		return model.Shard{}, recordCorrupt(index, "presence bit for pool disagrees with payload", map[string]any{
			"presence": presence, "pool": payload.Pool,
		})
	}
	if (presence&presenceSourceDigest != 0) != (payload.SourceDigest != "") {
		return model.Shard{}, recordCorrupt(index, "presence bit for source_digest disagrees with payload", nil)
	}
	if (presence&presenceRound != 0) != (payload.Round != 0) {
		return model.Shard{}, recordCorrupt(index, "presence bit for round disagrees with payload", nil)
	}
	if (presence&presenceLastAccess != 0) != (payload.LastAccess != 0) {
		return model.Shard{}, recordCorrupt(index, "presence bit for last_access disagrees with payload", nil)
	}
	if (presence&presenceMomentum != 0) != (payload.MomentumDigest != "") {
		return model.Shard{}, recordCorrupt(index, "presence bit for momentum_digest disagrees with payload", nil)
	}
	if (presence&presencePriority != 0) != (payload.Priority != 0) {
		return model.Shard{}, recordCorrupt(index, "presence bit for priority disagrees with payload", nil)
	}
	shard := model.Shard{
		ID:             payload.ID,
		Round:          payload.Round,
		Precision:      model.Precision(normalizePrecisionString(string(payload.Precision))),
		SizeBytes:      payload.SizeBytes,
		Digest:         strings.ToLower(payload.Digest),
		Pool:           normalizePoolString(string(payload.Pool)),
		LastAccess:     payload.LastAccess,
		SourceDigest:   strings.ToLower(payload.SourceDigest),
		MomentumDigest: strings.ToLower(payload.MomentumDigest),
		Priority:       payload.Priority,
	}
	if shard.Round == 0 {
		shard.Round = 1
	}
	if version >= 2 && shard.Pool == "" {
		return model.Shard{}, recordCorrupt(index, "version 2 record is missing pool", nil)
	}
	return shard, nil
}

func parseCRC(value string) int64 {
	value = strings.TrimSpace(value)
	for len(value) < 8 {
		value = "0" + value
	}
	if len(value) != 8 {
		return -1
	}
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) != 4 {
		return -1
	}
	return int64(uint32(raw[0])<<24 | uint32(raw[1])<<16 | uint32(raw[2])<<8 | uint32(raw[3]))
}

func normalizeFormat(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

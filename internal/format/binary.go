package format

import (
	"encoding/binary"
	"encoding/hex"
	"math"

	"mpckpt/internal/model"
)

const (
	presencePool = uint16(1 << iota)
	presenceSourceDigest
	presenceRound
	presenceLastAccess
	presenceMomentum
	presencePriority
)

func encodeBinary(manifest Manifest) ([]byte, *model.BizError) {
	if manifest.LossScalePrecision == "" {
		manifest.LossScalePrecision = model.LossScalePrecisionFixed64
	}
	order := binary.LittleEndian
	var out []byte
	header := make([]byte, binaryHeaderSize)
	copy(header[:4], binaryMagic[:])
	header[4] = byte(manifest.Version)
	header[5] = 1 // little endian marker
	order.PutUint16(header[6:8], manifest.RequiredMask)
	order.PutUint32(header[8:12], uint32(len(manifest.Shards)))
	order.PutUint64(header[12:20], uint64(manifest.LossScaleFixed))
	header[20] = lossPrecisionCode(manifest.LossScalePrecision)
	order.PutUint32(header[24:28], crc32IEEE(header[:24]))
	out = append(out, header...)

	for index, shard := range sortedShards(manifest.Shards) {
		record, err := encodeBinaryRecord(order, shard)
		if err != nil {
			err.Context["record_index"] = index + 1
			return nil, err
		}
		out = append(out, record...)
	}
	return out, nil
}

func encodeBinaryRecord(order binary.ByteOrder, shard model.Shard) ([]byte, *model.BizError) {
	digest, derr := digestBytes(shard.Digest)
	if derr != nil {
		return nil, derr
	}
	precision, ok := precisionCode(shard.Precision)
	if !ok {
		return nil, model.NewError(model.CodeSpecInvalid, "invalid precision", map[string]any{"shard_id": shard.ID})
	}
	presence := uint16(0)
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

	var payload []byte
	if len(shard.ID) > math.MaxUint16 {
		return nil, model.NewError(model.CodeSpecInvalid, "shard id is too long", map[string]any{"shard_id": shard.ID})
	}
	idLen := make([]byte, 2)
	order.PutUint16(idLen, uint16(len(shard.ID)))
	payload = append(payload, idLen...)
	payload = append(payload, shard.ID...)
	payload = append(payload, precision)
	size := make([]byte, 8)
	order.PutUint64(size, uint64(shard.SizeBytes))
	payload = append(payload, size...)
	payload = append(payload, digest...)

	if presence&presencePool != 0 {
		code, ok := poolCode(shard.Pool)
		if !ok {
			return nil, model.NewError(model.CodeSpecInvalid, "invalid pool", map[string]any{"shard_id": shard.ID})
		}
		payload = append(payload, code)
	}
	if presence&presenceSourceDigest != 0 {
		source, err := digestBytes(shard.SourceDigest)
		if err != nil {
			return nil, err
		}
		payload = append(payload, source...)
	}
	if presence&presenceRound != 0 {
		field := make([]byte, 8)
		order.PutUint64(field, uint64(shard.Round))
		payload = append(payload, field...)
	}
	if presence&presenceLastAccess != 0 {
		field := make([]byte, 8)
		order.PutUint64(field, uint64(shard.LastAccess))
		payload = append(payload, field...)
	}
	if presence&presenceMomentum != 0 {
		momentum, err := digestBytes(shard.MomentumDigest)
		if err != nil {
			return nil, err
		}
		payload = append(payload, momentum...)
	}
	if presence&presencePriority != 0 {
		if shard.Priority < math.MinInt32 || shard.Priority > math.MaxInt32 {
			return nil, model.NewError(model.CodeSpecInvalid, "priority exceeds int32", map[string]any{"shard_id": shard.ID})
		}
		field := make([]byte, 4)
		order.PutUint32(field, uint32(int32(shard.Priority)))
		payload = append(payload, field...)
	}

	var record []byte
	presenceBytes := make([]byte, 2)
	order.PutUint16(presenceBytes, presence)
	record = append(record, presenceBytes...)
	record = append(record, payload...)
	checksum := make([]byte, 4)
	order.PutUint32(checksum, crc32IEEE(record))
	record = append(record, checksum...)
	length := make([]byte, 4)
	order.PutUint32(length, uint32(len(record)))
	return append(length, record...), nil
}

func decodeBinary(data []byte) (Manifest, *model.BizError) {
	if len(data) < binaryHeaderSize {
		return Manifest{}, recordCorrupt(0, "binary header is truncated", map[string]any{"bytes": len(data)})
	}
	if string(data[:4]) != string(binaryMagic[:]) {
		return Manifest{}, model.NewError(model.CodeFormatUnsupported, "binary magic is not MPCK", nil)
	}
	version := int(data[4])
	order, ok := byteOrder(data[5])
	if !ok {
		return Manifest{}, recordCorrupt(0, "invalid endianness marker", map[string]any{"marker": data[5]})
	}
	expectedHeaderCRC := order.Uint32(data[24:28])
	if actual := crc32IEEE(data[:24]); actual != expectedHeaderCRC {
		return Manifest{}, recordCorrupt(0, "header CRC32 mismatch", map[string]any{
			"expected_crc32": expectedHeaderCRC, "actual_crc32": actual,
		})
	}
	requiredMask := order.Uint16(data[6:8])
	if requiredMask&^knownRequiredMask != 0 {
		return Manifest{}, model.NewError(model.CodeFormatUnsupported, "manifest requires unknown capabilities", map[string]any{
			"format": "binary", "format_version": version, "required_mask": requiredMask,
			"known_mask": knownRequiredMask,
		})
	}
	if version < 1 || version > MaxVersion {
		return Manifest{}, unsupportedVersion(FormatBinary, version)
	}
	lossPrecision, ok := lossPrecisionFromCode(data[20])
	if !ok {
		return Manifest{}, recordCorrupt(0, "invalid loss scale precision code", map[string]any{"code": data[20]})
	}
	recordCount := int(order.Uint32(data[8:12]))
	manifest := Manifest{
		Format:             FormatBinary,
		Version:            version,
		RequiredMask:       requiredMask,
		LossScaleFixed:     int64(order.Uint64(data[12:20])),
		LossScalePrecision: lossPrecision,
		Shards:             make([]model.Shard, 0, recordCount),
	}
	offset := binaryHeaderSize
	for index := 1; index <= recordCount; index++ {
		if offset+4 > len(data) {
			return Manifest{}, recordCorrupt(index, "record length prefix is truncated", map[string]any{"offset": offset})
		}
		recordLen := int(order.Uint32(data[offset : offset+4]))
		offset += 4
		if recordLen < 6 || offset+recordLen > len(data) {
			return Manifest{}, recordCorrupt(index, "record length is outside the input", map[string]any{
				"record_length": recordLen, "remaining": len(data) - offset,
			})
		}
		record := data[offset : offset+recordLen]
		offset += recordLen
		if recordLen < 6 {
			return Manifest{}, recordCorrupt(index, "record is too short", nil)
		}
		expectedCRC := order.Uint32(record[len(record)-4:])
		if actual := crc32IEEE(record[:len(record)-4]); actual != expectedCRC {
			return Manifest{}, recordCorrupt(index, "record CRC32 mismatch", map[string]any{
				"expected_crc32": expectedCRC, "actual_crc32": actual,
			})
		}
		shard, err := decodeBinaryRecord(order, record[:len(record)-4], index, version)
		if err != nil {
			return Manifest{}, err
		}
		manifest.Shards = append(manifest.Shards, shard)
	}
	if offset != len(data) {
		return Manifest{}, recordCorrupt(recordCount+1, "trailing bytes after declared records", map[string]any{
			"trailing_bytes": len(data) - offset,
		})
	}
	return manifest, nil
}

func decodeBinaryRecord(order binary.ByteOrder, record []byte, index, version int) (model.Shard, *model.BizError) {
	if len(record) < 2 {
		return model.Shard{}, recordCorrupt(index, "presence bitmap is truncated", nil)
	}
	presence := order.Uint16(record[:2])
	if presence&^knownPresenceMask != 0 {
		return model.Shard{}, recordCorrupt(index, "unknown presence bitmap bits", map[string]any{
			"presence": presence, "known_mask": knownPresenceMask,
		})
	}
	payload := record[2:]
	cursor := 0
	need := func(n int, field string) *model.BizError {
		if cursor+n > len(payload) {
			return recordCorrupt(index, "record payload is truncated", map[string]any{"field": field})
		}
		return nil
	}
	if err := need(2, "id_length"); err != nil {
		return model.Shard{}, err
	}
	idLen := int(order.Uint16(payload[cursor : cursor+2]))
	cursor += 2
	if idLen <= 0 {
		return model.Shard{}, recordCorrupt(index, "shard id is empty", nil)
	}
	if err := need(idLen, "id"); err != nil {
		return model.Shard{}, err
	}
	shard := model.Shard{ID: string(payload[cursor : cursor+idLen]), Round: 1}
	cursor += idLen
	if err := need(1, "precision"); err != nil {
		return model.Shard{}, err
	}
	precision, ok := precisionFromCode(payload[cursor])
	if !ok {
		return model.Shard{}, recordCorrupt(index, "invalid precision code", map[string]any{"code": payload[cursor]})
	}
	shard.Precision = precision
	cursor++
	if err := need(8, "size_bytes"); err != nil {
		return model.Shard{}, err
	}
	shard.SizeBytes = int64(order.Uint64(payload[cursor : cursor+8]))
	cursor += 8
	if err := need(32, "digest"); err != nil {
		return model.Shard{}, err
	}
	shard.Digest = hex.EncodeToString(payload[cursor : cursor+32])
	cursor += 32
	if presence&presencePool != 0 {
		if err := need(1, "pool"); err != nil {
			return model.Shard{}, err
		}
		pool, ok := poolFromCode(payload[cursor])
		if !ok {
			return model.Shard{}, recordCorrupt(index, "invalid pool code", map[string]any{"code": payload[cursor]})
		}
		shard.Pool = pool
		cursor++
	}
	if presence&presenceSourceDigest != 0 {
		if err := need(32, "source_digest"); err != nil {
			return model.Shard{}, err
		}
		shard.SourceDigest = hex.EncodeToString(payload[cursor : cursor+32])
		cursor += 32
	}
	if presence&presenceRound != 0 {
		if err := need(8, "round"); err != nil {
			return model.Shard{}, err
		}
		shard.Round = int64(order.Uint64(payload[cursor : cursor+8]))
		cursor += 8
	}
	if presence&presenceLastAccess != 0 {
		if err := need(8, "last_access"); err != nil {
			return model.Shard{}, err
		}
		shard.LastAccess = int64(order.Uint64(payload[cursor : cursor+8]))
		cursor += 8
	}
	if presence&presenceMomentum != 0 {
		if err := need(32, "momentum_digest"); err != nil {
			return model.Shard{}, err
		}
		shard.MomentumDigest = hex.EncodeToString(payload[cursor : cursor+32])
		cursor += 32
	}
	if presence&presencePriority != 0 {
		if err := need(4, "priority"); err != nil {
			return model.Shard{}, err
		}
		shard.Priority = int64(int32(order.Uint32(payload[cursor : cursor+4])))
		cursor += 4
	}
	if cursor != len(payload) {
		return model.Shard{}, recordCorrupt(index, "record payload has trailing bytes", map[string]any{
			"trailing_bytes": len(payload) - cursor,
		})
	}
	if version == 1 && shard.Pool != "" {
		// Pool is optional in v1, but is still valid when a legacy writer emitted it.
	}
	return shard, nil
}

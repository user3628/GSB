package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strings"
	"unicode/utf8"

	"replicationlag/internal/apperr"
	"replicationlag/internal/compat"
	"replicationlag/internal/domain"
)

const (
	binaryHeaderSize   = 9
	binaryHasPosition  = 1 << 0
	binaryHasVersion   = 1 << 1
	binaryHasLastWrite = 1 << 2
	binaryKnownBits    = binaryHasPosition | binaryHasVersion | binaryHasLastWrite
	maxBinaryRecord    = 64 << 20
)

type binaryCursor struct {
	data []byte
	pos  int
	base int
}

func (c *binaryCursor) take(n int) ([]byte, int, error) {
	if n < 0 || c.pos > len(c.data)-n {
		return nil, c.base + c.pos, fmt.Errorf("truncated record: need %d bytes at offset %d", n, c.base+c.pos)
	}
	start := c.pos
	c.pos += n
	return c.data[start:c.pos], c.base + start, nil
}

func (c *binaryCursor) u8() (uint8, int, error) {
	raw, offset, err := c.take(1)
	if err != nil {
		return 0, offset, err
	}
	return raw[0], offset, nil
}

func (c *binaryCursor) u16() (uint16, int, error) {
	raw, offset, err := c.take(2)
	if err != nil {
		return 0, offset, err
	}
	return binary.BigEndian.Uint16(raw), offset, nil
}

func (c *binaryCursor) u32() (uint32, int, error) {
	raw, offset, err := c.take(4)
	if err != nil {
		return 0, offset, err
	}
	return binary.BigEndian.Uint32(raw), offset, nil
}

func (c *binaryCursor) i64() (int64, int, error) {
	raw, offset, err := c.take(8)
	if err != nil {
		return 0, offset, err
	}
	return int64(binary.BigEndian.Uint64(raw)), offset, nil
}

func (c *binaryCursor) string32() (string, int, error) {
	length, offset, err := c.u32()
	if err != nil {
		return "", offset, err
	}
	if length > maxBinaryRecord {
		return "", offset, fmt.Errorf("string length %d exceeds limit", length)
	}
	raw, stringOffset, err := c.take(int(length))
	if err != nil {
		return "", stringOffset, err
	}
	if !utf8.Valid(raw) {
		return "", stringOffset, fmt.Errorf("string is not valid UTF-8")
	}
	return string(raw), stringOffset, nil
}

func importBinary(data []byte, detectedVersion int, matrix compat.Matrix, fallbackWriteMS int64) (ImportResult, error) {
	if len(data) < binaryHeaderSize {
		return ImportResult{}, apperr.New(apperr.CodeFormatCorrupt, "binary header is truncated", map[string]any{"offset": len(data)})
	}
	if string(data[:4]) != BinaryMagic {
		return ImportResult{}, apperr.New(apperr.CodeFormatCorrupt, "binary magic mismatch", map[string]any{"offset": 0, "expected": BinaryMagic})
	}
	version := int(data[4])
	if detectedVersion != version {
		return ImportResult{}, apperr.New(apperr.CodeFormatCorrupt, "binary version changed during detection", map[string]any{"offset": 4})
	}
	rule, decisionErr := matrix.Decide(BinaryFormat, version, nil)
	if decisionErr != nil {
		return ImportResult{}, decisionErr
	}
	count := binary.BigEndian.Uint32(data[5:9])
	result := ImportResult{
		Format:        BinaryFormat,
		FormatVersion: version,
		Decision:      rule.Decision,
		Observations:  make([]domain.Observation, 0, count),
		Degradations:  []Degradation{},
		SourceBytes:   len(data),
	}
	if rule.Decision == compat.DecisionDegrade {
		result.Degradations = append(result.Degradations, Degradation{
			Record: 0, Field: "format_version", Detail: fmt.Sprintf("binary/%d accepted with explicit degradation", version),
		})
	}

	offset := binaryHeaderSize
	for recordIndex := 0; recordIndex < int(count); recordIndex++ {
		frameOffset := offset
		if len(data)-offset < 4 {
			return ImportResult{}, apperr.New(apperr.CodeFormatCorrupt, "record length prefix is truncated", map[string]any{
				"offset": offset, "record": recordIndex + 1,
			})
		}
		payloadLength := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		if payloadLength < 0 || payloadLength > maxBinaryRecord {
			return ImportResult{}, apperr.New(apperr.CodeFormatCorrupt, "record length is invalid", map[string]any{
				"offset": frameOffset, "record": recordIndex + 1, "length": payloadLength,
			})
		}
		if len(data)-offset < payloadLength+4 {
			return ImportResult{}, apperr.New(apperr.CodeFormatCorrupt, "record payload or CRC is truncated", map[string]any{
				"offset": offset, "record": recordIndex + 1, "length": payloadLength,
			})
		}
		payload := data[offset : offset+payloadLength]
		crcOffset := offset + payloadLength
		storedCRC := binary.BigEndian.Uint32(data[crcOffset : crcOffset+4])
		actualCRC := crc32.ChecksumIEEE(payload)
		if storedCRC != actualCRC {
			return ImportResult{}, apperr.New(apperr.CodeFormatCorrupt, "record CRC32 mismatch", map[string]any{
				"offset": crcOffset, "record": recordIndex + 1, "stored_crc32": storedCRC, "actual_crc32": actualCRC,
			})
		}
		observation, details, decodeErr := decodeBinaryRecord(payload, frameOffset+4, version, rule, fallbackWriteMS, recordIndex+1)
		if decodeErr != nil {
			return ImportResult{}, decodeErr
		}
		for _, detail := range details {
			result.Degradations = append(result.Degradations, Degradation{Record: recordIndex + 1, Field: detail.Field, Detail: detail.Detail})
		}
		result.Observations = append(result.Observations, observation)
		offset = crcOffset + 4
	}
	if offset != len(data) {
		return ImportResult{}, apperr.New(apperr.CodeFormatCorrupt, "binary payload has trailing bytes beyond declared record count", map[string]any{
			"offset": offset, "trailing_bytes": len(data) - offset,
		})
	}
	return result, nil
}

func decodeBinaryRecord(payload []byte, base int, formatVersion int, rule compat.Rule, fallbackWriteMS int64, recordNumber int) (domain.Observation, []fieldDetail, error) {
	cursor := binaryCursor{data: payload, base: base}
	bitmap, bitmapOffset, err := cursor.u8()
	if err != nil {
		return domain.Observation{}, nil, binaryCorrupt(recordNumber, bitmapOffset, err.Error())
	}
	if bitmap&^binaryKnownBits != 0 {
		return domain.Observation{}, nil, apperr.New(apperr.CodeFormatUnsupported, "binary record has unknown mandatory bitmap fields", map[string]any{
			"record": recordNumber, "offset": bitmapOffset, "bitmap": bitmap, "unknown_mask": bitmap &^ binaryKnownBits,
		})
	}
	replicaLength, replicaOffset, err := cursor.u16()
	if err != nil {
		return domain.Observation{}, nil, binaryCorrupt(recordNumber, replicaOffset, err.Error())
	}
	replicaRaw, replicaOffset, err := cursor.take(int(replicaLength))
	if err != nil {
		return domain.Observation{}, nil, binaryCorrupt(recordNumber, replicaOffset, err.Error())
	}
	if !utf8.Valid(replicaRaw) {
		return domain.Observation{}, nil, binaryCorrupt(recordNumber, replicaOffset, "replica_id is not valid UTF-8")
	}
	replicaID := string(replicaRaw)
	timestamp, timestampOffset, err := cursor.i64()
	if err != nil {
		return domain.Observation{}, nil, binaryCorrupt(recordNumber, timestampOffset, err.Error())
	}
	sampledAt, lossy, err := compat.TimestampToMS(timestamp, rule.TimePrecision)
	if err != nil {
		return domain.Observation{}, nil, observationInvalid(replicaID, sampledAt, "sampled_at_ms", err)
	}
	lagMS, lagOffset, err := cursor.i64()
	if err != nil {
		return domain.Observation{}, nil, binaryCorrupt(recordNumber, lagOffset, err.Error())
	}
	segmentCount, segmentOffset, err := cursor.u16()
	if err != nil {
		return domain.Observation{}, nil, binaryCorrupt(recordNumber, segmentOffset, err.Error())
	}
	segments := make([]string, 0, segmentCount)
	for i := 0; i < int(segmentCount); i++ {
		segment, segmentStringOffset, segmentErr := cursor.string32()
		if segmentErr != nil {
			return domain.Observation{}, nil, binaryCorrupt(recordNumber, segmentStringOffset, segmentErr.Error())
		}
		segments = append(segments, segment)
	}
	if len(segments) == 0 {
		return domain.Observation{}, nil, observationInvalid(replicaID, sampledAt, "gtid_segments", fmt.Errorf("segment list is empty"))
	}
	gtid, gtidDetails, gtidErr := normalizeGTIDText(strings.Join(segments, ","), rule)
	if gtidErr != nil {
		return domain.Observation{}, nil, observationInvalid(replicaID, sampledAt, "gtid_segments", gtidErr)
	}

	position := ""
	if bitmap&binaryHasPosition != 0 {
		position, _, err = cursor.string32()
		if err != nil {
			return domain.Observation{}, nil, binaryCorrupt(recordNumber, cursor.base+cursor.pos, err.Error())
		}
	}
	normalizedPosition, positionDetails, positionErr := normalizePositionText(position, rule)
	if positionErr != nil {
		return domain.Observation{}, nil, observationInvalid(replicaID, sampledAt, "position_name", positionErr)
	}

	reportedRaw := ""
	versionPresent := bitmap&binaryHasVersion != 0
	if versionPresent {
		reportedRaw, _, err = cursor.string32()
		if err != nil {
			return domain.Observation{}, nil, binaryCorrupt(recordNumber, cursor.base+cursor.pos, err.Error())
		}
	}
	reportedVersion, versionDetails, versionErr := parseReportedVersion(reportedRaw, versionPresent, rule)
	if versionErr != nil {
		return domain.Observation{}, nil, observationInvalid(replicaID, sampledAt, "reported_version", versionErr)
	}

	lastWriteMS := fallbackWriteMS
	if bitmap&binaryHasLastWrite != 0 {
		lastWriteMS, _, err = cursor.i64()
		if err != nil {
			return domain.Observation{}, nil, binaryCorrupt(recordNumber, cursor.base+cursor.pos, err.Error())
		}
	}
	if cursor.pos != len(cursor.data) {
		return domain.Observation{}, nil, binaryCorrupt(recordNumber, cursor.base+cursor.pos, fmt.Sprintf("record has %d unconsumed bytes", len(cursor.data)-cursor.pos))
	}
	observation := domain.Observation{
		ReplicaID:       replicaID,
		SampledAtMS:     sampledAt,
		GTID:            gtid,
		PositionName:    normalizedPosition,
		LagMS:           lagMS,
		ReportedVersion: reportedVersion,
		FormatVersion:   formatVersion,
		LastWriteMS:     lastWriteMS,
	}
	if err := observation.Validate(); err != nil {
		return domain.Observation{}, nil, err
	}
	details := []fieldDetail{{Field: "sampled_at_ms", Detail: conversionDetail(rule.TimePrecision)}}
	if lossy {
		details = append(details, fieldDetail{Field: "sampled_at_ms", Detail: "sub-millisecond precision truncated during normalization"})
	}
	for _, detail := range append(append(gtidDetails, positionDetails...), versionDetails...) {
		details = append(details, fieldDetail{Field: "compatibility", Detail: detail})
	}
	return observation, details, nil
}

func binaryCorrupt(record, offset int, detail string) *apperr.Error {
	return apperr.New(apperr.CodeFormatCorrupt, "binary record is corrupt", map[string]any{
		"record": record, "offset": offset, "detail": detail,
	})
}

// BinaryEncodeOptions enables deterministic legacy fixtures in addition to the
// current v1 export.
type BinaryEncodeOptions struct {
	Version             int  `json:"version"`
	TimestampInSeconds  bool `json:"timestamp_in_seconds"`
	OmitReportedVersion bool `json:"omit_reported_version"`
}

func EncodeBinary(observations []domain.Observation) ([]byte, error) {
	return EncodeBinaryWithOptions(observations, BinaryEncodeOptions{Version: CurrentBinaryVersion})
}

func EncodeBinaryWithOptions(observations []domain.Observation, options BinaryEncodeOptions) ([]byte, error) {
	if options.Version < 0 || options.Version > 255 {
		return nil, fmt.Errorf("binary format version must fit in one byte")
	}
	ordered := domain.SortObservations(observations)
	var buffer bytes.Buffer
	buffer.WriteString(BinaryMagic)
	buffer.WriteByte(byte(options.Version))
	_ = binary.Write(&buffer, binary.BigEndian, uint32(len(ordered)))
	for _, observation := range ordered {
		payload, err := encodeBinaryPayload(observation, options)
		if err != nil {
			return nil, err
		}
		_ = binary.Write(&buffer, binary.BigEndian, uint32(len(payload)))
		buffer.Write(payload)
		_ = binary.Write(&buffer, binary.BigEndian, crc32.ChecksumIEEE(payload))
	}
	return buffer.Bytes(), nil
}

func encodeBinaryPayload(observation domain.Observation, options BinaryEncodeOptions) ([]byte, error) {
	var payload bytes.Buffer
	bitmap := byte(binaryHasPosition | binaryHasLastWrite)
	if !options.OmitReportedVersion {
		bitmap |= binaryHasVersion
	}
	payload.WriteByte(bitmap)
	replica := []byte(observation.ReplicaID)
	if len(replica) > 65535 {
		return nil, fmt.Errorf("replica_id is too long")
	}
	_ = binary.Write(&payload, binary.BigEndian, uint16(len(replica)))
	payload.Write(replica)
	timestamp := observation.SampledAtMS
	if options.TimestampInSeconds {
		if timestamp%1000 != 0 {
			return nil, fmt.Errorf("legacy binary seconds require millisecond timestamp divisible by 1000")
		}
		timestamp /= 1000
	}
	_ = binary.Write(&payload, binary.BigEndian, timestamp)
	_ = binary.Write(&payload, binary.BigEndian, observation.LagMS)

	sources := observation.GTID.Normalized().Sources
	if len(sources) > 65535 {
		return nil, fmt.Errorf("too many GTID segments")
	}
	_ = binary.Write(&payload, binary.BigEndian, uint16(len(sources)))
	for _, source := range sources {
		parts := make([]string, len(source.Intervals))
		for i, interval := range source.Intervals {
			parts[i] = interval.String()
		}
		writeString32(&payload, source.Source+":"+strings.Join(parts, ":"))
	}
	writeString32(&payload, observation.PositionName)
	if !options.OmitReportedVersion {
		writeString32(&payload, observation.ReportedVersion.String())
	}
	_ = binary.Write(&payload, binary.BigEndian, observation.LastWriteMS)
	return payload.Bytes(), nil
}

func writeString32(buffer *bytes.Buffer, value string) {
	_ = binary.Write(buffer, binary.BigEndian, uint32(len(value)))
	buffer.WriteString(value)
}

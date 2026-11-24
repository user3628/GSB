package policy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"example.com/replicabudget/internal/apperr"
)

const (
	recordTypeStrategy = byte(0x01)

	presenceVRAM       = uint16(1 << 0)
	presenceCompute    = uint16(1 << 1)
	presenceElastic    = uint16(1 << 2)
	presencePlacement  = uint16(1 << 3)
	knownPresenceBits  = presenceVRAM | presenceCompute | presenceElastic | presencePlacement
	maxBinaryPayload   = 16 << 20
	binaryHeaderLength = 10
)

// EncodeBinary emits a self-contained little- or big-endian v1 file. Each
// record has its own length and CRC32; the CRC covers type, presence, length
// prefix and payload.
func EncodeBinary(strategies []NewStrategy, order binary.ByteOrder) ([]byte, error) {
	if order == nil {
		order = binary.LittleEndian
	}
	endianMark := byte('L')
	if order == binary.BigEndian {
		endianMark = 'B'
	}
	buf := &bytes.Buffer{}
	buf.WriteString(BinaryMagic)
	buf.WriteByte(CurrentFormatVersion)
	buf.WriteByte(endianMark)
	writeU32(buf, order, uint32(len(strategies)))
	for i, strategy := range strategies {
		record, err := encodeBinaryRecord(strategy, order)
		if err != nil {
			return nil, apperr.Record(apperr.SpecInvalid, err.Error(), i+1, nil)
		}
		buf.Write(record)
	}
	return buf.Bytes(), nil
}

func encodeBinaryRecord(s NewStrategy, order binary.ByteOrder) ([]byte, error) {
	if len(s.ServiceID) > 65535 || len(s.QuotaGroupRef) > 65535 || len(s.PlacementZone) > 65535 {
		return nil, fmt.Errorf("string field exceeds binary uint16 length limit")
	}
	payload := &bytes.Buffer{}
	writeString16(payload, order, s.ServiceID)
	writeU32(payload, order, uint32(s.MinReplicas))
	writeU32(payload, order, uint32(s.MaxReplicas))
	writeU64(payload, order, uint64(s.TargetConcurrency))
	writeU32(payload, order, uint32(s.LatencyBudgetMS))
	writeString16(payload, order, s.QuotaGroupRef)
	writeU32(payload, order, uint32(s.WeightPermille))

	presence := uint16(0)
	if s.VRAMBytesPerReplica > 0 {
		presence |= presenceVRAM
	}
	if s.ComputeMilliPerReplica > 0 {
		presence |= presenceCompute
	}
	if s.ElasticEnabled {
		presence |= presenceElastic
	}
	if s.PlacementZone != "" {
		presence |= presencePlacement
	}
	if presence&presenceVRAM != 0 {
		writeU64(payload, order, s.VRAMBytesPerReplica)
	}
	if presence&presenceCompute != 0 {
		writeU64(payload, order, s.ComputeMilliPerReplica)
	}
	if presence&presenceElastic != 0 {
		payload.WriteByte(1)
	}
	if presence&presencePlacement != 0 {
		writeString16(payload, order, s.PlacementZone)
	}

	if payload.Len() > maxBinaryPayload {
		return nil, fmt.Errorf("record payload exceeds %d bytes", maxBinaryPayload)
	}
	record := &bytes.Buffer{}
	record.WriteByte(recordTypeStrategy)
	writeU16(record, order, presence)
	writeU32(record, order, uint32(payload.Len()))
	record.Write(payload.Bytes())
	crc := crc32.ChecksumIEEE(record.Bytes())
	writeU32(record, order, crc)
	return record.Bytes(), nil
}

func DecodeBinary(data []byte) ([]NewStrategy, InputInfo, error) {
	info := InputInfo{Format: FormatBinary, Version: -1}
	if len(data) < binaryHeaderLength || string(data[:4]) != BinaryMagic {
		return nil, info, apperr.New(apperr.FormatUnsupported, "binary magic/header is invalid")
	}
	info.Version = int(data[4])
	switch data[5] {
	case 'L':
		info.Endian = "little"
	case 'B':
		info.Endian = "big"
	default:
		return nil, info, apperr.WithDetails(apperr.FormatUnsupported, "binary endianness marker is unsupported", map[string]any{"marker": string(data[5])})
	}
	if info.Version != CurrentFormatVersion {
		return nil, info, apperr.WithDetails(apperr.FormatUnsupported, "binary format version is unsupported", map[string]any{"version": info.Version})
	}
	var order binary.ByteOrder = binary.LittleEndian
	if info.Endian == "big" {
		order = binary.BigEndian
	}
	count := int(readU32(data, order, 6))
	if count < 0 || count > 100_000 {
		return nil, info, apperr.Record(apperr.RecordCorrupt, "record count is outside the allowed range", 1, map[string]any{"record_count": count})
	}
	strategies := make([]NewStrategy, 0, count)
	offset := binaryHeaderLength
	for i := 0; i < count; i++ {
		index := i + 1
		recordStart := offset
		if offset+1 > len(data) {
			return nil, info, apperr.Record(apperr.RecordCorrupt, "truncated record type", index, nil)
		}
		recType := data[offset]
		offset++
		if recType != recordTypeStrategy {
			return nil, info, apperr.Record(apperr.RecordCorrupt, "unknown record type", index, map[string]any{"record_type": recType})
		}
		if offset+2 > len(data) {
			return nil, info, apperr.Record(apperr.RecordCorrupt, "truncated presence bitmap", index, nil)
		}
		presence := readU16(data, order, offset)
		offset += 2
		if unknown := presence &^ knownPresenceBits; unknown != 0 {
			return nil, info, apperr.Record(apperr.FormatUnsupported, "record contains unknown optional fields", index, map[string]any{"unknown_presence_bits": unknown})
		}
		if offset+4 > len(data) {
			return nil, info, apperr.Record(apperr.RecordCorrupt, "truncated length prefix", index, nil)
		}
		payloadLen := int(readU32(data, order, offset))
		offset += 4
		if payloadLen < 0 || payloadLen > maxBinaryPayload || offset+payloadLen+4 > len(data) {
			return nil, info, apperr.Record(apperr.RecordCorrupt, "length prefix exceeds available record bytes", index, map[string]any{"payload_length": payloadLen})
		}
		payloadStart := offset
		payloadEnd := payloadStart + payloadLen
		storedCRC := readU32(data, order, payloadEnd)
		actualCRC := crc32.ChecksumIEEE(data[recordStart:payloadEnd])
		if storedCRC != actualCRC {
			return nil, info, apperr.Record(apperr.RecordCorrupt, "record CRC32 mismatch", index, map[string]any{
				"stored_crc32": fmt.Sprintf("%08x", storedCRC), "actual_crc32": fmt.Sprintf("%08x", actualCRC),
			})
		}
		payload := data[payloadStart:payloadEnd]
		strategy, err := decodeBinaryPayload(payload, presence, order, index)
		if err != nil {
			return nil, info, err
		}
		strategies = append(strategies, strategy)
		offset = payloadEnd + 4
	}
	if offset != len(data) {
		return nil, info, apperr.Record(apperr.RecordCorrupt, "trailing bytes after declared record count", count+1, map[string]any{"trailing_bytes": len(data) - offset})
	}
	return strategies, info, nil
}

func decodeBinaryPayload(payload []byte, presence uint16, order binary.ByteOrder, index int) (NewStrategy, error) {
	r := &binaryPayloadReader{data: payload, order: order, index: index}
	serviceID, err := r.string16()
	if err != nil {
		return NewStrategy{}, err
	}
	minReplicas, err := r.u32()
	if err != nil {
		return NewStrategy{}, err
	}
	maxReplicas, err := r.u32()
	if err != nil {
		return NewStrategy{}, err
	}
	targetConcurrency, err := r.u64()
	if err != nil {
		return NewStrategy{}, err
	}
	latency, err := r.u32()
	if err != nil {
		return NewStrategy{}, err
	}
	quotaRef, err := r.string16()
	if err != nil {
		return NewStrategy{}, err
	}
	weight, err := r.u32()
	if err != nil {
		return NewStrategy{}, err
	}
	s := NewStrategy{
		ServiceID: serviceID, MinReplicas: int(minReplicas), MaxReplicas: int(maxReplicas),
		TargetConcurrency: int64(targetConcurrency), LatencyBudgetMS: int(latency),
		QuotaGroupRef: quotaRef, WeightPermille: int(weight),
	}
	if presence&presenceVRAM != 0 {
		v, err := r.u64()
		if err != nil {
			return NewStrategy{}, err
		}
		s.VRAMBytesPerReplica = v
	}
	if presence&presenceCompute != 0 {
		v, err := r.u64()
		if err != nil {
			return NewStrategy{}, err
		}
		s.ComputeMilliPerReplica = v
	}
	if presence&presenceElastic != 0 {
		v, err := r.byte()
		if err != nil {
			return NewStrategy{}, err
		}
		if v != 0 && v != 1 {
			return NewStrategy{}, apperr.Record(apperr.RecordCorrupt, "elastic flag must be 0 or 1", index, nil)
		}
		s.ElasticEnabled = v == 1
	}
	if presence&presencePlacement != 0 {
		v, err := r.string16()
		if err != nil {
			return NewStrategy{}, err
		}
		s.PlacementZone = v
	}
	if r.pos != len(r.data) {
		return NewStrategy{}, apperr.Record(apperr.RecordCorrupt, "payload has trailing bytes", index, map[string]any{"trailing_bytes": len(r.data) - r.pos})
	}
	return s, nil
}

type binaryPayloadReader struct {
	data  []byte
	pos   int
	order binary.ByteOrder
	index int
}

func (r *binaryPayloadReader) byte() (byte, error) {
	if r.pos+1 > len(r.data) {
		return 0, apperr.Record(apperr.RecordCorrupt, "payload is truncated", r.index, nil)
	}
	v := r.data[r.pos]
	r.pos++
	return v, nil
}

func (r *binaryPayloadReader) u32() (uint32, error) {
	if r.pos+4 > len(r.data) {
		return 0, apperr.Record(apperr.RecordCorrupt, "payload integer is truncated", r.index, nil)
	}
	v := r.order.Uint32(r.data[r.pos:])
	r.pos += 4
	return v, nil
}

func (r *binaryPayloadReader) u64() (uint64, error) {
	if r.pos+8 > len(r.data) {
		return 0, apperr.Record(apperr.RecordCorrupt, "payload integer is truncated", r.index, nil)
	}
	v := r.order.Uint64(r.data[r.pos:])
	r.pos += 8
	return v, nil
}

func (r *binaryPayloadReader) string16() (string, error) {
	if r.pos+2 > len(r.data) {
		return "", apperr.Record(apperr.RecordCorrupt, "payload string length is truncated", r.index, nil)
	}
	n := int(r.order.Uint16(r.data[r.pos:]))
	r.pos += 2
	if r.pos+n > len(r.data) {
		return "", apperr.Record(apperr.RecordCorrupt, "payload string length exceeds record payload", r.index, nil)
	}
	s := string(r.data[r.pos : r.pos+n])
	r.pos += n
	return s, nil
}

func writeU16(buf *bytes.Buffer, order binary.ByteOrder, v uint16) {
	var b [2]byte
	order.PutUint16(b[:], v)
	buf.Write(b[:])
}
func writeU32(buf *bytes.Buffer, order binary.ByteOrder, v uint32) {
	var b [4]byte
	order.PutUint32(b[:], v)
	buf.Write(b[:])
}
func writeU64(buf *bytes.Buffer, order binary.ByteOrder, v uint64) {
	var b [8]byte
	order.PutUint64(b[:], v)
	buf.Write(b[:])
}
func writeString16(buf *bytes.Buffer, order binary.ByteOrder, s string) {
	if len(s) > 65535 {
		s = s[:65535]
	}
	writeU16(buf, order, uint16(len(s)))
	buf.WriteString(s)
}
func readU16(data []byte, order binary.ByteOrder, off int) uint16 {
	return order.Uint16(data[off:])
}
func readU32(data []byte, order binary.ByteOrder, off int) uint32 {
	return order.Uint32(data[off:])
}

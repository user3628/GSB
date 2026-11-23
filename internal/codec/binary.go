package codec

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"math"
	"sort"

	"meshmigrate/internal/model"
)

const (
	BinaryMagic          = "MSPB"
	BinaryRecordMagic    = "MSPR"
	BinaryHeaderLen      = 64
	BinaryRecordHdrLen   = 12
	BinaryEndianLittle   = 0
	BinaryEndianBig      = 1
	BinaryCurrentVersion = 2
)

const (
	bitRateLimit = 1 << iota
	bitTimeout
	bitCircuitWindow
	bitRateWindow
	bitCapturedAt
	bitWindowAlignment
)

// EncodeBinary emits the compact deterministic binary document. The snapshot
// digest is SHA-256 over the concatenated on-disk record blocks.
func EncodeBinary(s model.ParsedSnapshot, bigEndian bool) ([]byte, error) {
	var order binary.ByteOrder = binary.LittleEndian
	endianMarker := byte(BinaryEndianLittle)
	if bigEndian {
		order = binary.BigEndian
		endianMarker = BinaryEndianBig
	}
	if s.FormatVersion <= 0 || s.FormatVersion > math.MaxUint8 {
		return nil, fmt.Errorf("format version out of range: %d", s.FormatVersion)
	}
	if len(s.SnapshotID) > math.MaxUint32 {
		return nil, fmt.Errorf("snapshot id too long")
	}
	if len(s.Policies) > math.MaxUint32 {
		return nil, fmt.Errorf("too many policies")
	}
	var recordBytes bytes.Buffer
	for i, p := range s.Policies {
		rec, err := encodeBinaryRecord(order, p)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		recordBytes.Write(rec)
	}
	header := make([]byte, BinaryHeaderLen)
	copy(header[0:4], []byte(BinaryMagic))
	header[4] = byte(s.FormatVersion)
	header[5] = endianMarker
	order.PutUint16(header[6:8], 0)
	order.PutUint64(header[8:16], s.SnapshotVersion)
	order.PutUint64(header[16:24], uint64(s.GeneratedAtMs))
	order.PutUint32(header[24:28], uint32(len(s.Policies)))
	order.PutUint32(header[28:32], uint32(len(s.SnapshotID)))
	// The whole-snapshot digest binds the immutable header fields (through
	// byte 31), snapshot id and every framed record. The digest slot itself is
	// excluded so the field can hold the result.
	digestInput := make([]byte, 0, 32+len(s.SnapshotID)+recordBytes.Len())
	digestInput = append(digestInput, header[0:32]...)
	digestInput = append(digestInput, s.SnapshotID...)
	digestInput = append(digestInput, recordBytes.Bytes()...)
	digest := sha256.Sum256(digestInput)
	copy(header[32:64], digest[:])
	out := make([]byte, 0, BinaryHeaderLen+len(s.SnapshotID)+recordBytes.Len())
	out = append(out, header...)
	out = append(out, s.SnapshotID...)
	out = append(out, recordBytes.Bytes()...)
	return out, nil
}

func encodeBinaryRecord(order binary.ByteOrder, p model.RawPolicy) ([]byte, error) {
	var bitmap uint16
	if p.Presence.RateLimit {
		bitmap |= bitRateLimit
	}
	if p.Presence.Timeout {
		bitmap |= bitTimeout
	}
	if p.Presence.CircuitWindow {
		bitmap |= bitCircuitWindow
	}
	if p.Presence.RateWindow {
		bitmap |= bitRateWindow
	}
	if p.Presence.CapturedAt {
		bitmap |= bitCapturedAt
	}
	if p.Presence.WindowAlignment {
		bitmap |= bitWindowAlignment
	}

	var payload bytes.Buffer
	putString := func(v string) error {
		if len(v) > math.MaxUint32 {
			return fmt.Errorf("string too long")
		}
		var l [4]byte
		order.PutUint32(l[:], uint32(len(v)))
		payload.Write(l[:])
		payload.WriteString(v)
		return nil
	}
	if err := putString(p.PolicyID); err != nil {
		return nil, err
	}
	if err := putString(p.VirtualServiceID); err != nil {
		return nil, err
	}
	var tmp8 [8]byte
	order.PutUint64(tmp8[:], p.PolicyRevision)
	payload.Write(tmp8[:])
	order.PutUint64(tmp8[:], uint64(p.LastWriteMs))
	payload.Write(tmp8[:])
	if err := putString(p.Match.Method); err != nil {
		return nil, err
	}
	if err := putString(p.Match.PathPrefix); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(p.Match.Headers))
	for k := range p.Match.Headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) > math.MaxUint16 {
		return nil, fmt.Errorf("too many match headers")
	}
	var tmp2 [2]byte
	order.PutUint16(tmp2[:], uint16(len(keys)))
	payload.Write(tmp2[:])
	for _, k := range keys {
		if err := putString(k); err != nil {
			return nil, err
		}
		if err := putString(p.Match.Headers[k]); err != nil {
			return nil, err
		}
	}
	if p.Presence.RateLimit {
		order.PutUint64(tmp8[:], p.RateLimitValue)
		payload.Write(tmp8[:])
	}
	if p.Presence.Timeout {
		order.PutUint64(tmp8[:], p.TimeoutValue)
		payload.Write(tmp8[:])
	}
	if p.Presence.CircuitWindow {
		order.PutUint64(tmp8[:], p.CircuitWindowVal)
		payload.Write(tmp8[:])
	}
	if p.Presence.RateWindow {
		order.PutUint64(tmp8[:], p.RateWindowValue)
		payload.Write(tmp8[:])
	}
	if p.Presence.CapturedAt {
		order.PutUint64(tmp8[:], uint64(p.CapturedAtValue))
		payload.Write(tmp8[:])
	}
	if p.Presence.WindowAlignment {
		payload.WriteByte(alignmentCode(p.WindowAlignment))
	}
	if payload.Len() > math.MaxUint32 {
		return nil, fmt.Errorf("record payload too large")
	}
	rec := make([]byte, BinaryRecordHdrLen+payload.Len()+4)
	copy(rec[0:4], []byte(BinaryRecordMagic))
	rec[4] = 1
	order.PutUint16(rec[5:7], bitmap)
	rec[7] = 0
	order.PutUint32(rec[8:12], uint32(payload.Len()))
	copy(rec[12:], payload.Bytes())
	sum := crc32.ChecksumIEEE(rec[:BinaryRecordHdrLen+payload.Len()])
	order.PutUint32(rec[BinaryRecordHdrLen+payload.Len():], sum)
	return rec, nil
}

// DecodeBinary validates all framing, CRCs, record count and snapshot digest
// before returning any policy. A single framing failure returns no partial data.
func DecodeBinary(data []byte) (*model.ParsedSnapshot, *Error) {
	if len(data) < BinaryHeaderLen {
		return nil, corrupt(0, "binary header truncated: need %d bytes, got %d", BinaryHeaderLen, len(data))
	}
	if string(data[0:4]) != BinaryMagic {
		return nil, corrupt(0, "bad binary magic: got %q want %q", string(data[0:4]), BinaryMagic)
	}
	var order binary.ByteOrder
	switch data[5] {
	case BinaryEndianLittle:
		order = binary.LittleEndian
	case BinaryEndianBig:
		order = binary.BigEndian
	default:
		return nil, corrupt(5, "invalid endian marker %d", data[5])
	}
	formatVersion := int(data[4])
	snapshotVersion := order.Uint64(data[8:16])
	generatedAtMs := int64(order.Uint64(data[16:24]))
	recordCount := order.Uint32(data[24:28])
	snapshotIDLen := order.Uint32(data[28:32])
	if uint64(recordCount) > uint64(len(data))/(BinaryRecordHdrLen+4) {
		return nil, corrupt(24, "record count %d cannot fit in %d input bytes", recordCount, len(data))
	}
	var digest [32]byte
	copy(digest[:], data[32:64])
	if uint64(BinaryHeaderLen)+uint64(snapshotIDLen) > uint64(len(data)) {
		return nil, corrupt(28, "snapshot id length %d exceeds input", snapshotIDLen)
	}
	snapshotID := string(data[BinaryHeaderLen : BinaryHeaderLen+int(snapshotIDLen)])
	pos := BinaryHeaderLen + int(snapshotIDLen)
	unknown := make([]string, 0)
	if order.Uint16(data[6:8]) != 0 {
		unknown = append(unknown, "header.flags")
	}
	rawPolicies := make([]model.RawPolicy, 0, recordCount)
	for i := 0; i < int(recordCount); i++ {
		offset := pos
		if len(data)-pos < BinaryRecordHdrLen+4 {
			return nil, corrupt(int64(pos), "record %d truncated before header/checksum", i)
		}
		if string(data[pos:pos+4]) != BinaryRecordMagic {
			return nil, corrupt(int64(pos), "record %d bad magic: got %q want %q", i, string(data[pos:pos+4]), BinaryRecordMagic)
		}
		recordVersion := int(data[pos+4])
		bitmap := order.Uint16(data[pos+5 : pos+7])
		declaredLen := order.Uint32(data[pos+8 : pos+12])
		payloadStart := pos + BinaryRecordHdrLen
		crcOffset := payloadStart + int(declaredLen)
		if declaredLen > uint32(len(data)) || crcOffset+4 > len(data) {
			return nil, corrupt(int64(pos+8), "record %d length prefix %d exceeds input at record offset %d", i, declaredLen, pos)
		}
		gotCRC := order.Uint32(data[crcOffset : crcOffset+4])
		wantCRC := crc32.ChecksumIEEE(data[pos : payloadStart+int(declaredLen)])
		if gotCRC != wantCRC {
			return nil, corrupt(int64(crcOffset), "record %d CRC32 mismatch: got %08x want %08x", i, gotCRC, wantCRC)
		}
		if recordVersion > 1 {
			unknown = append(unknown, fmt.Sprintf("recordVersion[%d]", recordVersion))
		}
		for bit := 6; bit < 16; bit++ {
			if bitmap&(1<<bit) != 0 {
				unknown = append(unknown, fmt.Sprintf("optionalBitmap.bit%d", bit))
			}
		}
		p, er := decodeBinaryPayload(order, data[payloadStart:crcOffset], bitmap, int64(offset))
		if er != nil {
			er.RecordIndex = i
			return nil, er
		}
		p.SourceOffset = int64(offset)
		p.SourceLine = i + 1
		rawPolicies = append(rawPolicies, p)
		pos = crcOffset + 4
	}
	if pos != len(data) {
		return nil, corrupt(int64(pos), "trailing bytes after %d records: %d bytes remain", recordCount, len(data)-pos)
	}
	digestInput := make([]byte, 0, 32+int(snapshotIDLen)+len(data)-BinaryHeaderLen-int(snapshotIDLen))
	digestInput = append(digestInput, data[0:32]...)
	digestInput = append(digestInput, data[BinaryHeaderLen:BinaryHeaderLen+int(snapshotIDLen)]...)
	digestInput = append(digestInput, data[BinaryHeaderLen+int(snapshotIDLen):]...)
	actual := sha256.Sum256(digestInput)
	if !bytes.Equal(actual[:], digest[:]) {
		return nil, corrupt(32, "snapshot digest mismatch: got sha256:%s want sha256:%s", hex.EncodeToString(digest[:]), hex.EncodeToString(actual[:]))
	}
	return &model.ParsedSnapshot{
		SnapshotVersion: snapshotVersion,
		SnapshotID:      snapshotID,
		GeneratedAtMs:   generatedAtMs,
		SourceFormat:    model.FormatBinary,
		FormatVersion:   formatVersion,
		PolicyCount:     int(recordCount),
		SourceDigest:    "sha256:" + hex.EncodeToString(digest[:]),
		Policies:        rawPolicies,
		UnknownFields:   uniqueStrings(unknown),
	}, nil
}

func decodeBinaryPayload(order binary.ByteOrder, payload []byte, bitmap uint16, recordOffset int64) (model.RawPolicy, *Error) {
	var p model.RawPolicy
	pos := 0
	readString := func(field string) (string, *Error) {
		if len(payload)-pos < 4 {
			return "", corrupt(recordOffset+int64(BinaryRecordHdrLen+pos), "%s length missing in record payload", field)
		}
		n := order.Uint32(payload[pos : pos+4])
		pos += 4
		if uint64(pos)+uint64(n) > uint64(len(payload)) {
			return "", corrupt(recordOffset+int64(BinaryRecordHdrLen+pos-4), "%s length %d exceeds payload", field, n)
		}
		v := string(payload[pos : pos+int(n)])
		pos += int(n)
		return v, nil
	}
	readU64 := func(field string) (uint64, *Error) {
		if len(payload)-pos < 8 {
			return 0, corrupt(recordOffset+int64(BinaryRecordHdrLen+pos), "%s truncated in record payload", field)
		}
		v := order.Uint64(payload[pos : pos+8])
		pos += 8
		return v, nil
	}
	var er *Error
	p.PolicyID, er = readString("policyId")
	if er != nil {
		return p, er
	}
	p.VirtualServiceID, er = readString("virtualServiceId")
	if er != nil {
		return p, er
	}
	v, er := readU64("policyRevision")
	if er != nil {
		return p, er
	}
	p.PolicyRevision = v
	v, er = readU64("lastWriteMs")
	if er != nil {
		return p, er
	}
	if v > math.MaxInt64 {
		return p, corrupt(recordOffset, "lastWriteMs overflows int64")
	}
	p.LastWriteMs = int64(v)
	p.Match.Method, er = readString("match.method")
	if er != nil {
		return p, er
	}
	p.Match.PathPrefix, er = readString("match.pathPrefix")
	if er != nil {
		return p, er
	}
	if len(payload)-pos < 2 {
		return p, corrupt(recordOffset+int64(BinaryRecordHdrLen+pos), "match header count truncated")
	}
	nHeaders := int(order.Uint16(payload[pos : pos+2]))
	pos += 2
	if nHeaders > 0 {
		p.Match.Headers = make(map[string]string, nHeaders)
	}
	for i := 0; i < nHeaders; i++ {
		k, e2 := readString("match.header.key")
		if e2 != nil {
			return p, e2
		}
		val, e2 := readString("match.header.value")
		if e2 != nil {
			return p, e2
		}
		p.Match.Headers[k] = val
	}
	p.RateLimitUnit = "bps"
	p.TimeoutUnit = "ms"
	p.CircuitWindowUnit = "ms"
	p.RateWindowUnit = "ms"
	p.CapturedAtUnit = "ms"
	if bitmap&bitRateLimit != 0 {
		v, er = readU64("rateLimitBps")
		if er != nil {
			return p, er
		}
		p.RateLimitValue = v
		p.Presence.RateLimit = true
	}
	if bitmap&bitTimeout != 0 {
		v, er = readU64("timeoutMs")
		if er != nil {
			return p, er
		}
		p.TimeoutValue = v
		p.Presence.Timeout = true
	}
	if bitmap&bitCircuitWindow != 0 {
		v, er = readU64("circuitWindowMs")
		if er != nil {
			return p, er
		}
		p.CircuitWindowVal = v
		p.Presence.CircuitWindow = true
	}
	if bitmap&bitRateWindow != 0 {
		v, er = readU64("rateWindowMs")
		if er != nil {
			return p, er
		}
		p.RateWindowValue = v
		p.Presence.RateWindow = true
	}
	if bitmap&bitCapturedAt != 0 {
		v, er = readU64("capturedAtMs")
		if er != nil {
			return p, er
		}
		if v > math.MaxInt64 {
			return p, corrupt(recordOffset, "capturedAtMs overflows int64")
		}
		p.CapturedAtValue = int64(v)
		p.Presence.CapturedAt = true
	}
	if bitmap&bitWindowAlignment != 0 {
		if len(payload)-pos < 1 {
			return p, corrupt(recordOffset+int64(BinaryRecordHdrLen+pos), "windowAlignment truncated")
		}
		code := payload[pos]
		pos++
		switch code {
		case 0:
			p.WindowAlignment = model.AlignmentNone
		case 1:
			p.WindowAlignment = model.AlignmentUp
		case 2:
			p.WindowAlignment = model.AlignmentDown
		default:
			return p, corrupt(recordOffset+int64(BinaryRecordHdrLen+pos-1), "invalid window alignment code %d", code)
		}
		p.Presence.WindowAlignment = true
	}
	if pos != len(payload) {
		return p, corrupt(recordOffset+int64(BinaryRecordHdrLen+pos), "record payload has %d unconsumed bytes (declared length mismatch)", len(payload)-pos)
	}
	return p, nil
}

func alignmentCode(v string) byte {
	switch v {
	case model.AlignmentUp:
		return 1
	case model.AlignmentDown:
		return 2
	default:
		return 0
	}
}

func uniqueStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	sort.Strings(in)
	out := in[:0]
	for _, v := range in {
		if len(out) == 0 || out[len(out)-1] != v {
			out = append(out, v)
		}
	}
	return out
}

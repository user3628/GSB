package codec

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math"

	"mitmpipeline/internal/domain"
)

var BinaryMagic = [4]byte{'M', 'T', 'L', 'P'}

const (
	bitmapProtocol       = 1 << 0
	bitmapTimeline       = 1 << 1
	bitmapFutureRequired = 1 << 15
)

func EncodeBinary(records []domain.Record, version int) ([]byte, error) {
	if version == 0 {
		version = CurrentVersion
	}
	if version != 1 && version != 2 {
		return nil, domain.NewError(domain.CodeFormatUnsupported, "encoder only writes versions 1 and 2").
			WithContext("requested_version", version)
	}
	if len(records) > math.MaxUint16 {
		return nil, domain.NewError(domain.CodeRecordCorrupt, "too many binary records")
	}

	out := &bytes.Buffer{}
	out.Write(BinaryMagic[:])
	out.WriteByte(byte(version))
	if err := binary.Write(out, binary.BigEndian, uint16(len(records))); err != nil {
		return nil, err
	}
	for i := range records {
		frame, err := encodeBinaryRecord(&records[i], version)
		if err != nil {
			return nil, err.(*domain.Error).WithContext("record_index", i+1)
		}
		if err := binary.Write(out, binary.BigEndian, uint32(len(frame)+crc32.Size)); err != nil {
			return nil, err
		}
		out.Write(frame)
		sum := crc32.ChecksumIEEE(frame)
		if err := binary.Write(out, binary.BigEndian, sum); err != nil {
			return nil, err
		}
	}
	return out.Bytes(), nil
}

func encodeBinaryRecord(record *domain.Record, version int) ([]byte, error) {
	body := &bytes.Buffer{}
	var bitmap uint16
	if version >= 2 && record.ProtocolVersion != "" {
		bitmap |= bitmapProtocol
	}
	if len(record.PolicyTimeline) > 0 {
		bitmap |= bitmapTimeline
	}
	if err := binary.Write(body, binary.BigEndian, bitmap); err != nil {
		return nil, err
	}
	if err := writeU16String(body, record.ConnectionID); err != nil {
		return nil, err
	}
	if err := writeU16String(body, record.SNI); err != nil {
		return nil, err
	}
	if bitmap&bitmapProtocol != 0 {
		if err := writeU16String(body, record.ProtocolVersion); err != nil {
			return nil, err
		}
	}
	if err := binary.Write(body, binary.BigEndian, record.ObservedStartMS); err != nil {
		return nil, err
	}

	chainPEM := record.CertChainPEM
	if chainPEM == "" {
		for _, entry := range record.Chain {
			chainPEM += entry.PEM
		}
	}
	if err := writeU32Bytes(body, []byte(chainPEM)); err != nil {
		return nil, err
	}
	if err := encodePolicy(body, record.Policy); err != nil {
		return nil, err
	}

	timeline := &bytes.Buffer{}
	if bitmap&bitmapTimeline != 0 {
		if err := binary.Write(timeline, binary.BigEndian, uint16(len(record.PolicyTimeline))); err != nil {
			return nil, err
		}
		for _, change := range record.PolicyTimeline {
			if err := binary.Write(timeline, binary.BigEndian, change.EffectiveOffsetNS); err != nil {
				return nil, err
			}
			if err := encodePolicy(timeline, change.Policy); err != nil {
				return nil, err
			}
		}
	}
	if err := writeU32Bytes(body, timeline.Bytes()); err != nil {
		return nil, err
	}

	eventsBlock := &bytes.Buffer{}
	if err := binary.Write(eventsBlock, binary.BigEndian, uint16(len(record.Events))); err != nil {
		return nil, err
	}
	for _, event := range record.Events {
		typeCode, err := eventTypeCode(event.Type)
		if err != nil {
			return nil, err
		}
		offset := event.OffsetNS
		if version == 1 {
			offset = offset / int64(1_000_000)
		}
		if err := binary.Write(eventsBlock, binary.BigEndian, uint32(event.Seq)); err != nil {
			return nil, err
		}
		if err := eventsBlock.WriteByte(typeCode); err != nil {
			return nil, err
		}
		if err := binary.Write(eventsBlock, binary.BigEndian, offset); err != nil {
			return nil, err
		}
	}
	if err := writeU32Bytes(body, eventsBlock.Bytes()); err != nil {
		return nil, err
	}
	return body.Bytes(), nil
}

func DecodeBinary(data []byte) ([]domain.Record, []domain.Degradation, error) {
	if len(data) < 7 || !bytes.HasPrefix(data, BinaryMagic[:]) {
		return nil, nil, corrupt(0, "binary magic or header is invalid")
	}
	version := int(data[4])
	if version > CurrentVersion {
		bit, _ := inspectFirstBitmap(data)
		return nil, nil, domain.NewError(domain.CodeFormatUnsupported, "unsupported binary format version").
			WithContextMap(map[string]any{"requested_version": version, "supported_versions": []int{1, 2}, "unknown_required_bit": bit})
	}
	if version < 1 {
		return nil, nil, domain.NewError(domain.CodeFormatUnsupported, "unsupported binary format version").WithContext("requested_version", version)
	}
	count := int(binary.BigEndian.Uint16(data[5:7]))
	pos := 7
	records := make([]domain.Record, 0, count)
	degradations := []domain.Degradation{}

	for recordIndex := 1; recordIndex <= count; recordIndex++ {
		if pos+4 > len(data) {
			return nil, nil, corrupt(recordIndex, "truncated record length prefix")
		}
		frameLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if frameLen < crc32.Size || pos+frameLen > len(data) {
			return nil, nil, corrupt(recordIndex, "record length prefix exceeds input")
		}
		bodyLen := frameLen - crc32.Size
		body := data[pos : pos+bodyLen]
		storedCRC := binary.BigEndian.Uint32(data[pos+bodyLen : pos+frameLen])
		if crc32.ChecksumIEEE(body) != storedCRC {
			return nil, nil, corrupt(recordIndex, "CRC32 mismatch")
		}
		pos += frameLen

		record, recordDegradations, err := decodeBinaryBody(body, version, recordIndex)
		if err != nil {
			return nil, nil, err
		}
		records = append(records, record)
		degradations = append(degradations, recordDegradations...)
	}
	if pos != len(data) {
		return nil, nil, corrupt(count+1, "trailing bytes after declared records")
	}
	return records, degradations, nil
}

func decodeBinaryBody(body []byte, version, recordIndex int) (domain.Record, []domain.Degradation, error) {
	r := bytes.NewReader(body)
	record := domain.Record{Format: domain.FormatBinary, FormatVersion: version}
	degradations := []domain.Degradation{}

	bitmap, err := readU16(r, recordIndex, "bitmap")
	if err != nil {
		return record, nil, err
	}
	if version <= CurrentVersion && bitmap&bitmapFutureRequired != 0 {
		return record, nil, domain.NewError(domain.CodeFormatUnsupported, "binary bitmap contains unknown required bit").
			WithContext("record_index", recordIndex).WithContext("bit", 15)
	}
	record.ConnectionID, err = readU16String(r, recordIndex, "connection_id")
	if err != nil {
		return record, nil, err
	}
	record.SNI, err = readU16String(r, recordIndex, "sni")
	if err != nil {
		return record, nil, err
	}
	if bitmap&bitmapProtocol != 0 {
		record.ProtocolVersion, err = readU16String(r, recordIndex, "protocol_version")
		if err != nil {
			return record, nil, err
		}
	} else if version == 1 {
		record.ProtocolVersion = DefaultProtocolVersion
		degradations = append(degradations, domain.Degradation{
			RecordIndex: recordIndex, ConnectionID: record.ConnectionID, Field: "protocol_version",
			Rule: "fill_default_protocol", Supplied: "", Filled: DefaultProtocolVersion,
			Reason: "v1 binary records do not carry protocol_version",
		})
	}
	if err := binary.Read(r, binary.BigEndian, &record.ObservedStartMS); err != nil {
		return record, nil, corrupt(recordIndex, "observed_start_unix_ms")
	}
	chain, err := readU32Bytes(r, recordIndex, "cert_chain")
	if err != nil {
		return record, nil, err
	}
	record.CertChainPEM = string(chain)
	policy, err := decodePolicy(r, recordIndex)
	if err != nil {
		return record, nil, err
	}
	record.Policy = policy

	timelineBytes, err := readU32Bytes(r, recordIndex, "policy_timeline")
	if err != nil {
		return record, nil, err
	}
	if bitmap&bitmapTimeline != 0 && len(timelineBytes) > 0 {
		tr := bytes.NewReader(timelineBytes)
		count, err := readU16(tr, recordIndex, "policy_timeline_count")
		if err != nil {
			return record, nil, err
		}
		for i := 0; i < int(count); i++ {
			var offset int64
			if err := binary.Read(tr, binary.BigEndian, &offset); err != nil {
				return record, nil, corrupt(recordIndex, "policy effective offset")
			}
			policy, err := decodePolicy(tr, recordIndex)
			if err != nil {
				return record, nil, err
			}
			record.PolicyTimeline = append(record.PolicyTimeline, domain.PolicyTimelineEntry{Policy: policy, EffectiveOffsetNS: offset})
		}
	}

	eventsBytes, err := readU32Bytes(r, recordIndex, "events_block")
	if err != nil {
		return record, nil, err
	}
	er := bytes.NewReader(eventsBytes)
	count, err := readU16(er, recordIndex, "event_count")
	if err != nil {
		return record, nil, err
	}
	for i := 0; i < int(count); i++ {
		var seq uint32
		if err := binary.Read(er, binary.BigEndian, &seq); err != nil {
			return record, nil, corrupt(recordIndex, "event_seq")
		}
		typeCode, err := er.ReadByte()
		if err != nil {
			return record, nil, corrupt(recordIndex, "event_type")
		}
		var offset int64
		if err := binary.Read(er, binary.BigEndian, &offset); err != nil {
			return record, nil, corrupt(recordIndex, "event_offset")
		}
		eventType, err := eventTypeForCode(typeCode)
		if err != nil {
			return record, nil, err.(*domain.Error).WithContext("record_index", recordIndex)
		}
		if version == 1 {
			offset *= int64(1_000_000)
		}
		record.Events = append(record.Events, domain.Event{Seq: int(seq), Type: eventType, OffsetNS: offset})
	}
	if r.Len() != 0 {
		return record, nil, corrupt(recordIndex, "unconsumed record bytes")
	}
	if version == 1 {
		degradations = append(degradations, domain.Degradation{
			RecordIndex: recordIndex, ConnectionID: record.ConnectionID, Field: "events.offset_ns",
			Rule: "interpret_v1_offset_as_milliseconds", Supplied: "millisecond_precision", Filled: "nanosecond_field_with_zero_submillisecond",
			Reason: "v1 binary event offsets have millisecond precision",
		})
	}
	return record, degradations, nil
}

func inspectFirstBitmap(data []byte) (uint16, error) {
	if len(data) < 11 || !bytes.HasPrefix(data, BinaryMagic[:]) {
		return 0, nil
	}
	pos := 7
	frameLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	if pos+4+2 > len(data) || frameLen < 6 {
		return 0, nil
	}
	return binary.BigEndian.Uint16(data[pos+4 : pos+6]), nil
}

func corrupt(recordIndex int, message string) *domain.Error {
	return domain.NewError(domain.CodeRecordCorrupt, message).WithContext("record_index", recordIndex)
}

func writeU16String(buf *bytes.Buffer, value string) error {
	if len(value) > math.MaxUint16 {
		return domain.NewError(domain.CodeRecordCorrupt, "string too long")
	}
	if err := binary.Write(buf, binary.BigEndian, uint16(len(value))); err != nil {
		return err
	}
	_, err := buf.WriteString(value)
	return err
}

func writeU32Bytes(buf *bytes.Buffer, value []byte) error {
	if uint64(len(value)) > math.MaxUint32 {
		return domain.NewError(domain.CodeRecordCorrupt, "block too long")
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(value))); err != nil {
		return err
	}
	_, err := buf.Write(value)
	return err
}

func readU16(r *bytes.Reader, recordIndex int, field string) (uint16, error) {
	var value uint16
	if err := binary.Read(r, binary.BigEndian, &value); err != nil {
		return 0, corrupt(recordIndex, "truncated "+field)
	}
	return value, nil
}

func readU16String(r *bytes.Reader, recordIndex int, field string) (string, error) {
	n, err := readU16(r, recordIndex, field+" length")
	if err != nil {
		return "", err
	}
	buf := make([]byte, n)
	if _, err := r.Read(buf); err != nil {
		return "", corrupt(recordIndex, "truncated "+field)
	}
	return string(buf), nil
}

func readU32Bytes(r *bytes.Reader, recordIndex int, field string) ([]byte, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, corrupt(recordIndex, "truncated "+field+" length")
	}
	if uint64(n) > uint64(r.Len()) {
		return nil, corrupt(recordIndex, field+" length exceeds record")
	}
	buf := make([]byte, n)
	if _, err := r.Read(buf); err != nil {
		return nil, corrupt(recordIndex, "truncated "+field)
	}
	return buf, nil
}

func encodePolicy(buf *bytes.Buffer, policy domain.Policy) error {
	if err := writeU16String(buf, policy.Version); err != nil {
		return err
	}
	mode := byte(1)
	if policy.Mode == domain.PolicyModeRoot {
		mode = 2
	}
	if err := buf.WriteByte(mode); err != nil {
		return err
	}
	if err := writeStringList(buf, policy.SPKIPins); err != nil {
		return err
	}
	return writeStringList(buf, policy.TrustedRoots)
}

func writeStringList(buf *bytes.Buffer, values []string) error {
	if len(values) > math.MaxUint16 {
		return domain.NewError(domain.CodeRecordCorrupt, "policy list too long")
	}
	if err := binary.Write(buf, binary.BigEndian, uint16(len(values))); err != nil {
		return err
	}
	for _, value := range values {
		if err := writeU16String(buf, value); err != nil {
			return err
		}
	}
	return nil
}

func decodePolicy(r *bytes.Reader, recordIndex int) (domain.Policy, error) {
	version, err := readU16String(r, recordIndex, "policy_version")
	if err != nil {
		return domain.Policy{}, err
	}
	modeByte, err := r.ReadByte()
	if err != nil {
		return domain.Policy{}, corrupt(recordIndex, "policy_mode")
	}
	mode := domain.PolicyModeSPKI
	if modeByte == 2 {
		mode = domain.PolicyModeRoot
	} else if modeByte != 1 {
		return domain.Policy{}, corrupt(recordIndex, "unsupported policy mode")
	}
	pins, err := readStringList(r, recordIndex, "spki_pins")
	if err != nil {
		return domain.Policy{}, err
	}
	roots, err := readStringList(r, recordIndex, "trusted_roots")
	if err != nil {
		return domain.Policy{}, err
	}
	return domain.Policy{Version: version, Mode: mode, SPKIPins: pins, TrustedRoots: roots}, nil
}

func readStringList(r *bytes.Reader, recordIndex int, field string) ([]string, error) {
	count, err := readU16(r, recordIndex, field+"_count")
	if err != nil {
		return nil, err
	}
	values := make([]string, 0, count)
	for i := 0; i < int(count); i++ {
		value, err := readU16String(r, recordIndex, field+"_item")
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func eventTypeCode(eventType string) (byte, error) {
	switch eventType {
	case domain.EventHandshakeStart:
		return 1, nil
	case domain.EventCertificateReceived:
		return 2, nil
	case domain.EventPinVerification:
		return 3, nil
	case domain.EventHandshakeComplete:
		return 4, nil
	case domain.EventAbnormalTermination:
		return 5, nil
	default:
		return 0, domain.NewError(domain.CodeInvalidTransition, "unknown event type").WithContext("event_type", eventType)
	}
}

func eventTypeForCode(code byte) (string, error) {
	switch code {
	case 1:
		return domain.EventHandshakeStart, nil
	case 2:
		return domain.EventCertificateReceived, nil
	case 3:
		return domain.EventPinVerification, nil
	case 4:
		return domain.EventHandshakeComplete, nil
	case 5:
		return domain.EventAbnormalTermination, nil
	default:
		return "", domain.NewError(domain.CodeRecordCorrupt, "unknown binary event type code").WithContext("code", code)
	}
}

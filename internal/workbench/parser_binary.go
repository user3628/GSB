package workbench

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

var binaryMagic = [4]byte{'D', 'N', 'S', 'L'}

const (
	binaryCommonHeader = 9
	binaryV1Header     = 26
	binaryV2Header     = 32
)

type sourceJob struct {
	AbsPath   string
	RelPath   string
	Format    string
	MaxEvents int
}

func parseBinaryFile(job sourceJob) SessionResult {
	result := SessionResult{Source: job.RelPath, Format: FormatBinary}
	data, err := os.ReadFile(job.AbsPath)
	if err != nil {
		result.Issues = append(result.Issues, Issue{
			Code:   CodeSourceOpen,
			Source: job.RelPath,
			Offset: 0,
			Detail: err.Error(),
		})
		result.Skipped = 1
		return result
	}
	result.Bytes = int64(len(data))

	versions := make(map[string]struct{})
	offset := 0
	for offset < len(data) {
		start := offset
		if job.MaxEvents > 0 && len(result.Events) >= job.MaxEvents {
			result.Issues = append(result.Issues, Issue{
				Code:   CodeEventBudget,
				Source: job.RelPath,
				Offset: int64(start),
				Detail: fmt.Sprintf("event budget %d reached; partial events retained", job.MaxEvents),
			})
			result.Skipped++
			break
		}
		if len(data)-offset < binaryCommonHeader {
			result.Issues = append(result.Issues, Issue{
				Code:   CodeTruncated,
				Source: job.RelPath,
				Offset: int64(start),
				Detail: fmt.Sprintf("binary record header truncated: need %d bytes, have %d", binaryCommonHeader, len(data)-offset),
			})
			result.Skipped++
			break
		}
		if !bytes.Equal(data[offset:offset+4], binaryMagic[:]) {
			result.Issues = append(result.Issues, Issue{
				Code:   CodeMagicInvalid,
				Source: job.RelPath,
				Offset: int64(start),
				Detail: fmt.Sprintf("expected magic DNSL, found %q", string(data[offset:offset+4])),
			})
			result.Skipped++
			break
		}
		version := data[offset+4]
		recordLen := int(binary.BigEndian.Uint32(data[offset+5 : offset+9]))
		if recordLen < binaryCommonHeader {
			result.Issues = append(result.Issues, Issue{
				Code:   CodeTruncated,
				Source: job.RelPath,
				Offset: int64(start),
				Detail: fmt.Sprintf("declared binary record length %d is smaller than common header", recordLen),
			})
			result.Skipped++
			break
		}
		if recordLen > len(data)-offset {
			result.Issues = append(result.Issues, Issue{
				Code:   CodeTruncated,
				Source: job.RelPath,
				Offset: int64(start),
				Detail: fmt.Sprintf("declared binary record length %d exceeds %d remaining bytes", recordLen, len(data)-offset),
			})
			result.Skipped++
			break
		}

		if version != 1 && version != 2 {
			result.Issues = append(result.Issues, Issue{
				Code:   CodeVersionUnsupported,
				Source: job.RelPath,
				Offset: int64(start),
				Detail: fmt.Sprintf("binary format version %d is not supported; record skipped", version),
			})
			result.Skipped++
			offset += recordLen
			continue
		}

		end := start + recordLen
		event, parseErr := parseBinaryRecord(job.RelPath, data[start:end], start)
		if parseErr != nil {
			result.Issues = append(result.Issues, Issue{
				Code:   CodeParse,
				Source: job.RelPath,
				Offset: int64(start),
				Detail: parseErr.Error(),
			})
			result.Skipped++
			offset = end
			continue
		}
		versions[event.ParserVersion] = struct{}{}
		result.Events = append(result.Events, event)
		offset = end
	}

	for version := range versions {
		result.ParserVersions = append(result.ParserVersions, version)
	}
	return result
}

func parseBinaryRecord(source string, data []byte, base int) (Event, error) {
	if len(data) < binaryCommonHeader {
		return Event{}, fmt.Errorf("record shorter than common header")
	}
	version := data[4]
	var (
		headerSize    int
		kindByte      byte
		flags         uint16
		txid          uint32
		tick          uint64
		qnameLen      uint16
		answerCnt     uint16
		additionalCnt uint16
		bodyPos       int
	)
	switch version {
	case 1:
		headerSize = binaryV1Header
		if len(data) < headerSize {
			return Event{}, fmt.Errorf("v1 header truncated: need %d bytes, have %d", headerSize, len(data))
		}
		kindByte = data[9]
		flags = binary.BigEndian.Uint16(data[10:12])
		txid = binary.BigEndian.Uint32(data[12:16])
		tick = uint64(binary.BigEndian.Uint32(data[16:20]))
		qnameLen = binary.BigEndian.Uint16(data[20:22])
		answerCnt = binary.BigEndian.Uint16(data[22:24])
		additionalCnt = binary.BigEndian.Uint16(data[24:26])
		bodyPos = headerSize
	case 2:
		headerSize = binaryV2Header
		if len(data) < headerSize {
			return Event{}, fmt.Errorf("v2 header truncated: need %d bytes, have %d", headerSize, len(data))
		}
		txid = binary.LittleEndian.Uint32(data[9:13])
		kindByte = data[13]
		flags = binary.LittleEndian.Uint16(data[14:16])
		tick = binary.LittleEndian.Uint64(data[16:24])
		qnameLen = binary.LittleEndian.Uint16(data[24:26])
		answerCnt = binary.LittleEndian.Uint16(data[26:28])
		additionalCnt = binary.LittleEndian.Uint16(data[28:30])
		bodyPos = headerSize
	default:
		return Event{}, fmt.Errorf("unsupported binary version %d", version)
	}

	var kind string
	switch kindByte {
	case 0:
		kind = KindQuery
	case 1:
		kind = KindResponse
	case 2:
		kind = KindCache
	default:
		return Event{}, fmt.Errorf("unknown event kind byte %d", kindByte)
	}
	if bodyPos+int(qnameLen) > len(data) {
		return Event{}, fmt.Errorf("question name length %d exceeds record", qnameLen)
	}
	qnameWireEnd := bodyPos + int(qnameLen)
	name, nameWire, consumed, err := ParseWireName(data[bodyPos:qnameWireEnd])
	if err != nil {
		return Event{}, fmt.Errorf("question name: %w", err)
	}
	if consumed != int(qnameLen) {
		return Event{}, fmt.Errorf("question name length declares %d bytes, parsed %d", qnameLen, consumed)
	}
	bodyPos = qnameWireEnd

	parseRecord := func(section string) (Record, error) {
		recordStart := bodyPos
		recordName, recordWire, used, err := ParseWireName(data[bodyPos:])
		if err != nil {
			return Record{}, fmt.Errorf("%s record name at byte %d: %w", section, bodyPos, err)
		}
		bodyPos += used
		var typeCode uint16
		var class uint16
		var ttl uint32
		var rdlen uint16
		if version == 1 {
			if bodyPos+10 > len(data) {
				return Record{}, fmt.Errorf("%s record v1 header truncated at byte %d", section, bodyPos)
			}
			typeCode = binary.BigEndian.Uint16(data[bodyPos : bodyPos+2])
			class = binary.BigEndian.Uint16(data[bodyPos+2 : bodyPos+4])
			ttl = binary.BigEndian.Uint32(data[bodyPos+4 : bodyPos+8])
			rdlen = binary.BigEndian.Uint16(data[bodyPos+8 : bodyPos+10])
			bodyPos += 10
		} else {
			if bodyPos+10 > len(data) {
				return Record{}, fmt.Errorf("%s record v2 header truncated at byte %d", section, bodyPos)
			}
			typeCode = binary.LittleEndian.Uint16(data[bodyPos : bodyPos+2])
			class = binary.LittleEndian.Uint16(data[bodyPos+2 : bodyPos+4])
			ttl = binary.LittleEndian.Uint32(data[bodyPos+4 : bodyPos+8])
			rdlen = binary.LittleEndian.Uint16(data[bodyPos+8 : bodyPos+10])
			bodyPos += 10
		}
		if bodyPos+int(rdlen) > len(data) {
			return Record{}, fmt.Errorf("%s record rdata length %d exceeds record at byte %d", section, rdlen, bodyPos)
		}
		rdata := data[bodyPos : bodyPos+int(rdlen)]
		bodyPos += int(rdlen)
		record := Record{
			Name:     recordName,
			NameWire: HexBytes(recordWire),
			Type:     TypeName(typeCode),
			Class:    class,
			TTL:      ttl,
			Offset:   int64(base + recordStart),
			Section:  section,
		}
		record.Address = AddressFromRData(typeCode, rdata)
		if record.Address == "" && len(rdata) > 0 {
			record.RDataHex = HexBytes(rdata)
		}
		return record, nil
	}

	answers := make([]Record, 0, answerCnt)
	for i := 0; i < int(answerCnt); i++ {
		record, recErr := parseRecord("answer")
		if recErr != nil {
			return Event{}, recErr
		}
		answers = append(answers, record)
	}
	additionals := make([]Record, 0, additionalCnt)
	for i := 0; i < int(additionalCnt); i++ {
		record, recErr := parseRecord("additional")
		if recErr != nil {
			return Event{}, recErr
		}
		additionals = append(additionals, record)
	}
	if bodyPos != len(data) {
		return Event{}, fmt.Errorf("record has %d trailing bytes after declared sections", len(data)-bodyPos)
	}

	return Event{
		Source:        source,
		Format:        FormatBinary,
		ParserVersion: fmt.Sprintf("v%d", version),
		Kind:          kind,
		TxID:          txid,
		Flags:         flags,
		Name:          name,
		NameWire:      HexBytes(nameWire),
		Tick:          tick,
		Time:          SampleLabel(tick),
		Offset:        int64(base),
		Answers:       answers,
		Additionals:   additionals,
	}, nil
}

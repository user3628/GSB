package workbench

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func parseTextFile(job sourceJob) SessionResult {
	result := SessionResult{Source: job.RelPath, Format: FormatText}
	data, err := os.ReadFile(job.AbsPath)
	if err != nil {
		result.Issues = append(result.Issues, Issue{
			Code: CodeSourceOpen, Source: job.RelPath, Detail: err.Error(),
		})
		result.Skipped = 1
		return result
	}
	result.Bytes = int64(len(data))
	versions := make(map[string]struct{})
	pos := 0
	for pos < len(data) {
		for pos < len(data) && (data[pos] == '\n' || data[pos] == '\r') {
			pos++
		}
		if pos >= len(data) {
			break
		}
		if job.MaxEvents > 0 && len(result.Events) >= job.MaxEvents {
			result.Issues = append(result.Issues, Issue{
				Code:   CodeEventBudget,
				Source: job.RelPath,
				Offset: int64(pos),
				Detail: fmt.Sprintf("event budget %d reached; partial events retained", job.MaxEvents),
			})
			result.Skipped++
			break
		}
		blockStart := pos
		digitStart := pos
		for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
			pos++
		}
		if pos == digitStart {
			result.Issues = append(result.Issues, Issue{
				Code:   CodeParse,
				Source: job.RelPath,
				Offset: int64(blockStart),
				Detail: "text block length prefix is missing",
			})
			result.Skipped++
			break
		}
		if pos >= len(data) || data[pos] != ':' {
			result.Issues = append(result.Issues, Issue{
				Code:   CodeParse,
				Source: job.RelPath,
				Offset: int64(blockStart),
				Detail: "text block length prefix is not followed by ':'",
			})
			result.Skipped++
			break
		}
		lengthText := string(data[digitStart:pos])
		lengthValue, parseErr := strconv.ParseUint(lengthText, 10, 64)
		if parseErr != nil {
			result.Issues = append(result.Issues, Issue{
				Code:   CodeParse,
				Source: job.RelPath,
				Offset: int64(blockStart),
				Detail: fmt.Sprintf("invalid text block length %q", lengthText),
			})
			result.Skipped++
			break
		}
		payloadStart := pos + 1
		if lengthValue > uint64(len(data)-payloadStart) {
			result.Issues = append(result.Issues, Issue{
				Code:   CodeLengthOverflow,
				Source: job.RelPath,
				Offset: int64(blockStart),
				Detail: fmt.Sprintf("text block length %d exceeds %d remaining bytes", lengthValue, len(data)-payloadStart),
			})
			result.Skipped++
			break
		}
		payloadEnd := payloadStart + int(lengthValue)
		payload := string(data[payloadStart:payloadEnd])
		event, localErr := parseTextBlock(job.RelPath, payload, int64(blockStart), int64(payloadStart))
		if localErr != nil {
			result.Issues = append(result.Issues, Issue{
				Code:   localErr.code,
				Source: job.RelPath,
				Offset: localErr.offset,
				Detail: localErr.detail,
			})
			result.Skipped++
		} else {
			versions[event.ParserVersion] = struct{}{}
			result.Events = append(result.Events, event)
		}
		pos = payloadEnd
		if pos < len(data) && data[pos] == '\r' {
			pos++
		}
		if pos < len(data) && data[pos] == '\n' {
			pos++
		}
	}
	for version := range versions {
		result.ParserVersions = append(result.ParserVersions, version)
	}
	return result
}

func parseTextBlock(source string, payload string, blockStart, payloadStart int64) (Event, *localParseError) {
	fields := make(map[string]string)
	for _, part := range strings.Split(payload, "|") {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			return Event{}, &localParseError{
				code: CodeParse, offset: payloadStart, detail: fmt.Sprintf("invalid text field %q", part),
			}
		}
		key = strings.ToLower(strings.TrimSpace(key))
		fields[key] = value
	}
	get := func(keys ...string) string {
		for _, key := range keys {
			if value, ok := fields[key]; ok {
				return value
			}
		}
		return ""
	}
	txidRaw := get("txid", "transaction_id", "id")
	txid, err := ParseTxID(txidRaw)
	if err != nil {
		return Event{}, &localParseError{code: CodeParse, offset: payloadStart, detail: err.Error()}
	}
	kindRaw := get("kind", "event")
	kind, err := ParseKind(kindRaw)
	if err != nil {
		return Event{}, &localParseError{code: CodeParse, offset: payloadStart, detail: err.Error()}
	}
	flags := uint16(0)
	if raw := get("flags"); raw != "" {
		value, parseErr := parseFlexUint32(raw)
		if parseErr != nil || value > 0xffff {
			return Event{}, &localParseError{code: CodeParse, offset: payloadStart, detail: fmt.Sprintf("invalid flags %q", raw)}
		}
		flags = uint16(value)
	}
	sampleRaw := get("sample", "time")
	if sampleRaw == "" {
		sampleRaw = "S000000"
	}
	tick, timeLabel, err := ParseSampleMarker(sampleRaw)
	if err != nil {
		return Event{}, &localParseError{code: CodeParse, offset: payloadStart, detail: err.Error()}
	}
	version := get("version", "parser_version")
	if version == "" {
		version = "v1"
	}
	if version != "v1" && version != "v2" {
		return Event{}, &localParseError{
			code:   CodeVersionUnsupported,
			offset: blockStart,
			detail: fmt.Sprintf("text parser version %q is unsupported", version),
		}
	}
	nameRaw := get("name", "qname", "question")
	canonical, nameWire, err := NormalizeName(nameRaw)
	if err != nil {
		return Event{}, &localParseError{code: CodeParse, offset: payloadStart, detail: err.Error()}
	}
	answers, recErr := parseTextRecords(source, get("answers", "answer"), canonical, "answer", payload, payloadStart)
	if recErr != nil {
		return Event{}, recErr
	}
	additionalsRaw := get("additionals", "additional", "authority")
	additionals, recErr := parseTextRecords(source, additionalsRaw, canonical, "additional", payload, payloadStart)
	if recErr != nil {
		return Event{}, recErr
	}
	return Event{
		Source:        source,
		Format:        FormatText,
		ParserVersion: version,
		Kind:          kind,
		TxID:          txid,
		Flags:         flags,
		Name:          canonical,
		NameWire:      HexBytes(nameWire),
		Tick:          tick,
		Time:          timeLabel,
		Offset:        blockStart,
		Answers:       answers,
		Additionals:   additionals,
	}, nil
}

func parseTextRecords(source, raw, question, section, payload string, payloadStart int64) ([]Record, *localParseError) {
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ";")
	records := make([]Record, 0, len(parts))
	searchFrom := 0
	for index, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		values := strings.Split(part, ",")
		if len(values) != 4 && len(values) != 5 {
			return nil, &localParseError{
				code: CodeParse, offset: payloadStart, detail: fmt.Sprintf("%s record %d must have 4 or 5 comma-separated fields", section, index),
			}
		}
		recordName := strings.TrimSpace(values[0])
		if recordName == "" {
			recordName = question
		}
		name, nameWire, err := NormalizeName(recordName)
		if err != nil {
			return nil, &localParseError{code: CodeParse, offset: payloadStart, detail: err.Error()}
		}
		typeCode, err := TypeCode(strings.TrimSpace(values[1]))
		if err != nil {
			return nil, &localParseError{code: CodeParse, offset: payloadStart, detail: err.Error()}
		}
		class := uint16(1)
		ttlIndex := 2
		addressIndex := 3
		if len(values) == 5 {
			classValue, classErr := parseFlexUint32(strings.TrimSpace(values[2]))
			if classErr != nil || classValue > 0xffff {
				return nil, &localParseError{code: CodeParse, offset: payloadStart, detail: fmt.Sprintf("invalid class %q", values[2])}
			}
			class = uint16(classValue)
			ttlIndex = 3
			addressIndex = 4
		}
		ttlValue, err := parseFlexUint32(strings.TrimSpace(values[ttlIndex]))
		if err != nil {
			return nil, &localParseError{code: CodeParse, offset: payloadStart, detail: fmt.Sprintf("invalid TTL %q", values[ttlIndex])}
		}
		address := strings.TrimSpace(values[addressIndex])
		if IsAddressType(TypeName(typeCode)) && address == "" {
			return nil, &localParseError{code: CodeParse, offset: payloadStart, detail: "address record has empty address"}
		}
		if address != "" {
			address, err = NormalizeAddress(address)
			if err != nil {
				return nil, &localParseError{code: CodeParse, offset: payloadStart, detail: err.Error()}
			}
		}
		relative := strings.Index(payload[searchFrom:], part)
		recordOffset := payloadStart
		if relative >= 0 {
			recordOffset += int64(searchFrom + relative)
			searchFrom += relative + len(part)
		}
		records = append(records, Record{
			Name:     name,
			NameWire: HexBytes(nameWire),
			Type:     TypeName(typeCode),
			Class:    class,
			TTL:      ttlValue,
			Address:  address,
			Offset:   recordOffset,
			Section:  section,
		})
	}
	return records, nil
}

func parseFlexUint32(raw string) (uint32, error) {
	s := strings.TrimSpace(raw)
	base := 10
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		base = 16
		s = s[2:]
	}
	value, err := strconv.ParseUint(s, base, 32)
	if err != nil {
		return 0, err
	}
	return uint32(value), nil
}

package workbench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
)

type jsonRecord struct {
	Name    string          `json:"name"`
	Type    json.RawMessage `json:"type"`
	Class   json.RawMessage `json:"class"`
	TTL     json.RawMessage `json:"ttl"`
	Address string          `json:"address"`
}

type jsonEvent struct {
	Event         string          `json:"event"`
	Kind          string          `json:"kind"`
	TxID          json.RawMessage `json:"txid"`
	Flags         json.RawMessage `json:"flags"`
	ParserVersion string          `json:"parser_version"`
	Sample        json.RawMessage `json:"sample"`
	Time          json.RawMessage `json:"time"`
	Name          string          `json:"name"`
	QName         string          `json:"qname"`
	Answers       []jsonRecord    `json:"answers"`
	Additionals   []jsonRecord    `json:"additionals"`
	Additional    []jsonRecord    `json:"additional"`
}

func parseJSONFile(job sourceJob) SessionResult {
	result := SessionResult{Source: job.RelPath, Format: FormatJSON}
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

	lineStart := 0
	lineNumber := 0
	for lineStart < len(data) {
		lineEnd := bytes.IndexByte(data[lineStart:], '\n')
		if lineEnd < 0 {
			lineEnd = len(data)
		} else {
			lineEnd += lineStart
		}
		line := data[lineStart:lineEnd]
		lineNumber++
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			lineStart = lineEnd + 1
			continue
		}
		if job.MaxEvents > 0 && len(result.Events) >= job.MaxEvents {
			result.Issues = append(result.Issues, Issue{
				Code:   CodeEventBudget,
				Source: job.RelPath,
				Offset: int64(lineStart),
				Detail: fmt.Sprintf("event budget %d reached at JSON line %d; partial events retained", job.MaxEvents, lineNumber),
			})
			result.Skipped++
			break
		}
		event, parseErr := parseJSONLine(job.RelPath, line, int64(lineStart), lineNumber)
		if parseErr != nil {
			code := CodeParse
			if parseErr.code != "" {
				code = parseErr.code
			}
			result.Issues = append(result.Issues, Issue{
				Code:   code,
				Source: job.RelPath,
				Offset: int64(lineStart) + parseErr.offset,
				Detail: fmt.Sprintf("JSON line %d: %s", lineNumber, parseErr.detail),
			})
			result.Skipped++
			lineStart = lineEnd + 1
			continue
		}
		versions[event.ParserVersion] = struct{}{}
		result.Events = append(result.Events, event)
		lineStart = lineEnd + 1
	}

	for version := range versions {
		result.ParserVersions = append(result.ParserVersions, version)
	}
	return result
}

type localParseError struct {
	code   string
	offset int64
	detail string
}

func parseJSONLine(source string, line []byte, base int64, lineNumber int) (Event, *localParseError) {
	var raw jsonEvent
	if err := json.Unmarshal(line, &raw); err != nil {
		return Event{}, &localParseError{code: CodeParse, detail: err.Error()}
	}
	version := raw.ParserVersion
	if version == "" {
		version = "v1"
	}
	if version != "v1" && version != "v2" {
		return Event{}, &localParseError{
			code:   CodeVersionUnsupported,
			detail: fmt.Sprintf("json parser version %q is unsupported", version),
		}
	}
	kindRaw := raw.Event
	if kindRaw == "" {
		kindRaw = raw.Kind
	}
	if kindRaw == "" {
		return Event{}, &localParseError{code: CodeParse, detail: "missing event/kind"}
	}
	kind, err := ParseKind(kindRaw)
	if err != nil {
		return Event{}, &localParseError{code: CodeParse, detail: err.Error()}
	}
	if len(raw.TxID) == 0 {
		return Event{}, &localParseError{code: CodeParse, detail: "missing txid"}
	}
	txid, err := ParseUint32Raw(raw.TxID)
	if err != nil {
		return Event{}, &localParseError{code: CodeParse, detail: fmt.Sprintf("txid: %v", err)}
	}
	var flags uint16
	if len(raw.Flags) > 0 {
		flags, err = ParseUint16Raw(raw.Flags)
		if err != nil {
			return Event{}, &localParseError{code: CodeParse, detail: fmt.Sprintf("flags: %v", err)}
		}
	}
	sampleRaw := raw.Sample
	if len(sampleRaw) == 0 {
		sampleRaw = raw.Time
	}
	tick, timeLabel, err := ParseSampleRaw(sampleRaw)
	if err != nil {
		return Event{}, &localParseError{code: CodeParse, detail: err.Error()}
	}
	nameRaw := raw.Name
	if nameRaw == "" {
		nameRaw = raw.QName
	}
	canonical, nameWire, err := NormalizeName(nameRaw)
	if err != nil {
		return Event{}, &localParseError{code: CodeParse, detail: err.Error()}
	}
	answers, recErr := parseJSONRecords(line, base, 0, canonical, "answer", raw.Answers)
	if recErr != nil {
		return Event{}, recErr
	}
	raw.Additionals = append(raw.Additionals, raw.Additional...)
	additionals, recErr := parseJSONRecords(line, base, len(answers), canonical, "additional", raw.Additionals)
	if recErr != nil {
		return Event{}, recErr
	}
	return Event{
		Source:        source,
		Format:        FormatJSON,
		ParserVersion: version,
		Kind:          kind,
		TxID:          txid,
		Flags:         flags,
		Name:          canonical,
		NameWire:      HexBytes(nameWire),
		Tick:          tick,
		Time:          timeLabel,
		Offset:        base,
		Answers:       answers,
		Additionals:   additionals,
	}, nil
}

func parseJSONRecords(line []byte, base int64, offsetIndex int, question string, section string, records []jsonRecord) ([]Record, *localParseError) {
	parsed := make([]Record, 0, len(records))
	for i, input := range records {
		recordName := input.Name
		if recordName == "" {
			recordName = question
		}
		name, nameWire, err := NormalizeName(recordName)
		if err != nil {
			return nil, &localParseError{code: CodeParse, detail: fmt.Sprintf("%s record %d name: %v", section, i, err)}
		}
		if len(input.Type) == 0 {
			return nil, &localParseError{code: CodeParse, detail: fmt.Sprintf("%s record %d missing type", section, i)}
		}
		var typeText string
		if err := json.Unmarshal(input.Type, &typeText); err != nil {
			var typeNumber uint16
			if numberErr := json.Unmarshal(input.Type, &typeNumber); numberErr != nil {
				return nil, &localParseError{code: CodeParse, detail: fmt.Sprintf("%s record %d type: %v", section, i, err)}
			}
			typeText = fmt.Sprintf("%d", typeNumber)
		}
		typeCode, err := TypeCode(typeText)
		if err != nil {
			return nil, &localParseError{code: CodeParse, detail: fmt.Sprintf("%s record %d: %v", section, i, err)}
		}
		class := uint16(1)
		if len(input.Class) > 0 {
			class, err = ParseUint16Raw(input.Class)
			if err != nil {
				return nil, &localParseError{code: CodeParse, detail: fmt.Sprintf("%s record %d class: %v", section, i, err)}
			}
		}
		if len(input.TTL) == 0 {
			return nil, &localParseError{code: CodeParse, detail: fmt.Sprintf("%s record %d missing ttl", section, i)}
		}
		ttl, err := ParseUint32Raw(input.TTL)
		if err != nil {
			return nil, &localParseError{code: CodeParse, detail: fmt.Sprintf("%s record %d ttl: %v", section, i, err)}
		}
		address := ""
		if input.Address != "" {
			address, err = NormalizeAddress(input.Address)
			if err != nil {
				return nil, &localParseError{code: CodeParse, detail: fmt.Sprintf("%s record %d address: %v", section, i, err)}
			}
		}
		recordOffset := base + int64(findNthJSONKey(line, "address", offsetIndex+i))
		parsed = append(parsed, Record{
			Name:     name,
			NameWire: HexBytes(nameWire),
			Type:     TypeName(typeCode),
			Class:    class,
			TTL:      ttl,
			Address:  address,
			Offset:   recordOffset,
			Section:  section,
		})
	}
	return parsed, nil
}

func findNthJSONKey(line []byte, key string, occurrence int) int {
	needle := []byte(`"` + key + `"`)
	pos := 0
	found := 0
	for {
		idx := bytes.Index(line[pos:], needle)
		if idx < 0 {
			return pos
		}
		pos += idx + len(needle)
		if found == occurrence {
			return pos
		}
		found++
	}
}

func NormalizeAddress(input string) (string, error) {
	addr, err := netip.ParseAddr(input)
	if err != nil {
		return "", err
	}
	return addr.String(), nil
}

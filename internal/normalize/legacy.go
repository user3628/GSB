package normalize

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"

	"auditdrift/internal/model"
)

const LegacyHeader = "AUDITDRIFT-LPB/1\n"

type LegacyRecord struct {
	Observation     model.Observation
	TimestampMicros int64
	Collector       string
}

func EncodeLegacyBlock(records []LegacyRecord) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(LegacyHeader)
	for _, record := range records {
		obs := record.Observation
		obs.Actual = model.CanonicalValue(obs.Actual)
		var fields []string
		fields = appendField(fields, "h", obs.Host)
		fields = appendField(fields, "b", obs.BaselineID)
		fields = appendField(fields, "ts_us", strconv.FormatInt(record.TimestampMicros, 10))
		switch obs.Actual.Kind {
		case model.KindNumber:
			fields = appendField(fields, "kind", "n")
			fields = appendField(fields, "value", strconv.FormatInt(obs.Actual.Number, 10))
		case model.KindSet:
			fields = appendField(fields, "kind", "set")
			fields = appendField(fields, "value", strings.Join(obs.Actual.Set, "\x1f"))
		case model.KindString, "":
			fields = appendField(fields, "kind", "s")
			fields = appendField(fields, "value", obs.Actual.Text)
		default:
			return nil, model.NewCodedError(model.ErrValueTypeMismatch, "unsupported actual kind %q", obs.Actual.Kind)
		}
		if obs.OptionalNote != "" {
			fields = appendField(fields, "note", obs.OptionalNote)
		}
		if record.Collector != "" {
			fields = appendField(fields, "collector", record.Collector)
		}
		inner := strings.Join(fields, "")
		buf.WriteString(strconv.Itoa(len(inner)) + ":" + inner)
	}
	return buf.Bytes(), nil
}

func appendField(frames []string, key, value string) []string {
	return append(frames, fieldFrame(key, value))
}

func fieldFrame(key, value string) string {
	payload := key + "=" + value
	return strconv.Itoa(len(payload)) + ":" + payload
}

func ParseLegacyBlock(r io.Reader, rule model.RoundingRule) (model.ParseResult, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return model.ParseResult{}, err
	}
	if !bytes.HasPrefix(data, []byte(LegacyHeader)) {
		return model.ParseResult{}, model.NewCodedError(model.ErrBaselineInvalid, "legacy block has invalid header")
	}
	pos := len(LegacyHeader)
	result := model.ParseResult{Rounding: []model.RoundingInfo{}}
	var cumulative int64
	recordNo := 0
	for pos < len(data) {
		recordNo++
		outer, next, err := readLengthPrefixed(data, pos)
		if err != nil {
			return model.ParseResult{}, model.NewCodedError(model.ErrValueTypeMismatch, "legacy record %d: %v", recordNo, err)
		}
		pos = next
		innerFields, err := parseFieldFrames(outer)
		if err != nil {
			return model.ParseResult{}, model.NewCodedError(model.ErrValueTypeMismatch, "legacy record %d fields: %v", recordNo, err)
		}
		host := firstField(innerFields, "h", "host")
		baselineID := firstField(innerFields, "b", "baseline_id")
		tsText := firstField(innerFields, "ts_us", "timestamp_us")
		if host == "" || baselineID == "" || tsText == "" {
			return model.ParseResult{}, model.NewCodedError(model.ErrValueTypeMismatch, "legacy record %d: required host, baseline_id and timestamp_us fields", recordNo)
		}
		tsUS, err := strconv.ParseInt(tsText, 10, 64)
		if err != nil {
			return model.ParseResult{}, model.NewCodedError(model.ErrValueTypeMismatch, "legacy record %d: invalid timestamp %q", recordNo, tsText)
		}
		kind := firstField(innerFields, "kind", "value_kind")
		valueText := firstField(innerFields, "value", "v")
		value, err := parseLegacyValue(kind, valueText, innerFields["item"])
		if err != nil {
			return model.ParseResult{}, model.NewCodedError(model.ErrValueTypeMismatch, "legacy record %d (%s): %v", recordNo, baselineID, err)
		}
		rounding := newMicrosRounding(host, baselineID, "legacy_lpb", tsUS, rule, &cumulative)
		result.Rounding = append(result.Rounding, rounding)
		result.Observations = append(result.Observations, model.Observation{
			Host:         host,
			BaselineID:   baselineID,
			Actual:       model.CanonicalValue(value),
			SampledAtMS:  rounding.NormalizedTimestampMS,
			OptionalNote: firstField(innerFields, "note", "optional_note"),
		})
	}
	return result, nil
}

func readLengthPrefixed(data []byte, pos int) ([]byte, int, error) {
	colon := bytes.IndexByte(data[pos:], ':')
	if colon < 0 {
		return nil, pos, fmt.Errorf("missing length prefix separator")
	}
	colon += pos
	lengthText := string(data[pos:colon])
	length, err := strconv.Atoi(lengthText)
	if err != nil || length < 0 {
		return nil, pos, fmt.Errorf("invalid length prefix %q", lengthText)
	}
	start := colon + 1
	end := start + length
	if end > len(data) {
		return nil, pos, fmt.Errorf("length prefix %d exceeds remaining block", length)
	}
	return data[start:end], end, nil
}

func parseFieldFrames(payload []byte) (map[string][]string, error) {
	fields := make(map[string][]string)
	pos := 0
	for pos < len(payload) {
		segment, next, err := readLengthPrefixed(payload, pos)
		if err != nil {
			return nil, err
		}
		pos = next
		text := string(segment)
		eq := strings.IndexByte(text, '=')
		if eq < 0 {
			return nil, fmt.Errorf("field segment %q has no =", text)
		}
		key := text[:eq]
		value := text[eq+1:]
		fields[key] = append(fields[key], value)
	}
	return fields, nil
}

func firstField(fields map[string][]string, keys ...string) string {
	for _, key := range keys {
		if values := fields[key]; len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

func parseLegacyValue(kind, text string, repeatedItems []string) (model.Value, error) {
	switch kind {
	case "n", "number":
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return model.Value{}, fmt.Errorf("invalid number %q", text)
		}
		return model.Value{Kind: model.KindNumber, Number: n}, nil
	case "set":
		items := repeatedItems
		if len(items) == 0 && text != "" {
			items = strings.Split(text, "\x1f")
		}
		return model.Value{Kind: model.KindSet, Set: items}, nil
	case "s", "string", "":
		return model.Value{Kind: model.KindString, Text: text}, nil
	default:
		return model.Value{}, fmt.Errorf("unsupported legacy kind %q", kind)
	}
}

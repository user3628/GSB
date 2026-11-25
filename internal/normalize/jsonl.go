package normalize

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"auditdrift/internal/model"
)

type jsonLine struct {
	Host          string          `json:"host"`
	BaselineID    string          `json:"baseline_id"`
	BaselineIDAlt string          `json:"baselineId"`
	Actual        json.RawMessage `json:"actual"`
	ActualValue   json.RawMessage `json:"actual_value"`
	SampledAtMS   int64           `json:"sampled_at_ms"`
	TimestampMS   int64           `json:"timestamp_ms"`
	OptionalNote  string          `json:"optional_note"`
}

func ParseJSONL(r io.Reader) (model.ParseResult, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	result := model.ParseResult{Rounding: []model.RoundingInfo{}}
	var cumulative int64
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var dto jsonLine
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		if err := dec.Decode(&dto); err != nil {
			return model.ParseResult{}, model.NewCodedError(model.ErrValueTypeMismatch, "jsonl line %d: invalid JSON: %v", lineNo, err)
		}
		baselineID := dto.BaselineID
		if baselineID == "" {
			baselineID = dto.BaselineIDAlt
		}
		rawActual := dto.Actual
		if len(rawActual) == 0 {
			rawActual = dto.ActualValue
		}
		value, err := ParseJSONValue(rawActual)
		if err != nil {
			return model.ParseResult{}, model.NewCodedError(model.ErrValueTypeMismatch, "jsonl line %d (%s): %v", lineNo, baselineID, err)
		}
		ts := dto.SampledAtMS
		if ts == 0 {
			ts = dto.TimestampMS
		}
		obs := model.CanonicalValue(value)
		result.Observations = append(result.Observations, model.Observation{
			Host:         dto.Host,
			BaselineID:   baselineID,
			Actual:       obs,
			SampledAtMS:  ts,
			OptionalNote: dto.OptionalNote,
		})
		result.Rounding = append(result.Rounding, newMillisRounding(dto.Host, baselineID, "jsonl", ts, &cumulative))
	}
	if err := scanner.Err(); err != nil {
		return model.ParseResult{}, err
	}
	return result, nil
}

func ParseJSONValue(raw json.RawMessage) (model.Value, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return model.Value{}, fmt.Errorf("actual value is required")
	}
	switch trimmed[0] {
	case '"':
		var text string
		if err := json.Unmarshal(trimmed, &text); err != nil {
			return model.Value{}, err
		}
		return model.Value{Kind: model.KindString, Text: text}, nil
	case '[':
		var items []string
		if err := json.Unmarshal(trimmed, &items); err != nil {
			var anyItems []any
			if err2 := json.Unmarshal(trimmed, &anyItems); err2 != nil {
				return model.Value{}, fmt.Errorf("unsupported set value: %w", err2)
			}
			items = make([]string, 0, len(anyItems))
			for _, item := range anyItems {
				items = append(items, fmt.Sprint(item))
			}
		}
		return model.Value{Kind: model.KindSet, Set: items}, nil
	case '{':
		var object struct {
			Kind   string   `json:"kind"`
			Number int64    `json:"number"`
			Text   string   `json:"text"`
			Set    []string `json:"set"`
		}
		dec := json.NewDecoder(bytes.NewReader(trimmed))
		dec.UseNumber()
		if err := dec.Decode(&object); err != nil {
			return model.Value{}, err
		}
		switch model.ValueKind(object.Kind) {
		case model.KindNumber:
			return model.Value{Kind: model.KindNumber, Number: object.Number}, nil
		case model.KindSet:
			return model.Value{Kind: model.KindSet, Set: object.Set}, nil
		case model.KindString:
			return model.Value{Kind: model.KindString, Text: object.Text}, nil
		default:
			return model.Value{}, fmt.Errorf("unsupported value kind %q", object.Kind)
		}
	default:
		var number json.Number
		if err := json.Unmarshal(trimmed, &number); err == nil {
			if n, err := number.Int64(); err == nil {
				return model.Value{Kind: model.KindNumber, Number: n}, nil
			}
			if f, err := number.Float64(); err == nil {
				return model.Value{}, fmt.Errorf("non-integer numeric value %v is not supported by fixed-point engine", f)
			}
		}
		var boolean bool
		if err := json.Unmarshal(trimmed, &boolean); err == nil {
			return model.Value{Kind: model.KindString, Text: strconv.FormatBool(boolean)}, nil
		}
		return model.Value{}, fmt.Errorf("unsupported actual value %s", strings.TrimSpace(string(trimmed)))
	}
}

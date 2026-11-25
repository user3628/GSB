package domain

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
)

const Scale int64 = 1_000_000

type Measure struct {
	Present bool
	Scaled  int64
	Source  string
}

func ParseFixedPoint(raw json.RawMessage) (Measure, *Error) {
	if len(raw) == 0 || string(raw) == "null" {
		return Measure{}, nil
	}

	text := strings.TrimSpace(string(raw))
	if len(text) >= 2 && text[0] == '"' && text[len(text)-1] == '"' {
		var decoded string
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return Measure{}, invalidMeasure(text, "measure string is not valid JSON")
		}
		text = decoded
	} else if text == "true" || text == "false" || text == "{}" || text == "[]" {
		return Measure{}, invalidMeasure(text, "measure must be a decimal string or number")
	}

	if text == "" {
		return Measure{}, invalidMeasure(text, "measure is empty")
	}

	negative := false
	switch text[0] {
	case '-':
		negative = true
		text = text[1:]
	case '+':
		text = text[1:]
	}
	if text == "" {
		return Measure{}, invalidMeasure(string(raw), "measure has no digits")
	}

	whole, fraction, hasDot := strings.Cut(text, ".")
	if hasDot && strings.Contains(fraction, ".") {
		return Measure{}, invalidMeasure(string(raw), "measure has multiple decimal points")
	}
	if whole == "" && fraction == "" {
		return Measure{}, invalidMeasure(string(raw), "measure has no digits")
	}
	if whole != "" && !allDigits(whole) {
		return Measure{}, invalidMeasure(string(raw), "measure contains non-decimal characters")
	}
	if fraction != "" && !allDigits(fraction) {
		return Measure{}, invalidMeasure(string(raw), "measure contains non-decimal characters")
	}
	if len(fraction) > 6 {
		return Measure{}, invalidMeasure(string(raw), "measure has more than six fractional digits")
	}

	if whole == "" {
		whole = "0"
	}
	digits := whole + fraction + strings.Repeat("0", 6-len(fraction))
	if negative {
		digits = "-" + digits
	}
	scaled, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return Measure{}, invalidMeasure(string(raw), "measure is outside int64 scaled range")
	}
	return Measure{Present: true, Scaled: scaled, Source: string(raw)}, nil
}

func invalidMeasure(value, reason string) *Error {
	return NewError(CodeSnapshotInvalid, "measure is not a valid fixed-point value", map[string]any{
		"field":   "event.measure",
		"value":   value,
		"reason":  reason,
		"scale":   Scale,
		"maximum": 6,
	})
}

func allDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func FormatFixed(scaled int64) string {
	if scaled == 0 {
		return "0.000000"
	}
	negative := scaled < 0
	var magnitude uint64
	if negative {
		magnitude = uint64(-(scaled + 1)) + 1
	} else {
		magnitude = uint64(scaled)
	}
	whole := magnitude / uint64(Scale)
	fraction := magnitude % uint64(Scale)
	result := strconv.FormatUint(whole, 10) + "." + leftPad(strconv.FormatUint(fraction, 10), 6)
	if negative {
		return "-" + result
	}
	return result
}

func leftPad(value string, width int) string {
	if len(value) >= width {
		return value
	}
	return strings.Repeat("0", width-len(value)) + value
}

func CheckedAddInt64(left, right int64) (int64, bool) {
	if right > 0 && left > math.MaxInt64-right {
		return 0, false
	}
	if right < 0 && left < math.MinInt64-right {
		return 0, false
	}
	return left + right, true
}

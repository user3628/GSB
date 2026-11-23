package codec

import (
	"bytes"
	"strings"

	"meshmigrate/internal/model"
)

// Detect determines the source format. "auto" requires either the binary magic
// or a first non-whitespace JSON object byte.
func Detect(data []byte, hint string) (string, *Error) {
	hint = strings.ToLower(strings.TrimSpace(hint))
	switch hint {
	case model.FormatBinary:
		return model.FormatBinary, nil
	case model.FormatJSONL:
		return model.FormatJSONL, nil
	case "", model.FormatAuto:
	default:
		return "", &Error{Code: CodeFormatUnsupported, Message: "unknown format hint " + hint, Offset: 0}
	}
	if len(data) >= len(BinaryMagic) && bytes.Equal(data[:len(BinaryMagic)], []byte(BinaryMagic)) {
		return model.FormatBinary, nil
	}
	for i, b := range data {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		case '{':
			return model.FormatJSONL, nil
		default:
			return "", corrupt(int64(i), "input is neither binary magic %q nor a JSON object", BinaryMagic)
		}
	}
	return "", corrupt(0, "empty input")
}

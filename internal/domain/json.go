package domain

import (
	"bytes"
	"encoding/json"
	"io"
)

// CanonicalJSON is the only JSON representation used in snapshots and reports.
// encoding/json sorts struct fields by declaration order and map keys, while
// this helper pins escaping and trailing newline behavior.
func CanonicalJSON(v any) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func marshalJSON(v any) ([]byte, error) {
	return CanonicalJSON(v)
}

func DecodeJSON(r io.Reader, v any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

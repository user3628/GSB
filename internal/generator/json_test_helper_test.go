package generator_test

import "encoding/json"

func jsonUnmarshal(blob []byte, v any) error {
	return json.Unmarshal(blob, v)
}

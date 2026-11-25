package store

import "encoding/json"

func modelJSONUnmarshal(raw []byte, target any) error {
	return json.Unmarshal(raw, target)
}

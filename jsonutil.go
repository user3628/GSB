package ledger

import "encoding/json"

func decodeJSON(data []byte, target any) error {
	return json.Unmarshal(data, target)
}

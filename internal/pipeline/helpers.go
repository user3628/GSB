package pipeline

import (
	"encoding/json"
	"encoding/pem"
)

func jsonMarshal(v any) []byte {
	data, _ := json.Marshal(v)
	return data
}

func jsonUnmarshal(data []byte, v any) {
	_ = json.Unmarshal(data, v)
}

// decodeBlocks 把 PEM 串拆成独立 PEM 块字符串，保持原有顺序。
func decodeBlocks(pemStr string) []string {
	rest := []byte(pemStr)
	var blocks []string
	for {
		block, remaining := pem.Decode(rest)
		if block == nil {
			break
		}
		blocks = append(blocks, string(pem.EncodeToMemory(block)))
		rest = remaining
	}
	return blocks
}

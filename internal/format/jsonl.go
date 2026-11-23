package format

import (
	"bytes"
	"encoding/json"
	"fmt"

	"gsb/internal/gwerr"
)

// 分片 JSON 行格式：
//
//	第 1 行: {"_meta":{"format":"grt-jsonl","version":2,"shard":0,"shards":1}}
//	其余行:  {"model_id":...,"endpoint":...,"vram_budget":"8192Mi","concurrency_cap":8,
//	          "toolchain_version":"tc-1.4.2","base_time_us":...,"retention_us":...,"last_write_us":...}
//
// 时间戳为微秒；保留期为相对微秒数，须结合 base_time 换算绝对过期时刻。
// v1 旧版缺 toolchain_version，显存为低精度 "vram_gib" 浮点。
type jsonlMeta struct {
	Meta struct {
		Format  string `json:"format"`
		Version int    `json:"version"`
		Shard   int    `json:"shard"`
		Shards  int    `json:"shards"`
	} `json:"_meta"`
}

// jsonlRaw 是 JSON 行解码出的原始记录（微秒时间，未归一化）。
type jsonlRaw struct {
	ModelID          string   `json:"model_id"`
	Endpoint         string   `json:"endpoint"`
	VRAMBudget       string   `json:"vram_budget"`       // v2: "8192Mi"/"8Gi"
	VRAMGiB          *float64 `json:"vram_gib"`          // v1: 低精度浮点
	ConcurrencyCap   *uint32  `json:"concurrency_cap"`   // v1 可能缺省
	ToolchainVersion *string  `json:"toolchain_version"` // v1 缺省
	BaseTimeUs       *int64   `json:"base_time_us"`
	RetentionUs      *int64   `json:"retention_us"`
	LastWriteUs      *int64   `json:"last_write_us"`
}

// DetectJSONL 检测数据是否为 JSON 行格式，是则返回声明的版本。
func DetectJSONL(data []byte) (int, bool) {
	line, _, found := bytes.Cut(data, []byte("\n"))
	if !found {
		line = data
	}
	var meta jsonlMeta
	if err := json.Unmarshal(bytes.TrimSpace(line), &meta); err != nil {
		return 0, false
	}
	if meta.Meta.Format != FormatJSONL {
		return 0, false
	}
	return meta.Meta.Version, true
}

// decodeJSONLRecords 逐行解码，损坏时返回 E_RECORD_CORRUPT 并指明字节偏移。
func decodeJSONLRecords(data []byte, version int) ([]jsonlRaw, error) {
	lines := bytes.Split(data, []byte("\n"))
	off := 0
	var recs []jsonlRaw
	for i, line := range lines {
		start := off
		off += len(line) + 1
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			continue
		}
		if i == 0 && bytes.Contains(trimmed, []byte(`"_meta"`)) {
			continue
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(trimmed, &raw); err != nil {
			return nil, gwerr.Corrupt(int64(start), fmt.Sprintf("第 %d 行 JSON 解析失败: %v", i+1, err))
		}
		if _, ok := raw["unknown_required_field"]; ok {
			return nil, gwerr.New(gwerr.EFormatUnsupported,
				"记录包含未知必填字段 unknown_required_field",
				map[string]any{"format": FormatJSONL, "version": version, "offset": start})
		}
		var rec jsonlRaw
		if err := json.Unmarshal(trimmed, &rec); err != nil {
			return nil, gwerr.Corrupt(int64(start), fmt.Sprintf("第 %d 行字段解析失败: %v", i+1, err))
		}
		if rec.ModelID == "" || rec.Endpoint == "" {
			return nil, gwerr.Corrupt(int64(start), fmt.Sprintf("第 %d 行缺少 model_id 或 endpoint", i+1))
		}
		recs = append(recs, rec)
	}
	return recs, nil
}

// EncodeJSONL 以原生 v2 版本编码路由表为分片 JSON 行。
func EncodeJSONL(recs []RouteRecord) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, `{"_meta":{"format":%q,"version":%d,"shard":0,"shards":1}}`+"\n",
		FormatJSONL, NativeJSONLVersion)
	for _, rec := range recs {
		line := map[string]any{
			"model_id":          rec.ModelID,
			"endpoint":          rec.Endpoint,
			"vram_budget":       fmt.Sprintf("%dMi", rec.VRAMBytes/(1<<20)),
			"concurrency_cap":   rec.Concurrency,
			"toolchain_version": rec.Toolchain,
			"base_time_us":      rec.BaseTimeMs * 1000,
			"retention_us":      rec.RetentionMs * 1000,
			"last_write_us":     rec.LastWriteMs * 1000,
		}
		b, _ := json.Marshal(line)
		buf.Write(b)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

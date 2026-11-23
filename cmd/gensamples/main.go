// 生成确定性本地样本到 samples/ 目录。
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"gsb/internal/format"
	"gsb/internal/router"
)

const baseMs = 1735689600000 // 2025-01-01T00:00:00Z

func rec(id, endpoint string, vramMi uint64, conc uint32, tc string, dayOffset int64) format.RouteRecord {
	base := baseMs + dayOffset*86400000
	return format.RouteRecord{
		ModelID: id, Endpoint: endpoint, VRAMBytes: vramMi << 20,
		Concurrency: conc, Toolchain: tc,
		BaseTimeMs: base, RetentionMs: 72 * 3600 * 1000,
		ExpiresAtMs: base + 72*3600*1000, LastWriteMs: base + 60000,
	}
}

func write(path string, data []byte) {
	if err := os.WriteFile(path, data, 0o644); err != nil {
		panic(err)
	}
	fmt.Println("生成", path)
}

func writeJSON(path string, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	write(path, append(b, '\n'))
}

func main() {
	dir := "samples"
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		panic(err)
	}
	recs := []format.RouteRecord{
		rec("llm-alpha", "http://127.0.0.1:9001", 8192, 8, "tc-1.4.2", 0),
		rec("llm-beta", "http://127.0.0.1:9002", 16384, 4, "tc-1.4.2", 1),
		rec("vlm-gamma", "http://127.0.0.1:9001", 24576, 2, "tc-1.3.0", 2),
		rec("emb-delta", "http://127.0.0.1:9002", 4096, 16, "tc-1.4.0", 3),
	}

	// 模型清单
	writeJSON(filepath.Join(dir, "models.json"), map[string]any{
		"manifest_version": 1,
		"models": []map[string]any{
			{"model_id": "llm-alpha", "family": "llm", "params": "7B"},
			{"model_id": "llm-beta", "family": "llm", "params": "13B"},
			{"model_id": "vlm-gamma", "family": "vlm", "params": "9B"},
			{"model_id": "emb-delta", "family": "emb", "params": "1B"},
		},
	})

	// 原生 v2：二进制与 JSON 行各一份（语义等价）
	write(filepath.Join(dir, "routes_v2.bin"), format.EncodeBinary(recs, true))
	write(filepath.Join(dir, "routes_v2.jsonl"), format.EncodeJSONL(recs))

	// 旧版 v1：缺工具链版本、显存为低精度 float32 GiB
	gib := []float32{8, 16, 24, 4}
	write(filepath.Join(dir, "legacy_v1.bin"), format.EncodeBinaryV1(recs, gib, true))
	legacyJSONL := `{"_meta":{"format":"grt-jsonl","version":1,"shard":0,"shards":1}}
{"model_id":"llm-alpha","endpoint":"http://127.0.0.1:9001","vram_gib":8,"base_time_us":1735689600000123,"retention_us":259200000456,"last_write_us":1735689660000789}
{"model_id":"llm-beta","endpoint":"http://127.0.0.1:9002","vram_gib":16,"concurrency_cap":4,"base_time_us":1735776000000000,"retention_us":259200000000,"last_write_us":1735776060000000}
`
	write(filepath.Join(dir, "legacy_v1.jsonl"), []byte(legacyJSONL))

	// 高版本 v3：含未知必填字段
	future := `{"_meta":{"format":"grt-jsonl","version":3,"shard":0,"shards":1}}
{"model_id":"llm-zeta","endpoint":"http://127.0.0.1:9003","vram_budget":"8192Mi","concurrency_cap":8,"toolchain_version":"tc-9.9.9","base_time_us":1735689600000000,"retention_us":259200000000,"last_write_us":1735689660000000,"unknown_required_field":{"quant":"int4"}}
`
	write(filepath.Join(dir, "future_v3.jsonl"), []byte(future))

	// 损坏样本：CRC 校验失败 / 长度前缀不符
	good := format.EncodeBinary(recs, true)
	corrupt := append([]byte(nil), good...)
	corrupt[len(corrupt)/2] ^= 0xFF
	write(filepath.Join(dir, "corrupt_crc.bin"), corrupt)
	badlen := append([]byte(nil), good...)
	badlen[13] = 0xFF // 第一条记录长度前缀（小端高字节）→ 与实际长度不符
	write(filepath.Join(dir, "corrupt_len.bin"), badlen)

	// 溢出样本：显存预算越界
	overflow := `{"_meta":{"format":"grt-jsonl","version":2,"shard":0,"shards":1}}
{"model_id":"llm-huge","endpoint":"http://127.0.0.1:9001","vram_budget":"18014398509481984Gi","concurrency_cap":1,"toolchain_version":"tc-1.4.2","base_time_us":1735689600000000,"retention_us":259200000000,"last_write_us":1735689660000000}
`
	write(filepath.Join(dir, "overflow.jsonl"), []byte(overflow))

	// 冲突样本：与 override 后的 llm-alpha 同工具链、同端点、同最后写入时间但内容不同 → 平局拒绝
	conflict := `{"_meta":{"format":"grt-jsonl","version":2,"shard":0,"shards":1}}
{"model_id":"llm-alpha","endpoint":"http://127.0.0.1:9001","vram_budget":"12288Mi","concurrency_cap":6,"toolchain_version":"tc-2.0.0","base_time_us":1735689600000000,"retention_us":259200000000,"last_write_us":1735689660000000}
{"model_id":"llm-omega","endpoint":"http://127.0.0.1:9002","vram_budget":"2048Mi","concurrency_cap":32,"toolchain_version":"tc-1.4.2","base_time_us":1735689600000000,"retention_us":259200000000,"last_write_us":1735689660000000}
`
	write(filepath.Join(dir, "conflict.jsonl"), []byte(conflict))

	// 覆盖样本：更高工具链版本 → overwrite
	override := `{"_meta":{"format":"grt-jsonl","version":2,"shard":0,"shards":1}}
{"model_id":"llm-alpha","endpoint":"http://127.0.0.1:9001","vram_budget":"8192Mi","concurrency_cap":8,"toolchain_version":"tc-2.0.0","base_time_us":1735689600000000,"retention_us":259200000000,"last_write_us":1735689660000000}
`
	write(filepath.Join(dir, "override.jsonl"), []byte(override))

	// 快照样本：seq=1 合法；seq=0 违反单调；format_version=99 不兼容
	snapRecs := []format.RouteRecord{
		rec("llm-alpha", "http://127.0.0.1:9002", 8192, 8, "tc-2.1.0", 10),
		rec("llm-sigma", "http://127.0.0.1:9001", 6144, 8, "tc-2.1.0", 10),
	}
	writeJSON(filepath.Join(dir, "snapshot_1.json"), router.SnapshotFile{
		Format: "grt-snapshot", FormatVersion: 2, SnapshotSeq: 1,
		GeneratedMs: baseMs + 10*86400000, Records: snapRecs,
	})
	writeJSON(filepath.Join(dir, "snapshot_stale.json"), router.SnapshotFile{
		Format: "grt-snapshot", FormatVersion: 2, SnapshotSeq: 0,
		GeneratedMs: baseMs, Records: snapRecs,
	})
	writeJSON(filepath.Join(dir, "snapshot_badver.json"), router.SnapshotFile{
		Format: "grt-snapshot", FormatVersion: 99, SnapshotSeq: 2,
		GeneratedMs: baseMs, Records: snapRecs,
	})
}

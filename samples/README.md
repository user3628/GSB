# 固定样本说明

这些文件由 `python scripts/make_samples.py` 确定性生成，基准时间为 `2024-01-01T00:00:00Z`（`1704067200000 ms`）。哈希与字节数记录在 `manifest.json`。

| 文件 | 用途 | 预期业务码/结论 |
|---|---|---|
| `binary_v2.bin` | 三路径完整 v2 二进制，含成功、timeout、refused | `accept` |
| `binary_big_endian_v2.bin` | 与 `binary_v2.bin` 等价的大端编码 | `accept` |
| `binary_v1_degraded.bin` | 旧版可选字段缺失 | `degrade_accept`，RTT/载荷缺省为 0 |
| `binary_bad_crc.bin` | 最后一个记录的 CRC 被翻转 | `E_FRAME_CORRUPT` |
| `binary_bad_length.bin` | 第一条记录长度前缀改为超大值 | `E_FRAME_CORRUPT` |
| `binary_v3_unsupported.bin` | 格式版本字节改为 3 | `E_FORMAT_UNSUPPORTED` |
| `jsonl_v2.jsonl` | 微秒时间戳；RTT 以 ms 声明；载荷以 KiB 声明 | `accept`，换算为 us/B |
| `jsonl_v1_degraded.jsonl` | v1 缺 RTT/载荷，含亚毫秒时间戳 | `degrade_accept` 与舍入审计 |
| `jsonl_unit_invalid.jsonl` | 负载荷值 | `E_UNIT_INVALID`，整批不落库 |
| `jsonl_v3_unsupported.jsonl` | 版本 3 | `E_FORMAT_UNSUPPORTED` |
| `jsonl_unknown_required.jsonl` | 声明未知必填特性 | `E_FORMAT_UNSUPPORTED` |

基准 21 条记录中：`local-edge-a` 有 8 条（1 timeout），`local-edge-b` 有 7 条（2 refused），`local-edge-c` 有 6 条（全部成功）。因此对 `local-edge-a` 的固定丢包率为 `1/8 = 12.50%`，排序分数最高路径为 `local-edge-b`。

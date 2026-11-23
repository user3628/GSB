# 固定样本

这些文件由 `meshpolicy samples -out samples` 确定性生成，不应手工修改 JSONL 行内容；修改后必须重新生成摘要。

| 文件 | 快照版本 | 说明 |
|---|---:|---|
| `v1-legacy.bin` | 1 | 二进制格式 v1；`checkout-rate` 缺少限流、熔断、窗口字段 |
| `v2-current.jsonl` | 2 | JSONL v2；含覆盖、跳过、新增和删除 |
| `v3-unknown-required.jsonl` | 3 | 格式 v3，声明未知必填特性 `geoAffinity` |
| `v4-duplicate-reject.jsonl` | 4 | 批内重复 `cart-circuit`，整批应拒绝 |
| `corrupt/v1-crc.bin` | - | 首记录 CRC32 损坏 |
| `corrupt/v1-length.bin` | - | 首记录长度前缀损坏 |
| `corrupt/v1-record-magic.bin` | - | 首记录 `MSPR` 魔数损坏 |
| `conflicts.json` | - | 端到端期望：顺序、决策、逐条冲突结局、最终版本 |
| `checksums.json` | - | SHA-256 校验和 |

## 导入顺序

1. 导入 `v1-legacy.bin`，期望 `downgrade_accept`，活动版本 1。
2. 导入 `v2-current.jsonl`，期望 `accept`，活动版本 2。
3. 导入 `v3-unknown-required.jsonl`，期望 `E_FORMAT_UNSUPPORTED`，活动版本仍 2。
4. 导入 `v4-duplicate-reject.jsonl`，期望 `E_BATCH_REJECTED`，活动版本仍 2。

`v2` 的期望冲突结局：

- `checkout-rate`：`overwrite`，revision 2 > 1。
- `catalog-timeout`：`overwrite`，revision 相同但 lastWriteMs 更晚。
- `legacy-report`：`skip`，旧 revision 3 > 新 revision 2，保留旧策略。
- `cart-circuit`：`overwrite`，新策略。
- `remove-me`：未出现在 v2，从新活动快照移除。

## 重新生成

```powershell
.\bin\meshpolicy.exe samples -out .\samples
```

```bash
bin/meshpolicy samples -out samples
```

重新生成后，运行 `scripts/run-e2e.sh` 或 `scripts/run-e2e.ps1` 复核。

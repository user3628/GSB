# 确定性样本说明

所有样本均为本地静态 JSON，不包含任何外部地址或账号信息。
快照摘要可随时通过 `python tools/fill_checksums.py` 重新计算
（`snapshot_bad_checksum_v2.json` 被工具刻意跳过，保留错误摘要）。

| 文件 | 类型 | 用途 |
| --- | --- | --- |
| `snapshot_v1.json` | 整份快照 v1 | 8 个资源的 4 层 DAG，正常主流程基线 |
| `increment_weight_auth.json` | 增量（base=1） | 修改 auth 权重与期望版本，级联影响 auth/order/shipment |
| `increment_add_risk.json` | 增量（base=2） | 新增 risk-control 并把 order 挂到其下游 |
| `increment_stale_base.json` | 增量（base=1） | 在版本推进后提交，验证 `E_STALE_BASE` 与并发竞争 |
| `increment_bad_weight.json` | 增量（base=1） | 权重 5000 越界，验证 `E_SPEC_INVALID` 且状态不变 |
| `snapshot_cycle_v9.json` | 整份快照 v9 | 制造 gateway→…→shipment→legacy-sync→gateway 环，另含独立根 standalone-report 用于部分分层输出 |
| `snapshot_bad_checksum_v2.json` | 整份快照 v2 | 故意错误摘要，验证 `E_SNAPSHOT_INVALID` 且旧快照可用 |

`snapshot_v1.json` 的摘要（规范化 SHA-256）：

```
e17388291a731672f0c07c73bfc96bfe939f966ccaf604fd28c3daae4b03ad64
```

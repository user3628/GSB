# 实际验证记录

验证日期：2026-11-21（Asia/Shanghai）

当前环境：Python 3.14.4、pytest 9.2.0。代码仅使用 Python 3.11 已具备的标准库能力；Windows 使用相同命令运行。

## 自动化测试

执行：

```bash
python3 -m pytest -q
```

结果：

```text
.......................                                                  [100%]
25 passed
```

覆盖点：

- 摘要篡改、版本回退、快照拒绝后活动状态不变。
- 取模、固定种子哈希、范围边界、空区间、IN、全分片扫描和非法键。
- 逻辑表引用环、算法引用环、重复物理表、范围重叠和空映射。
- 稳定排序、主键冲突裁决、全局去重、LIMIT/OFFSET 分页拼接。
- 过期基础版本、单表补丁、别名级联失效、全部缓存版本原子一致。
- 查询期间替换快照仍完成旧版本，不混用新映射。
- 并发重复请求只执行一次，重启后复用存储结果。
- 单写者竞争补丁时一个成功、另一个返回 `E_STALE_BASE`。
- HTTP 五类注入业务码互不相同，错误体结构一致。
- 真实启动子进程、跨表归并中 `os._exit(86)` 强杀、重启扫描意图、恢复报告与崩溃请求重放。
- 活动元数据指向损坏快照时，回退到版本最高的完整有效快照并拒绝已损坏版本查询。
- 补丁与缓存重建意图写入后、元数据提交前强杀，重启中止两类意图且保留旧快照。

## 完整演示

执行：

```bash
python3 scripts/demo.py
```

关键输出：

```text
V1 命中: commerce_db.orders_0003, shard=3, algorithm=mod
增量提交: base_version=1 -> active_version=2
重建缓存: orders, orders_view
复用缓存: catalog, events, order_items
V2 命中: commerce_db.orders_0002, shard=2, algorithm=hash
强制终止: exit_code=86, connection=RemoteDisconnected
恢复后: active_version=2, checksum 未变化
缓存布局等价: true
未完成查询: demo-crash-mid-merge -> ABORTED
崩溃请求重放: HTTP 503, E_EXECUTION_ABORTED
恢复报告: complete_exit_report_available=false
```

异常注入结果：

| 注入 | HTTP | 业务码 |
| --- | --- | --- |
| `invalid_shard_key` | 400 | `E_SHARD_KEY_INVALID` |
| `empty_physical_mapping` | 422 | `E_EMPTY_PHYSICAL_MAPPING` |
| `version_rollback` | 409 | `E_VERSION_ROLLBACK` |
| `range_overlap` | 409 | `E_ROUTE_CONFLICT` |
| `execution_interrupt` | 503 | `E_EXECUTION_ABORTED` |

恢复前后差异：

| 项目 | 强杀前 | 恢复后 |
| --- | --- | --- |
| 活动版本 | 2 | 2 |
| 活动摘要 | `ce317b59...9806f874b` | 相同 |
| 快照版本集合 | `[2, 1]` | `[2, 1]` |
| 缓存布局 | V2 全部逻辑表 | 等价 |
| 运行中查询批次 | - | 0 |
| 崩溃批次 | `demo-crash-mid-merge` 执行中 | `ABORTED` |
| 完整退出报告 | 无 | `false` |

完整事实和数据见：

- `artifacts/demo-report.json`
- `artifacts/recovery-report.json`

## 复现要求

演示不访问外部网络或数据库集群，不使用 Docker。服务、SQLite 文件、HTTP 端口和故障点均由本地脚本创建；重复执行会重建 `artifacts/demo-work`，报告覆盖为本次运行结果。


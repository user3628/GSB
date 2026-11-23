# 业务要求追踪矩阵

| 要求 | 实现位置 | 可核对证据 |
|---|---|---|
| 1. 两种格式编解码与损坏拒绝 | `internal/codec/binary.go`、`jsonl.go`、`errors.go` | `codec_test.go` 覆盖大小端、头部/记录魔数、长度前缀、CRC32、摘要篡改与偏移；固定 `samples/corrupt/*` |
| 2. 格式协商与兼容矩阵三态 | `internal/compat/matrix.json`、`matrix.go` | `matrix_test.go`；`/v1/matrix`；`samples/v1-legacy.bin` 为降级接受，`v2-current.jsonl` 为接受，`v3-unknown-required.jsonl` 为拒绝 |
| 3. bps/毫秒/微秒归一化与溢出 | `internal/normalize/normalize.go` | `normalize_test.go`；策略响应中的 `rounding` 字段；`E_NORMALIZE_OVERFLOW` |
| 4. 快照整体替换、版本单调与摘要 | `internal/model/model.go`、`internal/engine/engine.go`、`internal/store/import.go` | 摘要覆盖头部与全部记录；`E_VERSION_NOT_MONOTONIC`；旧版本绑定读取；失败保持旧活动版本 |
| 5. 派生结构级联失效与原子重建 | `internal/store/derived.go`、`import.go`、`read.go` | 路由/限流窗口/熔断/汇总共享快照版本；同一事务切换活动指针；`TestReadersSeeOnlyCompleteVersionDuringDerivedRebuild` |
| 6. 冲突解决与批次原子生效 | `internal/engine/engine.go` | 版本 > 最后写入时间 > 稳定语义摘要字典序；报告逐条 `overwrite|skip|reject`；`v4-duplicate-reject.jsonl` 验证整批拒绝 |
| 7. 单写者、多读者与请求幂等 | `internal/engine` 的 `writeMu`、SQLite WAL 读事务、`request_registry` | `TestConcurrentDuplicateImportAndRecovery`；端到端同 ID 重放逐字一致；不同内容 `E_CONFLICTING_REQUEST`；事务暂停期间读旧完整版本 |
| 8. SQLite、意图日志、恢复与回环 HTTP | `internal/store/*`、`internal/httpapi/server.go`、`internal/verify` | 启动恢复扫描；`docs/VERIFICATION.md` 的强杀结果；`scripts/kill-test.ps1` / `.sh`；`meshpolicy verify` |

## 验收场景对应

1. **两种格式互操作**
   - 二进制 v1 导入后检查默认值补齐。
   - JSONL v2 导入后检查 kbps/us 转 bps/ms、舍入余数与窗口对齐。
   - 三种损坏样本分别返回带偏移的 `E_FORMAT_CORRUPT`。

2. **兼容三态裁决**
   - `downgrade_accept`：v1 二进制。
   - `accept`：v2 JSONL。
   - `reject`：v3 未知必填 `geoAffinity`。
   - 同输入协商响应逐字一致。

3. **级联重建原子性**
   - v2 导入后活动版本、策略与全部派生结构均为版本 2。
   - 旧版本 1 策略和派生仍可按版本号独立读取，但不参与活动查询。
   - 重建事务暂停时并发查询仍看到版本 1 完整快照。

4. **强制终止恢复**
   - 在派生行已写入、活动指针未切换的事务内外部 `kill -9`。
   - 重启后只有版本 1；版本 2 快照行不存在。
   - 恢复报告记录 pending 意图、已落盘 facts、活动快照完整性计数和 `abortedBatches`。
   - 原请求 ID 重放返回持久化的 `E_IMPORT_ABORTED`。

## 明确不在范围内

- 不连接真实服务网格控制面。
- 不提供多节点分布式协调；单 SQLite 数据库由单服务进程写入。
- 不实现真实流量代理，只实现治理策略迁移、派生状态与查询验证。

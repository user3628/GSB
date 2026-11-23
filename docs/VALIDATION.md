# 实际验证记录

验证日期：2026-11-24（Asia/Shanghai）。工作目录内完成，未访问 DNS 服务、账号或其他线上数据源。

## 环境

```text
Go:       go1.27.4 linux/amd64（go.mod 声明 go 1.22；另用 go1.22.12 实测）
SQLite CLI: 3.46.1 2024-08-13（仅人工查看，已安装）
驱动:      modernc.org/sqlite v1.34.5（纯 Go，依赖要求 Go 1.21）
OS:        Linux/WSL2
```

依赖第一次拉取使用了 `GOPROXY=https://goproxy.cn,direct`；程序运行不需要网络。

## 构建、测试与跨平台

```bash
go test ./...
go test -race ./...
go vet ./...
GOOS=windows GOARCH=amd64 go build -o /tmp/dnswatch.exe ./cmd/dnswatch
GOTOOLCHAIN=go1.22.12 go test ./...
```

实际结果：

```text
ok  dns-cache-workbench/internal/verify
ok  dns-cache-workbench/internal/workbench
race tests passed
go vet passed
windows-build-ok
Go 1.22.12 tests passed
```

测试覆盖三源解析、名称/事务号归一、目标版本切换、报告字节确定性、快照兼容/整体替换/跳号回滚、事件预算、单并发槽队列和五类注入。

## 端到端演示

```bash
go run ./cmd/dnswatch demo --out artifacts/final-demo
```

实际关键输出：

```json
{
  "deterministic_bytes": true,
  "snapshot_version": 1,
  "finding_codes": [
    "E_ADDITIONAL_INJECT",
    "E_FLIP_FLOP",
    "E_TTL_MISMATCH",
    "E_UNSOLICITED"
  ],
  "v1_replay_code": "E_SNAPSHOT_VERSION",
  "injection_verification_passed": true
}
```

两次独立 SQLite 数据库上的报告逐字节一致：

```text
8f80a12fdcdf4f1387722e3fdbe54056063e398e859d29c846c2e99bd2425b39  run1/report.json
8f80a12fdcdf4f1387722e3fdbe54056063e398e859d29c846c2e99bd2425b39  run2/report.json
```

使用 Go 1.22.12 工具链重新运行 demo，两个报告仍得到同一 SHA-256：`8f80a12fdcdf4f1387722e3fdbe54056063e398e859d29c846c2e99bd2425b39`。

干净窗口解析结果：

| 源 | 格式 | 版本 | 事件 | 跳过 | 字节 |
| --- | --- | --- | ---: | ---: | ---: |
| `binary-v1.dnsbin` | binary | v1 | 2 | 0 | 92 |
| `binary-v2.dnsbin` | binary | v2 | 3 | 0 | 270 |
| `json-summary.jsonl` | json | v2 | 2 | 0 | 480 |
| `text-blocks.dnslog` | text | v1 | 3 | 0 | 420 |

四类命中及首个偏移：

| 业务码 | 源 | 偏移 | 事务号 | 说明 |
| --- | --- | ---: | ---: | --- |
| `E_UNSOLICITED` | `binary-v2.dnsbin` | 172 | 8194 | 无对应查询的应答 |
| `E_ADDITIONAL_INJECT` | `json-summary.jsonl` | 461 | 4097 | 附加区 `203.0.113.66` 属于无关名称 |
| `E_TTL_MISMATCH` | `text-blocks.dnslog` | 101 | 4097 | `192.0.2.10` 应答 TTL 60，缓存 TTL 12 |
| `E_FLIP_FLOP` | `text-blocks.dnslog` | 240 | 12291 | 1 个 tick 内从 `.1` 翻到 `.2` |

快照元数据：

```text
version=1
required_parser=v2
format_versions=binary:v1,binary:v2,json:v2,text:v1
content_hash=1bee7d5cc92a33152e6a9ac061b24c941bafc2bb7dbaba415205e52d088702e4
chain_hash=26a12a6f3ef5da2a1336e783b5fabcc2fd0dcc79eb809f65e32767567cf533af
```

## 五类异常注入

`verify-injections` 与 `demo` 内嵌验证均为 `all_passed: true`：

| 用例 | 期望/实际业务码 | 基线版本 | 结束版本 | 关键断言 |
| --- | --- | ---: | ---: | --- |
| 截断二进制 | `E_TRUNCATED` | 1 | 2 | 基线内容哈希不变；四项裁决保留 |
| 未知版本头 | `E_VER_UNSUPPORTED` | 1 | 2 | 记录跳过；基线内容哈希不变 |
| 文本长度越界 | `E_LENGTH_OVERFLOW` | 1 | 2 | 此前块保留；四项裁决保留 |
| 事务号名称冲突 | `E_NAME_CONFLICT` | 1 | 2 | 两侧源/偏移存在；四项裁决保留 |
| 快照版本跳号 | `E_SNAPSHOT_VERSION` | 1 | 1 | 事务回滚，链未增长 |

每例还检查了 `baseline_snapshot_content_unchanged`、`all_suspicious_verdicts_preserved`、`unaffected_source_event_counts_preserved`。快照跳号额外检查 `snapshot_jump_rolled_back`。

## 解析器版本切换

v2 目标：

```bash
go run ./cmd/dnswatch switch-version --db artifacts/final-demo/run1/dnswatch.db --version 1 --parser v2
```

```json
{
  "target_parser_version": "v2",
  "snapshot_version": 1,
  "required_parser_version": "v2",
  "compatible": true
}
```

v1 目标：

```bash
go run ./cmd/dnswatch switch-version --db artifacts/final-demo/run1/dnswatch.db --version 1 --parser v1
```

```json
{"error":{"code":"E_SNAPSHOT_VERSION","detail":"snapshot 1 requires parser v2; target parser v1 is incompatible and would be rejected as a whole"}}
```

对同一输入分别以 v1/v2 目标生成报告时，源摘要、事务配对与四项 findings 完全一致；差异只在兼容字段、业务码与快照目标/链哈希元数据。

## 人工查看 SQLite

```bash
sqlite3 -header -column artifacts/final-demo/run1/dnswatch.db \
  'select version,parent_version,required_parser_version,target_parser_version,
          substr(content_hash,1,12) as content_hash,
          substr(chain_hash,1,12) as chain_hash
   from snapshots order by version;'
```

实际输出：

```text
version  parent_version  required_parser_version  target_parser_version  content_hash  chain_hash
-------  --------------  -----------------------  ---------------------  ------------  ------------
1        0               v2                       v2                     1bee7d5cc92a  26a12a6f3ef5
```

## 复现

```bash
go test ./...
go run ./cmd/dnswatch demo --out /tmp/dnswatch-demo
```

`demo` 自动重新生成样本、重新扫描两次、比较报告字节并重跑五类注入；因此不依赖已保存的数据库或报告。

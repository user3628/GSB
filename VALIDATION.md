# 实际验证结果

环境：`go version go1.27.4 linux/amd64`（`go.mod` 指令锁定 `go 1.22`，全部
依赖的 Go 版本上限不超过 1.22），纯 Go 驱动 `modernc.org/sqlite v1.29.2`，
无 Docker、无外部数据库服务。`CGO_ENABLED=0` 可产出静态链接二进制。

## 1. 构建与静态检查

```
go build ./...        # 通过
go vet ./...          # VET_CLEAN
go test -race ./...   # 全部包 ok，无 DATA RACE
CGO_ENABLED=0 go build # 静态纯 Go 二进制构建通过
```

## 2. 单元/集成测试（27 个用例全部 PASS）

| 包 | 覆盖用例 |
|---|---|
| `codec` (8) | 五类落点的包含/越界、偏移映射、空载荷、字节上限、实体扩展回溯 |
| `parse` (4) | 容错解析、截断流、表面越界、来源偏移记录 |
| `session` (4) | 双侧逐条一致、乱序修正递增版本、确定性时钟聚合、P95 最近秩 |
| `store` (4) | 连续链、跳号回退、整体替换识别、结算往返持久化 |
| `scheduler` (3) | 预算隔离、在途/排队可观测、8 批次并发版本连续 |
| `app` (4) | 报告字节级一致、五类异常自检、预算检查、输入加载 |

## 3. 编码模拟裁决（确定性样本，12 条）

| 样本 | 裁决 | 规则 / 偏移(raw→landed) |
|---|---|---|
| sql-string-safe（单引号转义） | OK | `O''Reilly` 被约束 |
| sql-string-escape（转义后） | OK | 全部引号翻倍，未越界 |
| sql-string-mismatch（编码不匹配） | E_CTX_ESCAPE | SQL_STRING_UNESCAPED_QUOTE，2→2 |
| sql-ident-safe / escape | OK | 标识符引用中和 `"` |
| sql-ident-mismatch | E_CTX_ESCAPE | SQL_IDENTIFIER_UNESCAPED_QUOTE，2→2 |
| cmd-param-safe（参数化） | OK | 整体绑定为 `?` |
| cmd-meta-escape | E_CTX_ESCAPE | CMD_METACHAR_BOUNDARY `;`，4→4 |
| path-normalized-safe | OK | 无分隔符 |
| path-traversal-escape | E_CTX_ESCAPE | PATH_SEPARATOR_BOUNDARY `/`，2→2 |
| tpl-entity-safe（HTML 实体） | OK | `<b>` → `&lt;b&gt;` |
| tpl-interp-escape | E_CTX_ESCAPE | TEMPLATE_INTERP_CLOSE `}}`，5→5 |

合计 5 条 `E_CTX_ESCAPE`、7 条 `OK`，覆盖 SQL/命令/路径/模板四类注入面。

## 4. 字节级一致性

连续两次 `run`（不同工作目录、独立数据库）：

```
cmp run1/report.json run2/report.json  =>  完全相同
```

报告无墙钟、无随机字段；延迟时间字段仅为固定标记 `MONOTONIC_SAMPLE`。

## 5. 异常注入自检（五类 + 预算）

`injectctl selfcheck` 输出 `"all_passed": true`：

- empty_payload → `E_EMPTY_PAYLOAD`
- context_out_of_bounds → `E_CONTEXT_OOB`
- snapshot_version_gap → `E_SNAPSHOT_VERSION`（回退最近连续版本=1）
- concurrent_batch_contention → 6 批次全部 `OK`，快照链连续
- encode_byte_limit → 超限批次 `E_ENCODE_LIMIT`，干净批次不受影响
- 预算检查 → 超预算批次 `E_SAMPLE_BUDGET`，同批干净批次仍 `OK`

## 6. 强制终止恢复

```
injectctl crash   -workdir D   # 首个快照提交后硬退出，exit code 9
injectctl recover -workdir D
# 链状态 OK，版本 [1,2,3]，batch-01 已提交不回退，其余重新裁决并补快照
injectctl recover -workdir D   # 再次恢复，幂等：链仍 [1,2,3]
```

## 7. 资源控制 CLI

- `-maxsamples 3`：每个批次终态 `E_SAMPLE_BUDGET`，互不污染。
- `-maxencoded 12`：报告业务码集合出现 `E_ENCODE_LIMIT`，同时保留
  `OK` 与 `E_CTX_ESCAPE`，证明上限仅作用于超标样本。

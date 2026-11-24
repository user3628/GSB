# BIRE — 备份完整性与防篡改恢复引擎

面向 Windows 主机的备份仓库巡检、篡改定位与恢复点裁决工具。纯 Go 实现
（`modernc.org/sqlite`，无 CGO），可在 Windows 命令行直接构建运行，
不依赖 Docker、网络或外部账号。

## 构建与测试

```bat
go build -o bire.exe .
go test .
```

要求 Go 1.21+。样本与状态库均使用独立目录（`--samples` / `--state`）。

## 子命令

```
bire generate --samples <dir> [--config <plan.json>]
bire inspect --repo <name> --samples <dir> --state <dir> [--budget <n>] [--crash-before-commit]
bire batch --samples <dir> --state <dir> [--concurrency <n>] [--budget <n>] [--report <file>] [--inject-snapshot-gap <repo>]
bire replay --repo <name> --state <dir> --version <n>
bire restore --repo <name> --state <dir>
bire report --state <dir> [--out <file>]
bire stats --state <dir>
```

## 样本与清单格式

- 仓库目录：`repos/<name>/`，含 `index.json`（登记每代 `manifest_digest`
  与 `chain_digest`）、`manifests/gen-NNNNNN.json`、`objects/<sha256>`。
- 清单记录代次号、`prev_chain_digest`、文件条目（路径/长度/SHA-256）
  与清单自身摘要；摘要提交于规范化 JSON（条目按路径排序）。
- 链演进：`chain_0 = sha256("bire/genesis/v1")`，
  `chain_n = sha256(chain_{n-1} + ":" + manifest_digest_n)`。
- 样本内容仅由 repo/代次/路径确定性派生，无时间戳，重复生成字节一致。

## 业务码

| 代码 | 含义 |
|---|---|
| `E_MANIFEST_TRUNCATED` | 清单截断/不可解码 |
| `E_DIGEST_FORMAT` | 摘要字段非 64 位小写十六进制或长度非法 |
| `E_OBJECT_MISSING` | 内容对象缺失 |
| `E_OBJECT_DIGEST` | 对象长度或内容摘要与登记不符 |
| `E_CHAIN_BROKEN` | 链哈希断裂（记录期望/实际摘要） |
| `E_MANIFEST_REPLACED` | 清单被整体替换（按内容摘要差异识别，不比对代次号） |
| `E_NO_TRUSTED_RESTORE` | 不存在可信代次，不输出部分恢复清单 |
| `E_SNAPSHOT_VERSION` | 快照版本链跳号，回退到最近连续版本 |
| `E_SNAPSHOT_DIGEST` | 快照内容被整体替换（载荷摘要校验失败） |
| `E_GENERATION_BUDGET` | 单会话代次数超限，仅终止该会话 |

## 关键语义

- **篡改定位**：逐代重算清单摘要与链摘要，与登记值比对；整体替换通过
  内容摘要差异识别并写入演进轨迹。对象级篡改仅污染本代，清单级断裂
  才中断后续代次的链可信性。
- **恢复点裁决**：由新到旧选取链完整且对象校验全部通过的最近代次；
  恢复边界列出全部被排除代次及原因；替换代次永不入选。
- **快照**：每轮巡检在单个 SQLite 事务内提交递增版本快照（载荷 +
  内容摘要），旧快照全部保留可按版本回放；加载校验版本链连续性与
  载荷摘要，跳号/篡改回退到最近连续前缀且回退不推进版本号。
- **并发**：`batch` 以信号量限流，超限仓库排队等待；在途会话数经
  `sessions` 表与 `stats` 子命令可观测；会话失败可追溯到仓库与代次。
- **崩溃恢复**：提交前强杀（`--crash-before-commit`）后重启，未提交
  事务整体丢弃，已提交结论不回退，仓库可立即重新巡检。
- **确定性报告**：报告按仓库相对路径与代次号稳定排序，时间字段仅
  使用固定采样标记 `SAMPLE-T0`，输入不变时重复运行字节级一致。

## 异常注入（仅通过输入与参数）

`generate --config plan.json` 支持四类输入级注入：`object-digest`、
`delete-object`、`drop-generation`、`replace-manifest`；第五类快照版本
跳号经 `batch --inject-snapshot-gap <repo>` 注入。注入样本与正常样本
共存于同一批次，无需修改源码。

## 验证结果（本仓库实测）

- `go test .`：14 个测试全部通过，覆盖链校验、四类注入定位、恢复点
  裁决、快照版本递增/跳号回退/摘要篡改、并发上限与预算隔离、子进程
  强杀恢复、报告字节级一致（含跨目录同输入复现）。
- 交叉编译：`GOOS=windows GOARCH=amd64/arm64 go build` 成功。
- CLI 冒烟：generate → batch（concurrency=3，max_in_flight=3）→
  report 两次输出 `cmp` 一致；delta 恢复边界正确排除替换代次；
  快照跳号注入后 `restore` 返回 `E_SNAPSHOT_VERSION` 并回退到
  version 1，可信恢复终态未受污染。

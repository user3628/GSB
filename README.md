# meshpolicy

面向服务网格流量治理的本地策略迁移、兼容裁决与原子重建管线。项目从零实现，包含：

- 紧凑二进制与 JSON Lines 两套策略快照编解码。
- 显式版本兼容矩阵，支持 `accept`、`downgrade_accept`、`reject` 三态裁决。
- bps/毫秒归一化、微秒时间戳截断、舍入位与累计舍入误差审计。
- SQLite 持久化的快照整体替换、冲突裁决、派生结构原子重建与意图日志。
- 单写者导入、并发只读查询、请求幂等与跨版本绑定读取。
- 进程强杀后的启动恢复扫描和仅基于已落盘事实的恢复报告。
- 仅绑定回环地址的 HTTP API、固定样本生成器与端到端验证工具。

## 快速开始（Windows PowerShell）

要求 Go 1.21+。项目使用纯 Go 的 `modernc.org/sqlite`，无需 CGO 或本机 SQLite 安装。

```powershell
go version
cd <项目目录>
$env:GOPROXY = "https://goproxy.cn,direct"
go mod download
go build -o .\bin\meshpolicy.exe .\cmd\meshpolicy

# 生成确定性固定样本
.\bin\meshpolicy.exe samples -out .\samples

# 启动本地回环服务
.\bin\meshpolicy.exe serve -addr 127.0.0.1:18080 -db .\data\meshpolicy.db

# 另一个 PowerShell 窗口执行固定样本回放
.\bin\meshpolicy.exe verify -base-url http://127.0.0.1:18080 -samples .\samples

# 强制终止恢复验收
.\scripts\kill-test.ps1
```

也可直接使用脚本：

```powershell
.\scripts\run-e2e.ps1
```

Linux/POSIX 的等价脚本：

```bash
go build -o bin/meshpolicy ./cmd/meshpolicy
bash scripts/run-e2e.sh
bash scripts/kill-test.sh
```

离线构建时，可先在可联网环境执行 `go mod download` 填充模块缓存，再设置：

```powershell
$env:GOPROXY = "off"
go build -mod=mod -o .\bin\meshpolicy.exe .\cmd\meshpolicy
```

## HTTP API

服务强制只监听 `127.0.0.1`、`localhost` 或其他回环 IP。

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/healthz` | 健康状态与当前活动版本 |
| GET | `/v1/matrix` | 返回显式兼容矩阵 |
| POST | `/v1/negotiate` | 检测格式/版本并返回确定性三态裁决 |
| POST | `/v1/import` | 原子导入；必须携带 `X-Request-ID` |
| GET | `/v1/policies` | 查询活动快照策略；`?version=N` 绑定指定版本 |
| GET | `/v1/policies/{policyId}` | 查询单个策略；支持 `?version=N` |
| GET | `/v1/derived` | 查询路由索引、限流窗口、熔断状态与汇总；支持 `?version=N` |
| GET | `/v1/versions` | 列出活动版本和全部已提交快照 |
| GET | `/v1/imports/{reportId}` | 查询导入报告及逐条冲突结局 |
| GET | `/v1/recovery-report` | 最新恢复报告（仅在存在待恢复意图时生成） |

导入参数：

- `X-Request-ID`：必填，同一 ID 重放返回逐字一致的原响应。
- `?format=auto|binary|jsonl` 或 `X-Format`。
- `?truncation=floor|ceil|nearest`，默认 `floor`。
- 测试专用 `X-Test-Pause-After-Intent`、`X-Test-Pause-Mid-Rebuild` 仅在 `MESHPOLICY_ENABLE_TEST_HOOKS=1` 时可用。

主要错误码：

- `E_FORMAT_CORRUPT`：附 `byteOffset`，覆盖魔数、长度前缀、CRC32 与整体摘要错误。
- `E_FORMAT_UNSUPPORTED`：附 `unknownFields`。
- `E_NORMALIZE_OVERFLOW` / `E_INVALID_POLICY`：附记录行或字节偏移。
- `E_CONFLICTING_REQUEST`：同一请求 ID 的内容或选项不一致。
- `E_VERSION_NOT_MONOTONIC`：新快照版本未严格递增。
- `E_VERSION_RETIRED`：请求绑定的版本不可用。
- `E_BATCH_REJECTED`：批次中存在拒绝项，整批未生效；附 `reportId`。
- `E_IMPORT_ABORTED`：进程中断后恢复扫描已将批次标记中止。

## 固定样本

`samples/` 由 `meshpolicy samples` 确定性生成：

| 文件 | 用途 |
|---|---|
| `v1-legacy.bin` | 二进制 v1；缺限流/熔断/窗口字段，触发降级默认值 |
| `v2-current.jsonl` | JSONL v2；覆盖、跳过、新增、删除四类冲突结局 |
| `v3-unknown-required.jsonl` | 高版本未知必填特性 `geoAffinity`，应拒绝 |
| `v4-duplicate-reject.jsonl` | 批内重复策略 ID，应整批回滚 |
| `corrupt/*.bin` | CRC、长度前缀、记录魔数损坏样本 |
| `conflicts.json` | 导入顺序、期望结局与最终活动版本 |
| `checksums.json` | 固定样本 SHA-256 |

格式、摘要范围与字段布局见 `docs/FORMAT.md`；事务与恢复设计见 `docs/DESIGN.md`；实际验证记录见 `docs/VERIFICATION.md`。

## 项目结构

```text
cmd/meshpolicy/        CLI：serve、samples、verify
internal/codec/        二进制与 JSONL 编解码、偏移错误
internal/compat/       显式兼容矩阵与三态裁决
internal/normalize/    bps/ms 单位、时间戳舍入、窗口对齐、溢出检查
internal/model/        统一内部模型与摘要
internal/store/        SQLite schema、原子提交、派生结构、恢复
internal/engine/       导入状态机、冲突裁决、幂等、查询
internal/httpapi/      回环 HTTP API
internal/samples/      确定性样本生成
internal/verify/       端到端结果验证工具
scripts/               Windows/POSIX 回放与强杀恢复脚本
```

## 关键保证

- 任一条记录损坏时不返回部分数据；所有记录校验完成后才进入归一化。
- 快照摘要覆盖头部元数据、快照标识和全部记录，不只覆盖记录体。
- 活动版本指针、策略、全部派生结构和导入报告在同一 SQLite 事务提交。
- 任一策略拒绝时整批不写入策略/派生结构，旧活动版本保持可用。
- 所有写入前先持久化意图；启动时只依据已提交 SQLite 行生成恢复报告。
- 读事务与写事务隔离，重建暂停期间只读查询仍看到旧完整版本。

# MPCKPT

混合精度训练检查点编排组件的首个可运行版本。项目使用 Go 标准库和纯 Go SQLite 驱动，提供：

- FP16 显存驻留池、FP32 持久池、LRU/精度优先驱逐与恢复基线保护。
- `MPCK` 紧凑二进制和分片 JSONL 两种可互换清单格式。
- 显式兼容矩阵、降级补齐、未知必填能力拒绝和裁决持久化。
- 定点参数还原、IEEE 754 binary16 最近偶数舍入、偏差阈值校验。
- 版本化策略快照、增量片段、派生摘要/一致性/基线原子重建。
- 写盘字节预算、时间预算、有界等待队列和幂等提交。
- SQLite 意图记录、单写者事务、WAL 并发读和强制终止恢复。
- 本地回环 HTTP API 与可复现端到端验证器。

详细设计见 [docs/DESIGN.md](docs/DESIGN.md)，最近一次实际验证记录见 [docs/VERIFICATION.md](docs/VERIFICATION.md)。

## 环境

- Go 1.22 或以上。当前验证环境为 `go1.27.4 linux/amd64`，并已通过 Windows 交叉编译。
- SQLite 驱动为 `modernc.org/sqlite`，无需本地数据库服务或 Docker。
- 所有样本由内置固定种子生成，不读取外部模型文件。

```bash
go version
go mod download
go test ./...
```

## 运行

启动仅监听回环地址的服务。`-reset` 会在打开数据库前清理运行目录。

```bash
go run ./cmd/mpckpt serve \
  -dir ./run \
  -addr 127.0.0.1:18080 \
  -queue-cap 32 \
  -reset
```

端口必须由参数指定；服务拒绝绑定非回环地址。可选 `-compat /path/compat.json` 覆盖兼容矩阵。

执行完整验证（自行清理 `./run`，启动子进程，执行 SIGKILL 后重启并断言恢复）：

```bash
go run ./cmd/mpckpt verify -dir ./run -port 18081
```

验证器会写出：

- `run/sample-v2.mpck`：紧凑二进制样本。
- `run/sample-v1.jsonl`：历史低精度 JSONL 样本。
- `run/verification-results.json`：导入裁决、提交、驱逐、精度、幂等冲突和恢复明细。
- `run/serve.log`：子进程日志。

Windows PowerShell 示例：

```powershell
go run .\cmd\mpckpt verify -dir .\run -port 18081
```

## HTTP 接口

所有失败响应均为统一 JSON：

```json
{"code":"E_SPEC_INVALID","message":"...","context":{"field":"precision"}}
```

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/healthz` | 健康检查 |
| `POST` | `/v1/strategy` | 提交单调递增版本的精度策略快照 |
| `POST` | `/v1/checkpoints` | 提交检查点分片集并按预算分级写入 |
| `POST` | `/v1/incremental` | 基于当前策略版本的增量分片提交 |
| `POST` | `/v1/import` | 导入二进制或 JSONL 清单 |
| `POST` | `/v1/pools/reside` | 请求新 FP16 驻留并执行真实驱逐 |
| `GET` | `/v1/state?version=N` | 查询完整数据版本的分片、基线与池占用 |
| `GET` | `/v1/pools?version=N` | 查询驻留、持久、等待分片与占用 |
| `GET` | `/v1/precision?version=N` | 查询逐元素精度偏差和恢复报告 |
| `GET` | `/v1/derived?version=N` | 查询派生摘要、一致性结果和基线摘要 |
| `GET` | `/v1/imports?version=N` | 查询持久化的格式裁决与降级明细 |
| `GET` | `/v1/compatibility` | 查询全部格式裁决，包括拒绝记录 |
| `GET` | `/v1/recovery` | 查询强制终止恢复事件 |
| `POST` | `/v1/debug/crash-fault` | 仅用于验证器制造可恢复的在途写入/驱逐故障 |

HTTP 错误至少稳定覆盖：`E_SPEC_INVALID`、`E_RECORD_CORRUPT`、`E_FORMAT_UNSUPPORTED`、`E_POOL_FULL`、`E_PRECISION_DRIFT`、`E_STALE_SNAPSHOT`、`E_STALE_BASE`、`E_WRITE_QUEUE_FULL`、`E_CONFLICTING_REQUEST`。

所有变更请求必须携带唯一 `commit_id`。相同 `commit_id` 与相同内容返回已持久化结果；相同标识但内容不同返回 `E_CONFLICTING_REQUEST`。

策略快照同时支持嵌套和扁平两种请求形状：

```json
{"commit_id":"strategy-1","strategy":{"version":1,"generated_at":"2026-01-02T03:04:05Z","gpu_cap_bytes":96,"write_budget_bytes":4096,"time_budget_ms":1000,"loss_scale_initial_fixed":8000000,"loss_scale_growth":2,"max_abs_error_fixed":50000,"max_rel_error_ppm":100000}}
{"commit_id":"strategy-2","version":2,"generated_at":"2026-01-02T03:05:06Z","gpu_cap_bytes":128,"write_budget_bytes":8192,"time_budget_ms":1200,"loss_scale_initial_fixed":8000000,"loss_scale_growth":2,"max_abs_error_fixed":50000,"max_rel_error_ppm":100000}
```

## 验证结果摘要

最近一次 `2026-11-25` 本机运行结果：

```text
MPCKPT VERIFY: PASS
import binary v2  verdict=ACCEPT         dataset_version=1
import jsonl  v1  verdict=DEGRADE_ACCEPT dataset_version=2
full checkpoint   dataset_version=3
incremental       dataset_version=4 strategy_version=2
residency         dataset_version=5 evictions=1 freed_bytes=64
precision         within_policy=true max_abs_error_fixed=217
                  max_rel_error_ppm=355 cumulative_rounding_error_fixed=4645
forced terminate  recovery_events=1 dataset_version_preserved=true
rollback          r1-embed-master -> PENDING
                  r3-ffn-fp16-extra -> VRAM/RESIDENT
```

完整输出与逐项断言见 [docs/VERIFICATION.md](docs/VERIFICATION.md) 和 `run/verification-results.json`。

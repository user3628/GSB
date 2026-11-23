# ReleaseGate

ReleaseGate 是一个可嵌入的渐进式发布门禁领域组件。它读取确定性的发布阶段定义和 canary/稳定组观测事件，按阶段计算双组成功率、延迟分位、差异与置信区间，并在同一 SQLite 事务中完成门禁裁决、阶段推进和流量百分比更新。

组件面向 SRE 的本地测试环境，不依赖 Docker、外部账号或网络服务。SQLite 使用纯 Go 驱动 `modernc.org/sqlite`，因此 Windows 上无需 CGO。

## 能力概览

- 严格整批校验：阶段号递增、流量千分比、耗时溢出、分组标记、时间戳单调性、阶段引用。
- 定点数值契约：1 ppm 精度、6 位小数、精确有理数半偶舍入、审计残差和累计舍入误差。
- 两种分位：`nearest_rank` 与 `linear`，由发布定义显式选择。
- 双比例差异：绝对差异、相对差异、Wald 置信区间和运行时样本量误差上界。
- 稳定门禁：固定阈值次序、相对容差定点比较、`E_OBSERVE_MORE` 与 `E_GATE_FAILED` 分离。
- 六态状态机：`PREPARED`、`CANARY`、`OBSERVING`、`PROMOTING`、`ROLLED_BACK`、`ABORTED`。
- 意图优先事务：先持久化意图，再提交领域变更；重启扫描未完成意图和孤儿批次。
- 版本化快照：每次统计重算生成不可变版本；单写者、多读者、幂等批次和版本淘汰。
- 本地 HTTP API、固定种子样本、Bash 与 PowerShell 示例。

## 快速开始

### Windows 命令提示符

```bat
go version
go mod download
go test ./...
go vet ./...
go run ./cmd/fixturegen -out testdata -seed 20261124
go run ./cmd/releasegate -listen 127.0.0.1:18181 -db artifacts\demo\releasegate-demo.db
```

服务启动后，在另一个命令提示符中使用 curl 或 PowerShell 调用接口。完整自动示例：

```powershell
powershell -ExecutionPolicy Bypass -File .\examples\run-demo.ps1
```

### Linux、WSL 或 Git Bash

```bash
go test ./...
go vet ./...
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...
bash examples/run-demo.sh
```

示例默认监听 `127.0.0.1:18181`，可通过 `RELEASEGATE_PORT` 或 PowerShell `-Port` 覆盖。

强制终止演示需要显式开启故障注入：

```bat
set RELEASEGATE_ENABLE_FAULT_INJECTION=1
```

故障接口只用于本地验证；它写入持久意图后直接以退出码 `70` 终止进程，不返回 HTTP 响应。因此客户端会看到空回复，重启后的恢复报告只描述重启后能验证的已落盘事实。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/healthz` | 健康检查，返回 `service=releasegate` |
| `POST` | `/v1/releases` | 提交发布定义，创建 `PREPARED` 批次 |
| `GET` | `/v1/releases/{id}` | 查询当前发布头 |
| `POST` | `/v1/releases/{id}/start` | `PREPARED -> CANARY`，进入第一阶段 |
| `POST` | `/v1/releases/{id}/observations` | 整批摄入观测事件（`/batches` 为别名） |
| `POST` | `/v1/releases/{id}/gate` | 触发门禁裁决并原子更新状态与流量 |
| `GET` | `/v1/releases/{id}/stats` | 按版本查询阶段或累计统计、差异与误差契约 |
| `GET` | `/v1/releases/{id}/estimate` | 按版本查询差异估计 |
| `GET` | `/v1/releases/{id}/error-contract` | 查询误差说明与运行时上界 |
| `GET` | `/v1/releases/{id}/decisions` | 查询裁决历史 |
| `POST` | `/v1/releases/{id}/transitions` | 显式状态迁移；支持 `to` 或 `to_state` |
| `GET` | `/v1/releases/{id}/recovery-report` | 查询包含该发布的最新恢复报告 |
| `GET` | `/v1/recovery/report` | 查询全局最新恢复报告 |
| `POST` | `/v1/recovery/scan` | 立即执行恢复扫描 |
| `POST` | `/v1/releases/{id}/faults/terminate-after-intent` | 仅故障注入模式：写入意图后强制退出 |

失败响应统一为：

```json
{
  "error": {
    "code": "E_GATE_FAILED",
    "message": "release gate did not promote",
    "context": {
      "action": "E_GATE_FAILED",
      "gate_result": {"decision": {"triggered_threshold": "success.absolute_drop"}}
    }
  }
}
```

业务码与 HTTP 状态：

| 业务码 | HTTP | 含义 |
|---|---:|---|
| `E_RELEASE_INVALID` | 400 | 定义或整批观测无效；整批拒绝，部分不落库 |
| `E_CONFLICTING_BATCH` | 409 | `batch_id` 已用于不同内容或非提交状态 |
| `E_INSUFFICIENT_SAMPLES` | 422 | 无法支撑所请求分位或置信区间 |
| `E_OBSERVE_MORE` | 422 | 可计算但证据不足，需要继续观察 |
| `E_GATE_FAILED` | 409 | 证据充分且越过门槛，必须暂停 |
| `E_INVALID_TRANSITION` | 409 | 非法状态迁移，发布头不变 |
| `E_VERSION_RETIRED` | 409 | 绑定版本已被替换或已淘汰 |
| `E_NOT_FOUND` | 404 | 发布、版本或报告不存在 |

## 请求示例

```json
{
  "release_id": "demo-progressive-release",
  "definition": {
    "release_id": "demo-progressive-release",
    "quantile_method": "linear",
    "confidence_level_ppm": 9500,
    "stages": [
      {
        "number": 1,
        "target_traffic_per_mille": 100,
        "min_observation_samples": 100,
        "latency_quantile_per_mille": 950,
        "gate": {
          "max_abs_success_drop_ppm": 60000,
          "max_relative_success_drop_ppm": 100000,
          "max_latency_delta_us": 2000,
          "max_ci_half_width_ppm": 80000,
          "relative_tolerance_ppm": 1000
        }
      }
    ]
  }
}
```

观测批次中的每个事件必须包含 `timestamp_ms`、`instance_id`、`group`、`success`、`latency_us`。事件级 `stage` 可省略，此时继承批次 `stage`；若提供则必须一致。

## 状态机

```text
PREPARED --start--> CANARY --gate pass--> PROMOTING --gate pass--> PROMOTING (next stage)
                      |                      |
                      +--gate fail---------->OBSERVING
                      |                      |
                      +------rollback--------+----> ROLLED_BACK
                      +------abort-----------+----> ABORTED
```

合法迁移以 `domain.CanTransition` 为唯一来源。`PROMOTING -> OBSERVING` 用于显式进入下一阶段观察；门禁成功本身已经把阶段和流量推进到新值。非法迁移返回 `E_INVALID_TRANSITION`，事务不修改持久化状态。

## 数值与恢复契约

- 比例统一输出 ppm 整数，同时提供固定 6 位小数字符串。
- 比例、差异和相关中间值来自整数计数或有理数；已显示的舍入值不会再次参与计算。
- 分位来自原始微秒样本；跨阶段归并先按稳定键排序，因此与批次提交顺序无关。
- 置信区间使用 Wald 半宽；误差上界为 `z * sqrt(0.25/n_canary + 0.25/n_stable)`，在运行时由样本量计算。
- 门禁比较为定点比较；阈值内的相对容差视为未越界，固定次序为样本量、CI 半宽、绝对成功率下降、相对成功率下降、延迟下降。
- 每次摄入在单一事务内写入完整统计快照；读者只读取一个不可变版本。
- 每个写操作先写 `intents`，再提交领域变更；恢复扫描只记录重启后可从数据库验证的事实。

详见 [docs/NUMERIC_CONTRACT.md](docs/NUMERIC_CONTRACT.md) 与 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。

## 固定样本

`cmd/fixturegen` 使用 `seed=20261124` 和自实现 splitmix64 序列，不依赖 Go 版本相关随机实现。当前样本哈希：

| 文件 | SHA-256 |
|---|---|
| `release.json` | `bb2aca4d70f9bf206dff60852f3c2d99053055ce7969f5578a4915d1997abd3b` |
| `observations-stage-1.json` | `fd3ad73acf50b440c22287ad9ba050e834791216a11c68fe508ae02e39da112c` |
| `observations-stage-2.json` | `16c20730f61e1417a0b06cf1ca595477737abd4c41715248ad5e2f6a242c7cd3` |
| `observations-stage-3.json` | `f8faf413fce549ef01fc335aa80c097363466559e07d679fc4d8299651199fe8` |

## 实际验证

当前环境已执行：

```text
go test -count=1 ./...                    PASS
go test -race -count=1 ./...              PASS
go vet ./...                              PASS
GOOS=windows GOARCH=amd64 go build ./...  PASS
bash examples/run-demo.sh                 PASS
bash examples/run-go-run-smoke.sh         PASS
```

示例结果：

```text
stage1 canary_rate=0.970000 stable_rate=0.950000 abs_drop_upper_ppm=38357 error_bound_ppm=97998
gate1 outcome=PROMOTE promoted=True to_stage=2 traffic_per_mille=300
gate2 code=E_GATE_FAILED threshold=success.absolute_drop measured=240281 limit=60000
paused state=OBSERVING stage=2 traffic_per_mille=300 version=2
forced server_exit=70 curl_exit=52
recovery pending=1 recovered=1 orphan_batches_aborted=0 versions_restored=0
recovered state=OBSERVING stage=2 traffic_per_mille=300 version=2
final state=ROLLED_BACK traffic_per_mille=0
```

完整响应见 `artifacts/demo/demo-output.json`，运行记录见 `artifacts/verification.txt`。
恢复脚本可用 `bash scripts/verify.sh` 一键重跑，输出会覆盖 `artifacts/verification.txt`。

## 目录

```text
domain/       领域类型、整批校验、门禁规则、状态机
numeric/      精确有理数舍入、分位、Wald 区间、误差审计
store/        SQLite schema、版本快照、意图、恢复扫描
service/      单写者编排、幂等批次、统计重算、原子裁决
httpapi/      net/http JSON API 与统一错误信封
cmd/          releasegate 服务与 fixturegen 样本生成器
testdata/     固定发布定义与三阶段观测样本
examples/     Bash 与 PowerShell 端到端示例
artifacts/    实际运行输出与验证记录
```

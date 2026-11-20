# 网关鉴权与多窗口限流联合裁决中间件

本地、确定性、可复现的 Python 中间件。它接收带毫秒时间戳、路由键、客户端标识、令牌摘要和请求权重的事件批次，先做鉴权，再做 1 秒短窗与 60 秒长窗的自适应限流，最后为每条请求给出稳定的放行或拒绝裁决。

- 鉴权：固定本地 HMAC-SHA256 密钥、客户端配额组、毫秒级绝对过期边界。
- 窗口：1 s / 100 ms 短窗桶与 60 s / 1 s 长窗桶；左开右闭滑动窗口。
- 规模：窗口内事件不超过上限时全量整数精确计数，超过时使用确定性哈希蓄水池采样与 99% 区间。
- 一致性：鉴权与限流读取同一个 `base_version`；查询只看到完整提交版本。
- 并发：批次单写者串行，读查询并发；请求与批次两级幂等。
- 恢复：SQLite 两阶段意图提交；强制终止后扫描 PENDING 意图并标记批次中止。
- API：FastAPI；实验：固定 NumPy 样本与一键复现脚本。

算法公式、边界和恢复规则详见 `docs/design.md`。

## Windows 快速开始

要求 Python 3.11 或以上。以下命令在 PowerShell 中执行：

```powershell
py -3.11 -m venv .venv
Set-ExecutionPolicy -Scope Process Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pytest -q
```

SQLite 使用 Python 标准库 `sqlite3`，不需要单独安装服务。

### 复现实验

```powershell
# 默认大规模：100,000 条，完整实验约 8 秒
python scripts\reproduce.py --large-events 100000

# 百万级：当前工作区实测约 80 秒、峰值约 4.4 GiB、数据库约 814 MiB
python scripts\reproduce.py --large-events 1000000 --output artifacts\experiment_results_1m.json --workdir artifacts\experiment_1m
```

脚本会：

1. 生成固定种子的小规模 287 条样本；
2. 生成大规模样本，包含 1 秒热点突发；
3. 输出两个规模的裁决分布、触发窗口、采样率、CI 覆盖和区间宽度；
4. 用同一批 600 条请求分别走“全量精确”和“采样估计”路径；
5. 启动独立子进程，在意图已持久化后执行 `os._exit(91)`，再扫描并输出恢复报告。

结果写入 `artifacts/experiment_results.json`；JSON 中的数据库路径会在本地重建。

### 启动 HTTP API

先在终端 A 用固定样本初始化令牌注册表并启动服务：

```powershell
python -m gateway_middleware --db artifacts\api\gateway.sqlite3 --seed-sample small --port 8000
```

再在终端 B 提交固定样本并查询窗口、采样说明和恢复报告：

```powershell
python scripts\api_smoke.py --base-url http://127.0.0.1:8000
```

OpenAPI 文档：`http://127.0.0.1:8000/docs`。

## HTTP 接口

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/v1/batches` | 提交事件批次并返回逐请求裁决 |
| `GET` | `/v1/batches/{batch_id}` | 查询已提交批次 |
| `GET` | `/v1/requests/{request_id}` | 查询单请求完整裁决 |
| `GET` | `/v1/windows?route_key=&client_id=&version=&at_ms=` | 查询窗口计数、桶与动态配额 |
| `GET` | `/v1/sampling/errors?route_key=&client_id=&version=&at_ms=` | 查询采样方法与误差解释 |
| `GET` | `/v1/recovery-report` | 查询最近一次恢复报告 |
| `GET` | `/v1/recovery-report/{report_id}` | 查询指定恢复报告 |
| `GET` | `/v1/snapshots/{version}` | 查询完整提交快照元数据 |
| `GET` | `/health` | 健康状态与当前快照版本 |

批次请求示例：

```json
{
  "batch_id": "batch-001",
  "events": [
    {
      "event_seq": 0,
      "request_id": "req-001",
      "timestamp_ms": 1700000000000,
      "route_key": "/api/orders",
      "client_id": "c-standard-1",
      "token_id": "tok-normal-c-standard-1",
      "token_digest": "<HMAC-SHA256 hex>",
      "weight": 1
    }
  ]
}
```

固定样本的令牌摘要由本地固定密钥确定性生成，不访问任何真实鉴权服务。

## 裁决语义

- `ALLOW`：鉴权通过且两窗口都没有超过动态配额。
- `E_AUTH_DENIED`：令牌不存在、撤销、客户端不匹配、配额组不匹配或签名摘要不一致。
- `E_TOKEN_EXPIRED`：签名有效，但 `timestamp_ms > expires_at_ms`。
- `E_RATE_LIMITED`：短窗或长窗任一估计计数超过对应动态配额。

鉴权失败不进入窗口。限流拒绝仍进入窗口的“请求负载”计数，但不增加已放行权重；因此裁决中可以同时保留 `auth.ok=true`、`AUTH_OK` 与 `E_RATE_LIMITED`。

失败裁决和 HTTP 失败响应都带业务码。HTTP 层统一错误结构：

```json
{
  "ok": false,
  "error": {
    "code": "E_CONFLICTING_REQUEST",
    "message": "...",
    "details": {}
  }
}
```

已实现的业务码包括 `E_AUTH_DENIED`、`E_TOKEN_EXPIRED`、`E_RATE_LIMITED`、
`E_CONFLICTING_REQUEST`、`E_BATCH_NOT_FOUND`、`E_REQUEST_NOT_FOUND`、
`E_SNAPSHOT_INVALID`、`E_INVALID_TRANSITION`、`E_INVALID_INPUT` 和 `E_INTERNAL`。

## 一致性、并发与恢复

- 每个新批次从 `base_version` 读取同一个令牌与窗口快照，提交为 `base_version + 1`。
- 新裁决中 `read_version`、`window_version`、`token_version` 相同，`commit_version` 为目标版本。
- 批次写者独占，其他批次排队并在之后作为下一批次执行；查询并发读取完整提交。
- 同一 `batch_id` 内容相同直接重放；内容不同返回 `E_CONFLICTING_REQUEST`。
- 同一 `request_id` 内容相同只裁决一次并复用；内容不同返回 `E_CONFLICTING_REQUEST`。
- 先持久化 `PENDING` 意图，再原子提交裁决、窗口与快照。
- 强制终止后只能依据 SQLite 已持久化事实生成恢复报告，报告明确 `complete_exit_report=false`。

## 实际验证结果

当前工作区实际运行环境：Python 3.14、SQLite 标准库、FastAPI、NumPy。核心代码只使用跨平台路径与标准库 SQLite，Windows 使用同一命令集。

`python -m pytest -q`：**23 passed**。

固定样本实际结果：

| 规模 | 请求数 | ALLOW | E_AUTH_DENIED | E_TOKEN_EXPIRED | E_RATE_LIMITED | 耗时 |
|---|---:|---:|---:|---:|---:|---:|
| 小规模 | 287 | 276 | 4 | 1 | 6 | 0.029 s |
| 百万级 | 1,000,000 | 987 | 100 | 100 | 998,813 | 74.908 s |

百万级样本：

- 短窗采样率：`1/16` 共 7,703 条采样裁决；其余 `1/1` 精确。
- 长窗采样率：`1/16` 共 33,845 条，`1/8` 共 894,275 条，其余 `1/1`。
- 短窗采样窗口真值落在 99% 区间的比例：`100%`；平均区间宽度 `1388.39`。
- 长窗采样窗口真值落在 99% 区间的比例：`99.9612%`；平均区间宽度 `3123.19`。
- 同一 600 条批次精确/采样对照：两路径均 `59 ALLOW + 541 E_RATE_LIMITED`，裁决差异 0；
  最终短窗精确计数 600，采样估计 592，区间 `[349, 835]`，真值在区间内。
- 强制终止恢复：子进程退出码 `91`，扫描 1 个 PENDING 意图，回滚/中止 1 个批次，
  `state_version` 保持不变，恢复报告 `complete_exit_report=false`。

完整机器可读结果见：

- `artifacts/experiment_results.json`：默认 100,000 条实验；
- `artifacts/experiment_results_1m.json`：百万级实验与恢复；
- `artifacts/verification.txt`：测试与运行命令记录。

## 目录

```text
gateway_middleware/
  auth.py            鉴权与固定本地摘要
  windows.py         窗口、前缀和、采样与 CI
  quota.py           定点动态配额与阻尼
  service.py         批次裁决、单写者闸门、幂等
  store.py           SQLite 模式、意图、快照与恢复
  api.py             FastAPI 接口与统一错误
  sample_data.py     固定 NumPy 样本
  crash_worker.py    强制终止演示子进程
scripts/
  reproduce.py       小/大规模、精确/采样、恢复全流程
  api_smoke.py       HTTP 冒烟脚本
tests/               23 项 pytest
docs/design.md       算法与一致性设计
```

## 边界和限制

- 固定本地密钥只用于离线确定性实验，不是生产密钥管理方案。
- 服务以本地单进程单写者为设计中心；多进程部署需要外部分布式串行化。
- 百万级批次需要约 4.4 GiB 峰值内存和约 814 MiB SQLite 文件；小内存机器应降低批大小。
- 强制终止后不会生成“优雅退出”报告，只生成基于已提交事实的恢复报告。
- 采样区间是近似正态区间；裁决使用点估计，`exact_weight` 保留用于审计。

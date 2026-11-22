# LeaseFair：本地多进程分布式租约锁中间件

LeaseFair 是一个不依赖容器和外部服务的本地租约锁实现。它用 SQLite（Python 标准库 `sqlite3`）持久化租约、到期索引、幂等请求、意图日志、指标缓存与恢复报告，通过本地 HTTP 提供获取、续约、释放、仲裁、指标与诊断能力。

核心约束：

- 所有持久化时间均为 Unix epoch 毫秒整数；到期时刻只做整数加法。
- 占比、余量比例、占用率、公平度均为 1e-9 定点整数（Q9），规范路径不使用二进制浮点。
- 同一租约集合、同一 `now_ms` 的派生指标逐位一致；候选提交顺序不会改变授予结论。
- 业务变更先持久化 `prepared` 意图，再在 `BEGIN IMMEDIATE` 事务中同时提交租约、到期索引、指标缓存、意图完成状态与幂等响应。
- 进程被强制终止后，启动扫描只依据 SQLite 已提交事实恢复，并明确不承诺存在完整退出报告。

## Windows 从零运行

要求 Windows 10/11、Python 3.11、pip。无需 Docker，也无需安装 SQLite 服务。

```powershell
py -3.11 --version
py -3.11 -m venv .venv
Set-ExecutionPolicy -Scope Process Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e . --no-deps
python -c "import sqlite3; print(sqlite3.sqlite_version)"
python -m pytest -q
python scripts\demo.py
```

若机器没有 3.11，可先安装 uv，再执行 `uv python install 3.11`，随后仍按上面创建 venv。当前仓库已在 Python 3.11.18、SQLite 3.53.1 上实际验证。

启动本地服务：

```powershell
$env:LEASE_DB_PATH = "data\lease.db"
python -m uvicorn leasefair.api:app --host 127.0.0.1 --port 8787
```

打开 `http://127.0.0.1:8787/docs` 可查看 OpenAPI 文档。

## 目录

```text
leasefair/              核心服务、定点运算、指标、SQLite、HTTP
data/fixed_lease_samples.json  固定 TTL/权重/到期/候选样本
scripts/demo.py         固定样本、数值误差、强制终止恢复演示
scripts/crash_worker.py os._exit 故障注入子进程
tests/                  核心、定点、HTTP、多进程、恢复测试
docs/design.md          事务、恢复与数值设计说明
docs/verification.md    实际验证结果
```

`data/` 与 `artifacts/` 用于 SQLite、运行报告与演示输出；数据库文件已在 `.gitignore` 中排除。

## HTTP 接口

业务结果默认使用 HTTP 200，以稳定 `code` 表达业务结论。`E_CONFLICTING_REQUEST` 返回 409，请求校验失败返回 422，内部错误返回 500。

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/locks/{lock_key}/acquire` | 获取租约锁 |
| POST | `/v1/locks/{lock_key}/renew` | 携带当前版本续约 |
| POST | `/v1/locks/{lock_key}/release` | 携带当前版本释放 |
| POST | `/v1/locks/{lock_key}/arbitrate` | 只读计算多候选仲裁结论 |
| POST | `/v1/locks/{lock_key}/acquire-best` | 原子仲裁并授予 |
| GET | `/v1/locks/{lock_key}` | 当前锁状态 |
| GET | `/v1/metrics?now_ms=` | 当前完整提交快照的派生指标 |
| POST | `/v1/metrics/recompute` | 写意图并持久化重算指标 |
| GET | `/v1/diagnostics?now_ms=` | 时钟、容差、指标定义、缓存与恢复诊断 |
| GET | `/v1/recovery` | 最近一次启动恢复报告 |
| GET | `/v1/events?lock_key=` | 持久化授予/续约/释放/过期轨迹 |
| GET | `/v1/intents?phase=` | 持久化意图日志与前后镜像 |
| GET | `/v1/samples` | 固定样本 JSON |
| POST | `/v1/registry/keys` | 注册锁键，作为占用率分母 |

获取锁示例（PowerShell）：

```powershell
$body = @{
  request_id = "req-001"
  holder_id  = "worker-a"
  ttl_ms     = 30000
  weight     = 0.45
} | ConvertTo-Json

Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8787/v1/locks/alpha/acquire" `
  -ContentType "application/json" -Body $body
```

锁键含 `/` 时可使用 URL 编码，例如 `/v1/locks/tenant%2Falpha/acquire`。

续约示例：

```json
{
  "request_id": "req-002",
  "holder_id": "worker-a",
  "version": 1,
  "ttl_ms": 30000
}
```

多候选原子仲裁：

```json
{
  "request_id": "req-arb-001",
  "candidates": [
    {"holder_id": "holder-a", "ttl_ms": 7200000, "weight": 0.45},
    {"holder_id": "holder-b", "ttl_ms": 300, "weight": 0.35},
    {"holder_id": "holder-c", "ttl_ms": 300000, "weight": 0.6}
  ]
}
```

## 业务语义

### 版本与 TTL

- 首次授予版本为 1；每次由一个可用状态重新授予时版本递增，释放与过期不会回退版本。
- 授予到期时刻：`granted_at_ms + ttl_ms`。
- 续约到期时刻：`renewal_now_ms + ttl_ms`。这里使用当前注入时钟而非旧到期时刻，避免提前续约无限累积，也完全避免浮点秒运算。
- 续约、释放必须携带当前版本。旧版本返回 `E_STALE_LEASE`，且不会修改原到期时刻。
- 持有者不匹配返回 `E_NOT_HOLDER`；版本当前但已到期返回 `E_LEASE_EXPIRED`。
- 竞争活跃锁返回 `E_LOCK_HELD`，响应包含当前持有者、授予时刻、版本、到期时刻与剩余毫秒数。

### 派生指标

设活跃租约集合为 `L`，持有者 `i` 的活跃租约权重和为 `W_i`，其申报权重为 `D_i`：

```text
实际租约占比 share_i = W_i / sum_j(W_j)
目标权重占比 target_i = D_i / sum_j(D_j)    （j 仅限当前有活跃租约的持有者）
加权公平度 F = 1 - sum_i(target_i * abs(share_i-target_i))
续约余量比例 margin = min(expires-now, ttl) / ttl
集群占用率 = 活跃锁键数 / 已注册锁键数
再平衡相对偏差 = abs(share_i-target_i) / target_i
```

所有除法与乘法只在 Q9 最终结果处舍入一次。归一化使用“最大余数法、余数降序、持有者标识升序”分配残差，因此输入顺序不影响结果。公平度项按持有者标识排序后使用 Kahan/Decimal 补偿求和。

仅当相对偏差严格大于配置容差时生成再平衡建议；容差内保留为 `suppressed`，建议携带实际占比、目标占比、偏差、相对偏差和触发规则。

### 候选仲裁

每个候选的优先级是“假设将该候选租约加入当前活跃集合后的投影加权公平度”。候选按 `holder_id` 规范化后计算并比较：

```text
tie iff abs(a-b) * 10^9 <= tolerance_q * max(abs(a), abs(b))
```

容差内按 `holder_id` 字典序升序稳定打破平局。响应包含每个候选指标、双方比较值、相对差、容差与最终依据。默认公平度相对容差为 `0.000001`，默认再平衡相对容差为 `0.05`，可通过环境变量 `LEASE_FAIRNESS_RELATIVE_TOLERANCE`、`LEASE_REBALANCE_RELATIVE_TOLERANCE` 覆盖。

## 精度与误差

- 内部表示：有符号整数 Q 单位，`1.0 = 1_000_000_000`；定点输出固定 9 位小数。
- 输入权重以十进制文本持久化并量化到 Q9；量化误差逐项输出。
- 归一化误差逐持有者输出，实际占比与目标占比各自严格求和为 `1.000000000`。
- 公平度乘法项逐项输出舍入误差与 Kahan 补偿值。
- 每次指标响应分别输出公平度管线与余量/占比/占用/再平衡的累计绝对舍入误差；占比与公平度管线的保守上界为 `4*n_holders + n_leases` 个 Q 单位。
- 朴素浮点与补偿浮点对照明确标记为非规范审计路径；规范路径只有整数与声明精度的 Decimal。
- `data/fixed_lease_samples.json` 的大数小数混合样本中，朴素 `sum` 实测偏差 86 个 Q 单位，Kahan 对照为 0，并提供 `gamma_(n-1)*sum|x_i|` 与 `2u*sum|x_i|` 误差界。
- TTL 固定样本覆盖 300ms 到 7,200,000ms（四个数量级），并输出秒/分/时归一化的截断位数、截断误差和 `+123ms` 时钟偏移后的剩余寿命与余量变化。

## 持久化、并发与恢复

- SQLite 使用 WAL、`synchronous=FULL`、30 秒 busy timeout。
- 每个写操作使用 `BEGIN IMMEDIATE`，因此同一锁键获得严格串行裁决；SQLite 单写者约束下跨锁键也串行，读取不受写锁阻塞。
- 指标查询在一个只读事务中读取同一完整提交快照，不使用旧缓存值冒充实时查询。
- 授予、续约、释放先单独提交 `prepared` 意图，再在业务事务中同步修改租约、到期索引、指标缓存、意图完成状态与幂等响应。
- 写操作在业务事务提交前被强制终止时，SQLite 自动回滚业务行；重启保留并报告 `prepared` 意图，已持久化的异常状态也会按意图前镜像补偿。
- 恢复以 PID 存活检查隔离仍存活的本地进程（包括当前进程中的其他服务实例），避免启动恢复误回收正在执行的意图。
- 重复 `request_id` 且内容一致时复用首次响应；内容不一致返回 `E_CONFLICTING_REQUEST`。

恢复报告逐意图输出恢复前后镜像与 `changed` 差异标记；字段 `graceful_exit_report_guaranteed` 固定为 `false`，并明确报告范围仅为重启后 SQLite 已提交事实。

## 演示与故障注入

```powershell
python scripts\demo.py
```

演示会输出固定样本授予轨迹、指标、仲裁、候选顺序一致性、数值误差对照和时钟契约，并对两个独立数据库执行真实 `os._exit(91)`：

1. 授予在意图持久化后、业务事务提交前被终止：恢复后锁仍可用。
2. 续约在意图持久化后、业务事务提交前被终止：恢复后到期时刻与续约次数保持旧值。

可通过 `LEASE_FAULT_AFTER_PREPARE=acquire|renew` 复现故障点；该变量只用于本地验证，不应在生产环境设置。报告写入 `artifacts/demo_report.json`。

## 验证

```powershell
python -m pytest -q
```

当前实际结果：19 项测试通过，覆盖定点与时间契约、版本过期、权重指标、Kahan 误差对照、容差裁决、HTTP、线程幂等、3 个独立进程竞争、`os._exit` 恢复与授予/续约补偿路径。详细结果见 `docs/verification.md`。

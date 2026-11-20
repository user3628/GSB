# Batch Precision Workbench

面向本地单机推理服务的批处理调度与精度诊断工作台。项目完全离线运行，只绑定回环地址，不依赖 Docker、外部服务或真实账号数据。

![碎片降批诊断页](artifacts/diagnostic-page.png)

## 快速开始（Windows）

要求 Python 3.11 或更高版本。工作区已经按当前主机可安装版本创建了 `.venv`，Windows 上可重新执行以下脚本：

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\setup.ps1
.\scripts\run_server.ps1
```

打开 `http://127.0.0.1:8765/`。服务同时暴露 OpenAPI 文档：`http://127.0.0.1:8765/docs`。

运行全部测试与真实强杀恢复演示：

```powershell
.\scripts\verify.ps1
```

演示脚本会：

- 启动仅监听 `127.0.0.1` 的 uvicorn；
- 用固定样本完成正常组织与碎片降批；
- 触发精度漂移样本并保留失败的持久化批次；
- 在分位最终事务前调用 `os._exit(86)` 强制终止进程；
- 用重启后的同一 SQLite 扫描未完成意图，输出恢复报告；
- 将实际结果写入 `artifacts/demo_results.json`。

## 固定样本

| 文件 | 用途 |
| --- | --- |
| `samples/models.json` | 模型清单、字节/微秒量纲、节点与显存区域 |
| `samples/trace_normal.json` | 正常批次组织、优先级隔离、延迟上限拆分 |
| `samples/trace_fragment.json` | 连续显存不足时的降批、位图变化与混批拒绝 |
| `samples/trace_precision_drift.json` | 极小真值越过相对偏差上界的漂移样本 |
| `samples/dimensions_invalid.json` | 负数、未知后缀、64 位溢出和整批拒绝载荷 |

量纲解析支持 `512Mi`、`2Gi`、`1500us`、`3ms` 等写法，也支持 `B/KB/KiB/MB/MiB/GB/GiB` 与 `ns/us/ms/s`。所有值在进入业务层前转为有符号 64 位范围内的整数；非法量纲、负数、溢出的乘积和未知模型都会使整份提交失败且不写入轨迹。

## 架构

```text
app/
  units.py       量纲解析、溢出检查、半入舍入与 PPM
  fixedpoint.py  Neumaier 整数补偿求和、nearest-rank 分位
  spec.py        模型清单、轨迹、节点显存区域归一化
  scheduler.py   确定性批组织、最佳适配分配、碎片降批
  compare.py     真值路径、引擎路径、容差判定与误差链
  store.py       SQLite WAL、意图日志、两阶段提交、启动恢复
  service.py     业务编排、诊断视图、固定样本装载
  main.py        FastAPI、统一错误 JSON、静态页面
  static/        本地 HTML/CSS/JS 诊断页
```

### 批组织规则

1. 请求按 `(model_id, priority)` 隔离，不同模型或不同优先级不会混批。
2. 同组请求按到达时间与原始序号装入；批样本总数不得超过模型 `max_batch_samples`。
3. 批总延迟超过组内最严格期望上限时在请求边界切批，原因记为 `LATENCY_CAP`。
4. 节点显存被描述为连续区域；批必须装入单个最佳适配空闲区域，防止跨碎片拼接。
5. 请求本身超过单批样本/延迟约束时确定性拆分，保留原始请求标识、到达顺序与 `part_index/part_count`。
6. 无单块连续区域可容纳时，按原顺序裁剪样本并重新入队。每个降批批次都会记录降批前后的位图、区域大小和模型最大批约束断言。
7. 若其他模型请求可与当前区域共处但延迟之和超过严格上限，记录 `MIXED_BATCH_REJECTED_LATENCY_CAP`，不进行混批。

### 定点与分位契约

- 显存内部单位：`int64` 字节；所有乘法与累计做溢出检查。
- 延迟内部单位：`int64` 微秒；批延迟使用 Neumaier 补偿求和。
- 分位方法：确定性 nearest-rank，`rank = ceil(p/100 * n)`。
- 批级分位样本：每个批次成员贡献 `samples * declared_unit_cost_us`；运行级分位样本为已提交批次总延迟。
- 每个分位显式返回 `sample_count`、`rank` 和 `method`。空样本集或秩超界返回 `E_INSUFFICIENT_SAMPLES`，从不静默返回 0 或空值。
- 展示输出单位：`0.01ms`（10µs 量子），半入舍入 `round_half_up`；每个输出值记录舍入位、方向、丢弃位和误差，并累加每批累计舍入误差。
- 真值路径：直接按声明值做样本数乘加。
- 引擎路径：固定点乘积、补偿累计、显式输出量化。
- 容差：延迟绝对偏差不超过 `5µs` 且相对偏差不超过 `5000ppm`；显存绝对/相对偏差必须为 0。

### 两阶段持久化与恢复

每个业务写操作先在独立事务中提交意图记录，再执行业务事务：

1. `PENDING` 意图提交；
2. 运行与批次骨架提交，检查点为 `BATCHES_PERSISTED_QUANTILES_PENDING`；
3. 分位、真值与校验结果在最终事务中原子提交，同时把意图标记为 `COMMITTED`。

若进程在步骤 2 与 3 之间被强制终止，重启扫描会：

- 将未提交运行标记为 `ABORTED` / `E_PROCESS_TERMINATED`；
- 将未完成批次标记为 `ABORTED`；
- 不生成伪造分位或误差链；
- 生成只基于 SQLite 已持久化事实的恢复报告，并明确“强杀进程不会生成优雅退出报告”。

## 业务错误码

统一错误响应：

```json
{
  "ok": false,
  "error": {
    "code": "E_SPEC_INVALID",
    "message": "model or trace specification is invalid; the entire submission was rejected",
    "details": {"written": false}
  }
}
```

| 业务码 | HTTP | 含义 |
| --- | ---: | --- |
| `E_SPEC_INVALID` | 422 | 量纲、模型、节点或请求字段非法；整份拒绝 |
| `E_TRACE_INVALID` | 422 | 到达时间戳非单调；不写入轨迹或批次 |
| `E_INSUFFICIENT_SAMPLES` | 422 | 没有已提交分位观测，或秩超界 |
| `E_PRECISION_DRIFT` | 409 | 校验失败批次已持久化，并返回具体批次/字段 |
| `E_BATCH_NOT_FOUND` | 404 | 批次 UID 不存在 |

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/health` | 服务、SQLite 意图统计与容差 |
| `GET/POST` | `/api/samples`、`/api/samples/{name}` | 固定本地样本 |
| `POST` | `/api/traces` | 提交并归一化模型清单、节点与轨迹 |
| `POST` | `/api/traces/{trace_id}/organize` | 触发批次组织；可选演示强杀检查点 |
| `POST` | `/api/demo/run/{sample}` | 一键提交并组织固定样本 |
| `GET` | `/api/runs`、`/api/runs/{id}` | 运行清单与批次组成 |
| `GET` | `/api/runs/{id}/quantiles` | 运行级 P50/P95/P99 |
| `GET` | `/api/runs/{id}/validation` | 全量真值核对与命中率 |
| `GET` | `/api/batches/{uid}/metrics` | 分位、显存预算、定点路径与舍入 |
| `GET` | `/api/batches/{uid}/errors` | 误差传播链与逐项偏差 |
| `GET` | `/api/batches/{uid}/validation` | 单批核对结论 |
| `GET` | `/api/recovery/report` | 最近一次启动恢复报告 |
| `GET` | `/api/recovery/intents` | 意图日志 |

批次 UID 形如 `run-<uuid>:b0001`，运行间不会冲突。页面会同时展示短批次号。

## 页面展示

静态页完全使用本地资源，展示：

- 确定性串行批次时间轴；
- 初始/最终空闲显存水位与位图；
- 批次组成、优先级隔离和拆分原因；
- 碎片降批前后位图及混批延迟拒绝证据；
- 每批 P50/P95/P99、样本规模、秩与舍入位；
- 真值/估计、绝对/相对偏差、容差结论；
- 四阶段误差传播链；
- 恢复事件、事实边界与意图日志。

正常轨迹截图：`artifacts/diagnostic-normal.png`<br>
碎片降批截图：`artifacts/diagnostic-page.png`

## 实测记录

当前主机使用 Python `3.14.4`（满足 3.11+），虚拟环境位于 `.venv`。完整测试结果与演示结果见 `docs/VALIDATION.md`、`artifacts/demo_results.json`。

```text
33 passed
normal: COMMITTED, 5 batches, P50=40.00ms, P95=48.00ms, P99=48.00ms
fragment: COMMITTED, 6 batches, 2 fragmentation events, 2 mixed-batch latency rejections
precision drift: E_PRECISION_DRIFT, persisted VALIDATION_FAILED run
invalid dimension: E_SPEC_INVALID, written=false
forced termination: exit code 86
restart recovery: 1 pending intent, ABORTED run, 5 ABORTED batches
aborted run quantiles: E_INSUFFICIENT_SAMPLES
```

## 已知边界

- 时间轴是用于诊断的确定性串行重放，不是生产吞吐性能预测器。
- 调度采用单节点、单空闲连续区域的保守策略；不会跨区域拼接单个批次。
- 分位是离散成员贡献上的 nearest-rank 结果；样本规模始终随响应返回。
- 强制终止演示会真实退出 uvicorn 进程，仅应通过 `scripts/run_demo.py` 在关闭状态或独立数据库上执行。

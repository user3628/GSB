# 本地单机推理批处理与一致性诊断工作台

这是一个完全离线、确定性的 Go 工作台，用于评估本地推理服务的批次组织、交互延迟和计算结果一致性。它不加载真实模型、不访问加速器，也不依赖容器；模型清单、请求轨迹和执行结果都来自本地 JSON 与 SQLite 事实。

## 已实现能力

- 模型清单与请求轨迹整批解析：`512Mi`、`2Gi`、`1500us`、`3ms` 等量纲精确转为 int64 字节/微秒，负数、溢出、非法单位、未知模型、重复请求 ID、非单调到达均拒绝。
- 确定性批次组织：优先级隔离、模型隔离或显式混合、单批样本上限、节点 VRAM 预算、`arrival`/`priority` 两种组织顺序、拆分根批次/子批序号与拆分依据。
- 补偿求和、最近秩 P50/P95/P99、显式样本规模与 `E_INSUFFICIENT_SAMPLES`。
- 定点数值契约：内部整数微秒，毫秒输出按声明小数位半远离零舍入，逐批输出真值、绝对/相对偏差、舍入位和累计舍入误差。
- fast（低延迟）与 full（完整）两种组织模式，可逐批核对；fast 通过组织预算提前封批并输出推迟数量/原因。
- 迟到请求隔离、混合模型延迟门禁、容量不足、样本拆分守恒、重复提交冲突。
- SQLite 持久化轨迹、运行、批次、成员、分位、精度、推迟、混批拒绝、意图日志与恢复报告。
- 所有业务写先提交一条 `started` 意图，再提交业务事务，最后标记 `committed`；进程被强杀后启动扫描未完成意图。
- 本地 HTTP API、统一 JSON 错误体、纯本地静态诊断页、可重复演示与真实硬退出恢复脚本。

## 快速开始

要求 Go 1.22+。本项目固定使用纯 Go SQLite 驱动 `modernc.org/sqlite v1.34.5`。

```bash
go version
go mod download
go build ./...
go build -o ./bin/workbench ./cmd/workbench
./bin/workbench \
  -addr 127.0.0.1:8080 \
  -db ./workbench.db \
  -models ./samples/models.json \
  -web ./web
```

打开 <http://127.0.0.1:8080/>。服务默认拒绝非回环地址；若明确需要其他地址可加 `-allow-non-loopback`，但本任务的演示不需要。

演示强杀恢复时必须显式开启测试故障入口：

```bash
./bin/workbench -addr 127.0.0.1:8080 -db ./workbench.db \
  -models ./samples/models.json -web ./web -allow-exit-fault
```

Windows 本地构建：

```powershell
go build -o .\bin\workbench.exe .\cmd\workbench
.\bin\workbench.exe -addr 127.0.0.1:8080 -db .\workbench.db -models .\samples\models.json -web .\web
```

## 一键验证

```bash
./scripts/demo.sh          # fast/full、分位、误差、迟到、混批、拆分、容量、错误码
./scripts/crash-demo.sh    # 真实 os.Exit 强杀、重启扫描、证据恢复、再次组织
go test ./...
```

脚本使用 `127.0.0.1:18081` / `127.0.0.1:18082`，每次创建新的 `artifacts/` 数据库、日志与响应快照。演示数据库和日志刻意保留以便复核；`.gitignore` 不提交这些运行产物。

## 目录

```text
cmd/workbench/          CLI 入口、回环地址保护、SQLite 启动与恢复、优雅退出
internal/domain/        量纲、模型/轨迹规范化、业务错误、运行/批次结果契约
internal/scheduler/     确定性组织、补偿求和、最近秩分位、定点精度核验
internal/store/         SQLite schema、意图先行写路径、读取、恢复报告
internal/service/       编排、幂等 run_id、快照/全量模式、对比、异常注入
internal/api/           net/http API、统一错误体、静态资源
samples/                固定模型清单、轨迹与组织参数
scripts/                可重复演示与强杀恢复演示
web/                    无外部依赖的诊断页面
```

## 输入契约

### 模型清单

```json
{
  "version": "catalog-v1",
  "models": [
    {
      "model_id": "llm-small",
      "vram_per_sample": "512Mi",
      "cost_per_sample": "1500us",
      "max_batch_samples": 256
    }
  ]
}
```

- 字节内部单位为整数 bytes；`KiB/MiB/GiB/TiB` 与短写 `K/M/G/T` 使用 1024 进制，`KB/MB/GB/TB` 使用 1000 进制。
- 时间内部单位为整数 microseconds；支持 `ns`、`us`、`µs`、`ms`、`s`，纳秒必须能整除 1000us，否则拒绝而不是静默舍入。
- 支持等价字段 `vram_per_sample_bytes`、`cost_per_sample_us`；数字形式分别按 bytes、us 解释。
- 任一条目非法时整个清单不安装，返回 `E_SPEC_INVALID`。模型 ID 重复、量纲非法、负数/溢出、样本上限非正均属于此类。

### 请求轨迹

```json
{
  "trace_id": "demo",
  "version": "v1",
  "requests": [
    {
      "request_id": "r1",
      "arrival_ms": 0,
      "model_id": "llm-small",
      "samples": 40,
      "max_batch_latency": "300ms",
      "priority": 10
    }
  ]
}
```

- `arrival_ms` 必须非负且在轨迹内非递减；`samples` 必须正数；`priority` 非负。
- `max_batch_latency` 字符串按显式时长解析；数字按 milliseconds 解析。也接受 `max_batch_latency_ms`、`latency_limit`、`expected_batch_latency`。
- 解析在写库前完成。任何一条非法、模型未知或请求 ID 重复都使整条轨迹不生效，返回 `E_TRACE_INVALID`，不会部分入库。
- 同一 `trace_id/version` 的相同内容重复提交是幂等操作（`created=false`）；内容不同返回 `E_CONFLICTING_REQUEST`。

## 调度与批次规则

1. `organization_order=arrival` 为默认值，按轨迹到达顺序处理；`organization_order=priority` 先按优先级降序、再按到达时间与原子序号稳定排序。两种顺序都会在优先级变化时封批，不同优先级永不混批，且各自进入选项哈希与 `run_id`。
2. 默认不允许混合模型。`allow_mixed_models=true` 是显式声明；同优先级下，若候选请求加入后使整个混合批超过批内最小延迟上限，则拒绝混批，保留现有批并输出 `MIXED_MODEL_LATENCY_LIMIT`。
3. 批显存是成员 `samples * vram_per_sample` 的精确和，不得超过节点可用显存；节点总量预检不足时返回 `E_INSUFFICIENT_CAPACITY`，不创建任何运行或批次，原持久化批保持不变。
4. 每模型单批样本数上限独立执行。容量或模型上限导致一个请求不能完整装入时，按确定性顺序拆分到同根子批，子批保留 `root_batch_sequence`、`split_index`、`split_reason` 与 `split_detail`，所有已选请求样本总量守恒。
5. fast 模式以轨迹首个到达时间为基准，只组织 `arrival_ms <= first_arrival + organization_budget_ms` 的请求；其余请求进入 `deferred_requests`，原因 `ORGANIZATION_BUDGET_EXCEEDED`。full 模式使用全部可容纳请求。
6. `late_request_ids` 注入的迟到请求绝不修改已封批：
   - `late_policy=reject`：推迟，原因 `LATE_AFTER_SEAL`；
   - `late_policy=new_batch`：进入新封批，`late_sealed=true`，`split_reason=LATE_AFTER_SEAL_NEW_BATCH`；
   - `late_policy=merge_sealed`：立即返回 `E_CONFLICTING_REQUEST`，因为已发布批次不可变。
7. 非迟到请求不会加入已标记 `late_sealed` 的批次；边界原因记录为 `LATE_SEAL_BOUNDARY`。
8. 所有批次在计划确认时 `sealed=true`；快速重放相同轨迹/参数得到逐位相同的批次列表与 `plan_hash`。

## 延迟、分位与定点精度

### 批延迟

```text
batch_latency_us = Σ (allocated_samples_i * cost_per_sample_us_i)
```

成员项使用整数 Neumaier 补偿累加器，乘积和总量都做 int64 溢出检查。样本项是整数，因此正常补偿项为 0；该字段仍逐批输出，便于核对。

### 最近秩分位

- 每批的分位样本是该批每个已分配样本的单样本代价观测，样本数 `N = total_samples`；混合模型批因此有非退化分布。
- 方法：确定性 nearest-rank，`rank = ceil(p * N / 100)`（1-based），升序选择。
- 尾部可解释性下限独立声明：P50 至少 2 个样本、P95 至少 20、P99 至少 100。低于下限时该分位为不可用，查询指定分位返回 `E_INSUFFICIENT_SAMPLES`，不会补零。返回体始终包含 `method`、`sample_count`、`min_samples` 与原因。

### 定点输出

- 内部累计与真值均为整数 microseconds。
- `latency_decimals` 取 0..3；输出毫秒时对整数微秒做 half-away-from-zero 舍入，例如 `170496us` 在 2 位小数下输出 `170.50ms`，舍入误差 `+4us`。
- 每批输出：`truth_latency_us`、`reported_latency_us`、`rounding_place`、`rounding_error_us`、`cumulative_rounding_error_us`、`absolute_deviation_us`、`relative_deviation_ppm`、上下界与 `precision_ok`/`explanation`。
- 相对偏差按 `round(abs_dev * 1e6 / truth_us)` 的整数 ppm 表示。
- 默认容差在 HTTP 层为绝对 `100us`、相对 `10000ppm`；可用 `max_abs_deviation_us`、`max_relative_deviation_ppm` 覆盖。任一越界则整批标记 `precision_failed`，持久化事实仍可查询，组织响应返回 HTTP 422 与 `E_PRECISION_BREACH`。
- `faults.precision_offset_us` 是明确的测试探针偏移，用于验证越界检测；它改变 reported 值但不伪造 truth。

## 持久化、意图与恢复

SQLite 表包含：`models`、`catalog_meta`、`traces`、`trace_requests`、`runs`、`batches`、`batch_members`、`quantile_results`、`precision_results`、`deferred_requests`、`mix_rejections`、`intents`、`recovery_reports`。

写路径：

1. 单独事务提交 `intents.state='started'`，意图已持久化；
2. 提交业务事务；
3. 将意图标记 `committed`；失败则 `aborted`。

批次进一步拆成两个事实边界：`COMMIT_BATCH_BASE`（批次与成员）和 `COMMIT_BATCH_RESULTS`（分位、精度与完成状态）。因此中断后可以准确区分：

- 已提交的完整批次：保留；
- 只有 base 事实的批次：标记 `aborted`；
- 尚未开始/未提交的批次：不存在；
- 组织运行：标记 `aborted`，不会补造完成结论。

启动时自动执行恢复扫描并写入 `recovery_reports`。报告仅列出已持久化事实，`completion_claim` 对中断运行固定为 `NO_COMPLETION_CLAIM_FOR_INTERRUPTED_RUN`。恢复后再次组织同一轨迹时，中止运行保留，新的确定性尝试使用 `-a2`、`-a3` 后缀；已完成的尝试会直接幂等返回。

`-allow-exit-fault` 开启的测试中断点：

- `interrupt_after_batches=N`：第 N 个完整批次之后、下一事实边界之前硬退出，退出码 87；N 等于批数时在 finalize 前退出，退出码 89；
- `interrupt_after_quantiles=true`：首个批次 base 已提交、分位结果提交前退出，退出码 88。

## HTTP API

所有失败响应统一为：

```json
{
  "code": "E_INSUFFICIENT_SAMPLES",
  "message": "...",
  "context": {"batch_id": "...", "sample_count": 29, "min_samples": 100},
  "error": {"code": "E_INSUFFICIENT_SAMPLES", "message": "...", "context": {}}
}
```

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/healthz` | 本地健康端点 |
| GET/POST | `/api/v1/models` | 读取/整批安装模型清单 |
| GET/POST | `/api/v1/traces` | 列出/提交轨迹 |
| POST | `/api/v1/traces/{trace_id}/organize?version=v1` | 触发组织 |
| GET | `/api/v1/traces/{trace_id}/runs?version=v1` | 该轨迹运行列表 |
| GET | `/api/v1/traces/{trace_id}/comparison?version=v1` | 最新 fast/full 对比 |
| GET | `/api/v1/runs[/{run_id}]` | 运行列表/详情 |
| GET | `/api/v1/batches/{batch_id}` | 批次组成与 VRAM 预算明细 |
| GET | `/api/v1/batches/{batch_id}/quantiles[?p=50\|95\|99]` | 分位查询；指定分位不足时 422 |
| GET | `/api/v1/batches/{batch_id}/precision` | 舍入/误差说明 |
| GET | `/api/v1/batches/{batch_id}/verify` | 真值核对结论 |
| GET | `/api/v1/compare?left={run}&right={run}` | 任意两个兼容运行逐批对比 |
| GET | `/api/v1/recovery/report[?all=1]` | 最新/全部恢复报告 |
| GET | `/api/v1/intents?limit=100` | 意图日志 |
| GET | `/api/v1/diagnostics` | 页面聚合数据 |

组织示例：

```bash
curl -sS -X POST -H 'Content-Type: application/json' \
  --data-binary @samples/organize-full.json \
  'http://127.0.0.1:8080/api/v1/traces/demo/organize?version=v1'
```

主要业务码：

| 业务码 | HTTP | 含义 |
|---|---:|---|
| `E_SPEC_INVALID` | 400 | 模型清单整批非法 |
| `E_TRACE_INVALID` | 400 | 轨迹整批非法/未知模型/非单调到达 |
| `E_INSUFFICIENT_SAMPLES` | 422 | 请求分位低于声明样本下限 |
| `E_PRECISION_BREACH` | 422 | 绝对/相对误差越界，批次已标记失败 |
| `E_INSUFFICIENT_CAPACITY` | 409 | 节点容量不足，未提交新批次 |
| `E_CONFLICTING_REQUEST` | 409 | 重复/冲突提交或尝试修改已封批 |
| `E_LATENCY_LIMIT` | 422 | 已提交批次超期望批延迟，未标记通过 |
| `E_BAD_REQUEST` | 400 | 参数或方法错误 |
| `E_NOT_FOUND` | 404 | 事实不存在 |

## 实际验证结果

验证环境：`go1.27.4 linux/amd64`，模块目标 Go 1.22，`modernc.org/sqlite v1.34.5`。本地样本使用固定模型清单与固定轨迹，不访问真实模型或加速器。

`./scripts/demo.sh` 结果：**66 / 66 检查通过**。关键核对值：

- full run：`run-7979ad6b3e364fae7698fdbe`，`result_hash=ef7c5941feecc1b249174e0991100f35a7c0dd26109631598ceb41e8f4543e3b`，2 批，选中 7 请求，149 样本。
- full 批 1：120 样本、VRAM `96,636,764,160` 字节、延迟 `210000us`；P50=`1500us`、P95=`3000us`、P99=`3000us`，样本数 120；舍入误差 0。
- full 批 2：29 样本、延迟 `36325us`，2 位小数报告 `36.33ms`，舍入误差 `+5us`；P99 查询返回 HTTP 422 / `E_INSUFFICIENT_SAMPLES`，声明样本数 29、下限 100。
- fast run：`run-c7fdf2c120d0c791f317ed0c`，1 批、选中 4 请求、120 样本、推迟 3 请求，推迟原因 `ORGANIZATION_BUDGET_EXCEEDED`。
- fast/full 对比：`same_composition_pairs=1`，fast `deferred=3`，full `deferred=0`；逐批样本、VRAM、延迟与模型集合均可核对。
- 组织顺序：`order` 轨迹在 arrival 下为 `run-66dee2eb7875331eb291bf42`，首批 `low`；priority 下为 `run-7241276cf18e73d1c8611f1b`，首批 `high-1,high-2`。两者 `run_id`/`plan_hash` 不同、样本均守恒，priority 重放字节一致。
- 拆分样本：`run-a0ae9856074bc55a6f7eaf90` 产出 512 + 188 样本，延迟 `170496us` / `62604us`，舍入误差 `+4us` / `-4us`，累计 0；总量 700 守恒。
- 精度探针：`precision_offset_us=2000` 返回 `E_PRECISION_BREACH`，两个批次均 `precision_failed`，绝对偏差 2000/2005us，事实仍可查询。
- 容量不足：请求 200Gi、可用 128Gi 返回 `E_INSUFFICIENT_CAPACITY`，未提交新批次。
- 请求参数注入：`duplicate_request_id=r1` 返回 `E_CONFLICTING_REQUEST`；`vram_shortfall_bytes=127Gi` 返回 `E_INSUFFICIENT_CAPACITY`。
- 混批延迟门禁：`mix-reject` 返回一条 `MIXED_MODEL_LATENCY_LIMIT`，既有批保持不变。
- 业务码互异验证：`E_SPEC_INVALID`、`E_TRACE_INVALID`、`E_INSUFFICIENT_SAMPLES`、`E_PRECISION_BREACH`、`E_INSUFFICIENT_CAPACITY`、`E_CONFLICTING_REQUEST` 均独立触发。

`./scripts/crash-demo.sh` 结果：**PASS**。

- 强杀运行 `run-4ebf0ed442c5d84e868cd28b`，进程退出码 87，curl 收到空响应。
- 重启恢复报告 `clean=false`，中止运行 1 个，保留完整批次 `run-4ebf0ed442c5d84e868cd28b:b0001`，未完成意图 `COMMIT_BATCH_BASE` 被标记。
- 中止运行状态为 `aborted`，仅计已提交批次 VRAM `10,737,418,240` 字节，`batch_count=1`；未伪造第二个批次或完成结论。
- 再次组织生成 `run-4ebf0ed442c5d84e868cd28b-a2`，状态 `complete`、2 批；中止事实仍保留可查。

自动化测试覆盖：`go test ./...` 包含量纲/溢出、整批轨迹拒绝、确定性计划哈希、组织顺序身份/首批/重放、优先级隔离、迟到封批边界、混批延迟拒绝、容量错误、拆分守恒、P99 样本不足、精度越界、HTTP 业务码、意图日志、恢复报告与恢复后再组织。

## 设计边界

- 所有“显存”都是本地整数预算，不代表真实分配器；不加载 GPU 或模型。
- 分位总体是批内已分配样本的单样本代价观测，不是对实际墙钟重复实验的估计。
- 强制终止后的恢复只承诺事实边界与中止状态，不承诺补算未提交分位或生成完成结论。
- API 面向本机离线诊断，无认证；默认仅绑定回环地址。若开启非回环监听，应由调用方自行提供网络边界保护。
- 自然时间戳只用于运行/报告审计，不参与 `plan_hash` 或确定性批次结论。

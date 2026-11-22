# HotRoute：本地模型算子图热切换与迁移组件

HotRoute 在单机环境中维护多个带版本号的模型算子图，按路由/灰度计划选择版本，用 NumPy 确定性执行推理，并在 SQLite 中原子切换路由表、前后处理缓存、在途请求绑定索引和灰度计划。它不依赖容器、模型权重、GPU 或网络账号。

本仓库包含可运行实现、HTTP API、固定本地样本验证工具、自动化测试和技术说明。强制终止验证使用真实子进程与外部 `process.kill()`，不是模拟异常。

## 快速开始（Windows / Linux）

```powershell
py -3.10 -m pip install -e ".[test]"
py -3.10 -m pytest
py -3.10 -m hotroute verify
py -3.10 -m hotroute serve --db .\data\hotroute.db --host 127.0.0.1 --port 8765
```

`verify` 会：

1. 通过回环 HTTP 提交带序号和生成时间的完整快照文件；
2. 完成一次仅量化位宽变化的热切换；
3. 完成一次张量形状变化触发的缓存重建；
4. 验证切换前绑定、切换后完成的旧版本推理；
5. 验证 50% 灰度的定点归一化、补偿求和、分流和重复裁决；
6. 并发提交两个同基础版本增量，验证仅一个成功；
7. 外部强杀进程，重启后依据 SQLite 意图记录恢复；
8. 恢复后再次热切换，核对路由与四类派生产物均指向新快照。

实际结果写入 `artifacts/verification.json` 和 `artifacts/VERIFICATION.md`。

## 目录

```text
hotroute/
  graph.py       图解析、形状/dtype/量化/衔接校验
  signature.py   确定性签名与差异位掩码
  execution.py   固定种子 NumPy 推理、量化/反量化、摘要
  storage.py     SQLite schema、WAL、事务辅助
  engine.py      快照/增量、级联迁移、灰度、绑定、恢复
  api.py         Flask HTTP API
  cli.py         服务、快照文件加载、恢复、验证入口
  samples.py     固定本地算子图与快照/增量样本
  verify.py      真实 HTTP 验收流程
tests/           34 个自动化测试
artifacts/       实际验证报告
```

## 算子图格式

```json
{
  "inputs": [{"name": "input", "shape": [2, 4], "dtype": "float32"}],
  "nodes": [
    {
      "id": "linear",
      "op": "linear",
      "inputs": [
        {"name": "x", "shape": [2, 4], "dtype": "float32"},
        {"name": "weight", "shape": [4, 6], "dtype": "float32"}
      ],
      "outputs": [{"name": "hidden", "shape": [2, 6], "dtype": "float32"}],
      "quant_bits": 16,
      "preprocess": ["tokenize:tok-v1", "normalize:preproc-v1"],
      "postprocess": ["denormalize:preproc-v1"]
    }
  ],
  "outputs": ["linear"],
  "vocab_refs": ["tok-v1"]
}
```

规则：

- 图有且仅有一个主输入张量；每个节点按顺序消费前一个节点的输出，`inputs[0]` 必须与前一输出在形状和 dtype 上完全一致。
- `linear`/`add` 的第二个输入是本地固定种子生成参数，其形状也参与校验；节点固定只有一个输出。
- 形状维度必须为正整数，单张量最多 2000 万元素。允许 dtype：`float16`、`float32`、`float64`、`int32`、`int64`。量化位宽枚举：`8`、`16`、`32`。
- 支持算子：`identity`、`relu`、`tanh`、`scale`、`add`、`linear`、`layernorm`、`softmax`、`reshape`。
- 首错返回 `E_GRAPH_INVALID`，上下文包含 `node_id`、`field`、期望值和实际值。完整快照中的任一模型图失败时，整份提交不入库。
- `vocab_refs` 可在图级和节点级声明。提交时不强制词表存在，推理前会完整预检；缺失时返回 `E_VOCAB_MISSING`，不会执行部分图。

## 签名与差异位掩码

图签名是规范化 JSON 的 SHA-256，覆盖主输入、节点顺序、节点 ID、算子、输入/输出形状、dtype、量化位宽、前后处理步骤和词表引用。

| 位 | 值 | 名称 | 含义 |
|---|---:|---|---|
| 0 | 1 | `shape_changed` | 主输入、节点输入或输出形状变化 |
| 1 | 2 | `dtype_changed` | 任一衔接张量 dtype 变化 |
| 2 | 4 | `quant_bits_changed` | 量化位宽变化 |
| 3 | 8 | `op_added_removed_or_changed` | 算子/节点顺序/节点 ID/图输出变化 |
| 4 | 16 | `prepost_changed` | 前后处理步骤标识变化 |
| 5 | 32 | `vocab_refs_changed` | 特征词表版本依赖变化 |

`mask == 4` 时 `in_place_migratable=true`、`mode=in_place_quant`，否则需要重建请求缓冲。`mask == 0` 表示逻辑图未变化。

## 四类派生产物与迁移规则

每个当前模型版本维护 `route`、`preprocess_cache`、`inflight_index`、`gray_plan` 四类产物。一次切换返回 `migrations[].artifacts[]`，并汇总为 `rebuild`、`migrate`、`retained`、`created`、`updated` 清单。

| 差异 | preprocess_cache | inflight_index | gray_plan |
|---|---|---|---|
| 无差异且版本相同 | 保留 | 保留 | 保留 |
| 仅量化位宽 | 原地迁移，更新量化元数据 | 原地迁移 | 比例相同时迁移 |
| 形状/dtype | 重建 | 重建 | 重建 |
| 前后处理/词表 | 重建 | 重建 | 重建 |
| 仅算子变化 | 迁移 | 重建 | 重建 |

路由表项总是指向新活动版本。上述四类产物与迁移事件在同一个 `BEGIN IMMEDIATE` 事务中提交，因此外部读取不会看到“路由已新版本、缓存仍按旧形状”的中间状态。未受影响的产物保持代次，不做无谓重建。

## 完整快照与增量

完整快照文件必须包含 `snapshot_seq` 和带时区的 `generated_at`：

```json
{
  "snapshot_seq": 1,
  "generated_at": "2026-11-23T03:00:00Z",
  "known_vocabs": ["tok-v1", "preproc-v1"],
  "models": [
    {"model_id": "local-classifier", "version": "v1", "graph": {"...": "..."}}
  ],
  "active_models": {
    "local-classifier": {
      "version": "v1",
      "previous_version": "v1",
      "gray_ratio": 0.0
    }
  }
}
```

- 完整快照要求序号严格递增；原子替换模型版本、活动路由和词表集合，并清除已移除模型的活动派生产物。
- 相同图、相同活动版本的完整快照只更新快照元数据，四类产物代次保持不变。
- 完整快照若带非零灰度，必须同时携带旧版本图；仅携带新版本且灰度为 0 时，灰度旧目标规范为新版本，避免裁决引用已删除图。
- 增量只提交受影响模型，携带全局 `base_version`（上一 `state_version`）；也支持单一模型增量用模型级 `base_version`。基础版本不匹配返回 `E_STALE_BASE`，不改变已提交状态。
- 同一基础版本的两个并发增量由 SQLite 写事务串行化，只有一个成功，另一个得到 `E_STALE_BASE`。
- 文件加载 CLI：`python -m hotroute snapshot-load --db data/hotroute.db --file snapshot.json`，增量使用 `increment-load`。

## 灰度与在途绑定

比例用百万分之一为单位的定点整数表示：

```json
{"fixed_point": {"units": 500000, "scale": 1000000}, "rounding_error": "0.0"}
```

比例必须在 `[0, 1]` 内，否则返回 `E_GRAY_RATIO_INVALID`，上下文附 `ratio`、`current_ratio`、`legal_interval: [0, 1]`。分流累计使用 Decimal Kahan 补偿求和；灰度裁决在 `(state_token, model_id, request_id)` 上持久化缓存，因此同一快照内重复裁决目标版本逐位一致。

请求进入时绑定具体版本，并在 `bindings` 表保存图、词表集合和快照状态。后续热切换不会改绑；推理响应返回 `bound_version` 和 `bound_snapshot_seq`。进程重启时尚未完成的绑定被标记为 `interrupted`，不会伪装成成功。

## 推理与摘要

`POST /api/infer` 接收 `request_id`、`model_id`、`seed`，可选 `version`。没有显式版本时先走灰度裁决。样本输入和节点参数均由固定种子在本地生成；相同模型版本、相同 seed 的推理结果逐位一致，完成的请求直接复用持久化结果。

量化节点采用对称有符号、四舍五入到最近整数的量化/反量化规则。响应包含每个节点的：

- 输出张量形状、dtype、内容 SHA-256、样本值和统计量；
- `quant_bits`、量化是否应用、scale、最大/平均/累计绝对误差、误差 SHA-256；
- 图级 `result_digest`。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 服务/状态健康检查 |
| POST | `/api/snapshots` | 提交完整快照 |
| POST | `/api/increments` | 提交增量片段 |
| GET | `/api/migrations` | 查询迁移与重建清单，可筛选 `snapshot_seq`/`model_id` |
| GET | `/api/artifacts` | 查询四类产物当前元数据 |
| GET | `/api/state` | 当前快照、路由、产物和灰度计划 |
| POST | `/api/grays/decide` | 灰度裁决并绑定请求版本 |
| POST | `/api/grays/normalize` | 比例定点归一化 |
| POST | `/api/infer` | 按绑定版本执行推理 |
| GET | `/api/bindings/<request_id>` | 查询在途/完成绑定 |
| GET | `/api/recovery` | 最近一次启动恢复报告 |

成功响应统一为 `{"ok": true, "data": ...}`；失败响应统一为：

```json
{
  "ok": false,
  "code": "E_GRAPH_INVALID",
  "message": "adjacent tensors are not compatible in shape and dtype",
  "context": {"node_id": "norm", "field": "inputs[0]"}
}
```

主要业务码：`E_GRAPH_INVALID`、`E_STALE_BASE`、`E_SNAPSHOT_STALE`、`E_VOCAB_MISSING`、`E_GRAY_RATIO_INVALID`、`E_VERSION_NOT_FOUND`、`E_MODEL_NOT_FOUND`、`E_REQUEST_BINDING_MISMATCH`、`E_REQUEST_INTERRUPTED`、`E_STATE_EMPTY`、`E_VERSION_CONFLICT`、`E_BAD_REQUEST`、`E_PORT_UNAVAILABLE`。

## 意图持久化与强制终止恢复

- 快照替换、增量应用、请求绑定、灰度裁决和推理开始前，先单独提交 `intents.status=started`。
- 路由、缓存、在途索引、灰度计划、迁移事件和意图完成状态在同一数据库事务内生效。
- 进程可能在意图写入后、事务提交前、推理中任意时刻被强杀。SQLite WAL 会丢弃未提交事务。
- 下次启动扫描 `started` 意图并标记 `aborted`；`pending` 绑定标记 `interrupted`；删除序号高于最后完整快照的残留派生产物。
- `/api/recovery` 只报告 SQLite 中的持久化事实，明确返回 `exit_report_available=false` 和 `persisted_facts_only=true`，不承诺被强杀进程生成完整退出报告。

验证工具使用仅供验收的故障注入环境变量 `HOTROUTE_PAUSE_AFTER_INTENT_KIND`、`HOTROUTE_INTENT_MARKER`、`HOTROUTE_PAUSE_SECONDS`：意图提交后暂停并写 marker，外部进程检测到 marker 后执行 `kill()`。正常运行时这些变量不存在，不影响行为。

## 测试与实测结果

```text
python -m pytest
34 passed

python -m hotroute verify
{"passed": true}
```

本次实际验证的关键结果：

- 仅量化变化：`diff_mask=4`、`in_place_migratable=true`；`preprocess_cache`、`inflight_index`、`gray_plan` 全部迁移，`rebuild=[]`。
- 形状变化：`diff_mask=1`、`in_place_migratable=false`；前后处理缓存与在途索引重建。
- 灰度：`500000/1000000`，六次裁决在 `v2-quant`/`v3-shape` 间交替；重复裁决返回相同目标。
- 切换前绑定的 `v2-quant` 请求在形状切换后完成，版本未改绑，结果摘要与切换前参考推理逐位一致。
- 两个同基础版本并发增量：一个成功，一个 `E_STALE_BASE`。
- 外部强杀后恢复：快照保持序号 4；1 个未完成意图被标记中止，1 个待完成绑定被标记中断；未生成虚假退出报告。
- 恢复后提交序号 6 的新热切换，路由与全部产物快照序号均为 6。

完整机器可读结果见 `artifacts/verification.json`，人读摘要见 `artifacts/VERIFICATION.md`。

## 运行边界

- HTTP 服务绑定回环地址，使用 Flask 开发服务器即可满足本地验收；生产环境应替换为受控 WSGI 服务。
- SQLite 连接采用 WAL、`synchronous=FULL`、`BEGIN IMMEDIATE` 和 30 秒 busy timeout，面向单机多线程；不是多节点分布式协调器。
- 完整快照替换模型版本表；在途请求仍可完成，是因为绑定时已持久化完整图和词表快照。
- 样本规模刻意保持较小，以便确定性验证、快速重启和可审计的摘要输出。

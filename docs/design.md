# 技术设计说明

## 1. 确定性体系

- **规范化序列化**（`mmpipe/codec.py`）：字典键排序、固定分隔符 `(",", ":")`、
  `ensure_ascii=False`、禁用 `NaN` 记号、numpy 类型先转内置类型；浮点使用 Python
  `repr` 保证往返一致。所有产物、检查点、状态、日志均经此序列化，因此可逐字节比较。
- **版本指纹**：BLAKE2b-256（标准库 `hashlib`，跨平台稳定）。输入指纹与配置版本分离：
  - 阶段输入指纹完全由**原始输入 + 配置**推导（`pipeline.expected_inputs`），
    无需依赖已发布产物，恢复/失效判断可在执行前完成；
  - 阶段配置版本只包含该阶段相关配置键（`pipeline.STAGE_CONFIG_KEYS`），因此修改
    融合权重只改变 `fusion_scoring` 起的配置版本，不会误失效上游文本/视觉阶段。
- **文本嵌入**（`text_encoding.py`）：NFKC → 空白折叠 → 小写 → 分词（ASCII 字母数字
  成词，非 ASCII 字母/数字逐字成词）→ 每 token 取 BLAKE2b-128 摘要，前 8 字节定桶、
  第 9 字节定符号，累加后 L2 单位化，输出 64 维。
- **样本生成**（`samplegen.py`）：`numpy.random.RandomState(20251120)` 生成投影矩阵与
  导向特征；冲突样本以目标嵌入反向构造，确定性保证标签分布为
  consistent/weak/conflict 各有覆盖。

## 2. 检查点与原子发布

检查点信封：

```json
{
  "seq": 3, "stage": "fusion_scoring",
  "input_fingerprint": "...", "config_version": "...",
  "product_digest": "...", "product_path": "products/fusion_scoring_000003.json",
  "created_at": 1795154487486697413,
  "checksum": "<信封其余字段的 BLAKE2b-256>"
}
```

- 发布顺序：先原子写产物 → 再原子写检查点 → 更新 `current/` → 重建 `state.json`。
- 原子写（`platform_io.atomic_write`）：同目录 `mkstemp` 临时文件 → `flush/fsync` →
  `os.replace` → 目录 `fsync`。写入任意位置中断，目标文件都不会出现半写内容。
- 恢复时 `load_valid_checkpoints` 只接受校验和通过、字段完整、产物存在且序号从 1
  连续的记录；其余记录在工作区打开时由 `_heal_and_rebuild` 移入
  `checkpoints/quarantine/`，状态按最长连续有效链重建。
- 序号全局单调、连续：新检查点序号恒为有效链尾 + 1；失效重建产生的新发布继续追加，
  旧检查点不可变、不删除，仅由失效日志标记被取代的序号。

## 3. 优雅停止与恢复一致性

- `StopController` 双通道：线程内 `threading.Event` 与工作区 `stop.flag` 文件
  （便于跨进程/CLI `mmpipe stop` 与 HTTP `POST /stop`）。
- 每个阶段在线程池中按**不可拆分样本单元**执行（`pipeline._execute_stage`），
  停止检查位于单元边界：在途单元跑完，未开始单元取消；阶段未完成则不发布检查点，
  下次恢复整体重算该阶段，因此不存在"部分阶段产物"。
- 恢复后所有产物均为确定性函数的输出，且已发布阶段通过指纹匹配直接复用，最终四阶段
  产物与一次从零完整运行逐字节一致（由 `tests/test_pipeline.py` 与 `demo/run_demo.py`
  场景 2、7 断言）。
- 强制 kill 不承诺退出报告（符合任务约定）；重启后依赖原子写与检查点链自愈。

## 4. 失效传播

`scan_invalidations` 按阶段顺序比对已发布检查点的输入指纹与配置版本，发现首个不匹配
即调用 `Workspace.invalidate_from`，对该阶段及其全部下游：

1. 追加 `logs/invalidations.jsonl`（原因、组件、旧/新指纹、被取代检查点序号）；
2. 状态置为 `invalidated`（旧检查点保留可追溯）；
3. 下次运行时这些阶段重算并以新序号发布。

变更与最早失效阶段对应关系：

| 变更 | 最早失效阶段 |
| --- | --- |
| 文本片段 | text_encoding（全链路） |
| 视觉特征 | visual_alignment |
| 投影矩阵 | visual_alignment（其配置版本变化） |
| 融合权重/阈值 | fusion_scoring |
| 算子置信度 | fusion_scoring |

`GET /status` 与 `mmpipe status` 同时输出当前指纹、期望指纹、`matches_expected`
与失效详情，禁止旧版本结果冒充新结果。

## 5. 并发发布保护

`Workspace.publish` 在 `locks/workspace.lock` 上持跨进程锁执行：

- 比较有效链尾与调用方声明的 `expected_base_seq`，不一致 → `E_STALE_CHECKPOINT`；
- 目标阶段已发布（指纹相同=重复，或不同=需先失效）→ `E_STALE_CHECKPOINT`，不覆盖；
- 前置阶段未发布 → `E_INVALID_STAGE`；
- 通过后以链尾 + 1 发布，保证仅允许序号连续推进。

## 6. 错误码链路

| 错误码 | 触发场景 | 阶段 |
| --- | --- | --- |
| `E_MISSING_INPUT` | 文本为空/无 token、特征缺失、引用文本 id 不存在、算子置信度缺失 | 对应阶段 |
| `E_SHAPE_MISMATCH` | 特征维度 ≠ 投影输入维度、投影矩阵行数 ≠ 嵌入维度 | visual_alignment |
| `E_NUMERIC` | 特征/矩阵/相似度/融合分出现 NaN/Inf、零范数 | visual_alignment / fusion_scoring |
| `E_INVALID_STAGE` | 非法阶段名、前置阶段缺失、聚合产物未发布即查询 | 对应阶段 |
| `E_STALE_CHECKPOINT` | 重复/落后/未失效即重发的发布请求 | publish |
| `E_STOP_REQUESTED` | 单元边界收到停止（内部控制流，非数据错误） | 运行中阶段 |

单元级错误在阶段内聚合为 `StageFailure`，阶段失败时不发布任何检查点。

## 7. 验证工具（`mmpipe/verifier.py`）

`mmpipe-verify check` 覆盖：检查点序号从 1 起、连续且单调；损坏记录被忽略；
产物存在、摘要一致、规范化字节稳定；阶段顺序与状态一致；聚合计数完整；
冲突样本未被丢弃；无 NaN/Inf；失效日志结构一致。`compare` 子命令对两份产物做
逐字节比对并输出双方摘要。

# 架构与一致性设计

## 1. 处理模型

每次对齐运行由四个有序阶段组成：

```text
normalize → resample → fusion → summary
```

阶段不是内存中的临时步骤，而是可在检查点账本中审计的持久化产物。阶段产物采用内容寻址命名；检查点目录只引用当前指纹对应的产物。

### 统一网格

`timestamp_ms = start_ms + index * step_ms`，`index` 从 0 单调递增，最后一点不超过 `end_ms`。网格指纹包含 `step_ms`、`start_ms`、`end_ms` 与点数。

### 规范化

严格规则：

- 每条记录必须匹配文件对应的 `sensor_id`。
- `timestamp_ms` 必须是整数。
- 原始文件必须严格单调递增且无重复时间戳；否则返回 `E_TIME_INVALID`。
- 即使失败，也会对排序/去重后的时间戳生成确定性预览并写入诊断文件。
- 质量标记不在 `quality_accept` 内的记录保留在清洗报告中，但不参与插值。
- 非数值、NaN/Inf、超出有效区间的通道值被置空并记录详情。

### 重采样

对每个源、每个通道独立建立有效样本序列：

- 精确命中：`method=exact`。
- 两个有效样本之间：间隙大于 `max_gap_ms` 时不插值，保留 `missing` 原因。
- `ffill`：取前一个有效值。
- `linear`：使用 NumPy 浮点运算，按有效值区间钳制，记录 `clamped=true/false`。
- `nearest`：距离相等时确定性地选择较早时间戳。
- 观测区间之外：仅 `ffill`/`nearest` 且允许外推时可生成值，标记 `extrapolated=true`。

每个网格点记录 `source_timestamps_ms`、`method`、`requested_method`、`reason`、`extrapolated`、`clamped`。

### 融合

每个通道对有效候选源按以下键排序：

```text
priority 降序, source_id 升序（lexicographic 仲裁）
```

支持模式：

- `weighted_mean`：全部有效源按配置权重归一化贡献。
- `priority_group_weighted`：仅最高优先级组参与加权。
- `priority_override`：最高优先级源胜出，其余贡献为 0。

冲突记录包含全部参与源、优先级、权重、贡献、原值、方法、来源时间戳和外推标记。只有单源时标记 `single_source` 或 `extrapolated`。无有效源时仍输出网格点，并为每个缺失通道写入缺口记录；全部通道缺失的网格点标记为 `gap`。

## 2. 版本指纹与增量计划

运行前计算：

```text
current normalize-source fingerprints
current grid fingerprint
current normalize fingerprint
current resample-source fingerprints
current resample fingerprint
current fusion fingerprint
current summary fingerprint
```

与检查点头部 `catalog` 比较：

- 阶段完全匹配：整阶段复用。
- 阶段级指纹变化但部分源指纹匹配：复用未变化源，只重建变化源。
- 当前阶段无法复用：从该阶段向上执行；下游全部重新发布。

失效原因根据语义配置切片与源摘要比较得出，并持久化在 `state/invalidations.json` 与各检查点记录中。读取摘要、冲突或缺口时还会二次检查当前指纹与已发布指纹，避免“旧结果被当作当前结果”返回。

## 3. 检查点事务

### 正常发布

1. 取得 `state/publish.lock` 的排他 `flock`。
2. 读取并验证账本、锚点、哈希链。
3. 比较序号、运行 ID、阶段顺序、配置指纹。
4. 校验前置阶段、所有产物路径与 SHA-256。
5. 计算 `prev_record_hash` 与 `record_hash`。
6. 写 `checkpoint.pending.json`。
7. 临时文件原子替换 `checkpoints.jsonl`。
8. 临时文件原子替换 `checkpoints.anchor.json`。
9. 删除 pending 标记。

### 中断恢复

若在步骤 6 后中断：

- 只有 pending、账本与锚点仍是旧值：删除 pending，继续旧状态。
- 新账本已提交、锚点仍是旧值：根据 pending 中新锚点内容修复锚点。
- 两者均为新值：删除 pending。
- 三者不一致：`E_STALE_CHECKPOINT`，`kind=tampered`。

### 整体替换识别

- 旧账本被替换且序号低于锚点：`kind=rollback`。
- 账本字节摘要或头记录哈希不匹配：`kind=tampered`。
- 同运行配置指纹突变：`kind=config_changed`。
- 阶段产物文件摘要不匹配：`kind=tampered`。
- 只有严格连续的新记录会被接受。

## 4. 停止与恢复

停止是请求而非强制终止。运行器只在阶段产物发布完成后读取停止标记，因此不会留下半阶段状态。

恢复要求：

- 最近运行状态为 `stopped`、`paused`、`running` 或 `error`。
- 当前源摘要、语义配置指纹、配置指纹与停止时一致。
- 继续使用同一 `run_id`，新检查点阶段必须满足严格相邻。

若输入已改变，必须开始新的增量运行；恢复请求返回 `E_STALE_CHECKPOINT(kind=input_changed)`。

## 5. HTTP 与并发

HTTP 服务仅绑定 `127.0.0.1`，静态资源无外部依赖。所有运行使用工作区级 `state/run.lock` 非阻塞排他锁；已有运行返回 `E_RUN_ACTIVE`。检查点发布则使用独立的真实 `flock`，可安全承受多个工作进程竞争同一序号。

API 中所有错误以统一结构返回：

```json
{
  "ok": false,
  "error": {
    "code": "E_TIME_INVALID",
    "message": "...",
    "details": {}
  }
}
```

## 6. 确定性边界

以下内容不进入摘要或阶段产物，以免影响逐字节一致性：

- 当前时间、进程 ID、随机运行 ID
- 检查点顺序号
- 日志与 UI 状态

摘要只由语义配置、源内容、阶段指纹与对齐数据决定。固定种子样本的摘要由自动化测试硬编码守护。


# 架构与一致性设计

## 1. 模块边界

| 模块 | 职责 |
| --- | --- |
| `mmcoop/generator.py` | 固定种子生成文本 JSONL、图像 JSON、64 维特征和 PNG |
| `mmcoop/stages.py` | 四个纯计算阶段、规范化、哈希嵌入、投影、融合、聚合 |
| `mmcoop/pipeline.py` | 阶段内容摘要、版本指纹、增量复用、全量对照验证 |
| `mmcoop/checkpoints.py` | 原子检查点、真实文件锁、序号链、快照完整性裁决 |
| `mmcoop/service.py` | 构建事务边界、状态/结果查询、停止/恢复、数据编辑 |
| `mmcoop/api.py` | FastAPI HTTP 接口与后台构建线程 |
| `mmcoop/cli.py` | 命令行入口与本地 `uvicorn` 启动 |

## 2. 确定性边界

所有业务产物使用以下规范化 JSON 序列化：

- UTF-8；
- 对象键排序；
- 紧凑分隔符；
- `allow_nan=False`；
- 浮点统一保留 12 位小数，`-0.0` 归一为 `0.0`；
- 记录按 `text_id` / `image_id` 排序。

文本哈希嵌入不使用 Python 进程随机哈希，而是对规范化 token 和维序号逐项执行 SHA-256，永不依赖 `PYTHONHASHSEED`。

## 3. 阶段摘要与版本指纹

每个阶段保存独立的内容分量摘要：

| 阶段 | 直接分量 |
| --- | --- |
| 文本编码 | 文本内容、编码配置、结构版本 |
| 投影对齐 | 上游产物摘要、图像特征/关联/PNG、投影矩阵、结构版本 |
| 融合打分 | 上游产物摘要、语言/来源元数据、算子标签、权重、阈值、置信度、语言配置、结构版本 |
| 结果聚合 | 上游产物摘要、文本结构、聚合配置、结构版本 |

阶段输入指纹为：

```text
SHA256(canonical_json({stage, components}))
```

总版本指纹同时覆盖四个阶段的输入指纹和产物摘要。任一阶段输入变化后，从最早发生变化的阶段开始重建全部下游阶段。上游输入未变化的阶段直接复用已通过摘要校验的产物。

每次增量构建都会重新从零计算一份内存产物，并与复用/重建结果逐阶段进行字节比较。差异通过第一个 JSON 路径定位，例如 `$.records[0].fusion_score`。

## 4. 检查点事务模型

检查点记录包含：

```text
schema_version, sequence, stage, stage_index,
input_fingerprint, config_version, artifact_sha256,
artifact_path, upstream, components, record_sha256
```

发布顺序：

1. 获取 `locks/publish.lock` 的真实进程锁（POSIX `flock`，Windows `msvcrt.locking`）；
2. 扫描并校验既有检查点链；
3. 校验快照完整性裁决；
4. 校验前置阶段与上游产物摘要；
5. 将不可变产物原子写入 `artifacts/<stage>/<sha256>.json`；
6. 将检查点原子写入 `checkpoints/<8位序号>-<stage>.json`；
7. 原子更新 `state/published.json` 快照标记。

原子写采用同目录临时文件、`flush`、`fsync`、`os.replace` 和目录 `fsync`。临时文件不是检查点，扫描器不会读取。若在检查点完成后、标记更新前崩溃，扫描器会得到 `normal_advance` 并自动对齐标记。

## 5. 序号与并发裁决

- 新序号必须为当前有效链尾序号 + 1；
- 同阶段同输入指纹同产物摘要的重复发布返回 `E_STALE_CHECKPOINT`；
- 缺少直接前置阶段返回 `E_INVALID_STAGE`；
- 产物声明的上游摘要与当前上游产物不一致返回 `E_STALE_CHECKPOINT`；
- 无有效链但在目标序号发现损坏/重复文件时，发布前将其移入 `quarantine/checkpoints/`。

## 6. 完整性裁决

`state/published.json` 保存已发布链尾和阶段视图。`GET /api/checkpoints/integrity` 返回：

- `normal`：标记、检查点链、产物摘要和阶段指纹完全一致；
- `normal_advance`：有效检查点链领先于标记，通常由进程在两次原子写之间崩溃造成；
- `rollback`：标记链尾检查点消失或有效链序号回退；
- `tamper`：记录校验、产物摘要、阶段指纹、标记校验或检查点重复/损坏异常。

`rollback` 与 `tamper` 会阻止后续发布，避免在不可信快照上继续生成结果。整个目录连同标记一起被外部替换为另一份自洽旧快照时，本地系统无法区分其与合法快照；其余可观测的回退、摘录不符和链断裂均会裁决。

## 7. 停止语义

`stop.request` 是持久协作式停止标记。计算阶段在每条不可拆分样本记录前后检查该标记：

- 当前样本完成后不再启动下一条；
- 未完成阶段不发布检查点；
- 不影响已经发布的上游检查点；
- `resume` 清除停止标记，从当前有效阶段链继续；
- 强制终止不在承诺范围内，也不会伪造退出报告。

## 8. 业务错误边界

所有注入都在内存计算阶段触发，先完成全阶段预计算，再进入发布阶段。因此参数注入失败不会留下半个阶段结果或推进检查点序号。HTTP 统一返回：

```json
{
  "ok": false,
  "error": {
    "code": "E_...",
    "stage": "...",
    "sample_id": "...",
    "field": "...",
    "diagnostics": {}
  }
}
```

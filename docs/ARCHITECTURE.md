# 架构与一致性设计

## 统一内部模型

`PanopticModel` 是唯一的阶段间模型：

```text
shard_id, width, height
labels: int64[H, W]                 # 每个像素的全景段 id
segments: [
  segment_id, category_name, category_id, instance_index,
  pixel_count, bbox=(x, y, w, h), confidence
]
```

约束：

- 类别 id 从 1 开始，0 保留给未分配/过滤后的背景段。
- 全景段 id 由清单的 `instance_start_id` 起连续分配，必须大于最大类别 id。
- 类别按 `category_id` 顺序处理；连通域按行优先发现顺序处理，因而段 id 分配确定。
- `thing=true` 的类别按 4 连通域分离实例；`thing=false` 的类别合并为一个语义段。
- 每个实例段的 `instance_index` 从 0 递增；stuff 段固定为 0。
- 面积过滤作用于 thing 连通域和 stuff 总体区域；被过滤像素进入背景段 0。
- 模型构造时会验证段表、像素数、标签集合和总数 `sum(pixel_count)=H*W`。

## 四阶段

1. `decode`：读取 PNG 字节，统一转 RGB，按清单颜色映射得到类别 id 图。未知颜色中止阶段，写出诊断 JSON，不写阶段产物或检查点。
2. `merge`：按类别做连通域、面积与类别置信度过滤，合并为全景段 id 图和段属性表。
3. `encode`：同一模型产出 PNG 索引色、RLE 字符串、COCO 风格 JSON；任何超限格式被明确跳过并记录降级。
4. `validate`：执行 `model -> PNG -> model`、`model -> RLE -> JSON wrapper -> model`、`model -> COCO JSON -> model`，逐像素比较标签图，同时比较完整段属性表和像素数总和。

## 版本轴与阶段指纹

清单解析后生成确定性的 SHA-256 轴：

| 变化项 | 最早失效阶段 |
|---|---|
| 分片像素内容 | decode |
| 类别映射（名称/颜色/id） | decode |
| 清单结构版本 | decode |
| 实例起点编号 | merge |
| 最小面积阈值 | merge |
| 置信阈值 / 类别置信度 / thing 标记 | merge |
| 清单格式偏好 | encode |
| 调色板上限、段 id 位宽、PNG 压缩级别 | encode |
| 导出格式集合 | encode |
| 往返校验格式集合 | validate |

阶段指纹链：

```text
decode_fp   = hash(schema, source_fp, category_fp, source_encoding)
merge_fp    = hash(decode_fp, semantics_fp, instance_start, area, confidence)
encode_fp   = hash(merge_fp, mask_format, codec_fp, export_formats)
validate_fp = hash(encode_fp, validate_formats, codec_fp)
```

构建前比较当前指纹和最新检查点。最早不匹配阶段及其全部下游产物被删除并重建；上游匹配检查点直接复用。`state/invalidations.json` 记录原因、最早阶段、失效阶段、旧序号和预期指纹，`status` 接口同时显示当前待失效状态。失败前置校验不会写检查点或阶段产物。

## 检查点

每个阶段产物完全写成后，才发布一条不可变记录：

```json
{
  "sequence": 1,
  "shard_id": "shard_1_indexed",
  "stage": "decode",
  "input_fingerprint": "...",
  "manifest_version_fingerprint": "...",
  "artifact_digest": "...",
  "stage_fingerprint": "...",
  "artifact_files": [{"path": "...", "sha256": "..."}],
  "configuration_axes": {"...": "..."},
  "record_digest": "..."
}
```

写入步骤：同目录临时文件 → 写入/`fsync` → `os.replace` → 目录 `fsync`。因此中断最多留下不参与扫描的临时文件，不会出现半写 JSON。记录带自校验摘要，文件名序号和内容序号必须一致，日志序号必须从 1 连续。

发布在 `locks/checkpoints.lock` 的 `flock` 临界区内进行。调用方可以传入乐观 `expected_sequence`；落后、跳号、重复记录身份均返回 `E_STALE_CHECKPOINT`。构建器若发现另一个工作进程已发布完全相同且产物有效的阶段，会复用该记录。

## 停止与恢复

- `control/stop.json` 是停止请求。构建只在分片边界检查它，因此启动后的当前分片会完成四个不可拆分阶段。
- CLI 的 SIGINT/SIGTERM 处理器只写停止请求，不直接中断阶段写入。
- `resume` 清除停止请求；已有且指纹匹配的检查点复用，只继续缺失阶段和分片。
- 阶段产物内容不包含时间戳或绝对工作区路径，JSON 使用排序键；因此从零完整运行与“中途停止后恢复”的分片产物、验证摘要和检查点文件逐字节一致。
- 构建锁防止同一工作区同时执行两个构建；锁冲突返回 `E_BUSY`。

## 配置

工作区 `config.json` 主要字段：

```json
{
  "palette_max_colors": 256,
  "segment_id_bits": 24,
  "png_compress_level": 6,
  "export_formats": ["png_palette", "rle", "coco_json"],
  "validate_formats": ["png_palette", "rle", "coco_json"]
}
```

`segment_id_bits` 限制为 1..24，因为 PNG 调色板颜色以 24 位 RGB 表示段 id。超过限制不会截断，而是记录格式级降级。

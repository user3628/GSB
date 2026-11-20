# 掩码格式与往返规则

三种格式都携带完整段属性元数据，因此都能还原统一内部模型，而不只是像素 id。排序、JSON 键和压缩结果均为确定性输出。

## PNG 索引色掩码

- PNG 必须为索引色模式（Pillow `P`/`PA`）。
- 每个实际使用的调色板颜色编码段 id：`R = id >> 16`，`G = id >> 8`，`B = id`。
- 因此单段 id 上限为 `0xFFFFFF`，实际使用调色板项数不得超过配置上限（默认 256）。
- PNG `iTXt` 块键 `panopipe` 保存规范 JSON：格式名、分片 id、宽高、完整段属性表。
- 解码时从像素 RGB 恢复段 id，并校验元数据中的段 id、像素数和外接框与实际标签图一致；任何不一致返回 `E_FORMAT_ERROR`。
- 调色板项数超限返回 `E_PALETTE_CAPACITY`；位宽超限返回 `E_SEGMENT_ID_OVERFLOW`。编码器不会把大 id 映射到小索引后静默截断。

## RLE 字符串

```text
PANO-RLE-1:<base64url(header_json)>:<base64url(payload)>
```

`header_json` 使用排序键紧凑 JSON：

```text
format, shard_id, width, height, scan_order="row-major", segments
```

`payload` 是行优先像素序列的连续游程：

```text
varint(run_count_1), varint(segment_id_1),
varint(run_count_2), varint(segment_id_2), ...
```

所有 count 必须为正，count 之和必须等于 `width*height`。varint 为无符号 LEB128。解码器按行优先顺序重建标签图，并校验段元数据。base64url 不使用 `=` 填充。

用于往返链的 JSON 运输包装：

```json
{"format":"PANO_RLE_JSON_V1","rle":"PANO-RLE-1:..."}
```

`validate` 的 `rle_json` 链为 `model -> RLE string -> JSON wrapper -> RLE string -> model`。

## COCO 风格 JSON

顶层关键字段：

```json
{
  "format": "PANO_COCO_V1",
  "shard_id": "...",
  "image_id": "...",
  "file_name": "<shard>.png",
  "width": 64,
  "height": 48,
  "segments_info": [
    {
      "id": 100,
      "category_id": 4,
      "category_name": "person",
      "instance_index": 0,
      "iscrowd": 0,
      "area": 56,
      "bbox": [6, 20, 7, 8],
      "confidence": 1.0,
      "pixel_count": 56,
      "rle": {"size": [48, 64], "order": "row-major", "counts": "..."}
    }
  ]
}
```

每个段的 `rle.counts` 是二值掩码行优先游程计数的 LEB128 拼接后 base64url 字符串，首游程从背景 0 开始。解码器逐段恢复掩码，拒绝重叠、未覆盖像素、尺寸不一致和元数据/标签图不一致。背景段 0 也作为 `segments_info` 成员，保证模型可完整重建。

## 清单结构兼容

### v1 导入

- 接受结构版本 1；类别颜色按 `#AARRGGBB` 读取，默认丢弃 alpha 并归一为 `#RRGGBB`。
- 旧清单缺少实例起点时，按配置默认值推导，且自动保证大于最大类别 id；演示与本项目默认值为 100。
- 整数百分比阈值除以 100，例如 `60 -> 0.6`。
- v1 类别名按字典序分配确定性 category id；`thing` 由已知 stuff/thing 名称规则推导，也支持可选兼容扩展 `thing_categories`。
- v1 的 `png`/`rle`/`coco` 偏好别名归一为 `png_palette`/`rle`/`coco_json`。

### v2 导入与导出

- 支持全局 `categories` 或每分片 `categories`，类别可显式给出 `id`、`thing`、`confidence`。
- `instance_start_id` 必须大于最大类别 id；否则返回 `E_INVALID_CONFIG`。
- 空类别映射、重复颜色（归一后）、重复 id/name、非法阈值均返回 `E_INVALID_CONFIG`。
- 未知版本（例如 999）返回 `E_UNSUPPORTED_SCHEMA`，不会猜测或降级。
- v1 经兼容解析可导出为新结构；导出的 v2 再导入时得到相同内部模型。v2 也可导出 v1 形状，但若自定义实例起点不等于 v1 默认推导值，v1 无法无损表达该字段，这是旧结构本身的限制。

## 显式降级

降级项使用稳定字段：

```json
{
  "code": "D_PALETTE_CAPACITY_EXCEEDED",
  "error_code": "E_PALETTE_CAPACITY",
  "format": "png_palette",
  "limit": 128,
  "actual": 300,
  "action": "format_not_encoded",
  "lossless": false
}
```

可能的降级包括 `D_PALETTE_CAPACITY_EXCEEDED`、`D_SEGMENT_ID_BITS_EXCEEDED` 和格式偏好不可用时的 `D_FORMAT_FALLBACK`。降级会写入 `encode.meta.json`、`degradations.json`、验证报告以及构建返回结果。任何被拒绝的格式不会生成可疑/截断文件。

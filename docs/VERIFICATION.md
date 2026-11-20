# 实际验证记录

验证日期：2026-11-20（Asia/Shanghai）

## 环境

```text
Python 3.11.18
numpy 2.4.6
Pillow 13.0.0
pytest 9.2.0
操作系统：Ubuntu 26.04 WSL 本地环境
无 Docker、无 GPU、无外部数据集、无预训练模型
```

Python 3.11 由 `uv` 安装，依赖安装在项目内 `.venv`。HTTP 服务使用标准库 `http.server`，测试中成功绑定 `127.0.0.1` 临时端口；实际运行可使用：

```bash
panopipe serve --workspace demo-workspace --host 127.0.0.1 --port 8765
```

## 自动化测试

```text
$ .venv/bin/pytest -q
..........................                                               [100%]
26 passed
```

测试文件：

- `tests/test_manifest.py`：v1 ARGB/百分比/缺省实例起点归一、v1→v2 模型等价、v2→v1 形状、空类别、实例起点冲突、未知 schema、缺失文件、非法颜色。
- `tests/test_codecs.py`：三格式确定性编码与逐像素/属性往返、调色板容量降级、段 id 位宽降级。
- `tests/test_pipeline.py`：四阶段构建、16 条连续检查点、停止恢复逐字节一致、清除停止请求、各版本轴失效传播、调色板管线降级、非法阶段跳转与非法导入零写入、未知颜色诊断、段 id 溢出、多进程陈旧/重复发布。
- `tests/test_http.py`：回环 HTTP 导入、构建、状态、PNG 导出、停止/恢复。
- `tests/test_cli.py`：CLI 完整演示、固定摘要、状态查询、错误注入验证。

## 完整演示

命令：

```bash
panopipe demo --demo-dir demo --workspace demo-workspace --fresh
```

实际结果：

```text
ok: true
分片: shard_1_indexed, shard_2_rgb, shard_3_indexed, shard_4_rgb
source schema: 1
checkpoint_sequence: 16
summary_digest: 450a1e55e9877d53be02928aa733f5b45809136dbbb70ebab1e210fd2a64de9a
```

每分片验证结果：`png_palette=true`、`rle_json=true`、`coco_json=true`。模型摘要：

```text
shard_1_indexed  3b4fcb56d3c9bcf1ec9dec21509ea78f5797029c5d880558e8d4bfcda5bed9f8
shard_2_rgb      724bd2e1ea69ef93cc54ae4a82f7523f4b87264999f0baf37b1155fd771bc9f7
shard_3_indexed  7a91cab5d954f14eb35c49bd49a9de7abf33754ae20c2ba9da3db58143cfccb1
shard_4_rgb      f99b8a3b62e7a29f4ab4ab139a3c307bbd1fe5712d0546b8aacc032a507c5f44
```

同样 4 个分片从 v2 清单构建：

```text
ok: true
checkpoint_sequence: 16
summary_digest: 168906a8a34fc1d2703d3a55af3b69df3097449dcc7c54c3f943675338995e72
```

检查点检查：

```text
last 16 count 16 contiguous True
1..4   = shard_1_indexed decode/merge/encode/validate
13..16 = shard_4_rgb     decode/merge/encode/validate
```

状态检查：4 个分片的 4 个阶段全部 `ready`，`stop_requested=false`，待失效阶段数为 0。

输入文件类型检查：

```text
shard_1_indexed.png: 64x48, 4-bit colormap
shard_2_rgb.png:     64x48, 8-bit/color RGB
shard_3_indexed.png: 64x48, 4-bit colormap
shard_4_rgb.png:     64x48, 8-bit/color RGB
```

## 停止/恢复一致性

自动化测试先构建 2 个分片（8 条检查点）后停止，再恢复构建剩余 2 个分片，然后与另一工作区从零完整运行比较：

```text
summary_digest 相同
checkpoints/ 下 16 个 JSON 文件逐字节相同
shards/ 下全部阶段产物逐字节相同
检查点序号连续 1..16
```

## 错误注入

命令：

```bash
panopipe verify-errors --demo-dir demo --workspace .error-workspaces
```

实际结果：

```text
unknown_color      E_UNKNOWN_COLOR
  shard_id=unknown_color
  unknown_pixel_count=1
  unknown_color_count=1
  color=#FF00FF, pixel_count=1, sample_xy=[24,24]
  检查点序号保持 0，诊断写入 diagnostics/unknown_color.E_UNKNOWN_COLOR.json

segment_overflow   E_SEGMENT_ID_OVERFLOW
  max id=16777222, 24-bit limit=16777215
  D_SEGMENT_ID_BITS_EXCEEDED: png_palette/rle/coco_json 均不编码
  D_FORMAT_FALLBACK: no_lossless_format_available
  验证报告显式 error_codes=[E_SEGMENT_ID_OVERFLOW]

unknown_schema     E_UNSUPPORTED_SCHEMA
  version=999, supported=[1,2]
  无静默降级
```

## HTTP 实测

测试通过标准库 `urllib` 对实际绑定的 `127.0.0.1` 临时端口执行：

```text
GET  /api/health                         200
POST /api/import                         200, 4 shards
POST /api/build                          200, summary ok, sequence 16
GET  /api/status?shard_id=...            200, decode/validate ready
GET  /api/export?format=png              200, image/png, PNG magic + 64x48
POST /api/validate                       200, reused validate checkpoint
POST /api/stop                           200, stop_requested=true
POST /api/resume                         200, stop_requested=false
```

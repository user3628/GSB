# Panopipe：本地全景分割掩码处理与验证工具

本项目从零实现了一个不依赖真实数据集、预训练模型、GPU 或容器的本地全景分割掩码管线。输入是固定随机种子生成的 RGB / PNG 索引色标签图和 JSON 工程清单；输出是可互操作的 PNG 索引色掩码、RLE 字符串与 COCO 风格 JSON，并提供逐像素往返验证、检查点恢复、版本失效传播、并发发布保护和本地 HTTP 接口。

## 环境与实际版本

任务环境原本只有 Python 3.14 且不允许全局 `pip`。项目使用 `uv` 安装了独立 Python 3.11，并创建了项目内虚拟环境：

```text
Python 3.11.18
numpy 2.4.6
Pillow 13.0.0
pytest 9.2.0
```

重新建立环境：

```bash
python3 -m venv .bootstrap
.bootstrap/bin/pip install uv
.bootstrap/bin/uv python install 3.11
.bootstrap/bin/uv venv --python 3.11 .venv
.bootstrap/bin/uv pip install --python .venv/bin/python -e '.[test]'
```

之后可直接使用 `panopipe`，或以 `.venv/bin/python -m panopipe` 运行。

## 快速开始

一条命令生成演示数据、导入 v1 清单、完成四阶段构建并写出固定摘要：

```bash
panopipe demo --demo-dir demo --workspace demo-workspace --fresh
```

其他常用命令：

```bash
panopipe generate-demo --out demo --seed 20261120
panopipe import --manifest demo/manifest_v2.json --workspace ws2
panopipe build --workspace ws2
panopipe build --workspace ws2 --shard shard_1_indexed --stage encode
panopipe status --workspace ws2
panopipe fingerprints --workspace ws2
panopipe checkpoints --workspace ws2
panopipe export --workspace ws2 --shard shard_1_indexed --format rle
panopipe validate --workspace ws2
panopipe stop --workspace ws2
panopipe resume --workspace ws2 --build
panopipe verify-errors --demo-dir demo --workspace .error-workspaces
panopipe serve --workspace ws2 --host 127.0.0.1 --port 8765
```

HTTP 服务只允许绑定 `127.0.0.1` 或 `localhost`。主要端点：

```text
GET  /api/health
POST /api/import              {"manifest_path": "...", "parse_options": {...}}
POST /api/build               {"shard_ids": [...], "stages": [...], "resume": false}
POST /api/build/stage         {"shard_id": "...", "stage": "decode|merge|encode|validate"}
GET  /api/status
GET  /api/fingerprints
GET  /api/checkpoints
GET  /api/export?shard_id=...&format=png|rle|coco_json
POST /api/validate
POST /api/stop
POST /api/resume
```

## 演示数据

`demo/` 中包含：

- 4 个 64x48 分片：`shard_1_indexed`、`shard_3_indexed` 为调色板 PNG，`shard_2_rgb`、`shard_4_rgb` 为 RGB PNG。
- 多类别 `background/road/sky/person/car`，其中 `person` 与 `car` 各有两个相邻实例，且有 1 像素小连通域被面积阈值过滤为背景段 0。
- `manifest_v1.json`：旧结构，ARGB 十六进制颜色、整数百分比阈值、无 `instance_start_id`。
- `manifest_v2.json`：新结构。
- `manifest_v1_upgraded.json`：v1 经兼容解析后导出的 v2 清单。
- `errors/`：未知颜色、24 位段 id 越界、未知 schema 三类错误样本。

实际完整运行结果：

```text
分片数: 4
阶段检查点数: 16（4 分片 × 4 阶段）
检查点序号: 1..16 连续
每分片三种往返链: png_palette / rle_json / coco_json 全部逐像素与属性一致
v1 演示固定摘要: 450a1e55e9877d53be02928aa733f5b45809136dbbb70ebab1e210fd2a64de9a
v2 演示固定摘要: 168906a8a34fc1d2703d3a55af3b69df3097449dcc7c54c3f943675338995e72
```

`demo/expected_summary.json` 保存了 v1 演示的逐分片模型摘要和三格式校验结果。

## 测试结果

```text
.venv/bin/pytest -q
..........................                                               [100%]
26 passed
```

测试覆盖：格式无损往返与确定性、调色板/位宽降级、v1/v2 升级等价、未知 schema、RGB 与索引色解码、相邻实例分离、面积过滤、四阶段构建、检查点连续性、停止恢复字节一致、版本失效传播、非法阶段跳转、未知颜色诊断、段 id 溢出、多进程陈旧发布、HTTP 导入/构建/导出/停止恢复和 CLI 演示。

错误样本实测：

```text
未映射颜色          E_UNKNOWN_COLOR，诊断中包含分片、颜色、像素数、样本坐标，检查点数为 0
段 id 越界          E_SEGMENT_ID_OVERFLOW，显式记录 D_SEGMENT_ID_BITS_EXCEEDED，不写出截断格式
未知清单版本        E_UNSUPPORTED_SCHEMA，不做静默降级
非法阶段跳转        E_INVALID_STAGE，失败前不写产物
空类别/起点冲突     E_INVALID_CONFIG
陈旧/重复检查点     E_STALE_CHECKPOINT
```

## 文档

- `docs/ARCHITECTURE.md`：内部模型、阶段指纹、失效传播、检查点和停止/恢复语义。
- `docs/FORMATS.md`：三种掩码格式的精确字节布局、往返链和降级规则。
- `docs/VERIFICATION.md`：本机实际执行结果、固定摘要与错误注入记录。

代码入口：`panopipe/pipeline.py`、`panopipe/codecs.py`、`panopipe/checkpoints.py`、`panopipe/httpapi.py`、`panopipe/cli.py`。

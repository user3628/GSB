# 多模态特征对齐存储互操作引擎与诊断工作台

本地可运行、完全确定性（固定随机种子、无 GPU / 网络 / 外部服务依赖）的
多模态特征对齐引擎。在统一中间表示（IR）上完成维度校验、投影映射、特征
拼接与对齐摘要，并与三种本地落盘格式双向编解码；兼容解析结构版本 1
清单并显式升级为版本 2 等价结构；提供阶段检查点、优雅停止恢复、版本
失效传播、并发发布保护以及 HTTP + 命令行诊断工作台。

## 环境

- Python 3.10+（实测 Python 3.14.4）
- NumPy（实测 2.5.5）、PyYAML（实测 6.0.3）：`pip install -r requirements.txt`
- 不使用 Docker；命令行与 HTTP 工作台均可在无网络环境运行

## 快速开始

```bash
python3 sample_gen.py --out samples          # 1. 生成确定性样本
python3 align.py import -w ws --manifest samples/manifest_v2.yaml \
    --shards-dir samples/shards              # 2. 导入分片 + v2 清单
python3 align.py run -w ws                   # 3. 运行四阶段对齐
python3 align.py summary -w ws               # 4. 查看对齐摘要
python3 align.py serve -w ws --port 8765     # 5. 启动 HTTP 诊断工作台
python3 align.py demo --root demo_run        # 6. 一键确定性演示
python3 -m unittest discover -s tests -v     # 7. 运行验收测试
```

## 目录结构

```
sample_gen.py                # 固定种子(20261121)样本生成器
align.py                     # 命令行入口
align_engine/
  ir.py                      # 统一中间表示：工程/分片/投影/量化元数据
  quantize.py                # 逐维缩放因子 + 位宽量化，可按比例还原
  manifest.py                # v1/v2 清单解析、v1 显式升级、v2 回导
  codecs/                    # 三种落盘格式双向编解码
    json_project.py          #   内部 JSON 工程格式（单文件）
    npz_pack.py              #   .npz 向量包（固定 zip 时间戳，字节确定）
    npy_dir.py               #   .npy + 侧车 JSON 目录包
  checkpoint.py              # 原子检查点、文件锁、单调序号、哈希链、日志校验
  stages.py                  # 四阶段执行器
  pipeline.py                # 编排：指纹计划、失效传播、停止/恢复
  importer.py                # 样本分片 + 清单 -> 内部 JSON 工程
  service.py                 # CLI 与 HTTP 共享服务层
  workbench/server.py        # 本地 HTTP 诊断工作台（stdlib）
  workbench/demo.py          # 确定性端到端演示
tests/test_engine.py         # 16 项验收测试
```

## 样本与清单

`sample_gen.py` 以种子 20261121 生成（删除目录后重跑可字节级重建）：

- 3 个文本分片 `.npz`：`ids`、64 维向量、`languages`（zh/en/mixed 混排）
- 3 个视觉分片 `.npy` + `.json` 侧车：`ids`、96 维向量、`text_ids`、
  `operators`（含 1 个缺失算子标签 → 降级项；txt-0011 无视觉对应 → 未覆盖）
- `manifest_v2.yaml`：浮点缩放 0.75 + 显式 64×96 投影矩阵
- `manifest_v1.yaml`：整数百分比 75、视觉维度登记为 64、无投影矩阵，
  另以 `actual_vision_dim: 96` 显式声明实际视觉维度
- `inject/vision-shard-900-baddim.*`：侧车声明 96 维、实际 64 维的异常样本

## 核心设计

**统一中间表示（IR）**：工程 = 工程标识 + 结构版本 + 模态登记表
（text:64 / vision:96）+ 跨模态投影（浮点缩放系数 + 投影矩阵）+ 分片列表。
分片 = 分片标识 + 模态类型 + int32 量化向量矩阵 + 维度 + 量化位宽 +
逐维缩放因子 + 行 id + 模态元数据（语言 / 对应文本 id / 来源算子标签）。
三种格式均完整保存上述字段，互转不出现维度错位或分片标识丢失；
`dequantize` 可按缩放因子还原到位宽声明精度（误差 ≤ scale/2）。

**v1 显式升级**：视觉维度纠正为清单显式声明的 `actual_vision_dim`；
缩放按可配置规则 `pct_div_100` 换算（75 → 0.75）；投影矩阵由
`(text_dim, vision_dim)` 确定性生成（行单位化）。升级工程可回导为 v2
清单，且与直接导入 v2 的内部工程在向量矩阵、维度、缩放系数上逐字节一致
（`align.py upgrade` 输出逐项校验）。

**四阶段流水线**：① 维度校验（视觉实际维度 vs 清单声明、投影矩阵形状；
不符返回 `E_DIM_MISMATCH` 并附分片标识/声明维度/实际维度，失败单元不落
任何记录）→ ② 投影映射（浮点域 `normalize(P·(scale·v))`，文本行单位
归一化，输出确定性余弦相似度矩阵）→ ③ 拼接汇总（可配置拼接顺序/增益
合成 128 维对齐向量；置信度 = 余弦相似度 × 算子权重；算子标签缺失记
降级、文本 id 无视觉对应记未覆盖，均不静默补零；摘要含模态覆盖统计、
维度清单与配置版本指纹）→ ④ 格式导出（对齐工程写入三种格式）。

**检查点与恢复**：每分片每阶段完成时原子写入（临时文件 + rename，任意
位置中断不留半写记录）含分片标识、阶段名、输入内容指纹、结构版本指纹、
配置版本、产物摘要与单调序号的记录。停止请求在分片单元边界生效；恢复
后最终对齐向量、摘要与导出文件与从零完整运行逐字节一致（测试与演示均
验证，含分阶段逐阶段触发等价性）。

**版本指纹与失效传播**：每单元阶段指纹 = H(阶段, 输入内容指纹, 结构
指纹, 相关配置指纹)。分片向量、清单版本、投影矩阵、拼接规则、量化位宽、
算子权重任一变化 → 定位最早受影响阶段，该阶段及全部下游失效重建，上游
复用；失效原因、命中指纹与复用范围经 `status`/`invalidations`/
`fingerprints` 接口可观测。

**并发发布保护**：检查点发布经 flock 文件锁串行化，校验分片内序号连续
推进与哈希链衔接；落后/重复/跨分片错位发布返回 `E_STALE_CHECKPOINT`；
非法阶段跳转或缺失前置产物返回 `E_INVALID_STAGE`；检查点被整体替换
（序号回退、记录或产物摘要不匹配）在加载校验时返回
`E_STALE_CHECKPOINT` 并附字段级差异说明，不静默继续。

## HTTP 诊断工作台

`python3 align.py serve -w ws --port 8765`（默认 127.0.0.1，可用
`--port 0` 选临时端口）。`GET /` 为内置面板；JSON API：

- `POST /api/import` `{"manifest_path","shards_dir","extra_shards_dir"?}`
- `POST /api/run` `{"stage"?,"stop_after_units"?}` / `POST /api/resume`
- `POST /api/stop` / `POST /api/reset` / `POST /api/export`
- `POST /api/convert` `{"src","src_format","dst","dst_format","verify"?}`
- `POST /api/upgrade` `{"v1_path","v2_path","shards_dir","work_dir"}`
- `GET /api/status` `/api/fingerprints` `/api/summary` `/api/uncovered`
  `/api/checkpoints` `/api/invalidations` `/api/health`

错误响应统一为 `{"error":{"code","message","details"}}`，HTTP 400。

## 实际验证结果（本机实测）

- `python3 -m unittest discover -s tests`：**16/16 通过**
  （三格式双向互转保真、量化还原精度、v1 回导逐字节一致、维度错误码、
  非法阶段、序号/链保护、篡改检测、停止恢复一致、失效传播、HTTP API）
- `python3 align.py demo --root demo_run` 固定输出：
  - 完整运行摘要 digest `787faf4afda31cbebd085ffefb688108c93bb71e6a92acafe6dc54e65e2e0a2c`，
    统计：文本 11 / 视觉 10 / 对齐 9 / 降级 1 / 未覆盖 1
  - 三格式互转 `all_equal=true`；v1 升级回导 `equal=true`
  - 停止（3 单元后）+ 恢复与从零运行逐字节一致 `byte_identical=true`
  - 权重 0.8→0.9 后：validate/project 12 单元复用，concat/export 7 单元重建
  - 注入异常：`E_DIM_MISMATCH`（vision-shard-900-baddim，声明 96 实际 64）、
    `E_INVALID_STAGE`（缺失前置阶段）、`E_STALE_CHECKPOINT`（产物摘要篡改）
    均按预期返回

## 说明与限制

- 确定性依赖 NumPy 在本机的浮点实现；同一环境重复运行逐字节一致。
- 强制 kill 崩溃后检查点日志与文件可能不一致，下次运行会显式报
  `E_STALE_CHECKPOINT`（不静默继续）；执行 `align.py reset -w ws` 后
  重跑即可确定性重建，最终结果不变。
- 量化位宽在导入时生效；修改 `quantization.bits` 需重新导入分片。

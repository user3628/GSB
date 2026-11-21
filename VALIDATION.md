# Validation Record

执行环境：

```text
Python 3.14.4
NumPy 2.2.1
Pillow 13.0.0
OS: Linux/WSL, local filesystem
```

## 自动测试

命令：

```bash
python3 -m unittest discover -s tests -v
```

结果：`11 tests, OK`。

覆盖内容：

- 生成器在相同 seed 下输出逐字节一致的 manifest、ground truth 和所有 PNG。
- 两个独立工作区全量构建的 `report.json`、`poses.json` 逐字节一致。
- 5 分片、7 实例、103 可见关键点全部匹配；2 个不可见关键点产生显式空候选记录。
- 骨架变更最早失效阶段为 `assembly`，只重建 10 个下游单元，预处理与解码序号不变。
- 真值变更最早失效阶段为 `preprocess`；仅修改期望置信区间时最早阶段为 `validation`。
- 高斯半径、解码配置、装配配置、验证精度分别精确命中 `preprocess`、`decode`、`assembly`、`validation`；增量最终导出与新配置全量构建逐字节一致。
- 停止 3 个或 7 个单元后恢复，最终导出与全量运行逐字节一致，检查点序号 1..20 连续。
- 注入损坏 JSON、半写记录、重复序号和断链记录后恢复，最终报告仍与全量运行一致。
- 跨进程并发使用同一预期序号发布时，一个成功、一个稳定返回 `E_STALE_CHECKPOINT`。
- HTTP 导入、构建、状态、指纹、报告、逐关键点误差、停止、恢复与错误映射通过。

## 一键演示

命令：

```bash
python3 scripts/run_demo.py --out demo_workspace
```

所有 `29` 项检查为 `true`。关键结果：

```text
dataset_digest          = 677037a02e2a989ca6cd0bddf58164ffa6090f8bd54d679aa70bf1430c33c84e
baseline report_digest  = 7fcc54df67bdf80895efa4735607d68e7bfc93b1f417fb94619aebc5f63485ae
baseline report sha256  = 49a9a5cc72f16a258d8831595518df061373450c3da63d6136c0bd72ead17c8f
baseline poses sha256   = af7609441c6331188e02737b4d73d34086e429b01542122344fad59032f5f209
checkpoint head         = 20

shards                  = 5
truth instances         = 7
predicted instances     = 7
visible keypoints       = 103
matched keypoints       = 103
hits                    = 103
hit rate                = 1.0
RMSE                    = 0.2691347501 px
invisible keypoints     = 2
```

停止恢复：

```text
stopped after units     = 7
stopped checkpoint head = 7
resumed planned units   = 13
report bytes equal      = true
poses bytes equal       = true
checkpoint record bytes = true (20/20 files)
checkpoint chain        = 1..20 continuous
```

增量失效：

```text
skeleton change:             earliest=assembly,    rebuild=10, preprocess/decode reused
confidence interval change:  earliest=validation,  rebuild=5,  out-of-range instances=7
ground truth change:         earliest=preprocess,  rebuild=4
```

错误码注入：

```text
negative gaussian radius     -> E_INVALID_PARAM       (preprocess, shard_01)
heatmap size mismatch        -> E_INVALID_PARAM       (preprocess, shard_01)
unknown skeleton keypoint    -> E_POSE_INVALID        (assembly, shard_01)
multi-instance conflict      -> E_POSE_INVALID        (assembly, shard_03)
invalid stage jump           -> E_INVALID_STAGE       (current_stage=null)
stale checkpoint sequence    -> E_STALE_CHECKPOINT
```

## CLI 冒烟

```bash
python3 -m pose_pipeline build \
  --workspace /tmp/pose-cli-workspace \
  --manifest examples/demo_data/manifest.json
python3 -m pose_pipeline export \
  --workspace /tmp/pose-cli-workspace \
  --out /tmp/pose-cli-export
```

全量构建返回 `pipeline_complete=true`，导出文件为 `/tmp/pose-cli-export/report.json` 和 `/tmp/pose-cli-export/poses.json`。

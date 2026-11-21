# 实际验证记录

验证日期：2026-11-22（Asia/Shanghai）

## 1. 解释器与依赖

```text
Python 3.11.9 (main, Nov 20 2026, 11:33:02) [GCC 15.2.0]
NumPy 2.4.6
pytest 9.2.0
```

系统 Python 3.14.4 + NumPy 2.5.5 亦执行同一套测试与演示。

## 2. 自动测试

命令：

```bash
/home/zsl/.local/python3.11/bin/python3.11 -m pytest -q
```

实际结果：

```text
...........                                                              [100%]
11 passed
```

覆盖项：

1. 固定种子样本与独立状态目录产物的逐字节复现；
2. 标定可表示区间覆盖观测极值；
3. 11 位累加器溢出计数和累加器摘要；
4. 两种归因贡献和与端到端偏差一致；
5. 紧容差触发结构化 `E_TOLERANCE_EXCEEDED`；
6. NaN、全零通道、非法位宽错误及 CLI 整数退出码；
7. 5 个层单元后中断再恢复，四份产物逐字节一致；
8. 仅容差变化时只重建 `validation`；权重变化时从 `calibration` 失效；
9. 4 进程并发发布只有 1 个推进序号；
10. 注入 rename 失败时不留下半写检查点；
11. HTTP 导入、运行、状态、逐层、错误、验证和阶段执行。

## 3. 固定演示

命令：

```bash
python3.11 -m qpipe demo --clean-samples
```

退出码：`0`。

固定标识：

```text
config_id            = 4ec29207b9708947f335927ae6bd87e994bd7cde9f5af622cfd02a7c4a5c01c7
pipeline_fingerprint = b1581efde9841b9c16832af5a12edc3bba390f9cd005ca7a27384d39cfc80b98
final_digest         = 92c564c602a910dff5405c135265c5074594fec5972f82a3af0160f2626cbfde
```

阶段指纹：

```text
calibration 6969ea0b7fcbd9985f0819f952e48934ebdee31136ecc40156c447165bc6f972
forward     cc13b3acd9bf5465b01f8b4bd430c4e90554673903e94eb2b3b90b538f8b3847
attribution 09f69c332d30353e25c3ce23004356a6ef5aac84967708114fcb00abe093be9f
validation  50a613c8ec6a6a0df3549d676eb342d131f254ace31e3162990e64b173abaa0c
```

## 4. 标定与前向

覆盖的配置：

- 8 位对称、逐通道权重；
- 4 位非对称、逐通道权重；
- 8 位非对称、逐张量权重 + 4 位对称、逐通道激活；
- 4 位对称、逐张量权重 + 8 位非对称激活。

所有标定通道 `scale > 0`，零点位于 `[qmin, qmax]`，`representable_min/max` 覆盖观测极值。所有演示前向使用 `fixed_width_integer` 累加：

```text
l0  32-bit  overflow_clamp_count=0
l1  24-bit  overflow_clamp_count=0
l2  11-bit  overflow_clamp_count=14
l3  16-bit  overflow_clamp_count=0
```

`l2` 的 14 次钳制在层诊断中显式保留位置和摘要，未被静默吸收。

## 5. 误差与归因

```text
端到端 L1 偏差        = 5.490524593519915
端到端最大绝对偏差    = 0.28265213513437476  <= 4.0
端到端相对 L1 偏差    = 0.17794652150496001  <= 0.25
敏感度归因贡献和      = 5.490524593519915   residual 0.0
扰动重放归因贡献和    = 5.490524593519915   residual 0.0
声明求和容差          = 1e-9
```

两种归因排名：

```text
first-order sensitivity: l2=1, l3=2, l1=3, l0=4
layer perturbation replay: l3=1, l2=2, l1=3, l0=4
review_required_layers: [l2, l3]
```

## 6. 中断恢复一致性

演示将 `demo-resume` 在第 5 个层单元完成后优雅停止，退出码 `75`，保留 6 条已提交检查点（4 个标定层 + 标定阶段完成 + 前向第 1 层）。随后：

```bash
python3.11 -m qpipe resume --run-id demo-resume
```

恢复后：

```text
interrupted_after_layer_units = 5
full final_digest             = 92c564c6...
resumed final_digest          = 92c564c6...
result.json                   byte-identical = true
error_report.json             byte-identical = true
layer_errors.json             byte-identical = true
SHA256SUMS                    byte-identical = true
checkpoint_count              = 20
verifier checks               = 102, all passed
```

## 7. 增量失效

测试 1：只把 `l3.tolerance.max_abs` 从 `2.0` 改为 `5.0`。

```text
earliest_stage = validation
reason = validation / layer_tolerance_changed / l3
stage_sources:
  calibration = cache
  forward     = cache
  attribution = cache
  validation  = recomputed
```

测试 2：修改 `weights/shard_00.npz` 内容并作为新版本导入。

```text
earliest_stage = calibration
reason = shard_content_changed
stage_sources.calibration = recomputed
```

## 8. 并发与原子性

4 个独立 CLI 进程同时对同一运行、同一阶段发布 `layer_seq=1/l0`：

```text
exit codes sorted = [0, 5, 5, 5]
ledger next_layer_seq = 2
published_layers = 1
```

即只有 1 个发布成功，3 个稳定返回 `E_STALE_CHECKPOINT`。重复序号、跨层发布和缺失前向阶段分别由测试确认返回 `E_STALE_CHECKPOINT` / `E_INVALID_STAGE`。

原子检查点故障注入令最终 `os.replace` 抛出 `OSError`。结果为：

```text
checkpoint JSON 文件数 = 0
next_sequence = 1
```

未出现半写最终记录。

## 9. 异常样本

```text
NaN 激活        -> E_NUMERIC_INPUT, layer=l0, parameter=activation, position=[7,1]
全零权重通道    -> E_INVALID_PARAM, layer=l0, parameter=weight.quantization.scale
6 位非法配置    -> 标定阶段 E_INVALID_PARAM, layer=l0, parameter=weight.quantization.bits
```

CLI 对 NaN 样本实际退出码为 `3`。

## 10. HTTP 与逐阶段

HTTP 冒烟实际结果：

```text
GET  /api/health                200, loopback=true
POST /api/import                201
POST /api/run                   202, background job
GET  /api/jobs/{id}             status=pass
GET  /api/status                validation ledger=complete
GET  /api/layers                4 layers
GET  /api/errors                status=pass
POST /api/verify                ok=true
POST /api/stage                 status=stage_complete
```

逐阶段 CLI 依次执行 `calibration -> forward -> attribution -> validation`，最终摘要与一次完整运行相同，四份输出文件逐字节一致。未执行 `calibration` 时直接请求 `forward` 返回 `E_INVALID_STAGE`。

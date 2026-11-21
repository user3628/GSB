# qpipe: deterministic low-precision quantization verification

`qpipe` 是一个完全本地、可复现的低精度量化推理与结果验证工具。它从固定随机种子生成的 `.npz` 权重/校准分片和 JSON 算子清单出发，依次执行：

1. 量化参数标定；
2. 定点整数累加量化前向；
3. 高精度参考前向、反量化误差与双归因；
4. 逐层/端到端容差判定与结构化错误报告。

运行期不加载真实模型，不使用 GPU，不访问外部网络服务。HTTP 只绑定本地回环地址；CLI 在 Windows/POSIX 上均以整数退出码返回。

## 环境与准备

实际验证环境：

- Python 3.11.9：`/home/zsl/.local/python3.11/bin/python3.11`
- NumPy 2.4.6
- pytest 9.2.0
- 另在系统 Python 3.14.4 + NumPy 2.5.5 上验证通过

让下文命令直接找到该解释器：

```bash
export PATH=/home/zsl/.local/python3.11/bin:$PATH
```

安装依赖：

```bash
python3.11 -m pip install numpy pytest
```

生成确定性样本（固定种子 `20261122`）：

```bash
python3.11 -m qpipe generate --samples-dir samples --clean
```

样本包含 4 个权重分片、4 个校准激活分片，覆盖 8/4 位、对称/非对称、逐张量/逐通道、权重与激活离群值。另含：

- `samples/calibration_nan.npz`：NaN 注入；
- `samples/weight_zero_channel.npz`：全零权重通道；
- 三个无效配置清单，分别触发 NaN、零通道和非法位宽错误。

配置快照、内容寻址缓存、运行账本和检查点位于独立状态目录 `.qpipe/`，不会写入 `samples/`。

## 快速开始

一条命令运行完整演示、优雅中断/恢复、字节比对、验证器以及异常样本检查：

```bash
python3.11 -m qpipe demo --clean-samples
```

分步运行：

```bash
python3.11 -m qpipe import \
  --manifest samples/operator_manifest.json \
  --shapes samples/shape_declarations.json \
  --version v1

python3.11 -m qpipe run --run-id full --version v1
python3.11 -m qpipe status --run-id full
python3.11 -m qpipe layers --run-id full
python3.11 -m qpipe verify --run-id full
```

逐阶段执行（缺少前置阶段时返回 `E_INVALID_STAGE`）：

```bash
python3.11 -m qpipe run-stage --stage calibration --run-id staged
python3.11 -m qpipe run-stage --stage forward --run-id staged
python3.11 -m qpipe run-stage --stage attribution --run-id staged
python3.11 -m qpipe run-stage --stage validation --run-id staged
```

停止与恢复：

```bash
python3.11 -m qpipe stop
python3.11 -m qpipe resume --run-id full
python3.11 -m qpipe run --run-id resume-demo --stop-after-units 5
python3.11 -m qpipe resume --run-id resume-demo
```

本地 HTTP：

```bash
python3.11 -m qpipe serve --host 127.0.0.1 --port 8787
```

主要接口：

- `POST /api/import`
- `POST /api/run`、`POST /api/resume`
- `POST /api/stage`、`POST /api/stages/{stage}/run`
- `GET /api/status`、`GET /api/versions`
- `GET /api/layers`、`GET /api/errors`
- `GET /api/jobs/{job_id}`
- `POST /api/stop`
- `POST /api/verify`
- `POST /api/publish-layer`（锁与序号探针）

## 固定演示结果

最终有效配置指纹：

```text
config_id = 4ec29207b9708947f335927ae6bd87e994bd7cde9f5af622cfd02a7c4a5c01c7
pipeline_fingerprint = b1581efde9841b9c16832af5a12edc3bba390f9cd005ca7a27384d39cfc80b98
```

完整运行与中断后恢复运行共用同一输出摘要：

```text
final_digest = 92c564c602a910dff5405c135265c5074594fec5972f82a3af0160f2626cbfde
```

主演示判定通过：

```text
status = pass
end_to_end.max_abs_deviation = 0.28265213513437476  (limit 4.0)
end_to_end.relative_l1_deviation = 0.17794652150496001  (limit 0.25)
review_required_layers = [l2, l3]
l2 integer accumulator: 11 bits, 14 overflow clamps (explicitly counted)
```

逐层误差与双归因：

| layer | max abs error | relative L1 | sensitivity rank | replay rank | review |
|---|---:|---:|---:|---:|---|
| l0 | 0.0940217742 | 0.0254609829 | 4 | 4 | no |
| l1 | 0.3855559853 | 0.0523856580 | 3 | 3 | no |
| l2 | 0.4666045837 | 0.0891222668 | 1 | 2 | yes |
| l3 | 0.2826521351 | 0.1779465215 | 2 | 1 | yes |

两种归因归一化后的贡献和均精确等于端到端 L1 偏差 `5.490524593519915`，残差为 `0.0`（声明容差 `1e-9`）。

## 错误码与退出码

| 业务码 | CLI 退出码 | 含义 |
|---|---:|---|
| `E_INVALID_PARAM` | 2 | 零缩放、全零通道、非法位宽、模式、粒度或形状 |
| `E_NUMERIC_INPUT` | 3 | 输入含 NaN/Inf；不继续传播 |
| `E_TOLERANCE_EXCEEDED` | 4 | 层或端到端容差超界 |
| `E_STALE_CHECKPOINT` | 5 | 重复、落后、跳号或跨层发布 |
| `E_INVALID_STAGE` | 6 | 缺失前置阶段或阶段未完成 |
| `E_VERIFY_FAILED` | 7 | 验证器发现摘要/结构/字节一致性错误 |
| `E_INTERRUPTED` | 75 | 完成当前不可拆分单元后的可恢复退出 |
| `E_IO` | 74 | 本地原子写入/读取失败 |

参数解析错误使用 argparse 的标准非零退出码；未预期异常使用退出码 1。

## 确定性与恢复保证

- 所有 JSON 使用排序键、固定分隔符和 UTF-8 编码；`.npz` 样本使用固定 ZIP 元数据和稳定数组编码。
- 阶段缓存按内容指纹不可变发布；相同输入、解释器和 NumPy 环境下结果逐字节一致。
- 每个层/阶段记录先写同目录临时文件、`fsync`，再原子替换为最终检查点；最终检查点不可能是半写 JSON。
- 发布账本在真实 OS 文件锁下维护每阶段 `1..N` 层序号，并禁止覆盖已有摘要不同的发布。
- 恢复后的 `result.json`、`error_report.json`、`layer_errors.json` 和 `SHA256SUMS` 与完整运行逐字节一致。

## 测试

```bash
python3.11 -m pytest -q
```

当前结果：`11 passed`。测试覆盖样本可复现、标定极值覆盖、整数溢出计数、双归因求和、容差失败报告、NaN/零通道/非法位宽、停止恢复、增量失效、并发文件锁、原子检查点、HTTP 与逐阶段执行。

详细设计见 `docs/TECHNICAL.md`，实际验证记录见 `docs/VERIFICATION.md`。

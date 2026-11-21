# 验证记录

验证日期：2026-11-22  
执行环境：Linux（WSL Ubuntu 26.04），Python 3.14.4，NumPy 2.5.5，FastAPI 0.165.0，Uvicorn 0.54.0，PyYAML 6.0.3。未使用 Docker 或外部网络服务。

## 自动化测试

```text
$ pytest -q
46 passed, 1 warning in 4.94s
```

警告来自 FastAPI/Starlette `TestClient` 对当前 `httpx` 版本的弃用提示，不影响功能。

覆盖内容：

- 标准 RoPE 闭式结果、奇数维最后一维保留、分块旋转确定性、线性缩放、NTK 分段缩放。
- 因果/非因果点积、稳定 softmax、行归一化、NaN/Inf/零归一化因子的结构化错误。
- 内存单块/批次切分/行块切分与 `E_MEMORY_BUDGET` 明细。
- 检查点连续发布、重复/错位 `E_STALE_CHECKPOINT`、写中断无半文件、跨进程并发发布。
- 全量运行、双工作区固定摘要、缓存 payload 一致、延迟预算、增量复用、从零一致性、停止恢复、ZIP 导出。
- HTTP 运行、查询、配置增量、停止恢复、导出下载、结构化错误。

## 一键演示

```text
$ python -m rope_diag demo
all_checks_passed: true
checks: 17 / 17
```

摘要文件：`artifacts/demo_summary.json`。

默认配置版本：

```text
0cb88ad4a14911cea573c4390a8094642a9e289f7a849550125a5f0b2b1f4c2e
```

固定阶段产物摘要：

| 分片 | frequency_generation | block_rotation | scoring_normalization |
|---|---|---|---|
| short | `9a63ef4904576cf158d03f5f450a7bb9c68ecf9f8dd40dfac277913bf40aa052` | `824cef65fb614fb12c574f60851c5086da80cca65b1189288be8c0f9e7bf4a87` | `8598ad245be6bc982127289a310ed017b24a66e83c955ba86a17f54c9ca92d96` |
| long | `e04f72d96b84ceed50769c59ce3fd98b328ac9de8cf6cc623d129daa36df6fba` | `53bdf4e51c794e56b57a1b88bc56654d60e902ab1cc2fbf99b3a73c762c534cf` | `928d51ec7471a3c65101d5a271d7e6097b5802d7ac5c156de1e102502ab78d06` |
| odd | `24441e7e446b8ec5c35ccd60d7da3b825325bc596e299e242dbce12ec640927d` | `f98aba6d50b07382cf663c008913d0d4d33244268a4c8381ed2ec56ba1d6309f` | `bb80b68fbe9199e2ee06ec0640858fe0a8368888418da0e80d36492eabc51118` |

再次执行 `demo` 并重置工作区后，上述阶段摘要完全复现。

## 增量重算

配置编辑：

```json
{"rotation_block_size": 4}
```

结果：

```text
earliest_affected_stage = block_rotation
frequency_generation: short=reused, long=reused, odd=reused
block_rotation:       short=computed, long=computed, odd=computed
scoring_normalization: short=computed, long=computed, odd=computed
full consistency: all_byte_identical=true
```

频率阶段三个产物指纹保持不变；旋转与打分阶段重新计算后与从零全量构建逐字节一致。

## 停止与恢复

```text
run(stop_after_stage=frequency_generation) -> status=stopped
resume() -> status=completed
resumed stage digests == never-stopped fresh run stage digests: true
verify() -> all_byte_identical=true
```

停止时仅保留已完成阶段检查点；恢复后频率、旋转、打分摘要与一次从零完整运行一致。

## 错误注入证据

| 注入 | 返回码 | 关键字段 |
|---|---|---|
| 缩放系数 0、分块 0 | `E_INVALID_CONFIG` | `field=scaling_factor`, `reason` |
| 非法停止阶段 | `E_INVALID_STAGE` | `stage=illegal_stage`, `valid_stages` |
| NaN 查询向量 | `E_NUMERIC` | `shard_id`, `row_block_index`, `step_index`, `reason` |
| 延迟预算 0 ms | `E_LATENCY_BUDGET` | `query_id`, `elapsed_ms`, `budget_ms` |
| 受限内存预算 1 B | `E_MEMORY_BUDGET` | `fixed_output_bytes`, `minimum_working_bytes`, `required_bytes` |
| 已完成检查点后重复发布序号 1 | `E_STALE_CHECKPOINT` | `expected_sequence=4`, `last_sequence=3`, `requested_sequence=1` |

完整结构化内容见 `artifacts/demo_summary.json` 的 `error_injections`。

## HTTP 回环验证

```text
$ python scripts/http_smoke.py
all_checks_passed: true
```

验证项：

- `/` 返回包含 `RoPE Diagnostics` 的交互页面。
- `/api/health` 健康，会话已加载。
- `/api/status` 状态为 `completed`。
- 角度查询 payload 在缓存路径与重复请求间摘要一致。
- 0 ms 查询返回 HTTP 408 与 `E_LATENCY_BUDGET`。
- `/api/export` 生成的 ZIP 可通过下载接口取回，字节数一致。

报告：`artifacts/http_smoke.json`。

## 导出产物

最近一次演示导出：

```text
artifacts/demo_workspace/exports/rope-export-2b87296552a1301e.zip
size: 624267 bytes
```

ZIP 包含 `summary.json`、每分片/阶段 `reference.json` 与 `.npy` 数组。导出文件的 SHA-256 会随只读摘要中的运行耗时变化，但 ZIP 内阶段数组的内容摘要由内容寻址机制固定。

## 复现命令

```bash
python -m pip install -r requirements-dev.txt
pytest -q
python -m rope_diag demo
python scripts/http_smoke.py
python -m rope_diag --workspace artifacts/demo_workspace serve --host 127.0.0.1 --port 8765
```

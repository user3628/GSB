# 实际验证记录

验证日期：2026-11-20（Asia/Shanghai）  
运行环境：WSL2 Linux（Windows 宿主），CPython 3.11.9，NumPy 2.4.6，pytest 9.2.0。目标代码同时包含 Windows `msvcrt` 文件锁分支；本次实际执行发生在 WSL2，未声称在原生 Windows 进程上执行。

## 测试命令

```bash
.venv/bin/python -m pytest -q
.venv/bin/python scripts/demo_validation.py
```

结果：

```text
40 passed
```

`scripts/demo_validation.py` 返回 `ok: true`。本地端口检查 `127.0.0.1:8766` 当时可用。

另外实际启动了 `serve --host 127.0.0.1 --port 8766`，验证：

```text
GET /api/health   200
GET /api/shards   200
GET /api/status   200
POST /api/run     200 complete
```

通过 HTTP 运行得到的三个最终摘要与下表一致。

## 演示分片

| 分片 | NHWC 输入 | 算子数 | 完整运行最终摘要 |
| --- | --- | --- | --- |
| `small_odd_pad` | `[2,5,6,3]` | 2 | `4d1c770e1cc47495901a779144bf87ddec9ddea638d6f0194ddef39e1e8323e6` |
| `medium_odd_dilated` | `[1,9,11,5]` | 3 | `2c23089d629c8e792443cff879b1517d5ef1efab236af36e7e55e6ed2a141fc2` |
| `boundary_chain` | `[1,6,5,1]` | 4 | `d0e2db2c6e0e2adc18861ada17af2193492ca3e76a98ea81813b57119718f86b` |

这里的最终摘要为阶段产物 `.npy` 文件的 SHA-256，并与 `trace.json` 和导出的 `outputs/<shard>.npy` 一致。

## 优雅停止与恢复

演示脚本启动独立 CLI 进程，执行 `small_odd_pad` 时在阶段间写入停止请求。进程完成当前阶段后退出，停止报告记录：

```json
{
  "status": "stopped",
  "reason": "stop_request",
  "last_checkpoint_seq": 1
}
```

随后执行完整恢复。与同参数从零运行逐字节比较：

```text
output:small_odd_pad          true
output:medium_odd_dilated     true
output:boundary_chain         true
checkpoints:small_odd_pad     true
checkpoints:medium_odd_dilated true
checkpoints:boundary_chain    true
trace                          true
```

检查点日志的有效序号均为连续 `1..N`，恢复过程没有重复或跳号发布。

## 错误码注入

卷积缺少 `weights`：

```json
{
  "code": "E_INVALID_STAGE",
  "detail": {
    "shard_id": "small_odd_pad",
    "operator_index": 0,
    "operator_name": "conv2d",
    "reason": "missing required parameter 'weights'"
  }
}
```

越界缩放因子 `[1.0, 129.0]`：

```json
{
  "code": "E_INVALID_STAGE",
  "detail": {
    "shard_id": "small_odd_pad",
    "operator_index": 1,
    "operator_name": "interpolate",
    "reason": "scale is out of bounds; each axis must be in [1/64, 64]",
    "scale": [1.0, 129.0]
  }
}
```

形状或数值区间不一致由自动化测试验证为 `E_SHAPE_MISMATCH`，错误详情包含 `shard_id`、`operator_index`、命中块边界和 `output_index`。

## 失效传播

完整运行后，把 `medium_odd_dilated` 的阶段 1 从 bicubic/align_corners 改为 bilinear/half_pixel，并更新期望区间。再次运行结果：

```json
{
  "earliest_invalid_stage_index": 1,
  "reason": "alignment_mode_changed",
  "affected_stages": [1, 2],
  "reused_stages": [0],
  "old_final_digest": "2c23089d629c8e792443cff879b1517d5ef1efab236af36e7e55e6ed2a141fc2",
  "new_final_digest": "1a32e4afdc849dd4ebce4932f07b8b28d3b625e84e8a558e2dc2b358606a655b"
}
```

阶段 0 的检查点记录、输入摘要和产物摘要保持复用；阶段 1、2 被删除并重建。

## 并发与损坏恢复

自动化测试使用两个独立派生进程同时发布同一分片的阶段 0。结果严格为一个成功、一个 `E_STALE_CHECKPOINT`，活动检查点仅保留序号 1。

对包含 3 个有效检查点的日志注入无效 JSON 和重复记录后，加载器仍得到序号 `[1,2,3]`；修改第 2 条产物摘要破坏校验和后，加载器在最近一致的序号 1 停止，忽略其后记录。


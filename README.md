# 本地确定性 LLM 采样与解码策略引擎

这是一个不依赖真实模型、GPU、外部服务的本地推理采样引擎。输入是固定的
`.npy` logits 分片和 v1/v2 两种结构的 JSON 采样清单，输出可复现的逐步 token、
逐请求统计、阶段检查点、版本指纹与业务错误码。

核心实现位于 `sampling_engine/`，本地 HTTP 服务仅使用 Python 标准库并强制绑定
loopback 地址。详细设计、算法边界和验收记录见
[`docs/TECHNICAL.md`](docs/TECHNICAL.md)。

## 快速开始

要求 Python 3.10+、可写的本地工作目录；不需要 GPU 或网络服务。

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python -m sampling_engine generate-data
python -m sampling_engine demo --write-expected
pytest -q
```

`demo` 会分别在两个工作区完成“完整运行”和“运行两个请求 → 停止 → 恢复”，
比较最终 `result_digest`，并检查检查点序号连续。固定演示摘要写入
`artifacts/demo_expected.json`，完整结果写入 `artifacts/demo_result.json`。

## 常用 CLI

```bash
# 校验与互转清单
python -m sampling_engine validate-manifest --manifest data/manifest_v1.json
python -m sampling_engine convert \
  --manifest data/manifest_v2.json --target-version 1 \
  --output /tmp/manifest_v2_to_v1.json

# 导入 v2 与旧版 v1 清单
python -m sampling_engine import \
  --workspace work/manual \
  --manifest data/manifest_v2.json \
  --manifest data/manifest_v1.json \
  --manifest data/manifest_v2_large.json \
  --manifest data/manifest_v1_large.json

# 运行、停止后续跑、查询
python -m sampling_engine run --workspace work/manual --stop-after-requests 2
python -m sampling_engine status --workspace work/manual
python -m sampling_engine resume --workspace work/manual
python -m sampling_engine tokens --workspace work/manual --request-id r2_small
python -m sampling_engine summary --workspace work/manual
python -m sampling_engine summary --workspace work/manual --request-id r1_small

# 本地 HTTP；host/port 来自配置，且 host 只允许 127.0.0.1/localhost
python -m sampling_engine serve --config config/config.json
```

主要 HTTP 接口：

- `POST /api/import`：按路径或内联对象导入 v1/v2 清单。
- `POST /api/import-logits`：把 `.npy` 分片复制到工作区并返回内容摘要。
- `POST /api/run`、`POST /api/stop`、`POST /api/resume`。
- `GET /api/requests`、`GET /api/requests/{id}/status`。
- `GET /api/requests/{id}/fingerprints`、`GET /api/requests/{id}/temperature`。
- `GET /api/requests/{id}/tokens`、`GET /api/requests/{id}/summary`。
- `GET /api/corpus/summary`、`GET /api/result`。
- `GET /api/manifests/{id}/export?target_version=1`。
- `POST /api/snapshot/export`、`POST /api/snapshot/validate`。

## 清单结构

统一请求包含 `request_id`、`logits_path`、`temperature`、`top_k`、`top_p`、
`repetition_penalty`、`seed`、`steps` 和可选 `prompt_tokens`。缺少请求标识或
必需字段返回 `E_INVALID_MANIFEST`。

版本 2 使用浮点温度和显式 `top_p`：

```json
{
  "manifest_version": 2,
  "vocab": ["的", "token", "模型", "world"],
  "requests": [{
    "request_id": "demo",
    "logits_path": "logits/demo.npy",
    "temperature": 0.8,
    "top_k": 8,
    "top_p": 0.9,
    "repetition_penalty": 1.1,
    "seed": 123,
    "steps": 4,
    "prompt_tokens": [1]
  }]
}
```

版本 1 没有顶层 `top_p`，温度用整数百分比表示。导入时
`temperature = temp_percent / v1_temperature_scale`，缺少的 top-p 使用
`v1_default_top_p`。旧字段别名 `id`、`logits`、`temperature_percent` 也可读取。

```json
{
  "manifest_version": 1,
  "vocab": ["的", "token", "模型", "world"],
  "requests": [{
    "request_id": "legacy",
    "logits_path": "logits/demo.npy",
    "temp_percent": 80,
    "top_k": 8,
    "repetition_penalty": 1.1,
    "seed": 123,
    "steps": 4
  }]
}
```

v2 → v1 导出时，请求对象不含顶层 `top_p`，并写入 `x_downgrade` 降级账本及
`x_preserved_sampling` 无损往返扩展。再次导入时优先恢复扩展中的精确温度和
top-p；纯旧版清单则走换算与默认推导。

## 确定性与错误处理

- 概率归一化使用 float64 log-sum-exp；采样使用固定 `PCG64(seed)` 流和
  Gumbel-max，不直接连乘概率。
- 排序规则固定为“log probability 降序、token_id 升序”；top-p 跨越阈值的
  候选会被包含，随后在对数域重新归一化。
- NaN、正无穷或全负无穷 logits 返回 `E_NUMERIC`，附带 `request_id`、
  `step_index`、阶段和诊断摘要。
- 温度非正或超过上限返回 `E_INVALID_PARAM`，附带 `request_id` 和 `parameter`。
- 摘要同时给出 float64 值和 `Decimal(precision=60)` 高精度参考、相对误差与
  有效位数。

## 稳定业务错误码

| 错误码 | 含义 |
| --- | --- |
| `E_INVALID_MANIFEST` | 清单缺字段、缺请求标识、结构或词表非法 |
| `E_INVALID_PARAM` | 温度、top-k、top-p、种子、步数等参数非法 |
| `E_NUMERIC` | NaN/Inf/全负无穷、归一化或截断概率异常 |
| `E_INVALID_STAGE` | 非法阶段跳转或缺少前置阶段 |
| `E_STALE_CHECKPOINT` | 落后、重复、越序或跨请求发布检查点 |
| `E_SNAPSHOT_CONFLICT` | 版本回退、产物摘要不匹配或快照损坏 |
| `E_NOT_FOUND` | 请求、清单组或文件不存在 |
| `E_CONFLICT` | 本地锁超时或请求级执行冲突 |

## 演示数据

`data/` 由固定种子 `20261121` 生成，共 7 个请求：

- 24 词小词表与 512 词大词表，中英 token 混合。
- 每个 step 含并列最大值的 `r2_tie`。
- 注入 NaN 的 `r2_nan` 与注入 `+Inf` 的 `r2_inf`。
- 旧版整数温度百分比清单 `manifest_v1.json` / `manifest_v1_large.json`。
- 文件 SHA-256、形状、异常标记与清单摘要记录在
  `data/generator_metadata.json`。

## 实际验证结果

本次环境 Python 3.14.4、NumPy 2.5.5、pytest 9.2.0，`pytest -q` 为 43 passed。
固定完整运行摘要：

```text
164077104f4d8ce13657bb1ba4c8e937c3759f4db0614e7e8f24a5d1dc17ca0d
```

完整运行与“停止后恢复”摘要相同；5 个成功请求共 37 token，2 个异常请求分别
在 NaN step 2 和 +Inf step 0 返回 `E_NUMERIC`。语料级 NLL 为
`2.1326367605299152`，对 60 位 Decimal 参考的有效位数为 15 位。更多验证记录见
[`docs/TECHNICAL.md`](docs/TECHNICAL.md)。

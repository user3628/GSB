# 长上下文位置编码外推偏置诊断工作台（posbias-workbench）

本地、确定性、无 GPU / 无外部服务的位置相关偏置诊断工具。围绕固定随机种子生成的
Q/K 分片，计算 RoPE、NTK 缩放 RoPE、线性插值 RoPE 与 ALiBi 四种方案的位置相关偏置
矩阵、外推分档统计与单调性诊断，并与本地参考诊断指标做容差比对。系统不改动任何注意力
权重，只观测位置相关分量。

## 环境准备

```bash
# Python 3.11 固定虚拟环境（仓库根目录）
python3.11 -m venv .venv
.venv/bin/pip install -r posbias_workbench/requirements.txt
# 固定版本：numpy==2.0.2，PyYAML==6.0.2
```

## 目录结构

```
posbias_workbench/
├── configs/
│   ├── config_v2.yaml            # 版本 2 配置样例（四方案、三档、容差）
│   └── legacy_manifest_v1.yaml   # 结构版本 1 旧清单样例
├── posbias/
│   ├── errors.py                 # 稳定业务码 E_NUMERIC / E_SEQ_TOO_LONG ...
│   ├── cli.py                    # 命令行入口
│   ├── api/server.py             # 仅回环地址的 stdlib HTTP 服务
│   └── core/
│       ├── numeric.py            # 定点 int32 量化 + 对数域乘除 + 数值守卫
│       ├── config.py             # v2 校验、v1 兼容转换与稳定方案标识
│       ├── shards.py             # 固定种子分片生成 / 加载 / 哈希
│       ├── encodings.py          # RoPE 家族与 ALiBi 偏置矩阵核心
│       ├── binning.py            # 外推分档、差分与单调性诊断
│       ├── reference.py          # 参考指标生成与容差比对
│       ├── checkpoints.py        # 原子写、文件锁、序号与失效保护
│       ├── pipeline.py           # config→bias→binning→tolerance 四阶段
│       └── injection.py          # 异常注入入口
├── tests/test_workbench.py       # 14 个 unittest 用例，覆盖 8 项业务要求
├── scripts/demo.sh               # 一键确定性演示
└── data/                         # 分片、参考指标、检查点、量化产物、报告
```

## 快速开始

```bash
cd posbias_workbench
PY=../.venv/bin/python
$PY -m posbias.cli generate --shards 3 --batch 2 --heads 4 --seq-len 32 --head-dim 16
$PY -m posbias.cli reference      # 生成本地黄金参考指标
$PY -m posbias.cli run            # 四阶段诊断，输出 data/reports/diagnostic_report.json
$PY -m posbias.cli inject nan_shard
$PY -m posbias.cli serve --host 127.0.0.1 --port 8792
bash scripts/demo.sh              # 或一键演示 + 测试
```

## 配置（v2）

`configs/config_v2.yaml` 描述基础频率 `base_freq`、缩放系数 `scale`、ALiBi 斜率、
外推倍率档位 `bins`（边界必须连续）、训练长度 `train_length`、定点补齐规则
`padding_rule`（`zero_pad` / `replicate_last`）与逐指标容差 `tolerance`。

## HTTP 接口（全部绑定 127.0.0.1，启动前先做端口可用性预检）

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/health` `/config` `/status` `/report` `/schemes` `/invalidations` | 健康、配置、运行态、报告、分档诊断、失效原因 |
| POST | `/run` `/stop` `/resume` `/inject` | 运行（可后台）、请求停止、恢复、异常注入 |
| PUT | `/config` | 编辑编码配置，保存后下一跑次自动识别最早失效阶段并重建下游 |

## 关键设计

- **定点量化**：偏置矩阵以 int32 保存（`data/artifacts/*.npy`），每方案记录
  `quant_scale`；基频与缩放系数的乘除只在对数域进行（`numeric.log_domain_*`）。
- **确定性**：独立 `SeedSequence` 分片流 + 规范 JSON 指纹；重复运行报告与量化产物
  逐字节一致（见 `test_byte_identical_rerun`）。
- **空档保留**：无单元格的档位仍输出 `empty: true` 记录，边界与配置逐字段一致。
- **检查点**：临时文件 + fsync + `os.replace` 原子写；发布走 `flock` 文件锁并比较
  单调序号，落后 / 重复 / 错位发布返回 `E_STALE_CHECKPOINT`，缺前置阶段返回
  `E_INVALID_STAGE`。
- **旧清单兼容**：整数百分比 `scale_pct` 按 `percentage_divisor` 换算、缺省 ALiBi
  斜率按幂律补齐、整数倍率转浮点边界，全部记录在 provenance 中可回导；稳定方案标识
  由类型与参数哈希生成，回导后偏置 / 分档结果逐字节一致。

## 业务码

`E_NUMERIC`（零基频 / 负或零缩放 / 零量化因子 / NaN/Inf，带方案名与档位上下文）、
`E_SEQ_TOO_LONG`、`E_INVALID_PARAM`、`E_INVALID_CONFIG`、`E_STALE_CHECKPOINT`、
`E_INVALID_STAGE`。

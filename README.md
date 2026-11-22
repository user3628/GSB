# 合同扫描件条款区域检测与引证链冲突消解组件

本地可运行、可作为库集成的确定性组件：对多语种合同扫描件版面样本做候选框检测与 IoU 匹配，
将引证绑定区域坐标，解析中英文条款定位，归一适用期限/违约金比例/生效月份，
在同合同同条款定位下消解要素冲突，并导出可回导重建的版本化证据清单。

仅依赖 Python 3.10+ 标准库与 NumPy；无 GPU、无网络服务、无容器、无真实 OCR/数据集。

## 快速开始

```bash
python -V
python -c "import numpy"
pip install -r requirements.txt   # 仅 numpy
python -m unittest discover -s/tests -v
python demo.py
```

## 目录

- `contract_chain/`：库实现（检测、解析、定点归一、消解、证据、检查点、增量、CLI、HTTP）
- `tests/test_pipeline.py`：18 个确定性单元/集成测试
- `demo.py`：端到端演示（全量、证据回导、增量、配置失效传播、逐字节对照）
- `docs/TECHNICAL.md`：架构、定点域、指纹链、锁与恢复、接口说明

## 命令行

```bash
python -m contract_chain.cli --workspace ./cc_ws init
python -m contract_chain.cli --workspace ./cc_ws full
python -m contract_chain.cli --workspace ./cc_ws status
python -m contract_chain.cli --workspace ./cc_ws demotions
python -m contract_chain.cli --workspace ./cc_ws reconstruct
python -m contract_chain.cli --workspace ./cc_ws stop
python -m contract_chain.cli --workspace ./cc_ws resume
python -m contract_chain.cli --workspace ./cc_ws http --port 8765
```

## 库用法

```python
from contract_chain import Pipeline, PipelineConfig, build_sample
from contract_chain.evidence import import_manifest, reconstruct_decisions

pipeline = Pipeline("./workspace", PipelineConfig())
pipeline.run_full(build_sample())
manifest = import_manifest("./workspace/evidence_manifest.json")
verdicts = reconstruct_decisions(manifest)
```

## 确定性与错误码

- 规范化 JSON + SHA-256 摘要 + 原子 `os.replace`；同一输入/配置产物逐字节一致。
- 错误码：`E_REF_PARSE`、`E_UNIT_UNRESOLVED`、`E_EVIDENCE_BROKEN`、
  `E_STALE_CHECKPOINT`、`E_INVALID_STAGE`、`E_INCREMENTAL_MISMATCH`。
- 增量重建在临时工作区执行全量对照，差异时报告首个阶段与 `ref_id`。

## 内置样本覆盖

固定种子样本含 3 页、11 条引证，覆盖中英文条款句式、中文数字、多候选歧义、
无法解析引证、未知/缺失单位、反转区间、同条款多要素冲突、多合同隔离与新鲜度差异。

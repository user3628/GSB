# 验证记录

## 执行环境

当前工作区实际执行环境：

| 项目 | 值 |
|---|---|
| OS | Linux x86_64（Ubuntu 26.04 用户空间） |
| Python | 3.14.4 |
| NumPy | 2.5.5 |
| Pillow | 13.0.0 |
| pytest | 9.2.0 |

目标锁定环境在 `requirements.txt`：Python 3.11、NumPy 1.26.4、Pillow 10.4.0、pytest 8.3.3。当前环境没有 Python 3.11 或 Windows；尝试通过 `uv python install 3.11` 下载独立 CPython 时网络带宽不足以在会话内完成。因此，此处不虚报 Windows 本机导入验证已完成。`scripts/verify_windows.ps1` 可在 Windows + Python 3.11 上执行完整安装、导入检查和测试。

## 自动化测试

最近一次完整执行：

```text
$ pytest -q
..................................                               [100%]
34 passed in 4.79s
```

随后新增“跨材质错位发布”断言；对应文件单独执行：

```text
$ pytest -q tests/test_checkpoints.py
......                                                           [100%]
6 passed in 0.48s
```

覆盖范围：

- Unicode NFKC、空白、全半角、大小写、标点空提示、逐字节控制向量。
- 1–16 八度、NaN 定位、单线程/多线程逐元素一致。
- 色卡唯一排序、空调色板、尺寸不符、非法量化通道、最近邻并列规则。
- Decimal 参考、显式抽样位置、每层误差界、有效位数、预算溢出、归因复核。
- 全量生成、重跑一致、停止恢复一致、单/多线程摘要一致、增量失效起点。
- 原子记录、产物摘要篡改、整体快照回退、重复发布、跨材质错位、多进程竞争。
- CLI 生成、状态、报告、版本查询和结构化业务错误。

## 固定样本演示

样本包含 4 条材质：

- `terra_cotta`：4 八度，24×18，引用 `earth` 色卡。
- `brushed_steel`：6 八度，32×20，引用 `metal` 色卡。
- `indigo_velvet`：5 八度，20×14，引用 `textile` 色卡。
- `moss_stone`：7 八度，26×18，使用基础色回退色板。

运行：

```bash
python -m matgen demo --workspace .
```

代码与样本冻结后的完整运行摘要记录如下：

```text
run_digest: cd88ce30105758425424823eb41271999ee138d80f0708cc60a129ca6c09bcbc
```

报告中的误差预算默认值：

```text
octave_error_budget = 5e-13
total_error_budget  = 0.01
```

`total_used_bound` 包含不可避免的 8 位量化半 LSB，约 `1.96e-3`，总预算占用约 `19.6%`。逐八度浮点误差界约为 `1e-15` 到 `1e-14`，远低于逐八度预算。

## 注入异常

`samples/invalid_recipes.json` 与 `samples/expected_errors.json` 固定覆盖：

| material_id | 注入 | 业务码 |
|---|---|---|
| `bad_octaves` | 八度列表为空 | `E_OCTAVE_RANGE` |
| `empty_palette` | 引用显式空分片 | `E_PALETTE_INVALID` |
| `nan_noise` | 第 2 个八度振幅为 NaN | `E_NUMERIC`, `octave_index=1` |
| `dimension_mismatch` | 分片声明 7×4、实际 8×4 | `E_DIM_MISMATCH` |
| `empty_prompt` | 仅含中文标点 | `E_EMPTY_PROMPT` |

所有错误均包含 `material_id`；NaN 错误额外包含零基八度序号。

## 复现命令

```bash
python scripts/generate_fixtures.py --root .
python -m matgen demo --workspace .
python -m matgen status --workspace .
pytest -q
```

若需要验证停止恢复：

```bash
rm -rf .matgen .matgen.anchor.json out
python -m matgen generate --workspace . --stop-after 2
python -m matgen resume --workspace .
```

不要使用容器或外部服务；上述命令仅访问工作区本地文件。


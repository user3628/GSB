# matgen-local：本地确定性纹理与材质生成组件

该组件从本地 JSON 配方和固定种子色卡生成多层程序化纹理、材质参数、误差报告与可恢复检查点。全流程只使用 CPU、NumPy、Pillow 和标准库，不加载真实生成模型，不访问网络、GPU 或容器。

## 已交付内容

- 32 维确定性描述词控制向量：Unicode NFKC、空白折叠、全半角统一、小写化、固定 SHA-256 哈希种子。
- 逐八度 SplitMix64 值噪声：平滑边界插值、振幅归一化、串行/多线程固定顺序归约。
- 色卡分片读取、最近 RGB 调色、控制向量调制、确定重采样与 RGBA8 量化。
- Decimal(70) 高精度参考路径：逐八度累加界、相对误差、有效位数、调色/量化传播与总预算。
- 误差贡献排序与逐八度扰动重放交叉校验，输出待复核八度与偏差来源。
- 四阶段原子检查点、哈希链账本、外部锚点、文件锁、优雅停止、恢复和增量失效。
- 库 API、`python -m matgen` CLI、4 条本地配方、注入异常样本和固定摘要演示。

## 快速开始

当前工作区已经包含 `samples/`、`out/` 和 `.matgen/` 演示结果。重新执行：

```bash
python -m matgen demo --workspace .
```

预期完成状态与固定摘要见 `docs/VALIDATION.md`。

常用命令：

```bash
python -m matgen generate --workspace .
python -m matgen status --workspace .
python -m matgen report --workspace . --material-id terra_cotta
python -m matgen versions --workspace . --config samples/config.json
python -m matgen stop --workspace .
python -m matgen resume --workspace .
python -m matgen validate-recipes --workspace .
```

`generate --stop-after 2` 可确定性地模拟“完成两个不可拆分材质单元后停止”。`resume` 会清除停止标志并从检查点继续。

## 安装与锁定环境

目标运行时为 Python 3.11 + `requirements.txt` 中的精确版本：

```text
numpy==1.26.4
Pillow==10.4.0
pytest==8.3.3
```

Linux/macOS：

```bash
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.txt
python scripts/verify_environment.py
pytest -q
```

Windows PowerShell（本地验证脚本）：

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\verify_windows.ps1
```

该脚本创建 `.venv`、安装锁定依赖、验证 NumPy/Pillow/pytest 可导入并运行测试。当前执行环境为 Linux 且只有 Python 3.14，无法在此容器中声称已完成 Windows 本机导入验证；详见 `docs/VALIDATION.md`。

## 配方格式

`samples/recipes.json` 是配方数组，单条示例：

```json
{
  "material_id": "terra_cotta",
  "description": "粗糙 陶土 Terracotta 暖色 Surface",
  "base_color": [0.72, 0.34, 0.22],
  "roughness": 0.82,
  "metalness": 0.01,
  "resolution": [24, 18],
  "palette_card": "earth",
  "noise": {
    "seed": 104729,
    "base_resolution": [40, 30],
    "octaves": [
      {"frequency": 1.0, "amplitude": 0.62},
      {"frequency": 2.0, "amplitude": 0.437}
    ]
  }
}
```

- `resolution` 为最终纹理 `[width, height]`。
- `noise.base_resolution` 可省略；省略时按配置从目标分辨率派生并限制在 `[noise_base_min, noise_base_max]`。
- 八度数支持 1 到 16。频率范围 `(0, 4096]`，振幅范围 `(0, 1e6]`。
- `palette_card` 可省略，此时使用由基础色生成的 16 级确定性回退色板。
- 色卡分片由 `samples/color_cards.json` 的 `box`、`declared_size` 声明；PNG 与声明尺寸不一致返回 `E_DIM_MISMATCH`。

## 库 API

```python
from matgen import generate_all, get_error_report, get_status, load_config, request_stop

config = load_config("samples/config.json")
result = generate_all(
    workspace=".",
    recipes_path="samples/recipes.json",
    cards_path="samples/color_cards.json",
    config=config,
    output_root="out",
)

status = get_status(".")
error_report = get_error_report(".", "terra_cotta")
```

主要模块：

- `matgen.prompt`：规范化、分词、固定哈希控制向量。
- `matgen.noise`：值噪声、八度合成、重采样。
- `matgen.palette`：色卡加载、调制、最近邻调色、8 位量化。
- `matgen.diagnostics`：高精度参考、误差传播、预算和归因。
- `matgen.checkpoints`：原子提交、账本、锁、快照分类。
- `matgen.pipeline`：四阶段编排、缓存复用、输出和报告。

## 业务错误码

| 错误码 | 场景 |
|---|---|
| `E_EMPTY_PROMPT` | 空描述或仅含标点 |
| `E_OCTAVE_RANGE` | 八度数、频率或振幅越界 |
| `E_PALETTE_INVALID` | 空调色板、未知分片、非法量化通道 |
| `E_DIM_MISMATCH` | 色卡、分片、目标分辨率声明不一致 |
| `E_NUMERIC` | NaN/Inf 或非有限误差界，包含 octave 序号（适用时） |
| `E_INVALID_STAGE` | 缺少前置阶段或查询缺失阶段 |
| `E_STALE_CHECKPOINT` | 重复、回退、篡改、跨材质错位发布 |
| `E_INVALID_RECIPE` / `E_INVALID_CONFIG` / `E_IO` | 输入或文件系统错误 |

## 产物

- `out/<material_id>/texture.rgba8`：规范 8 位纹理，无平台字节序问题。
- `out/<material_id>/texture.png`：同一纹理的本地预览。
- `out/<material_id>/material.json`：材质参数与调色板来源。
- `out/<material_id>/diagnostics.json`：显式抽样位置、Decimal 参考、误差界、有效位数、预算和归因。
- `out/report.json`：按 `material_id` 排序的汇总与固定 `run_digest`。
- `.matgen/checkpoints/`：四阶段检查点；`.matgen/ledger.jsonl`：哈希链账本；`.matgen.anchor.json`：外部回退锚点。

更多实现细节与公式见 `docs/DESIGN.md`，实际测试与环境记录见 `docs/VALIDATION.md`。


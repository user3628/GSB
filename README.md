# 检查点与 WAL 回放组件（wckpt）

面向本地单机存储引擎的页式表空间、WAL 追加/轮转、检查点、自适应回放与强杀恢复组件。
纯 Python 3.10+ 标准库（`sqlite3`）+ `numpy`，无外部服务、无容器、无网络依赖。

## 环境准备（Windows）

```bat
python --version
pip install -r requirements.txt
```

## 目录

- `wckpt/`：组件源码（引擎、WAL、检查点、回放、恢复、SQLite 存储）。
- `scripts/generate_samples.py`：生成确定性样本到 `data/`。
- `scripts/run_demo.py`：端到端演示，输出 `artifacts/demo_report.json`。
- `tests/`：单元 / 集成 / 子进程强杀恢复测试。
- `docs/design.md`：技术设计；`docs/requirements_matrix.md`：需求覆盖矩阵。
- `run_demo.bat` / `run_tests.bat` / `run_demo.ps1`：Windows 入口。

## 运行

```bat
REM 生成样本并跑端到端演示
run_demo.bat

REM 完整测试
run_tests.bat
```

Linux/macOS：

```bash
python scripts/generate_samples.py
python scripts/run_demo.py
python -m unittest discover -s tests -p "test_*.py"
```

## 关键能力

- WAL 段文件轮转、CRC 校验、按 LSN 范围检索、截断与归档。
- 检查点整体版本替换、增量检查点、SHA256 校验与单调序号。
- 精确 / 批量两条自适应回放路径，逐页结果一致，支持中断续算。
- 单写者多读者、请求幂等去重。
- SQLite 意图持久化，强制终止后基于持久化事实恢复一致状态。

业务错误码：`E_WAL_CORRUPT`、`E_CHECKPOINT_INVALID`、`E_REPLAY_MISMATCH`、
`E_TRUNCATE_UNSAFE`。

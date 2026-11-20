#!/usr/bin/env bash
# 本地确定性演示：准备虚拟环境（Python 3.11 + NumPy），运行演示与测试。
# 全程仅使用本地文件，不依赖网络服务（仅首次建环境时 pip 安装 NumPy）。
set -euo pipefail
cd "$(dirname "$0")"

PY=python3.11
if [ ! -x .venv/bin/python ]; then
  "$PY" -m venv .venv
  .venv/bin/python -m pip install -q -r requirements.txt
fi

echo "== 运行演示 demo.py =="
.venv/bin/python demo.py

echo
echo "== 运行测试 =="
.venv/bin/python -m unittest discover -s tests -v


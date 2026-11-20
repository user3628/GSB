# 依赖说明（本地、无外部服务、无 GPU）

## 运行环境
- Python >= 3.10（开发验证环境：Python 3.14，NumPy 2.5）
- NumPy >= 1.24：仅用于阶段二的水平起点/宽度直方图与中位数计算。

## 标准库使用（无需安装）
- `http.server` / `urllib`：本地 HTTP 应用与请求处理
- `fcntl`：跨进程文件锁（发布保护）
- `hashlib` / `json`：内容指纹与确定性 JSON 序列化
- `os` / `tempfile` / `argparse` / `threading` / `multiprocessing`：
  原子写入、CLI、并发与隔离基线构建

## 明确不使用
- 不使用任何真实 OCR 模型、真实数据集、网络外部服务或 GPU；
- 不使用 Docker 或其他容器环境；
- HTTP 仅监听 `127.0.0.1` 本地回环地址。

## 命令行入口
- CLI：`python -m layout_order.cli ...`
- HTTP：`python -m layout_order.http_app --host 127.0.0.1 --port 8080`

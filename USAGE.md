# 运行手册

## 环境准备

```bash
python3 --version            # 需 64 位 Python 3.10+
python3 -m venv .venv
. .venv/bin/activate         # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```

仅依赖 numpy 2.x 与 Python 标准库（`wave`、`http.server`、`fcntl`/`msvcrt`），
不安装任何深度学习框架或音频工具包，不使用 Docker。

## 一键演示

```bash
python scripts/demo.py
```

生成样本 → 正常四阶段解码 → 流式/恢复一致性 → 优雅停止/恢复 → 五类异常注入 →
打印 `results/demo_report.json`。

并发发布保护（两个本地进程，无需管理员权限）：

```bash
python scripts/concurrent_publish_check.py
```

## 命令行

```bash
# 1. 生成确定性样本与注入夹具
python -m asr_pipeline.cli --data-dir data --workspace workspace generate

# 2. 完整四阶段运行
python -m asr_pipeline.cli --data-dir data --workspace workspace run

# 3. 分阶段触发（可任意组合 frontend,decode,align；summary 触发全流程汇总）
python -m asr_pipeline.cli --data-dir data --workspace workspace stage --stages frontend
python -m asr_pipeline.cli --data-dir data --workspace workspace stage --stages decode,align

# 4. 查询阶段状态与版本指纹
python -m asr_pipeline.cli --data-dir data --workspace workspace status
python -m asr_pipeline.cli --data-dir data --workspace workspace fingerprint

# 5. 优雅停止与恢复
python -m asr_pipeline.cli --data-dir data --workspace workspace stop
python -m asr_pipeline.cli --data-dir data --workspace workspace resume

# 6. 异常注入（只经输入文件/参数，不改源码）
python -m asr_pipeline.cli --data-dir data/injections --workspace ws_inj inject --kind silent
python -m asr_pipeline.cli --data-dir data --workspace ws_bad --hop-ms 0 inject --kind bad_frame

# 7. 预算参数（全局帧预算/单线程配额/单分片延迟）
python -m asr_pipeline.cli --data-dir data --workspace ws_budget \
  --global-budget 150 --worker-quota 100 --shard-latency 100 run
```

## HTTP 接口

```bash
python -m asr_pipeline.cli serve --host 127.0.0.1 --port 8080
```

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| GET | `/version` | 版本与各阶段指纹、失效观测 |
| GET | `/status` | 阶段状态与汇总 |
| GET | `/diagnostics` | 不一致/越界诊断上下文 |
| POST | `/import` | 导入分片目录、工作区与配置参数 |
| POST | `/stage` | 分阶段解码，body `{"stages":["frontend"]}` |
| POST | `/run` | 完整四阶段解码 |
| POST | `/stop` | 请求优雅停止 |
| POST | `/resume` | 从最近一致检查点恢复 |

示例：

```bash
curl -s -X POST localhost:8080/import -H 'Content-Type: application/json' \
  -d '{"data_dir":"data","workspace":"workspace_http"}'
curl -s -X POST localhost:8080/run -d '{}'
curl -s localhost:8080/status
```

## 测试与基准

```bash
python -m unittest discover -s tests -v
```

固定基准保存在 `results/baseline.json`，其中 `result_fingerprint` 用于对比
流式解码、断点恢复与从零完整运行三者的一致性。

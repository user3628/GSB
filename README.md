# 本地超标告警根因定位管线（Offline SLI RCA Pipeline）

面向本地确定性可观测性数据集的离线告警降噪与根因分析管线。接收带毫秒时间戳的
SLI 样本与带版本号的服务依赖拓扑，产出分位聚合索引、告警抑制索引与根因候选评分，
支持带基础版本的增量提交、级联原子重建、单写者/多读者并发与强制终止恢复。

- 纯本地：仅依赖 Python 标准库 `sqlite3`（WAL），无外部数据库/监控系统/容器
- 确定性：固定样本与拓扑，最近秩分位与定点评分保证逐位一致
- HTTP：FastAPI + uvicorn，仅监听回环地址，统一 JSON 错误信封与业务码

## 快速开始

```bash
pip install -r requirements.txt
python scripts/generate_data.py     # 生成 data/ 下确定性样本与拓扑
python -m pytest                    # 24 个测试，含 SIGKILL 恢复用例
python scripts/demo.py              # 无 HTTP 端到端演示
python -m uvicorn rca_pipeline.app:create_app --factory --host 127.0.0.1 --port 8000
```

## 目录结构

```
rca_pipeline/   领域模块（解析、拓扑、分位、告警、RCA、级联、恢复、服务、HTTP）
scripts/        generate_data.py 确定性数据；demo.py 端到端演示
tests/          pytest 测试套件
data/           生成的确定性样本与拓扑文件
docs/           design.md 设计说明；verification.md 验证报告
```

## 业务错误码

`E_SAMPLE_INVALID`、`E_STALE_BASE`、`E_INSUFFICIENT_SAMPLES`、
`E_INVALID_TRANSITION`、`E_CONFLICTING_BATCH`、`E_TOPOLOGY_INVALID`、
`E_CHECKSUM_MISMATCH`、`E_REBUILD_FAILED`、`E_NOT_FOUND`。

错误响应统一格式：

```json
{"ok": false, "error": {"code": "E_STALE_BASE", "message": "...", "context": {}}}
```

## 核心 HTTP 接口（前缀 /api/v1）

`POST /samples/batches`、`POST /topology/changes`、`GET /alerts`、
`POST /alerts/{id}/transitions`、`GET /aggregations/quantiles`、
`GET /suppressions`、`GET /rca/{alert_id}`、`GET /diagnostics`、
`GET /recovery`、`POST /faults/inject`。

详细设计见 `docs/design.md`，实际运行结果见 `docs/verification.md`。

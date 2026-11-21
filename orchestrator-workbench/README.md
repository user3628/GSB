# 资源编排依赖图工作台（Resource Orchestration Workbench）

本地、零外部依赖的服务资源依赖图编排与诊断工作台。支持三种可互换快照格式、
版本兼容协商、定点权重归一化、拓扑分层/编排批次、影响面分析、增量更新、
快照原子切换、单写者并发控制、幂等提交以及基于意图日志的崩溃恢复。

## 环境准备

- Python 3.11+（开发与验证使用 Python 3.14，标准库 `sqlite3`/`zlib`/`struct`/`json`）
- 不使用 Docker；无外部数据库；HTTP 仅绑定回环地址

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python scripts/generate_samples.py   # 生成 samples/ 确定性样本
```

## 目录结构

```
app/
  errors.py            业务错误码与 WorkbenchError
  models.py            Pydantic 模型（快照、记录、报告、增量片段等）
  codecs/
    binary.py          紧凑二进制：魔数/版本字节/长度前缀/CRC32/存在位图
    jsonl.py           分片 JSON 行（浮点权重、允许缺字段）
    yaml_like.py       旧版 YAML 子集行（仅标识+父列表，无权重）
    registry.py        格式自动识别与编解码分发
  compat.py            显式兼容矩阵与 accept/degraded_accept/reject 裁决
  normalize.py         千分比定点归一化、舍入规则、预算溢出检查
  graph.py             Kahn 分层、稳定批次排序、环检测、numpy 下游闭包
  store.py             SQLite 持久化、意图日志、原子快照切换、恢复
  service.py           导入/导出/诊断/增量/重建编排服务
  http_api.py          FastAPI 路由与统一错误信封
  main.py              uvicorn 入口（127.0.0.1，支持 WORKBENCH_DB 覆盖）
scripts/
  generate_samples.py  生成确定性本地样本
  run_demo.py          端到端 HTTP 演示（含强制终止恢复）
tests/                 pytest 测试套件（21 个用例）
samples/               生成的本地样本
```

## 三种持久化格式

- **二进制（binary v1）**：头部 34 字节 = 魔数 `ROWB`(4) + 版本/标志(2) +
  快照序号(4) + 16 字节时间戳 + 记录数(4) + 整包 CRC32(4)；每条记录为
  `长度前缀(4) + 载荷 + 记录 CRC32(4)`，载荷以 32 位存在位图声明
  `budget/weight_permille/expected_version/parents` 是否存在。
- **JSON 行（jsonl v1）**：首行快照头，其后每行一条记录；`weight` 为浮点，
  `budget/weight/expected_version` 均可缺省。
- **YAML 子集（yaml v1）**：每行 `id: [parent, ...]`，仅标识与父列表，
  元数据放在 `# @key value` 注释头；天然无权重/预算字段。

## 兼容矩阵（显式声明，见 `app/compat.py`）

| 规则 ID | 格式/版本 | 裁决 | 依据 |
| --- | --- | --- | --- |
| `BIN-V1-NATIVE` | binary v1 | accept | 原生千分比定点，位图+CRC 完整 |
| `JSONL-V1-FLOAT-DEGRADE` | jsonl v1 | degraded_accept | 浮点权重量化到千分比，缺字段补默认 |
| `YAML-V1-LEGACY-DEGRADE` | yaml v1 | degraded_accept | 无权重/预算/期望版本，按声明默认补齐 |
| `FUTURE-REQUIRED-REJECT` | binary/jsonl v2+ | reject | 含未知必填字段，返回 `E_VERSION_UNSUPPORTED` |

## 归一化与舍入

- 内部统一为千分比定点整数；二进制原样校验，JSON 浮点 `w*1000` 后按调用方
  声明规则（`half_up/half_even/floor/ceil/truncate`）量化，YAML 缺省补 0。
- 每条记录输出 `discarded_fraction`（舍入位，单位千分之一）与累计舍入误差。
- 预算为 `0..2^32-1` 非负整数，越界/负值/重复标识进入拒绝清单，不进入依赖图。

## 拓扑与批次

- Kahn 分层，保证所有父资源层级严格小于子资源；环返回 `E_DEPENDENCY_CYCLE`
  并在 `details.cycle` 给出环上标识。
- 同层按 `(权重降序, 标识升序)` 稳定排序，重复计算逐位一致。
- 影响面使用 numpy 邻接矩阵传递闭包，输出下游集合、受影响层级与批次范围。

## 事务、并发与恢复

- 所有写操作经单写者锁串行；SQLite WAL 保证只读查询永远看到完整已提交版本。
- 意图先以自动提交方式落盘，再进入主写事务；提交成功才推进 `active_snapshot_seq`。
- 同一 `submit_id` 重复提交复用首次结果；同一基础版本的并发增量仅一个成功，
  另一个得到 `E_STALE_BASE`。
- 进程被 SIGKILL 后，未提交事务由 SQLite 回滚，重启时扫描 pending 意图并标记
  `aborted`，活动指针保持在最后一个完整快照，恢复报告仅基于已持久化事实。

## 业务错误码

| 代码 | 触发场景 |
| --- | --- |
| `E_FORMAT_INVALID` | 魔数/CRC/长度前缀/JSON 行/YAML 行畸形 |
| `E_VERSION_UNSUPPORTED` | 高版本含未知必填字段，兼容矩阵拒绝 |
| `E_SNAPSHOT_INVALID` | 归一化失败、序号非单调、父资源缺失等 |
| `E_DEPENDENCY_CYCLE` | 依赖图存在环 |
| `E_STALE_BASE` | 增量基础版本不是当前版本 |
| `E_NOT_FOUND` | 资源/快照不存在 |

## 运行

```bash
# 测试
.venv/bin/python -m pytest -q

# 本地 HTTP 服务（回环；端口可用 WORKBENCH_DB 之外的 uvicorn 参数调整）
.venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8765

# 端到端演示（自动选择空闲回环端口）
.venv/bin/python scripts/run_demo.py
```

## HTTP 接口摘要

- `GET /health`、`GET /recovery`
- `POST /snapshots/import?rounding_rule=half_even`、`POST /snapshots/dry-run`
- `GET /snapshots`、`GET /snapshots/active`、`GET /snapshots/{seq}`
- `GET /snapshots/export/{binary|jsonl|yaml}`
- `GET /diagnostics/impact/{resource_id}`
- `POST /increments`、`POST /rebuild?submit_id=...`

# pkq：本地分块列存的拉取式点查服务

面向本地分块列存文件的点查服务：客户端提交点查请求（主键等值集合、投影列、
可接受页大小、结果截止时间），拿到结果句柄与首页后按页序号递增拉取，直到结果
耗尽。服务端按主键范围切分取数任务，在**全局并发上限 + 单任务内存预算 + 整轮
时间预算**三重约束下调度，解码三种编码块后按主键过滤投影，命中行按主键稳定
顺序写入分页结果缓冲；会话可在两次拉取之间被强制终止，重启后已物化页可继续
拉取、未完成主键范围重新排队，且续拉页序列与从未中断**逐页逐行一致**。

## 环境

- Python 3.11+（开发验证环境为 WSL2 Linux / Python 3.14，代码跨平台，Windows 同样适用）
- 隔离环境：`python -m venv .venv && .venv/Scripts/pip install -r requirements.txt`（Windows）
  或 `.venv/bin/pip install -r requirements.txt`（Linux/macOS）
- 依赖固定于 `requirements.txt`：fastapi 0.165.0 / uvicorn 0.54.0 / numpy 2.5.5 / pytest 9.2.0 / httpx 0.28.1
- 不使用 Docker；所有数据来自本地确定性样本，无外部依赖

## 快速开始

```bash
# 1. 生成确定性样本（三种编码、空值位图、跨块主键、损坏块、不同解压代价）
.venv/bin/python tools/gen_sample.py            # 输出到 artifacts/sample/

# 2. 启动服务（自动创建 SQLite 可写目录与分页缓冲临时目录，回环端口）
.venv/bin/python -m pkq.app --db data/pkq.db --snapshot artifacts/sample/snapshot_v1 --port 18311

# 3. 提交点查并拉取
curl -X POST localhost:18311/query -H 'content-type: application/json' \
     -d '{"query_id":"q1","keys":[0,7,35,5005],"page_size":4}'
curl -X POST localhost:18311/pull  -H 'content-type: application/json' \
     -d '{"handle":"<handle>","page_seq":1}'

# 4. 测试与验证
.venv/bin/python -m pytest tests/ -q
.venv/bin/python tools/verify.py --keep         # 明细写入 artifacts/verify_run/verify_result.json
```

## 模块结构

| 模块 | 职责 |
| --- | --- |
| `pkq/encoding.py` | fixed / rle / dict 三种编码的确定性编解码；块头（主键区间、行数、编码类型、摘要）校验；解压后内存估计 |
| `pkq/columnar.py` | 快照清单（版本、生成时间、列定义、升序分块索引、整体摘要）写入/加载；版本单调校验；`SnapshotStore` 原子替换 |
| `pkq/planner.py` | 点查切分：定位命中分块、合并索引相邻块为任务，区间不重叠且覆盖全部命中块 |
| `pkq/scheduler.py` | 三重预算调度与裁决轨迹：并发上限、有界等待队列、内存拆分/降级、轮时间（代价）预算 |
| `pkq/store.py` | SQLite 持久化：会话、任务、写前意图记录、行缓冲、分页缓冲；恢复扫描 |
| `pkq/service.py` | 业务编排：提交幂等、单写者物化、多读者拉取、页边界主键保护 |
| `pkq/app.py` | FastAPI 入口与统一错误响应 |
| `tools/gen_sample.py` | 确定性样本生成（固定种子，逐位可复现） |
| `tools/verify.py` | 结果验证工具（见下） |

## 关键设计

**快照加载（需求 1）**：清单声明单调递增版本号、生成时间、列定义（dtype +
空值位图）、按主键升序的分块索引与整体 sha256 摘要。加载先校验摘要与区间
不重叠，再校验版本单调；任一失败抛 `E_SNAPSHOT_INVALID`，旧快照继续服务；
成功后 `SnapshotStore` 整体替换，绝不混合新旧分块索引。

**块解码（需求 2）**：块文件 = magic + 头长 + 头 JSON（含自身摘要）+ 列
payload。解码前校验块号、主键区间、原始行数、编码类型与逐列 sha256；失败抛
`E_BLOCK_CORRUPT` 并在 `details.block_id` 指明块号。解码结果带空值位图，块
内主键升序；同一输入多次解码逐位一致（纯函数 + 固定字节序）。

**任务切分（需求 3）**：主键集合为空 → 空任务列表（提交返回空结果句柄）；
数量超 `max_keys_per_query` → `E_QUERY_TOO_LARGE` 且 `details.limit` 附当前
上限；切分结果（任务编号、区间、块列表、内存/代价估计）落库并输出给预算裁决。

**三重预算调度（需求 4）**：任务按主键区间升序裁决——超出全局并发上限者进入
有界等待队列，队列满 `E_SCAN_QUEUE_FULL`；解压后内存估计超单任务预算者按块
内行范围拆分为子任务重试，单行仍超预算则降级跳过并记录原因；整轮时间预算
（确定性解压代价单位）耗尽时剩余任务标记 `NOT_EXECUTED` 进入下一轮，而非
静默丢弃。执行用 `max_workers=并发上限` 的线程池，结果按提交顺序归并，输出
与轨迹完全确定。每条轨迹记录 `{round, task_id, decision, reason, ...}`。

**拉取式分页（需求 5）**：提交返回句柄、总命中估计与首页（首页随提交原子
登记为已消费）；页大小 = min(客户端页大小, 配置上限)；首轮在结果截止内给
出首页或 `NOT_READY`；切页时若下一行与页末行同主键则扩展页，绝不切断同主
键命中行；页序号越界或对已耗尽句柄继续拉取 → `E_PAGE_OUT_OF_RANGE`；结果
耗尽返回确定的 `EXHAUSTED` 状态而非空页。

**持久化与恢复（需求 6）**：每个取数任务的命中行写入行缓冲后，在**同一
SQLite 事务**内先写意图记录（`intents` 表）再提交；页切割与预算占用登记也
在该事务内（需求 7：查询看不到页可拉取而预算未登记的中间态）。强制终止时
未提交事务整体回滚。重启 `recover()` 只扫描已持久化事实：有意图且已提交的
任务标记可复用（不重算），其余恢复为 `READY` 重新排队；恢复报告不含任何
内存态推断，也不承诺强制终止后生成完整退出报告。

**并发一致（需求 7）**：同一会话的物化由会话级写锁串行（单写者）；状态查
询（`/sessions/{handle}`）为只读可并发；页消费用
`UPDATE ... WHERE state='MATERIALIZED'` 原子完成，并发拉取同页仅一个成功，
其余 `E_PAGE_CONSUMED`；同一 `query_id` 重复提交复用同一句柄（内容指纹一致
时），内容不一致 `E_CONFLICTING_QUERY`。

**稳定归并（需求 8）**：任务按主键区间升序提交、块内行升序、同主键行保持
解码顺序，拼接后整体主键升序；分页拼接结果与不分页一次取尽逐行一致。

## HTTP 接口

| 方法/路径 | 说明 |
| --- | --- |
| `POST /query` | 提交点查：`{query_id, keys[], columns[]?, page_size?, deadline_sec?}` → `{handle, status, total_estimate, page_size, first_page?, next_page_seq}` |
| `POST /pull` | 拉取页：`{handle, page_seq}` → `{status: OK/NOT_READY/EXHAUSTED, rows[], row_count}` |
| `GET /sessions/{handle}` | 只读状态：物化状态、下一可取页序号、已物化页数、任务进度、已消费主键上界 |
| `GET /sessions/{handle}/trace` | 该会话的预算裁决轨迹 |
| `POST /snapshots/load` | 加载/替换快照（版本单调 + 摘要校验） |
| `GET /snapshots/current` | 当前快照版本与列定义 |
| `GET /recovery-report` | 重启后的恢复报告（仅持久化事实） |
| `GET /config` | 生效配置 |

失败响应统一为 `{"error": {"code", "message", "details"}}`，覆盖
`E_SNAPSHOT_INVALID`(409)、`E_BLOCK_CORRUPT`(422)、`E_QUERY_TOO_LARGE`(413)、
`E_SCAN_QUEUE_FULL`(429)、`E_PAGE_OUT_OF_RANGE`(416)、`E_PAGE_CONSUMED`(409)、
`E_CONFLICTING_QUERY`(409)、`E_SESSION_NOT_FOUND`(404)。

## 样本说明（`tools/gen_sample.py`，固定种子确定性生成）

- `snapshot_v1/`：版本 1，12 块 × 100 行；`pk` 定长编码、`val_a` 游程编码、
  `val_b`/`name` 字典编码；每列带空值位图；每 10 个主键重复 3 次（同主键多
  行命中，用于页边界保护验证）；块解压代价分三档（50–80 / 100–700 / 900–1500）
- `snapshot_v2/`：版本 2，用于版本单调与整体替换验证
- `snapshot_corrupt/`：块 5 payload 被篡改，用于 `E_BLOCK_CORRUPT`
- `snapshot_badsum/`：清单摘要被篡改，用于 `E_SNAPSHOT_INVALID`

## 实际验证结果（2026-11-22，本机）

`pytest tests/ -q`：**31 passed**（编码往返/块头校验、快照版本与摘要、切分
不重叠覆盖、调度拆分/降级/轮预算/队列满、意图恢复、页边界、单消费者、
API 错误码、进程级 kill 恢复 e2e）。

`tools/verify.py --keep`（真实 uvicorn 子进程 + SIGKILL）：

- 场景 A 不间断拉取：3 页（5/4/2 行）共 11 行；场景 B 物化中途 SIGKILL、
  重启恢复续拉：3 页 11 行，**逐页逐行一致**
- 恢复报告：`reused_tasks=[t0000..t0003]`（已提交复用不重算），
  `requeued_tasks=[t0004]`（未提交重新排队），已物化 2 页、下一可取页序号 2
- 分页拼接结果与不分页一次取尽真值逐行一致（11 行，不重不漏）
- 裁决轨迹：A 含 QUEUED×3 / SPLIT×5 / RUN×16 / DONE×16；B 仅含恢复后
  重跑任务的 SPLIT/RUN/DONE
- 错误码核验：`E_QUERY_TOO_LARGE`、`E_CONFLICTING_QUERY`、幂等复用、空键
  空句柄、`E_PAGE_OUT_OF_RANGE`、`E_SNAPSHOT_INVALID`（版本+摘要）、旧快照
  保留、`E_BLOCK_CORRUPT`(block_id=5)、`E_SCAN_QUEUE_FULL` 全部通过
- 明细：`artifacts/verify_run/verify_result.json`

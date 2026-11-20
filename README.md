# 文档图像版面阅读顺序分析引擎（本地确定性）

一个完全本地、确定性、可恢复、可增量的文档版面分析引擎。输入为固定随机种子
生成器产出的 JSON 版面样本，输出四阶段检查点、块阅读顺序、跨栏统计、顺序
置信度与页面级摘要。**不依赖真实 OCR 模型、真实数据集、外部服务或 GPU，
不使用容器。**

## 目录结构

```
layout_order/
  config.py            # 四阶段常量与可配置参数（按阶段分组的配置指纹）
  errors.py            # 稳定业务错误码 E_BOX_INVALID / E_COLUMN_UNASSIGNED / ...
  hashing.py           # 规范化 JSON、SHA-256 内容指纹（逐字节一致的基础）
  generator.py         # 固定种子样本生成器（4 个文档，覆盖全部场景）
  engine.py            # 管线编排、失效传播、优雅停止、增量/全量一致性校验
  store.py             # 原子检查点、文件锁、单调序号、失效日志
  stages/
    normalize.py       # 阶段一：坐标换算、框合并、页眉页脚识别
    column.py          # 阶段二：直方图/聚类栏推断、通栏标记
    order.py           # 阶段三：块序列化、DAG 裁决、环检测、置信度
    summary.py         # 阶段四：汇总、跨栏统计、顺序质量、页面摘要
  cli.py               # 命令行应用
  http_app.py          # 标准库本地 HTTP 应用
tests/                 # 30 个 pytest 用例
scripts/               # 停止恢复 / 错误注入 / HTTP 演示脚本
fixtures/              # 错误注入清单与配置覆盖样例
docs/                  # 设计说明、API 说明、验证报告
workspace/             # 运行期目录：samples/checkpoints/fingerprints/exports/locks
```

## 快速开始

```bash
python -m pip install -r requirements.txt        # 仅 numpy；pytest 可选
python -m pytest tests/ -q                        # 30 passed

# 生成样本并全量分析
python -m layout_order.cli --workspace workspace generate
python -m layout_order.cli --workspace workspace analyze
python -m layout_order.cli --workspace workspace status
python -m layout_order.cli --workspace workspace sequence --doc-id doc_double_mixed
python -m layout_order.cli --workspace workspace verify   # 增量 vs 全量逐字节比对
```

## 确定性样本（固定种子 20261121）

| doc_id | 覆盖场景 |
| --- | --- |
| `doc_single_zh` | 单栏中文、页眉页脚 |
| `doc_double_en` | 双栏英文、通栏标题、页眉页脚 |
| `doc_double_mixed` | 双栏中英混排、顶部通栏标题 + 中部通栏小节标题 |
| `doc_single_mixed_fragments` | 单栏中英混排、同行碎片框合并、页眉页脚 |

每个文档显式携带 `reference_order`，顺序质量指标只依赖该参考序列计算；
参考缺失时指标明确置空，绝不推断。

## 四阶段管线

1. **normalize**：归一化坐标 → 像素坐标（固定 3 位小数）；按页边距离 +
   小字号档位识别页眉/页脚；对水平/垂直间隙 `< merge_gap_px`（默认 12px）
   的相邻框用并查集确定性合并；越界/零面积 → `E_BOX_INVALID`。
2. **column**：水平起点与宽度直方图 + 1-D 聚类判定 1/2 栏；栏区间互不重叠
   且连续覆盖有效版面宽度；宽度比 ≥ 0.80 的框标记为通栏（`column_index=-1`）；
   无正文框或无法归属 → `E_COLUMN_UNASSIGNED`；记录判定依据与投票框数。
3. **order**：页眉置顶、页脚置底；正文按「栏内自上而下、栏间自左向右」构建
   有向无环偏好图并做 Kahn 拓扑排序；通栏块支持 `flow/span_first/column_first`
   三种可配置裁决；环检测/空块/重复框 → `E_ORDER_INVALID`；置信度由相邻块
   垂直间隙分（权重 0.6）与栏归属分（权重 0.4）显式加权。
4. **summary**：块序列、跨栏统计、置信度、页面摘要、顺序准确率/逆序数，
   以及对完整结果计算的 64 位 `result_digest`。

## 检查点、停止恢复与版本失效

- 每个阶段以 `临时文件 + fsync + os.replace` **原子写入**
  `workspace/checkpoints/<doc_id>/stage-<n>-<stage>.json`，记录含
  `doc_id / stage / seq（单调序号）/ input_fingerprint / config_version /
  stage_fingerprint / product_digest / product_summary / product`。
- 优雅停止：CLI `analyze --stop-after <stage>` 或 `request-stop`（停止标记
  文件）。引擎在文档/阶段边界检查，完成当前不可拆分单元后退出；再次运行即
  从下一阶段恢复，恢复结果与全量运行**逐字节一致**（见 `scripts/demo_resume.sh`）。
- 版本指纹 = `digest(输入指纹 + 阶段名 + 该阶段配置子集)`。任一相关输入或参数
  变化都会定位到**最早受影响阶段**，删除该阶段及全部下游产物并在
  `workspace/fingerprints/invalidation.jsonl` 记录原因；未受影响上游继续复用。
- 并发保护：`fcntl` 文件锁 + 文档内序号连续性校验。落后/重复/跨文档错位发布 →
  `E_STALE_CHECKPOINT`；缺失前置阶段或未知阶段 → `E_INVALID_STAGE`；被拒绝的
  发布不改变任何已有状态。

## 增量更新与一致性校验

```bash
python -m layout_order.cli --workspace workspace update \
  --input workspace/samples/documents.json --doc-id doc_single_zh
python -m layout_order.cli --workspace workspace verify
```

单文档更新只重建失效链路，随后在**临时隔离工作区**做一次从零全量构建，
逐字节比对四个阶段的 `product_digest` 与最终结果；不一致返回
`E_INCREMENTAL_MISMATCH` 并附带差异阶段。

## HTTP 应用（仅标准库，默认 127.0.0.1）

```bash
python -m layout_order.http_app --host 127.0.0.1 --port 8080
# 或一键演示
bash scripts/demo_http.sh 8099
```

端点：`POST /api/samples/import`、`POST /api/analyze`、`POST /api/stop`、
`POST /api/resume`、`GET /api/status[?doc_id=]`、
`GET /api/documents/<id>/sequence|spanning|result`、
`POST /api/documents/<id>/update`、`GET /api/invalidations`、`GET /api/health`。

## 演示与错误注入

```bash
bash scripts/demo_resume.sh                 # 停止-恢复-逐字节比对
python scripts/demo_error_injection.py      # 7 类错误注入，全部命中稳定错误码
bash scripts/demo_http.sh 8099              # HTTP 全流程
```

错误注入参数清单见 `fixtures/error_injections.json`，配置覆盖样例见
`fixtures/config_overrides.json`。

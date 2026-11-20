# 技术设计说明

## 1. 确定性与逐字节一致

- 所有落盘 JSON 经 `hashing.canonical_json`：`sort_keys=True`、
  紧凑分隔符、`ensure_ascii=False`、固定尾随换行，跨平台/跨运行一致。
- NumPy 标量在序列化前递归转 Python 原生类型，避免 `np.float64` 表示差异。
- 像素坐标统一 `round(..., 3)`，归一化坐标 `round(..., 4)`；所有排序都带
  确定性 tie-breaker（`(top, left, box_id)`）。
- 样本生成器使用模块级固定种子 `20261121` 的 `random.Random`，随机数仅用于
  毫米级安全抖动与片段选择，版面结构固定。

## 2. 阶段一：规范化与合并

- 校验先于计算：坐标必须在 `[0,1]`、`x1>x0 且 y1>y0`、`box_id` 唯一，
  违规立即抛 `E_BOX_INVALID`（含 `doc_id` 与 `box_id`）。
- 页眉/页脚：仅小字号档位（`xsmall/small`）且距顶/底边归一化距离 ≤
  `header_footer_margin`（默认 0.08）才归类，其余一律 body。
- 合并：仅在 body 框之间进行；并查集按确定性顺序扫描框对，水平相邻
  （同阅读行、垂直重叠达标、水平间隙 < 阈值）或垂直相邻（同栏、水平重叠
  达标、垂直间隙 < 阈值）合并。合并框 id 为 `M0001...`，保留
  `source_box_ids` 与每对间隙记录，保证可追溯、可复算。

## 3. 阶段二：栏推断

- 有效版面宽度 = body 框最小左边缘到最大右边缘。
- 直方图（`numpy.histogram`，默认 8 箱）同时记录水平起点与宽度分布，作为
  判定依据；中位数框宽比 ≥ `single_column_ratio` 判单栏，否则对候选框水平
  起点做 1-D 聚类（簇间间隔 > `cluster_gap_ratio * 有效宽度`）。
- 相邻簇边界取两簇最大右边缘与最小左边缘的中点，因此栏区间互不重叠且首尾
  恰好覆盖有效版面宽度（`columns[0].x0 == effective.x0`，
  `columns[-1].x1 == effective.x1`）。
- 宽度比 ≥ `spanning_width_ratio` 的框直接标通栏；其余框按最大水平重叠归属
  栏，零重叠则 `E_COLUMN_UNASSIGNED`。证据块记录 `decision` 与
  `voting_box_count`。

## 4. 阶段三：阅读顺序裁决

- 每栏内部构建严格纵向链；通栏块在 `flow` 模式下仅与每列紧邻其上下的栏块
  连边（同高通栏优先），从而保证图是 DAG，不存在环。
- `span_first` / `column_first` 通过整体前置/后置边实现固定优先级。
- Kahn 拓扑排序的就绪键为
  `(通栏优先键, column_index, top, left, block_id)`，跨列并列时稳定地
  自左向右，因此实现「栏内自上而下、栏间自左向右」且无需会成环的整列桥接边。
- 环检测：拓扑输出数 ≠ 节点数时，收集剩余节点及其块位置抛 `E_ORDER_INVALID`；
  空块、重复 `source_box_id` 同样拒绝并禁止写入。
- 置信度：对相邻正文块，垂直间隙分 = `max(0, 1-gap/gap_reference_px)`，
  栏归属分（同栏/跨通栏=1.0，相邻栏=0.8，跨多栏=0.5），按配置权重加权平均。

## 5. 阶段四：汇总与质量

- 展开合并块后核对原始框全覆盖，缺失即 `E_ORDER_INVALID`。
- 顺序质量严格依赖样本 `reference_order`：把每个块映射到其源框在参考序列中的
  最小秩，计算全逆序数、相邻逆序数与
  `order_accuracy = 1 - inversions / pair_total`；无参考序列时
  `reference_available=false` 且指标为 `null`。
- 最终对完整结果计算 SHA-256 `result_digest`，用于跨运行/跨工作区比对。

## 6. 持久化、锁与序号

- 原子写：临时文件 `*.tmp` → `flush/fsync` → `os.replace` → 目录 `fsync`，
  中断只会保留旧记录或完整新记录，不产生半写 JSON。
- 锁：每文档一把 `locks/<doc_id>.lock`，`fcntl.flock` 跨进程互斥；进程内用
  线程本地引用计数支持嵌套（同进程重入不死锁）。
- 序号：阶段序号固定 1..4，发布要求当前最大已发布序号恰为 `seq-1`，且前置
  阶段存在；相同指纹幂等放行，不同指纹的同序号发布拒绝为
  `E_STALE_CHECKPOINT`；`claimed_doc_id` 不一致拒绝为跨文档错位。

## 7. 版本指纹与失效传播

- 阶段指纹 = `digest({input_fingerprint, stage, config: 阶段配置子集})`。
  summary 无独立参数，其指纹随输入与全局 `config_version` 变化。
- 恢复运行时逐阶段比对现有检查点指纹：命中即复用，不命中则通过
  `invalidate_from_locked` 删除该阶段及下游全部记录（在已持有文档锁的路径内
  调用，避免重复放锁），并追加 `invalidation.jsonl` 事件（原因、期望/现存
  指纹、被删阶段）。
- 因为每阶段输入指纹由上游产物内容决定，参数变化天然定位到最早受影响阶段，
  上游未变产物继续复用，旧版本结果不可能被当作新结果返回。

## 8. 业务错误码

| 错误码 | 触发场景 | HTTP |
| --- | --- | --- |
| `E_BOX_INVALID` | 越界坐标、零面积、重复/缺失框标识 | 422 |
| `E_COLUMN_UNASSIGNED` | 无正文框、栏数为零、框无法归属栏 | 422 |
| `E_ORDER_INVALID` | 空块、重复框、裁决成环、覆盖不全 | 422 |
| `E_STALE_CHECKPOINT` | 落后/重复/跨文档错位发布 | 409 |
| `E_INVALID_STAGE` | 缺失前置阶段、非法/未知阶段跳转 | 400 |
| `E_INCREMENTAL_MISMATCH` | 增量与全量构建摘要不一致 | 409 |
| `E_DOC_NOT_FOUND` / `E_NOT_FOUND` | 文档或阶段产物不存在 | 404 |

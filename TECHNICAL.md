# 技术说明

## 1. 模块边界

| 模块 | 职责 |
|---|---|
| `models.py` | `Word`、`ChunkInfo`、`WorkspaceState`、时间轴校验、摘要计算 |
| `formats.py` | JSON/SRT/WebVTT 编解码、迁移、丢失报告 |
| `audio.py` | WAV/清单校验、`TranscriptionAdapter` 协议、确定性参考文本桩 |
| `workspace.py` | 版本发布、差异、合并、快照、恢复、停止排空 |
| `locking.py` | POSIX `flock` / Windows `msvcrt.locking` 互斥 |
| `atomic.py` | 同目录临时文件、fsync、`os.replace` 原子写入 |
| `http_api.py` / `cli.py` | 本地 HTTP 与命令行应用 |
| `demo.py` | 固定种子数据与完整验收演示 |

统一时间轴只使用 `Word`：整数 `start_ms`/`end_ms`、文本、语言、置信度、稳定 `entry_id`、`source_version`、来源 ID 和合并来源版本。更换 `TranscriptionAdapter` 只改变候选词生成，不改变格式、版本或快照语义。

## 2. 格式契约

JSON 为规范格式，包含 `schema_version=2`、`time_unit=ms`、分片元数据、全量词条、配置指纹和时间轴摘要。

SRT/WebVTT 每条词一个 cue。元数据封套形如：

```text
[[WB-META]]{"chunk_id":"chunk-01","confidence":0.9585,"entry_id":"...","language":"zh-CN",...}
```

封套位于 cue 负载中，因此即使经过标准字幕文件路径仍可恢复。解析器将封套从业务文本中剥离。若外部工具移除了封套，导入结果仍合法，但 `loss_report` 会逐 cue 列出默认字段和生成值，不会静默丢失。

往返检查覆盖整条链：规范 JSON → 解码基线 → SRT/WebVTT → 再编码 JSON → 解码最终结果。比较条目全字段以及每个分片的空闲间隙/重叠区间向量，任何变化返回 `E_ROUNDTRIP_MISMATCH`。

## 3. 迁移规则

旧 JSON（schema < 2 或含 `segments`）按以下显式规则升级：

- 秒转毫秒，四舍五入到整数；
- 缺失条目 ID 时生成包含分片、位置和内容摘要的稳定 ID；
- 缺失语言使用分片语言/default；
- 缺失置信度使用 `0.5`；
- 缺失分片时长时仅在可由词条最大结束时间唯一推断时补齐。

每条迁移写入 `migrations`，包含源/目标 schema、规则列表、条目数及导入版本；词条 `source_entry_ids` 保存旧 ID。未知 schema、未知时间单位、不可推断结构或损坏 JSON 返回 `E_INCOMPAT_FORMAT`。

## 4. 版本、差异与合并

每次成功发布严格令 `version = current + 1`。快照保存 `parent_version`、`parent_snapshot_digest`、全量词条和一次性差异记录：

- `changes.added`；
- `changes.deleted`；
- `changes.modified`（before/after 与 `source_version`）。

合并以两版本共同祖先做三方合并。同 ID 分歧、修改与删除交叉、或不同条目时间区间交叉，均形成冲突候选。策略：

- `last-wins`：右侧版本优先，左侧候选完整写入冲突审计；
- `mark-conflict`：保留确定性的左侧候选并在活动条目写 `conflict_id`，右侧候选完整写入审计。

所有合并结果条目记录 `merged_from_versions`，审计记录双方原始条目、解决策略和保留条目 ID。活动时间轴不能包含重叠词，因此 `mark-conflict` 仍选择单一活动候选，但不会丢弃另一候选记录。

`/api/forks` 允许用当前单调版本号发布一个父版本较旧的内容分支，使差异合并能在单工作区内真实覆盖冲突路径；版本号本身不跳号。

## 5. 原子快照与恢复

工作区布局：

```text
snapshot.json                 # 当前一致快照（提交点）
versions/v00000001.json       # 每个已发布版本的自包含快照
.workbench.lock               # 跨进程互斥文件
.stopping.json                # 停止排空标记（存在时拒绝新编辑）
```

快照包含版本、父摘要、输入分片指纹、格式配置指纹、全量时间轴摘要、词条、分片、历史和迁移记录；`snapshot_digest` 覆盖除自身外的全部字段。写入流程为同目录临时文件 → fsync → `os.replace` → 目录 fsync。任意崩溃点都不会留下半写目标文件。

当前快照是提交点，版本归档随后写入。若进程在两者之间被强制终止，下一次读会验证版本连续性和父摘要，并从当前快照修复缺失归档；不会重算已发布版本。

加载时依次校验：

1. 快照自身摘要、时间轴摘要、时间轴合法性；
2. 格式配置指纹与当前配置；
3. 输入 WAV 存在且 SHA-256 未变；
4. 与 `versions/` 最新归档比较。

判定只用快照内部版本/摘要/指纹，不读取文件时间戳。版本低于归档为 `rollback`；同版本摘要不同为 `tamper`；摘要/时间轴不一致为 `summary_mismatch`；输入变化为 `input_fingerprint_mismatch`；仅版本号顺序连续且父摘要存在于归档时才接受推进。所有拒绝路径均为 `E_STALE_SNAPSHOT`，且不覆写被替换文件。

`stop` 先获取与编辑相同的文件锁，因此会等待当前不可拆分编辑完成；写入 `.stopping.json` 后，后续发布返回 `E_STOPPING`。`recover` 验证并加载最新一致快照后清除停止标记。无法捕获的 `SIGKILL` 不生成退出报告，恢复只依赖已落盘快照。

## 6. 并发发布

发布临界区顺序为：获取文件锁 → 重新加载当前快照 → 比较调用方 `base_version` → 在内存中应用并完整校验编辑 → 原子提交快照。落后或重复发布在写盘前返回 `E_STALE_VERSION`，详情包含 `replayable_edits`。文件锁各平台实现：

- POSIX：`fcntl.flock(LOCK_EX | LOCK_NB)`；
- Windows：`msvcrt.locking(LK_NBLCK/LK_UNLCK)`。

测试使用两个独立进程同时以 base version 1 发布；恰好一个得到 version 2，另一个得到 `E_STALE_VERSION`，未出现版本 3 或时间轴损坏，随后可在 version 2 上成功重放右侧编辑。

## 7. 时间轴异常

导入与编辑均在提交前校验：

- 负起点；
- 结束早于开始；
- 零长度；
- 超出分片实际时长；
- 同分片重叠；
- 大于 `max_gap_ms` 的空闲间隙；
- 非单调导入顺序、未知分片、重复条目 ID、非法置信度。

错误统一为 `E_TIMELINE_ERROR`，详情至少含 `chunk_id`、`entry_index`、`entry_id`、`violation`，测试覆盖每种异常并断言版本号与归档没有变化。

## 8. 实测结果

本机实际执行：

- `python -m pytest -q`：42 passed。
- `python -m transcript_workbench --root . demo`：成功生成 `artifacts/demo_summary.json`。
- 演示中的 3 个分片共 6 条 JSON→SRT/WebVTT→JSON 链全部 `exact=true`，无元数据丢失。
- 演示版本历史 1-5 的 `kind` 为 bootstrap/edit/fork/merge/migration，各版本 47 条目。
- 回退、损坏样本、摘要篡改三种快照替换均返回 `E_STALE_SNAPSHOT`；合法版本推进被接受。
- 非法时间轴返回 `E_TIMELINE_ERROR` 且版本保持不变；停止后编辑返回 `E_STOPPING`，恢复回到 version 5。

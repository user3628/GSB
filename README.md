# 本地语音识别分片转写编辑工作台

该工作台从固定种子的本地 WAV 分片出发，由可替换转写适配层生成统一词级时间轴，并提供 SRT、WebVTT、JSON 双向转换、多版本编辑/差异/合并、原子快照、并发发布保护与中断恢复。运行过程不调用 ASR 模型、云服务或任何外部站点。

## 快速开始

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 生成固定种子样本（3 个 WAV、manifest、旧结构 JSON、快照样本等）
.venv/bin/python -m transcript_workbench --root . generate-data

# 完整确定性验收演示，并写出 artifacts/demo_summary.json
.venv/bin/python -m transcript_workbench --root . demo

# 全量测试
.venv/bin/python -m pytest -q
```

辅助脚本：

```bash
scripts/run_demo.sh
scripts/run_tests.sh
scripts/run_http.sh              # 127.0.0.1:8765，stop 请求会触发优雅退出
```

当前验证环境为 Python 3.14.4；项目要求 Python 3.11+。依赖版本固定于 `requirements.txt`。

## 演示数据

`generate-data` 在 `demo_data/` 生成并可重复再生成：

- `audio/chunk-01.wav`、`chunk-02.wav`、`chunk-03.wav`：16 kHz、单声道、16 位 PCM，种子 `20261120`。
- `manifest.json`：分片顺序、参考文本、语言标签和时长。
- `legacy_transcript.json`：schema 0、秒为时间单位、缺少语言/置信度/条目 ID 的旧结构。
- `replacement_snapshot.json`：可用于整体替换工作区 `snapshot.json` 的 version 1 快照。
- `corrupt_snapshot.json`：内容被篡改但保留旧摘要的损坏快照。
- `invalid_timeline.json`：负起点、结束早于开始等非法时间区间。

`demo` 会创建/清理自己的运行目录，将版本摘要、格式往返结果与错误码计数写入 `artifacts/demo_summary.json`。

## CLI 示例

```bash
# 初始化版本 1
.venv/bin/python -m transcript_workbench --workspace artifacts/workspace \
  init --manifest demo_data/manifest.json

# 历史、差异、往返检查
.venv/bin/python -m transcript_workbench --workspace artifacts/workspace versions
.venv/bin/python -m transcript_workbench --workspace artifacts/workspace diff --left 1 --right 2
.venv/bin/python -m transcript_workbench --workspace artifacts/workspace roundtrip --version 2

# 导出三种格式
.venv/bin/python -m transcript_workbench --workspace artifacts/workspace \
  export --format srt --version 2 --out artifacts/version-2.srt

# 原子编辑或导入
.venv/bin/python -m transcript_workbench --workspace artifacts/workspace \
  edit --base-version 2 --ops '[{"op":"delete_word","entry_id":"..."}]'
.venv/bin/python -m transcript_workbench --workspace artifacts/workspace \
  import --format json --file demo_data/legacy_transcript.json --chunk chunk-01 --base-version 2

# 停止排空与恢复
.venv/bin/python -m transcript_workbench --workspace artifacts/workspace stop
.venv/bin/python -m transcript_workbench --workspace artifacts/workspace recover
```

所有业务错误以 JSON 输出到 stderr，包含稳定错误码和定位信息。

## HTTP 接口

```bash
scripts/run_http.sh
```

主要端点：

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/api/health` | 服务与工作区状态 |
| POST | `/api/workspaces/import` | 导入任务清单并创建版本 1 |
| GET | `/api/state` | 当前或指定版本全量时间轴 |
| GET | `/api/versions` | 全部单调版本历史 |
| GET | `/api/versions/{a}/diff/{b}` | 版本差异 |
| POST | `/api/edits` | 乐观并发原子编辑 |
| POST | `/api/forks` | 从旧版本发布分支快照，用于真实冲突合并 |
| POST | `/api/imports` | JSON/SRT/WebVTT 导入与迁移 |
| POST | `/api/exports` | JSON/SRT/WebVTT 导出 |
| POST | `/api/merge` | `last-wins` 或 `mark-conflict` 合并 |
| POST | `/api/roundtrip` | 往返与时隙/重叠一致性校验 |
| POST | `/api/stop` | 排空并写停止标记；脚本启动时随后优雅退出 |
| POST | `/api/recover` | 从最新一致快照恢复 |
| GET | `/api/snapshot` | 快照版本、摘要、指纹和归档列表 |

编辑请求示例：

```json
{
  "base_version": 1,
  "operations": [
    {
      "op": "update_word",
      "entry_id": "w-chunk-01-0000-86b63a1e",
      "text": "大家",
      "confidence": 0.977
    }
  ]
}
```

当另一进程先发布后，落后请求返回 HTTP 409 / `E_STALE_VERSION`，错误详情带 `replayable_edits` 与 `replay_against_version`；调用方可用最新版本号重放，不会产生跳号或半写入时间轴。

## 格式与错误码

- SRT 时间：`HH:MM:SS,mmm`；WebVTT 时间：`HH:MM:SS.mmm`；内部与 JSON 均为整数毫秒。
- 每条词一个 cue，导出 SRT/WebVTT 时在 cue 负载中写入 `[[WB-META]]{...}` 元数据行。导入时该行不进入字幕文本，可无损恢复语言、置信度、条目 ID 与来源版本。
- 第三方字幕没有该元数据时仍可导入，但会返回 `loss_report`，明确列出被默认化的 `confidence`、`language`、生成的 `entry_id` 等字段。
- 业务错误：`E_TIMELINE_ERROR`、`E_INCOMPAT_FORMAT`、`E_STALE_SNAPSHOT`、`E_STALE_VERSION`、`E_STOPPING`、`E_NOT_FOUND`、`E_INVALID_REQUEST`、`E_LOCK_TIMEOUT`。

## 当前验证结果

2026-11-20 在本机虚拟环境实际运行：

```text
42 passed
```

`artifacts/demo_summary.json` 记录固定摘要：3 个分片、版本 1-5 条目数均为 47、6 次格式链检查全部逐字段一致、合并冲突 1 次、旧结构迁移成功，以及 `E_TIMELINE_ERROR=1`、`E_STALE_SNAPSHOT=3`、`E_STOPPING=1`。

更完整的架构、原子性与恢复规则见 [TECHNICAL.md](TECHNICAL.md)。

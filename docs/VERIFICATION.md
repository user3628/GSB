# 实际验证记录

验证时间：2026-11-26（Asia/Shanghai）

环境：

- WSL Ubuntu，Linux amd64
- Go `go version go1.27.4 linux/amd64`；`go.mod` 声明 `go 1.22`
- SQLite CLI `3.46.1`
- 依赖已 vendor，可离线构建

一键命令：

```bash
./scripts/verify.sh
```

最近一次实际输出保存在 `docs/verification-output.txt`，摘要如下：

```text
[1/6] go test ./...
ok  	tm-sync/internal/service
ok  	tm-sync/internal/tm
[2/6] build binary
[3/6] deterministic demo + incremental byte comparison
demo: 12 source segments, 2 expected E_SEGMENT_EMPTY, 2 increments byte-identical
[4/6] SIGKILL recovery byte comparison
recovery: committed checkpoints only; export matches continuous v1 byte-for-byte
recovery: mid-increment SIGKILL matches v2 byte-for-byte
[5/6] concurrent publication mutex
publish: accepted=1 stale=15 other=0
[6/6] HTTP smoke + graceful stop
http: suggestion/reason-chain/status ok; graceful stop ok
VERIFY_OK
```

## 逐字节一致性哈希

一次验证工作目录中的导出 SHA-256：

```text
ddf20b29c979145617d7c2bf992031801c660dfaafdb85ca8c8f40422fa7a1e6  export-v1.json
ddf20b29c979145617d7c2bf992031801c660dfaafdb85ca8c8f40422fa7a1e6  sigkill-recovered-v1.json

98eab12bd6ba16e4049ee998eee2ba2093ee4e3c9f22e33441d7e12a4455fe02  export-v2-incremental.json
98eab12bd6ba16e4049ee998eee2ba2093ee4e3c9f22e33441d7e12a4455fe02  export-v2-full.json
98eab12bd6ba16e4049ee998eee2ba2093ee4e3c9f22e33441d7e12a4455fe02  mid-increment-sigkill-recovered-v2.json

5d6e0095a3abe781198ce8de157dbf41232bfb5d2227030894279678620245a9  export-v3-incremental.json
5d6e0095a3abe781198ce8de157dbf41232bfb5d2227030894279678620245a9  export-v3-full.json
```

以上文件均通过 `cmp` 逐字节比较。

## 已验证业务场景

- 固定种子生成 3 个分段源文件、2 个增量文件和 12 个段。
- `seg-003-empty-target` 与 `seg-008-punct-only` 稳定返回 `E_SEGMENT_EMPTY`，且不产生活动单元。
- “请立即提交。”归并 3 次出现；增量修改 `seg-004` 后，`seg-001`、`seg-004`、`seg-005` 与共享单元/旧建议进入失效集合。
- “请及时保存。”归并 2 次出现；第二个增量同样传播到对端段。
- 增量后的活动建议包含新译文 `Commit.` / `Store.`，出现计数仍分别为 3 / 2。
- 旧建议可通过 HTTP 查询到 `TRANSLATION_CHANGED -> SHARED_UNIT_AFFECTED -> SUGGESTION_INVALIDATED` 原因链。
- 旧 `base_version=1` 重放返回 `E_STALE_BASE`，详情给出 `current_version=3`。
- 旧版本号写入返回 `E_VERSION_ROLLBACK`。
- 同版本不同摘要的版本记录第二次写入被识别为 `whole_replacement`，全部 93 个活动单元失效；重建后仍与规范全量导出逐字节一致。
- 16 个进程竞争同一段内序号 1：1 个接受，15 个返回 `E_STALE_CHECKPOINT`，无通用 `database is locked`。
- 缺前置检查点返回 `E_MISSING_PREREQUISITE`；显式从 `segmentation` 跳到 `merge` 返回 `E_INVALID_STAGE_TRANSITION`，均带 `segment_id`。

## 手工 HTTP 检查示例

```bash
go run ./cmd/tm-sync demo --dir demo-data --db demo-data/tm.db
go run ./cmd/tm-sync serve --db demo-data/tm.db --addr 127.0.0.1:18080

curl -sS 'http://127.0.0.1:18080/api/v1/suggestions?q=%E6%8F%90%E4%BA%A4&invalid=1'
curl -sS 'http://127.0.0.1:18080/api/v1/stages'
curl -sS -X POST 'http://127.0.0.1:18080/api/v1/recover'
curl -sS -X POST 'http://127.0.0.1:18080/api/v1/admin/stop'
```

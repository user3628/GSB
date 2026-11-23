# 实际验证记录

执行日期：2026-11-24（Asia/Shanghai）

环境：

- Go：`go1.27.4 linux/amd64`（模块兼容声明为 Go 1.22）
- SQLite：`modernc.org/sqlite v1.29.10`，CGO 关闭
- 验证数据：`samples/entries.json`
- HTTP/文件测试：本地回环地址与临时目录

## Go 测试

```text
$ go test -count=1 ./...
?   	ciartifacts/cmd/cachectl	[no test files]
ok  	ciartifacts/internal/cache	0.616s
ok  	ciartifacts/internal/httpapi	0.041s
?   	ciartifacts/internal/verify	[no test files]
```

## 竞态测试

```text
$ go test -race -count=1 ./...
?   	ciartifacts/cmd/cachectl	[no test files]
ok  	ciartifacts/internal/cache	2.266s
ok  	ciartifacts/internal/httpapi	1.096s
?   	ciartifacts/internal/verify	[no test files]
```

## 静态检查

```text
$ go vet ./...
(无输出，退出码 0)
```

## Windows 交叉编译

```text
$ GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/cachectl-windows-amd64.exe ./cmd/cachectl
$ ls -lh /tmp/cachectl-windows-amd64.exe
-rwxr-xr-x 1 zsl zsl 18M Nov 24 05:26 /tmp/cachectl-windows-amd64.exe
```

测试二进制也完成 Windows 交叉编译：

```text
$ GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test -c -o /tmp/cache-tests-windows.exe ./internal/cache
-rwxr-xr-x 1 zsl zsl 12M Nov 24 05:31 /tmp/cache-tests-windows.exe
$ GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go test -c -o /tmp/httpapi-tests-windows.exe ./internal/httpapi
-rwxr-xr-x 1 zsl zsl 18M Nov 24 05:33 /tmp/httpapi-tests-windows.exe
```

## 固定样本校验

```text
$ go run ./cmd/cachectl validate-samples --samples samples/entries.json
build sha256:f10ec3ffef16a8dace9b58801e0da28819c98af4c8933410d7e7cacc952f9e9b go1.22.5 bytes=15 digest=sha256:70d5a7f7f762df5aaa93a52f0d5e4058b66682476efef74e9fa05d9347a2f0c1
test sha256:f2ca1bb6c7e907d06dafe4687e579fce76b37e4e93b7605022da52e6ccc26fd2 go1.22.5 bytes=14 digest=sha256:e209f2bd515d8a962aa014c8acaa1d9a9b2cfe542bec8544fcd9bd9b035973af
package sha256:4e9416efcf77b84013f263e1f9008c656861298be5aabc82f458e6e47c60d9ff go1.22.5 bytes=15 digest=sha256:4dafa89060f67ab612419782aec97fe1941cc597f57ca54c905cc0e997fba0f4
lint sha256:d9500972a5c358f30c5bb31ef33cb887fd2994b6f31f643038ead8603f1b0595 go1.22.5 bytes=17 digest=sha256:d3ae45b26611366cbfb477587a8c74f863c78c1392cb144cb57fa0ccda175ad0
recovery sha256:920576a4f78b1146a81b828bd251a42f3b3845dddf480dde3884353e267efec3 go1.22.5 bytes=18 digest=sha256:73a9372462faf445333e909def8cd4508a1bd0d8889a0fbd5c8fc0d19e901ca5
validated 5 entries; batch is ready for import
```

## 端到端 HTTP 验证

```text
$ go run ./cmd/cachectl verify --samples samples/entries.json
[PASS] write-available                  expected=HTTP 201, state=AVAILABLE | actual=HTTP 201, state=AVAILABLE
[PASS] write-then-hit                   expected=HTTP 200, hit=true, hits=1, content=artifact-alpha\n | actual=HTTP 200, hit=true, hits=1, content="artifact-alpha\n"
[PASS] invalid-transition               expected=HTTP 409, E_INVALID_TRANSITION | actual=HTTP 409, E_INVALID_TRANSITION
[PASS] conflicting-write                expected=HTTP 409, E_CONFLICTING_WRITE | actual=HTTP 409, E_CONFLICTING_WRITE
[PASS] digest-mismatch                  expected=HTTP 422, E_DIGEST_MISMATCH | actual=HTTP 422, E_DIGEST_MISMATCH
[PASS] expired-rejected                 expected=HTTP 410, E_ENTRY_EXPIRED | actual=HTTP 410, E_ENTRY_EXPIRED
[PASS] capacity-eviction                expected=HTTP 200, evicted=[build test lint], used<=15, released=46 | actual=HTTP 200, evicted=[build test lint], used=15, released=46
[PASS] evicted-miss-reason              expected=HTTP 404, E_CACHE_MISS, reason=EVICTED | actual=HTTP 404, E_CACHE_MISS, reason=EVICTED
[PASS] snapshot-monotonic               expected=HTTP 409, E_SNAPSHOT_SEQUENCE | actual=HTTP 409, E_SNAPSHOT_SEQUENCE
[PASS] snapshot-switch                  expected=HTTP 200, sequence advanced | actual=HTTP 200, sequence=6
[PASS] forced-kill-write-recovery       expected=state=PENDING, intent rolled back, HTTP report visible, artifact orphan removed | actual=state=PENDING, report_entries=1, orphans=1
[PASS] forced-kill-eviction-recovery    expected=state=AVAILABLE, eviction intent rolled back, HTTP report visible | actual=state=AVAILABLE, report_evictions=1
RESULT 12/12 PASS overall=true
```

其中 `go test` 额外包含：

- 24 路并发命中得到从 1 到 24 的无重复、无跳号统计。
- 16 路同键同内容并发写入仅 1 次非幂等生效。
- 非法样本批次在 SQLite 中写入 0 条记录。
- 摘要校验失败不增加命中统计。
- 非法迁移不改变条目版本与状态。
- 快照校验和、摘要或序号失败时旧快照仍可命中。
- 子进程被 `Process.Kill` 强制终止后，写入与淘汰两种未完成意图分别正确恢复。

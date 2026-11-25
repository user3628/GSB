# Verification Record

Date: 2026-11-26 (Asia/Shanghai)

## Toolchain

```text
$ go version
go version go1.27.4 linux/amd64

$ pkg-config --modversion sqlite3
3.46.1

go.mod:
  go 1.22
  modernc.org/sqlite v1.34.5
```

The driver is pure Go; runtime validation did not require CGO.

## Automated Tests

```text
$ go vet ./...
(no output)

$ CGO_ENABLED=0 go build ./...
(no output)

$ go test -race ./...
?    profilehotspot/cmd/demo        [no test files]
?    profilehotspot/cmd/server      [no test files]
?    profilehotspot/internal/apperr [no test files]
ok   profilehotspot/internal/engine   1.013s
?    profilehotspot/internal/generator [no test files]
ok   profilehotspot/internal/httpapi  1.445s
ok   profilehotspot/internal/model    1.013s
ok   profilehotspot/internal/service  3.176s
?    profilehotspot/internal/storage  [no test files]
```

Covered assertions include:

- invalid weight/empty stack/invalid target rate returns `E_SAMPLE_INVALID` and leaves commit state unchanged;
- a conflicting sample in a two-sample batch inserts neither sample and leaves duplicate counters unchanged;
- duplicate cross-channel observations count once, increment duplicate counters, and keep reservoir size at capacity;
- small service population uses exact path; larger population explicitly switches to approximate;
- every approximate frame's absolute error is at or below the retained-mass bias upper bound, and exact share lies inside the confidence interval;
- in-tolerance rank perturbations preserve ID-based order and top tie conclusion;
- same-base concurrent increments produce exactly one success and one `E_STALE_BASE`;
- policy increment rebuilds reservoir references/hotspots atomically while historical version remains unchanged;
- batch replay does not change duplicate counters; changed payload under the same batch ID returns `E_CONFLICTING_REQUEST`;
- concurrent readers using `/snapshot` observe one consistent commit sequence while writers ingest and rebuild;
- real child processes exit with status 137 both halfway through reservoir replacement and after hotspot aggregation; restart reports one scanned/rolled-back intent each time, SQLite rolls back the open transaction, and retry advances only after recovery.

## One-Command Demo

Command:

```bash
PORT=18082 ./scripts/demo.sh
```

Actual output:

```text
== build ==
== phase 1: exact -> approximate, differences, cascade rebuild ==
policy v1 committed at commit_seq=1
small batch: unique=3 mode(checkout)=exact commit_seq=2
large batch: observations=43 mode(checkout)=approximate selected=32 population=43 bias_bound=0.143078
hotspot conclusion: frame=http.server tie_group=0 tie_size=2 ranking_tolerance_ppm=5000
increment: policy v2 commit_seq=4 rebuilt_reservoirs=4 atomic=true same_tx_boundary=true
state saved to /tmp/tmp.vC5f4iuTr9/demo-state.json
== phase 2: forced termination during reservoir replacement ==
sending batch demo-crash to the forced-termination fault point; expected connection loss
server terminated as planned: Post "http://127.0.0.1:18082/v1/samples": EOF
crash server exit status=137
== phase 3: restart, recovery report, idempotent retry ==
recovery: integrity=ok scanned=1 rolled_back=1 repaired=0 last_commit=4
retry after recovery: batch=demo-crash commit_seq=5 new_samples=12
DEMO_OK
{"recovered_at":"2026-11-25T18:33:28.354866451Z","last_complete_commit_seq":4,"current_policy_version":2,"scanned_intents":1,"rolled_back_intents":1,"repaired_committed_intents":0,"orphans_removed":{"reservoirs":0,"hotspots":0,"samples":0,"batch_items":0,"sample_batches":0,"policies":0,"channel_hits":0},"integrity_check":"ok","rollback_target":"state_meta.commit_seq","complete":true}
version-2 snapshot written to /tmp/tmp.vC5f4iuTr9/version-2-snapshot.json
artifacts: /tmp/tmp.vC5f4iuTr9
DEMO_SCRIPT_OK
```

The demo is deterministic for the fixed seed `42`; only timestamps, temp paths, and elapsed durations vary.

# Verification Record

Date: 2026-11-23 (Asia/Shanghai)

## Environment

```text
Python 3.14.4
NumPy 2.2.1
Pillow 13.0.0
multiprocessing start methods: forkserver, fork, spawn
```

The implementation targets Python 3.10+. The recorded run used Python 3.14.4 on Linux with the explicit `spawn` process context. The same runtime path uses `msvcrt.locking` on Windows.

## Commands executed

```bash
python -m image_workbench generate-demo --out demo
python -m image_workbench plan --workspace demo
python -m image_workbench run --workspace demo --fresh
python -m unittest discover -s tests -v
```

Additional repeat runs used temporary workspaces for sequential comparison, forced worker exit, stop/resume, invalidation and HTTP checks.

## Fixed demo result

Configuration version:

```text
dc048b68a64e22d2c4a4c4873cc77b6d2cc11737b6e77474796489f4551d2626
```

Task/budget outcome:

```text
total=5 accepted=3 verified=3 failed=0 incomplete=0
rejected_chain=1 rejected_budget=1
global reserved steps=30/30
worker slots=10/12, 10/12, 10/12
accepted operator steps=9
```

Reject decisions:

```text
t040 estimated=18, worker_step_quota=12, global_remaining=0
     reason_codes=[E_BUDGET_GLOBAL_EXCEEDED, E_WORKER_QUOTA_EXCEEDED]
t050 operator_position=0, reason=CROP_EMPTY_INTERSECTION, code=E_CHAIN_INVALID
```

Verified output pixel summaries:

```text
t010 size=[48,32] sha256=fe6436189e9b76d2346b6f93f8606334fb9b5393012c73189eeae4a56d293eeb
t020 size=[40,40] sha256=c8df5dfe24d45e1813ffe7010e6a1abc0049af621baeabc7abac3123c7edd2b6
t030 size=[32,32] sha256=6e26a3e1f42f1add5bd58ab07bf7ecd53d650f2ada1fcb9838a32c4eb543f5ec
```

Report hashes:

```text
verification.json sha256=7947cedb4a9c7a6dfe885c7abfec09804aacc7f05b41e0425e8a3458ae5806b0
summary content sha256=45dc629c63d2e970af5d2d62f3a85be2ee20e7fc15c98e91eff3d5135b09f1b6
run_summary.json sha256=7e2f84bcacd4a0dd103a42f1b109f84c348113db8e99e300b5c165e11e377774
```

Checkpoint sequences for accepted tasks:

```text
t010: parsed=1 executed=2 size_checked=3 exported=4
t020: parsed=1 executed=2 size_checked=3 exported=4
t030: parsed=1 executed=2 size_checked=3 exported=4
t040: parsed=1 (budget rejected; no execution)
t050: parsed=1 (chain rejected; no execution)
```

## Parallel versus sequential determinism

A second generated workspace was run with `--sequential`.

```text
verification.json byte-identical: true
schedule.json byte-identical:     true
versions.json byte-identical:     true
run_summary.json byte-identical:  true
t010.png byte-identical:          true
t020.png byte-identical:          true
t030.png byte-identical:          true
```

This holds because final reports omit sequence, reuse, PID and timing data and sort tasks by `task_id`.

## Graceful stop and resume

A sequential run used a 120 ms test-only task delay. After `t010` exported, `control/STOP` was written.

```text
run result: stopped / E_STOPPED
final reports after stop: none
resume result: completed
resumed report hashes equal to clean run: true
resumed checkpoint sequences: 1,2,3,4 for t010/t020/t030
```

## Forced worker exit and resume

A test-only environment hook terminated a worker immediately after its `executed` checkpoint:

```bash
IMAGE_WORKBENCH_TEST_CRASH_AFTER_STAGE=executed \
IMAGE_WORKBENCH_TEST_CRASH_TASK=t010 \
python -m image_workbench run --workspace "$workspace"
```

Observed first run:

```text
status=completed
verified=2, incomplete=1
t010 status=incomplete, error=E_WORKER_PROCESS_EXITED
t010 checkpoint stages: parsed(1), executed(2)
```

Sibling task results remained valid. `resume` completed `t010` with size_checked(3), exported(4); final verification and summary hashes matched the clean run exactly.

## Incremental invalidation

Starting from a completed demo:

1. Changed only `t010.instruction`.
   - earliest stage: `parsed`
   - changed components: `["instruction"]`
   - t010 sequence advanced from `1,2,3,4` to `5,6,7,8`
   - final status remained `verified` because the normalized chain did not change.
2. Changed only `t020.expected_output.width`.
   - earliest stage: `size_checked`
   - changed components: `["expected_output_size"]`
   - t020 parsed stayed at seq 1 and executed stayed at seq 2
   - size_checked advanced from seq 3 to seq 5 (old exported seq 4 became logically invalid)
   - report status: `failed`, error `E_SIZE_MISMATCH`
3. Changed only `global_step_budget` from 30 to 29.
   - earliest stage: `parsed` for all tasks because the budget configuration is part of the parse fingerprint
   - replan accepted `t010,t020`; `t030` became budget-rejected
   - invalidation history retained the config fingerprint change.

The persistent history is queryable with:

```bash
python -m image_workbench versions --workspace demo
```

## Checkpoint publication and recovery tests

The automated suite verifies:

- four concurrent spawned publishers targeting sequence 1 under the same task: exactly one success, three `E_STALE_CHECKPOINT`;
- stale expected sequence rejected with `E_STALE_CHECKPOINT`;
- first published stage `executed` without `parsed` rejected with `E_INVALID_STAGE`;
- missing predecessor artifact rejected with `E_INVALID_STAGE`;
- artifact with another task filename rejected with `E_STALE_CHECKPOINT`;
- corrupt JSON and fabricated trailing records ignored;
- duplicate sequence records ignored;
- repair quarantines bad records and resumes from the last contiguous record;
- a record moved into `checkpoints/_quarantine/` remains available for diagnosis.

## HTTP verification

An independent `python -m image_workbench serve` subprocess was started on an ephemeral loopback port and exercised over real HTTP:

```text
health True
budget accepted/rejected 3 2
staged run partial ['schedule', 'versions']
status t010 next_seq 2
resumed completed {'accepted': 3, 'verified': 3, 'rejected_budget': 1, 'rejected_chain': 1}
verification sha 7947cedb4a9c7a6dfe885c7abfec09804aacc7f05b41e0425e8a3458ae5806b0
summary sha      45dc629c63d2e970af5d2d62f3a85be2ee20e7fc15c98e91eff3d5135b09f1b6
```

The automated suite additionally starts a real `ThreadingHTTPServer` on an ephemeral loopback port and verifies:

```text
GET  /api/health              -> ok
GET  /api/budget              -> 3 accepted, 2 rejected
POST /api/run {until_stage:parsed}
                              -> partial, schedule+versions reports
GET  /api/status              -> t010 parsed checkpoint next_seq=2
POST /api/stop                -> stop_requested=true
POST /api/resume {until_stage:exported}
                              -> completed, 3 verified
GET  /api/reports?name=verification.json
                              -> schema_version=1
POST /api/run {until_stage:bogus}
                              -> HTTP 400, E_INVALID_STAGE
```

## Automated test result

```text
Ran 21 tests in about 2.7s
OK
```

Test groups: deterministic operators and PNG round-trip; chain validation; budget scheduling; checkpoint locking/sequence/recovery; end-to-end parallel/sequential equality; graceful stop/resume; invalidation propagation (instruction, size, digest, input pixels, budget); HTTP API.

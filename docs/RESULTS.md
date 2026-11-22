# Actual Verification Results

## Environment

| Item | Value |
|---|---|
| Execution environment exposed to this run | Ubuntu 26.04 / WSL, Linux x86_64 |
| Target delivery environment | Windows local workspace; PowerShell entry points included |
| Python | `3.11.18` created with `uv` |
| NumPy | `2.2.1` |
| SQLite | Python standard-library `sqlite3`, WAL mode |
| Container/runtime | None |
| External network target | None |

The environment did not initially contain Python 3.11. It was installed locally with `uv python install 3.11`; `.venv` was created from that interpreter. All commands below were run in the mounted workspace without Docker.

## Sample Generation

```text
$ .venv/bin/python scripts/generate_samples.py --clean
{"events": 14400, "files": 13, "ok": true, "out": ".../samples", "records": 48}
```

The generator uses seed `20261123`, 4 service/SLI series, 12 five-minute buckets and 300 events per series/bucket. It writes current v2, legacy v1 and future v3 files in both binary and JSONL formats.

Sample integrity tests regenerate every fixture into a temporary directory and compare bytes:

```text
test_generator_is_byte_for_byte_deterministic ... ok
test_pinned_sha256_values_match_files ... ok
test_pinned_business_codes_are_reproducible ... ok
```

## Automated Tests

Command:

```bash
.venv/bin/python -m unittest discover -s tests -v
```

Observed:

```text
Ran 38 tests in 2.888s

OK
```

Coverage groups:

- binary/JSONL interchangeability and big-endian roundtrip;
- header/record truncation, bad CRC and field-bound offsets;
- legacy conversion and future-required-field rejection;
- floor/ceil/half-up normalization and cumulative error;
- zero total, insufficient samples, fixed decimal formatting and quantiles;
- order-independent query results and short/long divergence;
- unattributable event/snapshot difference;
- conflict overwrite/skip/reject and whole-batch rollback;
- monotonic snapshot sequence and old-state preservation;
- alert persistence and write intents;
- forced process exit followed by restart recovery;
- HTTP success/error envelopes;
- deterministic sample regeneration and demo execution.

## Direct Corrupt-Input Checks

Observed from `codec.decode(...)`:

| File | Result | Offset |
|---|---|---:|
| `corrupt/current-v2-truncated.bin` | `E_RECORD_CORRUPT`: record extends past end of file | `40` |
| `corrupt/current-v2-bad-crc.bin` | `E_RECORD_CORRUPT`: record CRC32 mismatch | `116` |
| `corrupt/current-v2-truncated.jsonl` | `E_RECORD_CORRUPT`: window record is not valid JSON | `10479` |
| `invalid/count-invalid.jsonl` | `E_COUNT_INVALID`: good_events exceeds total_events | `170` |

## HTTP Interoperability Demo

Command:

```bash
.venv/bin/python scripts/demo_interop.py
```

The demo starts local `127.0.0.1` HTTP servers, uses the public JSON API for every operation, and writes `artifacts/demo/demo-report.json`.

Observed flags:

```json
{
  "ok": true,
  "verification": {
    "atomic_batch_rollback": true,
    "baseline_alert": "CRITICAL",
    "binary_jsonl_bidirectional": true,
    "conflict_override_and_skip": true,
    "crash_recovery": true,
    "future_rejected_without_state_change": true,
    "legacy_degraded": true
  }
}
```

### Baseline Values

| Window | Good | Total | Late | SLI | Burn |
|---:|---:|---:|---:|---:|---:|
| 5m | 274 | 300 | 2 | `0.913333` | `8.666667` |
| 30m | 1653 | 1800 | 12 | `0.918333` | `8.166667` |
| 1h | 3310 | 3600 | 25 | `0.919444` | `8.055556` |

Grade: `CRITICAL`. `search-api/latency` grade: `OK`.

### Conflict Results

`merge-overwrite.jsonl`:

```json
{"accepted": 1, "inserted": 0, "overwritten": 1, "rejected": 0, "skipped": 1}
```

`merge-reject.jsonl`:

```json
{
  "status": "ROLLED_BACK",
  "counts": {"accepted": 0, "inserted": 0, "overwritten": 0, "rejected": 1, "skipped": 1}
}
```

The HTTP response was `409 E_IMPORT_CONFLICT`; the active snapshot sequence and record count were identical before and after.

### Roundtrip and Downgrade

- Exported current binary, imported it into a fresh database, and queried: every 5m/30m/1h SLI, burn and grade was identical.
- Exported current JSONL, loaded it into a fresh database, and queried: every SLI, burn and grade was identical.
- Loaded legacy v1 binary and legacy v1 JSONL: both reported `DEGRADE`, both preserved SLI/burn/grade, and both set late count to zero for all 48 records. JSONL v1 cumulative rounding error was `-19023` microseconds.
- Future v3 binary and JSONL returned `E_FORMAT_UNSUPPORTED`; active state was byte-for-byte unchanged.

### Forced-Exit Recovery

The demo invoked the local crash probe, which:

1. committed a pending intent;
2. started an uncommitted transaction that set `record_count=999999`;
3. exited with code `17` without committing.

After server restart:

```json
{
  "action": "ROLLED_BACK_TO_LAST_COMPLETE_VERSION",
  "batch_id": "demo-crash-batch",
  "active_snapshot_sequence": 2026112301,
  "active_record_count": 48
}
```

The active snapshot, 48-row window index and prior activation metadata were preserved.

## Acceptance Summary

| Acceptance item | Result |
|---|---|
| Binary and JSONL bidirectional import/export | Passed |
| Legacy downgrade and future rejection | Passed |
| Interrupted process recovery | Passed |
| Window SLI and budget results match pinned samples | Passed |
| Atomic conflict rollback | Passed |
| Unified JSON errors with business codes | Passed |

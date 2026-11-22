# SLI/SLO Error-Budget Interoperability Toolkit

A deterministic, local-only Python component for SRE budget reviews and alert validation. It accepts time-windowed request events, normalizes window snapshots from two interchangeable formats, calculates 5m/30m/1h SLI and error-budget burn rates, and persists reports, alerts and crash-recovery intents in SQLite.

No Docker, monitoring account, network target or external service is required.

## Delivered Capabilities

- Compact binary snapshot codec: magic, version, endian marker, 40-byte header CRC, per-record length prefix, per-record CRC32 and optional-field bitmap.
- JSON Lines codec: manifest line plus one self-contained record per line with microsecond timestamps.
- Explicit version matrix: v1 degrades, v2 accepts, v3 rejects with `E_FORMAT_UNSUPPORTED` before importing records.
- v1 compatibility: missing `late_count` becomes `0`; relative window-end offsets are converted to absolute milliseconds.
- Microsecond-to-millisecond normalization with caller-selected `floor`, `ceil` or `half_up`, per-record rounding bit and cumulative signed rounding error.
- Deterministic 5m/30m/1h SLI, error-budget burn rate, fixed 6-place `ROUND_HALF_UP` output and zero-total handling.
- Multi-window alert grading only when the short and long windows cross the same threshold. Divergence details are always emitted.
- Cross-source event/snapshot checks; unexplained differences return `E_INCONSISTENT_READINGS` with both readings and deltas.
- Conflict resolution by format version, late count and last-write time. Any rejected row rolls back the whole import batch.
- Whole-snapshot atomic replacement with monotonic sequence validation, locked SQLite commit and memory-index swap.
- Intent journal before every business write; restart recovery marks interrupted batches aborted and preserves the last complete snapshot.
- Local JSON HTTP API, CLI and repeatable HTTP-only interoperability demo.

## Quick Start on Windows

Prerequisites: Python 3.11 installed and available as `py -3.11`, `python3.11`, or passed explicitly.

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\bootstrap.ps1
```

If Python is in a custom location:

```powershell
.\bootstrap.ps1 -PythonExe "C:\Python311\python.exe"
```

The bootstrap creates `.venv`, installs `numpy==2.2.1`, regenerates samples, runs all tests and runs the demo.

Start the local API:

```powershell
.\run_server.ps1 -Db "artifacts\slo-budget.db" -Port 8765
```

## Quick Start on Linux/WSL

```bash
./bootstrap.sh
# or, after activating .venv
.venv/bin/python scripts/generate_samples.py --clean
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python scripts/demo_interop.py
```

## Common CLI Operations

```bash
# Compatibility matrix
.venv/bin/python -m slo_budget.cli compatibility

# Preflight without importing
.venv/bin/python -m slo_budget.cli preflight samples/windows/future-v3-required.bin

# Load a whole snapshot
.venv/bin/python -m slo_budget.cli replace \
  --db artifacts/review.db samples/windows/current-v2.bin --batch-id review-001

# Calculate every declared window plus alert grade
.venv/bin/python -m slo_budget.cli query \
  --db artifacts/review.db --service checkout-api --sli-type availability \
  --as-of-ms 1767229200000 --events-path samples/events/requests.jsonl

# Export both interchangeable formats
.venv/bin/python -m slo_budget.cli export \
  --db artifacts/review.db artifacts/export.bin --format binary
.venv/bin/python -m slo_budget.cli export \
  --db artifacts/review.db artifacts/export.jsonl --format jsonl

# Query recovery state after a restart
.venv/bin/python -m slo_budget.cli recovery --db artifacts/review.db
```

All failures print a JSON object with a stable business code. Format failures also carry a byte `offset`.

## HTTP API

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Local health and active snapshot |
| GET | `/v1/compatibility` | Explicit compatibility matrix and policies |
| POST | `/v1/preflight` | Detect format/version and return `ACCEPT`, `DEGRADE` or `REJECT` |
| POST | `/v1/import` | Atomic record merge with conflict report |
| POST | `/v1/snapshots/replace` | Atomic whole-snapshot replacement |
| POST | `/v1/export` | Export active records as `binary` or `jsonl` |
| GET | `/v1/query` | Multi-window SLI/burn/alert calculation |
| POST | `/v1/analyze` | Calculate directly from a request-event JSONL file |
| GET | `/v1/state` | Active snapshot, counts and recent versions |
| GET | `/v1/windows` | Active normalized records |
| GET | `/v1/imports` | Persisted import/replacement reports |
| GET | `/v1/alerts` | Persisted alert grades |
| GET | `/v1/recovery` | Recovery reports and intent journal |

Example:

```bash
curl -s http://127.0.0.1:8765/v1/sli \
  '?service=checkout-api&sli_type=availability&as_of_ms=1767229200000'
```

Error envelope:

```json
{
  "ok": false,
  "error": {
    "code": "E_RECORD_CORRUPT",
    "message": "record CRC32 mismatch",
    "path": "samples/windows/current-v2.bin",
    "offset": 116,
    "details": {"record_index": 0}
  }
}
```

## Fixed Sample Result

The deterministic samples use seed `20261123`, 14,400 events, 48 five-minute records, and `as_of_ms=1767229200000`. Full hashes and pinned codes are in `samples/samples.json` and `samples/SHA256SUMS`.

| Service / SLI | 5m SLI | 5m burn | 1h SLI | 1h burn | Grade |
|---|---:|---:|---:|---:|---|
| checkout-api / availability | 0.913333 | 8.666667 | 0.919444 | 8.055556 | CRITICAL |
| checkout-api / latency | 0.983333 | 1.666667 | 0.987778 | 1.222222 | WATCH |
| search-api / availability | 1.000000 | 0.000000 | 0.997778 | 0.222222 | OK |
| search-api / latency | 0.993333 | 0.666667 | 0.994444 | 0.555556 | OK |

Pinned outcomes include:

- v1 binary/JSONL: `DEGRADE`
- v2 binary/JSONL: `ACCEPT`
- v3 binary/JSONL: `E_FORMAT_UNSUPPORTED`
- truncated binary/JSONL and bad CRC: `E_RECORD_CORRUPT`
- invalid count sample: `E_COUNT_INVALID`
- ambiguous conflict sample: `E_IMPORT_CONFLICT` with atomic batch rollback
- sequence regression: `E_SNAPSHOT_INVALID`

## Validation

Current run:

```text
Python 3.11.18
numpy 2.2.1
Tests: 38 passed
Demo: binary/JSONL roundtrip, legacy degradation, future rejection,
      override/skip conflict, atomic rollback, forced-exit recovery: all passed
```

See `docs/RESULTS.md` for exact commands and observed values.

## Documentation

- `docs/FORMAT.md` - byte layout, JSONL schema, version rules and normalization.
- `docs/SAMPLES.md` - seed, timeline, checksums, pinned codes and expected metrics.
- `docs/OPERATIONS.md` - SQLite schema, intent protocol, atomic replacement, API and conflict policy.
- `docs/RESULTS.md` - actual validation output and environment notes.

## Project Layout

```text
slo_budget/          core package: codecs, calculator, storage, service, HTTP, CLI
scripts/             deterministic sample generator and HTTP interop demo
tests/               unittest suite
samples/             checked-in deterministic binary/JSONL/event fixtures
docs/                protocol, sample and operations documentation
artifacts/demo/      demo report produced by scripts/demo_interop.py
```

## Design Notes

- The supported deployment model is one active API process per SQLite database. SQLite transactions protect durability; an in-process lock protects the active index and commit/swap sequence.
- Internal timestamps are integer milliseconds. Binary input is already milliseconds; JSONL input is microseconds and must declare a truncation rule.
- Window membership is `[start, end)`. Late tolerance is inclusive at `as_of_ms - timestamp_ms <= late_tolerance_ms`.
- Decimal metrics are emitted as fixed-place strings to avoid float drift and to make cross-run comparison byte-stable.
- The current execution environment is Linux/WSL, while the project and PowerShell entry points are designed for Windows. No OS-specific container or service is used.

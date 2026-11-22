# Deterministic Samples

All fixtures are generated locally by `scripts/generate_samples.py`. There is no network access, monitoring endpoint or account dependency.

## Fixed Inputs

| Parameter | Value |
|---|---|
| Seed | `20261123` |
| Event RNG | NumPy `default_rng(20261123)` |
| Base timestamp | `1767225600000` = `2026-01-01T00:00:00Z` |
| `as_of_ms` | `1767229200000` = `2026-01-01T01:00:00Z` |
| Bucket | `300000` ms |
| Buckets | 12 |
| Services / SLIs | 4 series |
| Events | 14,400 |
| Window records | 48 |
| Events per service/SLI bucket | 300 |
| Snapshot sequences | current `2026112301`, legacy `2026112201`, future `2026112302` |
| Rounding | JSONL timestamps contain 1..999 us remainders; default `floor` recovers binary milliseconds |
| Boundary policy | window `[start,end)`, late tolerance inclusive |

The four series are:

| Service | SLI | Error pattern | Expected 5m grade |
|---|---|---|---|
| `checkout-api` | `availability` | 21..28 errors / 300 per bucket | `CRITICAL` |
| `checkout-api` | `latency` | 3..5 errors / 300 per bucket | `WATCH` |
| `search-api` | `availability` | 0..2 errors / 300 per bucket | `OK` |
| `search-api` | `latency` | 1..3 errors / 300 per bucket | `OK` |

Late counts are deterministic per bucket and are a subset of total events. v1 fixtures intentionally omit `late_count`, so the degraded view reports zero.

## Files and SHA-256

The authoritative machine-readable copy is `samples/samples.json`; the checked-in checksum list is `samples/SHA256SUMS`.

| File | SHA-256 |
|---|---|
| `events/requests.jsonl` | `ca2ae7c555f456f0484b8140d1f9d151fdaa49472ba759f120265c55aae2941d` |
| `windows/current-v2.bin` | `a3f4c4a4a79963a2843ad5bdce6625f7c2715626cf60cd395a4692b2760eb6c3` |
| `windows/current-v2.jsonl` | `363a12fd56eec8b0b1b4e5ded24063f09375e38b1854a5f9910c22f7dd1d6a7e` |
| `windows/legacy-v1.bin` | `42c39f9002b031bacae0cabb09c9764b21bd96c178a060018c6b70c20d1522b8` |
| `windows/legacy-v1.jsonl` | `0237666c8fca4410b647646fbec093e7740f2507dfc73c6fc16897f1baca3df2` |
| `windows/future-v3-required.bin` | `7b8f2ce8461fb00700cfe0e8a0dbf3b7d496d3f07a5f4b1fbc4df2ed85b178a5` |
| `windows/future-v3-required.jsonl` | `88822879489ce82221cfa6b36a360c4589c3a819d84aa882f38da7cce3649604` |
| `corrupt/current-v2-truncated.bin` | `e5ddff32e82fb452a67848bd9b39d74a9d09c3029e0669600eec354fc1586fde` |
| `corrupt/current-v2-bad-crc.bin` | `83938c99c93ae694d5b0f22524e7834e5632a0debc153b87ef2c1dc7f1b7b053` |
| `corrupt/current-v2-truncated.jsonl` | `510b23e2507458860984281d03d03f87c0cb61d9b42efca7970884af6cd8bcb5` |
| `invalid/count-invalid.jsonl` | `3793933d48ce393bb3e6bffdd721f3cc3e55184b72abca0f7f133c75eb50b4bc` |
| `conflicts/merge-overwrite.jsonl` | `f2d458a677bf8fc65349fb30754382f3b741c0a8e1e1b3445fe1a44b7978fefb` |
| `conflicts/merge-reject.jsonl` | `4c0948dc01a9143add1ead29162894ff803a8992edf9b1d78d992ca1eac2666f` |

## Pinned Decisions and Business Codes

| Fixture | Preflight / import decision | Code or outcome |
|---|---|---|
| `current-v2.bin` | `ACCEPT` | 48 records |
| `current-v2.jsonl` | `ACCEPT` | 48 records, sub-ms flooring reported |
| `legacy-v1.bin` | `DEGRADE` | late count `0`, relative time converted |
| `legacy-v1.jsonl` | `DEGRADE` | late count `0`, relative time converted |
| `future-v3-required.bin` | `REJECT` | `E_FORMAT_UNSUPPORTED`; no record imported |
| `future-v3-required.jsonl` | `REJECT` | `E_FORMAT_UNSUPPORTED`; no record imported |
| `current-v2-truncated.bin` | decode failure | `E_RECORD_CORRUPT`, offset `40` |
| `current-v2-bad-crc.bin` | decode failure | `E_RECORD_CORRUPT`, offset `116` |
| `current-v2-truncated.jsonl` | decode failure | `E_RECORD_CORRUPT`, offset `10479` |
| `count-invalid.jsonl` | decode failure | `E_COUNT_INVALID`, offset `170` |
| `merge-overwrite.jsonl` | import success | overwrite `1`, skip `1` |
| `merge-reject.jsonl` | atomic rollback | reject `1`, skip `1`, `E_IMPORT_CONFLICT` |

## Expected Query Values

Default policy: SLO target `0.990000`, minimum samples `100`, windows 5m/30m/1h, six fixed decimal places, `ROUND_HALF_UP`.

| Series | Window | SLI | Burn rate | Grade |
|---|---:|---:|---:|---|
| checkout-api / availability | 5m | `0.913333` | `8.666667` | `CRITICAL` |
| checkout-api / availability | 30m | `0.918333` | `8.166667` | `CRITICAL` |
| checkout-api / availability | 1h | `0.919444` | `8.055556` | `CRITICAL` |
| checkout-api / latency | 5m | `0.983333` | `1.666667` | `WATCH` |
| checkout-api / latency | 30m | `0.987778` | `1.222222` | `WATCH` |
| checkout-api / latency | 1h | `0.987778` | `1.222222` | `WATCH` |
| search-api / availability | 5m | `1.000000` | `0.000000` | `OK` |
| search-api / availability | 1h | `0.997778` | `0.222222` | `OK` |
| search-api / latency | 5m | `0.993333` | `0.666667` | `OK` |
| search-api / latency | 1h | `0.994444` | `0.555556` | `OK` |

`search-api/availability` 5m has zero bad events but 300 total events, so it is not the zero-total case. The zero-total case is covered by tests with no records; it returns deterministic `sli=1.000000`, `burn_rate=0.000000`, `sample_status=NO_DATA`, and `business_code=E_INSUFFICIENT_SAMPLES`.

## Reproduction

```bash
.venv/bin/python scripts/generate_samples.py --clean
(cd samples && sha256sum -c SHA256SUMS --strict)
.venv/bin/python -m unittest tests.test_samples -v
```

`tests/test_samples.py` regenerates all fixtures into a temporary directory and compares every byte, including `samples.json` and `SHA256SUMS`.

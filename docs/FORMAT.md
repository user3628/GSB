# Snapshot Format Specification

## 1. Internal Record

After decoding and normalization every record contains:

| Field | Type | Meaning |
|---|---|---|
| `service` | UTF-8 string | Service identity |
| `sli_type` | UTF-8 string | SLI class, e.g. `availability` |
| `window_start_ms` | int64 | Absolute window start, milliseconds since Unix epoch |
| `window_length_ms` | int64 | Positive window length |
| `good_events` | uint64 | Good event count |
| `total_events` | uint64 | Total event count |
| `late_count` | uint64 | Late-marked event count, subset of total |

Imported records also retain `format_version`, `last_write_ms`, source format and batch metadata for deterministic conflict handling.

## 2. Compact Binary Format

### 2.1 Header (40 bytes)

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | Magic `53 4c 4f 53 4e 41 50 00` (`SLOSNAP\0`) |
| 8 | 1 | Format version |
| 9 | 1 | Endian marker: `0x4c` little, `0x42` big |
| 10 | 2 | Header flags; bit 0 = has required extensions |
| 12 | 8 | Snapshot sequence |
| 20 | 8 | Generated-at milliseconds |
| 28 | 4 | Declared record count |
| 32 | 4 | CRC32 of header bytes `0..31` |
| 36 | 4 | Reserved; must be zero |

All integers after the magic/version/endian marker use the declared endian. A bad endian marker, header CRC, reserved bytes or truncated header returns `E_RECORD_CORRUPT` at the offending offset.

### 2.2 Record Envelope

| Field | Size | Notes |
|---|---:|---|
| Record length | 4 | Bytes after this prefix, including trailing CRC |
| Body | length - 4 | Bitmap + fields |
| CRC32 | 4 | CRC32 of body only |

The length prefix is checked before slicing. Truncation, a length below 4, a record extending beyond the file, or a CRC mismatch returns `E_RECORD_CORRUPT` with the record/CRC offset.

### 2.3 Body Prefix

| Field | Encoding |
|---|---|
| Optional-field bitmap | uint32 |
| Service byte length | uint16 |
| Service | UTF-8 |
| SLI byte length | uint16 |
| SLI type | UTF-8 |

Bitmap bits:

| Bit | Meaning |
|---:|---|
| 0 | `late_count` is present (required in v2) |
| 1 | `last_write_ms` is present |
| 2..30 | Optional extensions unknown to this runtime; rejected as unsupported |
| 31 | Unknown required extension; rejected as unsupported |

Service and SLI byte lengths are bounds-checked against the declared limits before UTF-8 decoding. Oversized fields return `E_RECORD_CORRUPT` with the field offset.

### 2.4 Version 1 Body

After the body prefix:

| Field | Type |
|---|---|
| `window_end_ago_ms` | uint64 |
| `window_length_ms` | uint32 |
| `good_events` | uint64 |
| `total_events` | uint64 |
| optional `late_count` | uint32, if bitmap bit 0 |
| optional `last_write_ms` | uint64, if bitmap bit 1 |

Degradation rule:

```text
window_start_ms = generated_at_ms - window_end_ago_ms - window_length_ms
late_count     = 0 when absent
last_write_ms  = generated_at_ms when absent
```

Each converted record emits a degradation item listing the source values, action and result.

### 2.5 Version 2 Body

| Field | Type |
|---|---|
| `window_start_ms` | uint64 |
| `window_length_ms` | uint32 |
| `good_events` | uint64 |
| `total_events` | uint64 |
| `late_count` | uint32, required |
| optional `last_write_ms` | uint64, if bitmap bit 1 |

### 2.6 Version 3

Version 3 is intentionally unsupported by this runtime. A v3 header flag/version or record bitmap bit 31 causes `E_FORMAT_UNSUPPORTED` before any record is imported. Existing active data is unchanged.

## 3. JSON Lines Format

The first non-empty line is a manifest. Each subsequent non-empty line is one independent window record.

### 3.1 v2 Manifest

```json
{"format":"jsonl","format_version":2,"generated_at_ms":1767229200000,"kind":"snapshot_manifest","record_count":48,"required_extensions":[],"snapshot_sequence":2026112301}
```

### 3.2 v2 Record

```json
{"good_events":274,"kind":"window_record","last_write_ms":1767229200000,"late_count":2,"service":"checkout-api","sli_type":"availability","total_events":300,"window_length_ms":300000,"window_start_us":176722890000123}
```

JSONL window starts are microseconds. Normalization is explicit:

```text
floor:   floor(us / 1000)
ceil:    floor(us / 1000) + (remainder != 0)
half_up: floor((us + 500) / 1000)
```

For each record the decoder reports `source_value`, `normalized_ms`, `remainder_us`, `rounding_bit`, `rounding_error_us` and rule. Batch rounding error is the signed sum `normalized_ms*1000 - source_us`.

### 3.3 v1 Record

```json
{"good_events":274,"kind":"window_record","service":"checkout-api","sli_type":"availability","total_events":300,"window_end_ago_us":299999877,"window_length_ms":300000}
```

`late_count` is absent and becomes zero. Conversion:

```text
absolute_start_us = generated_at_ms*1000 - window_end_ago_us - window_length_ms*1000
window_start_ms   = normalize_us(absolute_start_us, truncation_rule)
```

### 3.4 v3 / Required Extensions

Any non-empty unknown `required_extensions` list, at manifest or record level, rejects the whole file with `E_FORMAT_UNSUPPORTED` before record import.

## 4. Version Matrix

| Version | Binary | JSONL | Result |
|---:|---|---|---|
| 1 | relative time, no late count | relative time, no late count | `DEGRADE` |
| 2 | absolute ms, required late count | absolute us, required late count | `ACCEPT` |
| 3+ | unknown required fields | unknown required extensions | `REJECT` / `E_FORMAT_UNSUPPORTED` |

## 5. Validation and Error Codes

| Condition | Code |
|---|---|
| Truncation, bad CRC, malformed record, oversized service/SLI field | `E_RECORD_CORRUPT` |
| Unsupported version or required extension | `E_FORMAT_UNSUPPORTED` |
| `good_events > total_events`, `late_count > total_events`, invalid length | `E_COUNT_INVALID` |
| Snapshot sequence not strictly increasing | `E_SNAPSHOT_INVALID` |
| Ambiguous conflict with equal tie-breakers | `E_IMPORT_CONFLICT` |

All format-level errors include a zero-based byte `offset`. Here `offset` is required for `E_RECORD_CORRUPT`; the decoders report the most specific available location.

## 6. Boundaries

- Window interval: `[window_start_ms, window_start_ms + window_length_ms)`.
- Late tolerance: `as_of_ms - event_timestamp_ms <= late_tolerance_ms` inclusive.
- A snapshot header must carry format version, snapshot sequence and generated-at time.
- Whole-snapshot replacement requires `incoming_sequence > active_sequence`.

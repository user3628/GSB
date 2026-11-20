"""Statistics snapshot model, validation and versioned loading.

A snapshot is a local JSON file with a version string and a generation
timestamp. Loading validates the whole document; on any violation an
E_SNAPSHOT_INVALID error is raised and the previously loaded snapshot
is kept untouched (atomic replacement, never a mix of old and new).
"""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime

from .errors import SnapshotInvalidError, SnapshotNotLoadedError, SnapshotVersionMismatchError
from .numeric import clamp01, kahan_sum

SNAPSHOT_FORMAT = "qopt-snapshot/1"


def _fail(message, **details):
    raise SnapshotInvalidError(message, details)


def _require(cond, message, **details):
    if not cond:
        _fail(message, **details)


def _is_number(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


class Histogram:
    """Equi-depth histogram over non-null values of one column.

    Bucket i spans (boundaries[i], boundaries[i+1]] (right-closed); the
    first bucket is closed on both ends. Degenerate buckets with
    boundaries[i] == boundaries[i+1] are point masses (heavy hitters).
    Cumulative fractions use Kahan compensated summation so skewed
    bucket counts do not silently lose precision.
    """

    __slots__ = ("boundaries", "counts", "ndv_per_bucket")

    def __init__(self, boundaries, counts, ndv_per_bucket):
        self.boundaries = tuple(boundaries)
        self.counts = tuple(counts)
        self.ndv_per_bucket = tuple(ndv_per_bucket)

    @property
    def total_count(self):
        return kahan_sum(self.counts)

    def cumulative_fractions(self):
        """Cumulative bucket fractions via a running Kahan accumulator."""
        total = self.total_count
        if total <= 0.0:
            return tuple(0.0 for _ in self.counts)
        out = []
        running = 0.0
        compensation = 0.0
        for count in self.counts:
            t = running + count
            if abs(running) >= abs(count):
                compensation += (running - t) + count
            else:
                compensation += (count - t) + running
            running = t
            out.append(clamp01((running + compensation) / total))
        return tuple(out)


class ColumnStats:
    __slots__ = ("name", "ndv", "null_frac", "histogram")

    def __init__(self, name, ndv, null_frac, histogram=None):
        self.name = name
        self.ndv = ndv
        self.null_frac = null_frac
        self.histogram = histogram


class TableStats:
    __slots__ = ("name", "row_count", "pages", "columns")

    def __init__(self, name, row_count, pages, columns):
        self.name = name
        self.row_count = row_count
        self.pages = pages
        self.columns = columns

    def column(self, name):
        return self.columns.get(name)


class Correlation:
    __slots__ = ("table", "columns", "kind", "value")

    def __init__(self, table, columns, kind, value):
        self.table = table
        self.columns = tuple(columns)
        self.kind = kind  # "functional_dependency" | "coefficient"
        self.value = value


class IndexSpec:
    __slots__ = ("name", "table", "columns", "pages")

    def __init__(self, name, table, columns, pages):
        self.name = name
        self.table = table
        self.columns = tuple(columns)
        self.pages = pages


class Snapshot:
    __slots__ = ("version", "generated_at", "tables", "indexes", "correlations", "fingerprint")

    def __init__(self, version, generated_at, tables, indexes, correlations, fingerprint):
        self.version = version
        self.generated_at = generated_at
        self.tables = tables
        self.indexes = indexes
        self.correlations = correlations
        self.fingerprint = fingerprint

    def table(self, name):
        return self.tables.get(name)

    def indexes_for(self, table):
        return tuple(idx for idx in self.indexes if idx.table == table)

    def correlations_for(self, table):
        return tuple(c for c in self.correlations if c.table == table)

    def column_ndv(self, table, column):
        t = self.tables.get(table)
        if t is None:
            return None
        c = t.columns.get(column)
        return None if c is None else c.ndv


def _parse_histogram(table, column, raw):
    ctx = {"table": table, "column": column}
    _require(isinstance(raw, dict), "histogram must be an object", **ctx)
    boundaries = raw.get("boundaries")
    counts = raw.get("counts")
    ndv_per_bucket = raw.get("ndv_per_bucket")
    _require(isinstance(boundaries, list) and all(_is_number(b) for b in boundaries),
             "histogram.boundaries must be a list of numbers", **ctx)
    _require(isinstance(counts, list) and all(_is_number(c) for c in counts),
             "histogram.counts must be a list of numbers", **ctx)
    _require(isinstance(ndv_per_bucket, list)
             and all(isinstance(v, int) and not isinstance(v, bool) for v in ndv_per_bucket),
             "histogram.ndv_per_bucket must be a list of integers", **ctx)
    _require(len(boundaries) == len(counts) + 1,
             "histogram.boundaries must have exactly len(counts)+1 entries",
             boundaries=len(boundaries), counts=len(counts), **ctx)
    _require(len(ndv_per_bucket) == len(counts),
             "histogram.ndv_per_bucket must match len(counts)", **ctx)
    _require(all(boundaries[i] <= boundaries[i + 1] for i in range(len(boundaries) - 1)),
             "histogram.boundaries must be non-decreasing", **ctx)
    _require(all(c >= 0 for c in counts), "histogram.counts must be non-negative", **ctx)
    _require(all(v >= 0 for v in ndv_per_bucket),
             "histogram.ndv_per_bucket must be non-negative", **ctx)
    return Histogram(
        boundaries=tuple(float(b) for b in boundaries),
        counts=tuple(float(c) for c in counts),
        ndv_per_bucket=tuple(int(v) for v in ndv_per_bucket),
    )


def _parse_tables(raw):
    _require(isinstance(raw, dict) and raw, "snapshot.tables must be a non-empty object")
    tables = {}
    for name in sorted(raw):
        spec = raw[name]
        _require(isinstance(spec, dict), "table stats must be an object", table=name)
        row_count = spec.get("row_count")
        pages = spec.get("pages")
        _require(isinstance(row_count, int) and not isinstance(row_count, bool) and row_count >= 0,
                 "table.row_count must be a non-negative integer", table=name)
        _require(isinstance(pages, int) and not isinstance(pages, bool) and pages >= 0,
                 "table.pages must be a non-negative integer", table=name)
        raw_cols = spec.get("columns")
        _require(isinstance(raw_cols, dict) and raw_cols,
                 "table.columns must be a non-empty object", table=name)
        columns = {}
        for col_name in sorted(raw_cols):
            c = raw_cols[col_name]
            _require(isinstance(c, dict), "column stats must be an object",
                     table=name, column=col_name)
            ndv = c.get("ndv")
            null_frac = c.get("null_frac")
            _require(isinstance(ndv, int) and not isinstance(ndv, bool) and ndv >= 0,
                     "column.ndv must be a non-negative integer", table=name, column=col_name)
            _require(_is_number(null_frac) and 0.0 <= null_frac <= 1.0,
                     "column.null_frac must be in [0,1]", table=name, column=col_name)
            hist = c.get("histogram")
            columns[col_name] = ColumnStats(
                name=col_name,
                ndv=int(ndv),
                null_frac=float(null_frac),
                histogram=_parse_histogram(name, col_name, hist) if hist is not None else None,
            )
        tables[name] = TableStats(name=name, row_count=row_count, pages=pages, columns=columns)
    return tables


def _parse_indexes(raw, tables):
    _require(isinstance(raw, list), "snapshot.indexes must be a list")
    seen = set()
    indexes = []
    for spec in raw:
        _require(isinstance(spec, dict), "index spec must be an object")
        name = spec.get("name")
        table = spec.get("table")
        columns = spec.get("columns")
        pages = spec.get("pages")
        _require(isinstance(name, str) and name, "index.name must be a non-empty string")
        _require(name not in seen, "index names must be unique", index=name)
        seen.add(name)
        _require(table in tables, "index.table must reference a known table",
                 index=name, table=table)
        _require(isinstance(columns, list) and columns
                 and all(isinstance(c, str) for c in columns),
                 "index.columns must be a non-empty list of column names", index=name)
        for c in columns:
            _require(c in tables[table].columns,
                     "index column must exist in table statistics",
                     index=name, table=table, column=c)
        _require(isinstance(pages, int) and not isinstance(pages, bool) and pages >= 0,
                 "index.pages must be a non-negative integer", index=name)
        indexes.append(IndexSpec(name=name, table=table, columns=tuple(columns), pages=pages))
    indexes.sort(key=lambda i: i.name)
    return tuple(indexes)


def _parse_correlations(raw, tables):
    _require(isinstance(raw, list), "snapshot.correlations must be a list")
    out = []
    for spec in raw:
        _require(isinstance(spec, dict), "correlation spec must be an object")
        table = spec.get("table")
        columns = spec.get("columns")
        kind = spec.get("kind")
        value = spec.get("value")
        _require(table in tables, "correlation.table must reference a known table", table=table)
        _require(isinstance(columns, list) and len(columns) >= 2
                 and all(isinstance(c, str) for c in columns),
                 "correlation.columns must list at least two columns", table=table)
        for c in columns:
            _require(c in tables[table].columns,
                     "correlation column must exist in table statistics",
                     table=table, column=c)
        _require(kind in ("functional_dependency", "coefficient"),
                 "correlation.kind must be 'functional_dependency' or 'coefficient'",
                 table=table)
        _require(_is_number(value) and 0.0 <= value <= 1.0,
                 "correlation.value must be in [0,1]", table=table)
        out.append(Correlation(table=table, columns=tuple(columns),
                               kind=kind, value=float(value)))
    out.sort(key=lambda c: (c.table, c.columns, c.kind))
    return tuple(out)


def parse_snapshot(doc):
    """Validate a decoded JSON document and build an immutable Snapshot."""
    _require(isinstance(doc, dict), "snapshot root must be a JSON object")
    _require(doc.get("format") == SNAPSHOT_FORMAT,
             "unsupported snapshot format", expected=SNAPSHOT_FORMAT, got=doc.get("format"))
    version = doc.get("version")
    generated_at = doc.get("generated_at")
    _require(isinstance(version, str) and version, "snapshot.version must be a non-empty string")
    _require(isinstance(generated_at, str), "snapshot.generated_at must be an ISO-8601 string")
    try:
        datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    except ValueError:
        _fail("snapshot.generated_at is not valid ISO-8601", generated_at=generated_at)
    tables = _parse_tables(doc.get("tables"))
    indexes = _parse_indexes(doc.get("indexes", []), tables)
    correlations = _parse_correlations(doc.get("correlations", []), tables)
    fingerprint = hashlib.sha256(
        json.dumps(doc, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return Snapshot(
        version=version,
        generated_at=generated_at,
        tables=tables,
        indexes=indexes,
        correlations=correlations,
        fingerprint=fingerprint,
    )


def load_snapshot(path):
    """Read and validate a snapshot file; raises E_SNAPSHOT_INVALID on failure."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
    except FileNotFoundError:
        _fail("snapshot file not found", path=str(path))
    except json.JSONDecodeError as exc:
        _fail("snapshot file is not valid JSON", path=str(path), error=str(exc))
    return parse_snapshot(doc)


class SnapshotRegistry:
    """Holds the currently active snapshot with atomic version replacement."""

    def __init__(self):
        self._current = None

    @property
    def current(self):
        return self._current

    def load(self, path):
        """Validate the new snapshot fully, then swap it in atomically.

        If validation fails the previously active snapshot is preserved,
        so old and new statistics are never mixed.
        """
        snapshot = load_snapshot(path)
        self._current = snapshot
        return snapshot

    def require(self, version=None):
        if self._current is None:
            raise SnapshotNotLoadedError("no statistics snapshot is loaded")
        if version is not None and version != self._current.version:
            raise SnapshotVersionMismatchError(
                "requested snapshot version does not match the loaded snapshot",
                details={"requested": version, "loaded": self._current.version},
            )
        return self._current

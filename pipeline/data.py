from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import E_INVALID_CONFIG, PipelineError
from .util import read_json, read_jsonl, sha256_file


@dataclass(frozen=True)
class BatchSpec:
    batch_id: str
    batch_no: int
    tier: str
    relative_path: str
    path: Path
    row_count: int
    declared_labels: tuple[str, ...]
    languages: tuple[str, ...]
    channels: tuple[str, ...]
    content_digest: str

    def metadata(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "batch_no": self.batch_no,
            "tier": self.tier,
            "path": self.relative_path,
            "row_count": self.row_count,
            "labels": list(self.declared_labels),
            "languages": list(self.languages),
            "channels": list(self.channels),
            "content_digest": self.content_digest,
        }


@dataclass(frozen=True)
class Dataset:
    root: Path
    index_path: Path
    seed: int
    batches: tuple[BatchSpec, ...]

    @property
    def languages(self) -> tuple[str, ...]:
        return tuple(sorted({value for batch in self.batches for value in batch.languages}))

    @property
    def channels(self) -> tuple[str, ...]:
        return tuple(sorted({value for batch in self.batches for value in batch.channels}))

    @property
    def labels(self) -> tuple[str, ...]:
        return tuple(sorted({value for batch in self.batches for value in batch.declared_labels}))


def _string_list(value: Any, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise PipelineError(E_INVALID_CONFIG, f"{field} must be a list of non-empty strings")
    return tuple(sorted(set(value)))


def load_dataset(index_path: Path | str) -> Dataset:
    index_file = Path(index_path).resolve()
    try:
        raw = read_json(index_file)
    except Exception as exc:
        raise PipelineError(E_INVALID_CONFIG, "unable to read dataset index", {"path": str(index_file), "reason": str(exc)}) from exc
    if not isinstance(raw, dict) or not isinstance(raw.get("batches"), list):
        raise PipelineError(E_INVALID_CONFIG, "dataset index must contain a batches list")
    root = index_file.parent
    seed = raw.get("seed")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise PipelineError(E_INVALID_CONFIG, "dataset seed must be an integer")
    batches: list[BatchSpec] = []
    seen_ids: set[str] = set()
    seen_numbers: set[int] = set()
    for item in raw["batches"]:
        if not isinstance(item, dict):
            raise PipelineError(E_INVALID_CONFIG, "batch metadata must be an object")
        batch_id = item.get("batch_id")
        batch_no = item.get("batch_no")
        tier = item.get("tier")
        relative_path = item.get("path")
        if not isinstance(batch_id, str) or not batch_id:
            raise PipelineError(E_INVALID_CONFIG, "batch_id must be a non-empty string")
        if batch_id in seen_ids:
            raise PipelineError(E_INVALID_CONFIG, "duplicate batch_id", {"batch_id": batch_id})
        if isinstance(batch_no, bool) or not isinstance(batch_no, int) or batch_no < 1:
            raise PipelineError(E_INVALID_CONFIG, "batch_no must be a positive integer", {"batch_id": batch_id})
        if batch_no in seen_numbers:
            raise PipelineError(E_INVALID_CONFIG, "duplicate batch_no", {"batch_no": batch_no})
        if not isinstance(tier, str) or not tier:
            raise PipelineError(E_INVALID_CONFIG, "tier must be a non-empty string", {"batch_id": batch_id})
        if not isinstance(relative_path, str) or not relative_path:
            raise PipelineError(E_INVALID_CONFIG, "batch path must be a non-empty string", {"batch_id": batch_id})
        batch_path = (root / relative_path).resolve()
        if root not in batch_path.parents and batch_path != root:
            raise PipelineError(E_INVALID_CONFIG, "batch path escapes dataset root", {"batch_id": batch_id})
        if not batch_path.is_file():
            raise PipelineError(E_INVALID_CONFIG, "batch file is missing", {"batch_id": batch_id, "path": str(batch_path)})
        declared_labels = _string_list(item.get("labels", []), f"{batch_id}.labels")
        languages = _string_list(item.get("languages", []), f"{batch_id}.languages")
        channels = _string_list(item.get("channels", []), f"{batch_id}.channels")
        row_count = item.get("row_count")
        if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
            raise PipelineError(E_INVALID_CONFIG, "row_count must be a non-negative integer", {"batch_id": batch_id})
        batches.append(
            BatchSpec(
                batch_id=batch_id,
                batch_no=batch_no,
                tier=tier,
                relative_path=relative_path,
                path=batch_path,
                row_count=row_count,
                declared_labels=declared_labels,
                languages=languages,
                channels=channels,
                content_digest=str(item.get("content_digest", "")),
            )
        )
        seen_ids.add(batch_id)
        seen_numbers.add(batch_no)
    batches.sort(key=lambda batch: (batch.batch_no, batch.batch_id))
    actual_numbers = [batch.batch_no for batch in batches]
    if actual_numbers != list(range(1, len(batches) + 1)):
        raise PipelineError(E_INVALID_CONFIG, "batch_no values must be contiguous starting at 1", {"batch_numbers": actual_numbers})
    return Dataset(root=root, index_path=index_file, seed=seed, batches=tuple(batches))


def load_batch_documents(batch: BatchSpec) -> list[dict[str, Any]]:
    rows = read_jsonl(batch.path)
    if len(rows) != batch.row_count:
        raise PipelineError(
            E_INVALID_CONFIG,
            "batch row count differs from index estimate",
            {"batch_id": batch.batch_id, "estimated": batch.row_count, "actual": len(rows)},
        )
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise PipelineError(E_INVALID_CONFIG, "batch row must be an object", {"batch_id": batch.batch_id})
        doc_id = row.get("doc_id")
        if not isinstance(doc_id, str) or not doc_id:
            raise PipelineError(E_INVALID_CONFIG, "doc_id must be a non-empty string", {"batch_id": batch.batch_id})
        if doc_id in seen:
            raise PipelineError(E_INVALID_CONFIG, "duplicate doc_id in batch", {"batch_id": batch.batch_id, "doc_id": doc_id})
        seen.add(doc_id)
        enriched = dict(row)
        enriched["batch_id"] = batch.batch_id
        if "text" not in enriched and "body" in enriched:
            enriched["text"] = enriched["body"]
        enriched.setdefault("text", "")
        if "lang" not in enriched and "language" in enriched:
            enriched["lang"] = enriched["language"]
        if "channel" not in enriched and "source_channel" in enriched:
            enriched["channel"] = enriched["source_channel"]
        if "channel" not in enriched and "source" in enriched:
            enriched["channel"] = enriched["source"]
        if "label" not in enriched and "category" in enriched:
            enriched["label"] = enriched["category"]
        if "risk_level" not in enriched and "risk" in enriched:
            enriched["risk_level"] = enriched["risk"]
        if not isinstance(enriched["text"], str):
            raise PipelineError(E_INVALID_CONFIG, "text must be a string", {"batch_id": batch.batch_id, "doc_id": doc_id})
        output.append(enriched)
    return sorted(output, key=lambda row: row["doc_id"])


def actual_batch_digest(batch: BatchSpec) -> str:
    return sha256_file(batch.path)


def dataset_manifest(dataset: Dataset, config_digest: str) -> dict[str, Any]:
    return {
        "seed": dataset.seed,
        "config_digest": config_digest,
        "batches": [batch.metadata() for batch in dataset.batches],
    }

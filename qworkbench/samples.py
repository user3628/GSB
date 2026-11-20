from __future__ import annotations

import copy
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .errors import BusinessError
from .util import (
    atomic_write_json,
    canonical_json,
    read_json,
    read_jsonl,
    sha256_json,
    write_jsonl_atomic,
)


REQUIRED_TEXT_FIELDS = ("instruction", "input", "output", "source", "lang")
DATASET_SPEC_FILE = "dataset_spec.json"
IMPORT_DIR = "imported"


def _invalid(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_INVALID_INPUT", message, details)


def _validate_sample(raw: Any, location: str) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise _invalid("sample must be an object", location=location)
    sample = copy.deepcopy(dict(raw))
    sample_id = sample.get("id")
    if not isinstance(sample_id, str) or not sample_id:
        raise _invalid("sample.id must be a non-empty string", location=location)
    for field in REQUIRED_TEXT_FIELDS:
        if field not in sample or not isinstance(sample[field], str):
            raise _invalid(f"sample.{field} must be a string", sample_id=sample_id, location=location)
    annotation_key = None
    if "annotation_score" in sample:
        annotation_key = "annotation_score"
    elif "quality_annotation_score" in sample:
        annotation_key = "quality_annotation_score"
    if annotation_key is not None:
        value = sample[annotation_key]
        if value is None:
            sample.pop(annotation_key, None)
        elif isinstance(value, bool) or not isinstance(value, (int, float)):
            raise _invalid("annotation score must be numeric or null", sample_id=sample_id)
        elif not math.isfinite(float(value)):
            raise _invalid("annotation score must be finite", sample_id=sample_id)
        else:
            sample["annotation_score"] = float(value)
            if annotation_key != "annotation_score":
                sample.pop(annotation_key, None)
    return sample


def validate_samples(raw_samples: Iterable[Any]) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(raw_samples):
        sample = _validate_sample(raw, f"sample[{index}]")
        sample_id = sample["id"]
        if sample_id in seen:
            raise _invalid("duplicate sample id across imported data", sample_id=sample_id)
        seen.add(sample_id)
        samples.append(sample)
    samples.sort(key=lambda item: item["id"])
    return samples


def validate_annotations(raw_annotations: Any) -> dict[str, float]:
    annotations: dict[str, float] = {}
    entries: Iterable[Any]
    if raw_annotations is None:
        return annotations
    if isinstance(raw_annotations, Mapping):
        entries = ({"id": key, "score": value} for key, value in raw_annotations.items())
    elif isinstance(raw_annotations, Sequence) and not isinstance(raw_annotations, (str, bytes)):
        entries = raw_annotations
    else:
        raise _invalid("annotations must be an object or an array of {id,score}")
    for index, entry in enumerate(entries):
        if not isinstance(entry, Mapping):
            raise _invalid("annotation entry must be an object", index=index)
        sample_id = entry.get("id")
        if not isinstance(sample_id, str) or not sample_id:
            raise _invalid("annotation.id must be a non-empty string", index=index)
        score = entry.get("score")
        if isinstance(score, bool) or not isinstance(score, (int, float)):
            raise _invalid("annotation.score must be numeric", sample_id=sample_id)
        value = float(score)
        if not math.isfinite(value):
            raise _invalid("annotation.score must be finite", sample_id=sample_id)
        if sample_id in annotations and annotations[sample_id] != value:
            raise _invalid("conflicting annotation for sample", sample_id=sample_id)
        annotations[sample_id] = value
    return annotations


@dataclass
class SampleDataset:
    samples: list[dict[str, Any]]
    annotations: dict[str, float]
    files: list[Path]
    annotation_file: Path | None
    file_manifest: list[dict[str, Any]]
    sample_fingerprint: str
    annotation_fingerprint: str

    @classmethod
    def load(
        cls,
        files: Sequence[Path | str],
        annotation_file: Path | str | None = None,
    ) -> "SampleDataset":
        paths = [Path(path).resolve() for path in files]
        if not paths:
            raise _invalid("at least one JSONL sample file is required")
        missing = [str(path) for path in paths if not path.exists()]
        if missing:
            raise _invalid("sample file does not exist", missing=missing)
        paths.sort(key=lambda path: str(path))
        raw_samples: list[dict[str, Any]] = []
        file_manifest: list[dict[str, Any]] = []
        for path in paths:
            try:
                records = read_jsonl(path)
            except (OSError, ValueError) as exc:
                raise _invalid("unable to read sample JSONL", path=str(path), reason=str(exc)) from exc
            raw_samples.extend(records)
            file_manifest.append(
                {
                    "name": path.name,
                    "path": str(path),
                    "records": len(records),
                    "sha256": sha256_json(records),
                }
            )
        samples = validate_samples(raw_samples)
        annotation_path = Path(annotation_file).resolve() if annotation_file else None
        annotations: dict[str, float] = {}
        if annotation_path is not None:
            if not annotation_path.exists():
                raise _invalid("annotation file does not exist", path=str(annotation_path))
            try:
                annotations = validate_annotations(read_jsonl(annotation_path))
            except (OSError, ValueError) as exc:
                raise _invalid(
                    "unable to read annotation JSONL",
                    path=str(annotation_path),
                    reason=str(exc),
                ) from exc
        else:
            embedded = [sample["annotation_score"] for sample in samples if "annotation_score" in sample]
            if embedded:
                annotations = {
                    sample["id"]: sample["annotation_score"]
                    for sample in samples
                    if "annotation_score" in sample
                }
        sample_fingerprint = sha256_json(
            [
                {
                    key: sample.get(key)
                    for key in ("id", "instruction", "input", "output", "source", "lang")
                }
                for sample in samples
            ]
        )
        annotation_fingerprint = sha256_json(
            [{"id": key, "score": annotations[key]} for key in sorted(annotations)]
        )
        return cls(
            samples=samples,
            annotations=annotations,
            files=paths,
            annotation_file=annotation_path,
            file_manifest=file_manifest,
            sample_fingerprint=sample_fingerprint,
            annotation_fingerprint=annotation_fingerprint,
        )

    def summary(self) -> dict[str, Any]:
        sources: dict[str, int] = {}
        langs: dict[str, int] = {}
        for sample in self.samples:
            sources[sample["source"]] = sources.get(sample["source"], 0) + 1
            langs[sample["lang"]] = langs.get(sample["lang"], 0) + 1
        return {
            "samples": len(self.samples),
            "annotations": len(self.annotations),
            "files": self.file_manifest,
            "annotation_file": str(self.annotation_file) if self.annotation_file else None,
            "sample_fingerprint": self.sample_fingerprint,
            "annotation_fingerprint": self.annotation_fingerprint,
            "by_source": dict(sorted(sources.items())),
            "by_lang": dict(sorted(langs.items())),
        }


def write_imported_dataset(
    workdir: Path,
    samples: Iterable[Any],
    annotations: Any = None,
) -> dict[str, Any]:
    valid_samples = validate_samples(samples)
    valid_annotations = validate_annotations(annotations)
    import_dir = workdir / IMPORT_DIR
    import_dir.mkdir(parents=True, exist_ok=True)
    shard_count = max(1, (len(valid_samples) + 19) // 20)
    sample_files: list[str] = []
    per_shard = (len(valid_samples) + shard_count - 1) // shard_count if valid_samples else 0
    for index in range(shard_count):
        start = index * per_shard
        chunk = valid_samples[start : start + per_shard]
        path = import_dir / f"shard-{index + 1:02d}.jsonl"
        write_jsonl_atomic(path, chunk)
        sample_files.append(str(path.relative_to(workdir)))
    annotation_rel: str | None = None
    if valid_annotations:
        annotation_path = import_dir / "annotations.jsonl"
        records = [
            {"id": sample_id, "score": valid_annotations[sample_id]}
            for sample_id in sorted(valid_annotations)
        ]
        write_jsonl_atomic(annotation_path, records)
        annotation_rel = str(annotation_path.relative_to(workdir))
    spec = {
        "schema_version": 1,
        "sample_files": sample_files,
        "annotation_file": annotation_rel,
        "samples": len(valid_samples),
    }
    atomic_write_json(workdir / DATASET_SPEC_FILE, spec)
    dataset = SampleDataset.load(
        [workdir / path for path in sample_files],
        workdir / annotation_rel if annotation_rel else None,
    )
    return {**spec, "summary": dataset.summary()}


def load_imported_spec(workdir: Path) -> tuple[list[Path], Path | None] | None:
    spec_path = workdir / DATASET_SPEC_FILE
    if not spec_path.exists():
        return None
    spec = read_json(spec_path)
    files = [workdir / item for item in spec.get("sample_files", [])]
    annotation = workdir / spec["annotation_file"] if spec.get("annotation_file") else None
    return files, annotation


def discover_data_dir(data_dir: Path) -> tuple[list[Path], Path | None]:
    files = sorted(data_dir.glob("*.jsonl"))
    files = [path for path in files if path.name != "annotations.jsonl"]
    annotation = data_dir / "annotations.jsonl"
    return files, annotation if annotation.exists() else None


def dataset_from_directory(data_dir: Path) -> SampleDataset:
    files, annotation = discover_data_dir(data_dir)
    return SampleDataset.load(files, annotation)


def canonical_sample_line(sample: Mapping[str, Any]) -> str:
    return canonical_json(sample)

"""Local JSONL sample loading and atomic incremental input updates."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .errors import EngineError
from .util import (
    atomic_write_json,
    digest_file,
    digest_json,
    read_json,
    stable_sorted_records,
)


@dataclass(frozen=True)
class InputBundle:
    records: list[dict[str, Any]]
    shard_files: list[str]
    shards_digest: str
    replacements_digest: str
    samples_digest: str
    answers_digest: str
    source_snapshot: list[dict[str, Any]]
    answer_overrides: dict[str, dict[str, str]]
    benchmarks: tuple[str, ...]
    models: tuple[str, ...]
    batches: tuple[str, ...]
    evidence_samples: int

    @property
    def stage1_input_digest(self) -> str:
        return digest_json(
            {
                "shards": self.shards_digest,
                "replacements": self.replacements_digest,
                "effective_samples": self.samples_digest,
                "answers": self.answers_digest,
            }
        )


def _error(message: str, **details: Any) -> EngineError:
    return EngineError("E_INVALID_STAGE", message, details=details)


def _require_text(record: dict[str, Any], field: str, aliases: Iterable[str] = ()) -> str:
    for key in (field, *aliases):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value
    raise _error(f"sample record is missing required field {field}", field=field)


def normalize_record(raw: dict[str, Any], *, source: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise _error("each sample record must be a JSON object", source=source)
    run_id = _require_text(raw, "run_id")
    model_id = _require_text(raw, "model_id")
    benchmark_id = _require_text(raw, "benchmark_id")
    sample_id = _require_text(raw, "sample_id")
    source_batch = _require_text(raw, "source_batch", ("batch_id", "batch"))
    output = raw.get("output")
    if output is None:
        output = raw.get("raw_output")
    if output is None:
        output = raw.get("output_text")
    if output is None:
        output = raw.get("prediction")
    if output is None:
        output = ""
    if not isinstance(output, str):
        output = str(output)
    reference = raw.get("reference_answer")
    if reference is None:
        reference = raw.get("reference")
    if reference is None:
        reference = raw.get("answer")
    if reference is not None and not isinstance(reference, str):
        reference = str(reference)
    language = raw.get("language", raw.get("lang", ""))
    if not isinstance(language, str):
        language = str(language)
    question_type = raw.get(
        "question_type", raw.get("task_type", raw.get("type", "unknown"))
    )
    if not isinstance(question_type, str):
        question_type = str(question_type)
    normalized = {
        "run_id": run_id,
        "model_id": model_id,
        "benchmark_id": benchmark_id,
        "sample_id": sample_id,
        "output": output,
        "reference_answer": reference,
        "language": language,
        "source_batch": source_batch,
        "question_type": question_type,
    }
    normalized["record_key"] = digest_json(
        [run_id, model_id, benchmark_id, sample_id]
    )
    return normalized


def _read_shards(samples_dir: Path) -> tuple[list[dict[str, Any]], list[Path], str]:
    if not samples_dir.exists():
        raise _error("samples directory is missing", path=str(samples_dir))
    shard_paths = sorted(samples_dir.glob("*.jsonl"), key=lambda path: path.name)
    if not shard_paths:
        raise _error("no JSONL sample shards were found", path=str(samples_dir))
    records: list[dict[str, Any]] = []
    shard_digests: list[dict[str, str]] = []
    for path in shard_paths:
        try:
            data = path.read_bytes()
        except OSError as exc:
            raise _error("unable to read sample shard", path=str(path), error=str(exc))
        shard_digests.append({"name": path.name, "digest": digest_file(path)})
        for line_number, raw_line in enumerate(data.decode("utf-8").splitlines(), 1):
            if not raw_line.strip():
                continue
            import json

            try:
                raw = json.loads(raw_line)
            except ValueError as exc:
                raise _error(
                    "sample shard contains invalid JSON",
                    path=str(path),
                    line=line_number,
                    error=str(exc),
                ) from exc
            records.append(
                normalize_record(raw, source=f"{path.name}:{line_number}")
            )
    return records, shard_paths, digest_json(shard_digests)


def _read_overrides(
    path: Path,
) -> tuple[dict[str, list[dict[str, Any]]], str]:
    if not path.exists():
        return {}, digest_json({"version": 1, "batches": {}})
    try:
        raw = read_json(path)
    except (OSError, ValueError) as exc:
        raise _error("batch replacement file is invalid", path=str(path), error=str(exc))
    if not isinstance(raw, dict) or not isinstance(raw.get("batches", {}), dict):
        raise _error("batch replacement file must contain a batches object", path=str(path))
    batches: dict[str, list[dict[str, Any]]] = {}
    for batch_id, records in raw.get("batches", {}).items():
        if not isinstance(batch_id, str) or not batch_id:
            raise _error("batch replacement keys must be non-empty strings")
        if not isinstance(records, list):
            raise _error(
                "batch replacement entries must be record lists", batch_id=batch_id
            )
        normalized: list[dict[str, Any]] = []
        for index, record in enumerate(records):
            item = normalize_record(record, source=f"replacement:{batch_id}:{index}")
            if item["source_batch"] != batch_id:
                raise _error(
                    "replacement record source_batch does not match its batch key",
                    batch_id=batch_id,
                    sample_id=item["sample_id"],
                )
            normalized.append(item)
        batches[batch_id] = normalized
    return batches, digest_json(raw)


def _assemble_records(
    raw_records: list[dict[str, Any]],
    replacements: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    raw_batches = {record["source_batch"] for record in raw_records}
    missing_batches = sorted(set(replacements) - raw_batches)
    if missing_batches:
        raise _error(
            "batch replacement references a batch that does not exist",
            batches=missing_batches,
        )
    effective = [
        record
        for record in raw_records
        if record["source_batch"] not in replacements
    ]
    for batch_id in sorted(replacements):
        effective.extend(replacements[batch_id])
    return stable_sorted_records(effective)


def _source_snapshot(record: dict[str, Any], answer: str | None) -> dict[str, Any]:
    return {
        "record_key": record["record_key"],
        "run_id": record["run_id"],
        "model_id": record["model_id"],
        "benchmark_id": record["benchmark_id"],
        "sample_id": record["sample_id"],
        "output_digest": digest_json(record["output"]),
        "reference_answer_digest": digest_json(answer),
        "has_reference_answer": answer is not None,
        "language": record["language"],
        "question_type": record["question_type"],
        "source_batch": record["source_batch"],
    }


def assemble_inputs(
    raw_records: list[dict[str, Any]],
    replacements: dict[str, list[dict[str, Any]]],
    answer_overrides: dict[str, dict[str, str]],
    *,
    shard_files: list[str],
    shards_digest: str,
    replacements_digest: str,
) -> InputBundle:
    records = _assemble_records(raw_records, replacements)
    if not records:
        raise _error("the effective sample set is empty")

    seen_keys: set[str] = set()
    references_by_sample: dict[tuple[str, str], list[str | None]] = {}
    benchmarks = sorted({record["benchmark_id"] for record in records})
    benchmark_set = set(benchmarks)
    for benchmark_id, mapping in answer_overrides.items():
        if benchmark_id not in benchmark_set:
            raise _error(
                "answer update references an unknown benchmark",
                benchmark_id=benchmark_id,
            )
        if not isinstance(mapping, dict):
            raise _error(
                "benchmark answer overrides must be an object",
                benchmark_id=benchmark_id,
            )
    sample_ids_by_benchmark: dict[str, set[str]] = {benchmark: set() for benchmark in benchmarks}
    for record in records:
        key = record["record_key"]
        if key in seen_keys:
            raise _error(
                "duplicate sample record key",
                sample_id=record["sample_id"],
                model_id=record["model_id"],
                benchmark_id=record["benchmark_id"],
                run_id=record["run_id"],
            )
        seen_keys.add(key)
        sample_key = (record["benchmark_id"], record["sample_id"])
        sample_ids_by_benchmark[record["benchmark_id"]].add(record["sample_id"])
        references_by_sample.setdefault(sample_key, []).append(
            record.get("reference_answer")
        )

    for benchmark_id, mapping in answer_overrides.items():
        for sample_id, answer in mapping.items():
            if sample_id not in sample_ids_by_benchmark[benchmark_id]:
                raise _error(
                    "answer update references an unknown sample",
                    benchmark_id=benchmark_id,
                    sample_id=sample_id,
                )
            if not isinstance(answer, str):
                raise _error(
                    "answer override values must be strings",
                    benchmark_id=benchmark_id,
                    sample_id=sample_id,
                )

    effective_answers: dict[tuple[str, str], str | None] = {}
    for sample_key, references in references_by_sample.items():
        benchmark_id, sample_id = sample_key
        if sample_id in answer_overrides.get(benchmark_id, {}):
            effective_answers[sample_key] = answer_overrides[benchmark_id][sample_id]
            continue
        distinct = sorted({answer for answer in references if answer is not None})
        if len(distinct) > 1 or (distinct and any(answer is None for answer in references)):
            raise _error(
                "conflicting reference answers exist for one benchmark sample",
                benchmark_id=benchmark_id,
                sample_id=sample_id,
            )
        effective_answers[sample_key] = distinct[0] if distinct else None

    snapshots: list[dict[str, Any]] = []
    resolved_records: list[dict[str, Any]] = []
    for record in records:
        answer = effective_answers[(record["benchmark_id"], record["sample_id"])]
        resolved = dict(record)
        resolved["reference_answer"] = answer
        resolved_records.append(resolved)
        snapshots.append(_source_snapshot(record, answer))

    semantic_samples = [
        {
            "run_id": record["run_id"],
            "model_id": record["model_id"],
            "benchmark_id": record["benchmark_id"],
            "sample_id": record["sample_id"],
            "output": record["output"],
            "language": record["language"],
            "question_type": record["question_type"],
            "source_batch": record["source_batch"],
        }
        for record in resolved_records
    ]
    answer_rows = [
        {
            "benchmark_id": benchmark_id,
            "sample_id": sample_id,
            "answer": answer,
        }
        for (benchmark_id, sample_id), answer in sorted(effective_answers.items())
    ]
    models = tuple(sorted({record["model_id"] for record in resolved_records}))
    batches = tuple(sorted({record["source_batch"] for record in resolved_records}))
    return InputBundle(
        records=resolved_records,
        shard_files=shard_files,
        shards_digest=shards_digest,
        replacements_digest=replacements_digest,
        samples_digest=digest_json(semantic_samples),
        answers_digest=digest_json(answer_rows),
        source_snapshot=sorted(snapshots, key=lambda item: item["record_key"]),
        answer_overrides=copy.deepcopy(answer_overrides),
        benchmarks=tuple(benchmarks),
        models=models,
        batches=batches,
        evidence_samples=len(resolved_records),
    )


def load_inputs(samples_dir: Path, answers_path: Path) -> InputBundle:
    raw_records, shard_paths, shards_digest = _read_shards(samples_dir)
    replacements_path = samples_dir / "replacements.json"
    replacements, replacements_digest = _read_overrides(replacements_path)
    if answers_path.exists():
        try:
            raw_answers = read_json(answers_path)
        except (OSError, ValueError) as exc:
            raise _error("answer override file is invalid", path=str(answers_path), error=str(exc))
        if not isinstance(raw_answers, dict):
            raise _error("answer override root must be an object", path=str(answers_path))
        overrides = raw_answers.get("overrides", {})
        if not isinstance(overrides, dict):
            raise _error("answer override file must contain an overrides object")
        answer_overrides = overrides
    else:
        answer_overrides = {}
    bundle = assemble_inputs(
        raw_records,
        replacements,
        answer_overrides,
        shard_files=[path.name for path in shard_paths],
        shards_digest=shards_digest,
        replacements_digest=replacements_digest,
    )
    return bundle


def _load_raw_sources(
    samples_dir: Path,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]], dict[str, Any]]:
    raw_records, _, _ = _read_shards(samples_dir)
    replacements_path = samples_dir / "replacements.json"
    if replacements_path.exists():
        raw = read_json(replacements_path)
    else:
        raw = {"version": 1, "batches": {}}
    replacements, _ = _read_overrides(replacements_path)
    return raw_records, replacements, raw


def update_benchmark_answers(
    samples_dir: Path,
    answers_path: Path,
    benchmark_id: str,
    answers: dict[str, str],
    *,
    replace: bool = False,
) -> dict[str, Any]:
    bundle = load_inputs(samples_dir, answers_path)
    if benchmark_id not in bundle.benchmarks:
        raise _error("benchmark does not exist", benchmark_id=benchmark_id)
    if not isinstance(answers, dict) or not all(
        isinstance(sample_id, str) and isinstance(answer, str)
        for sample_id, answer in answers.items()
    ):
        raise _error("answers must be an object mapping sample ids to strings")
    known_samples = {
        record["sample_id"]
        for record in bundle.records
        if record["benchmark_id"] == benchmark_id
    }
    unknown = sorted(set(answers) - known_samples)
    if unknown:
        raise _error(
            "answer update references unknown samples",
            benchmark_id=benchmark_id,
            sample_ids=unknown,
        )

    overrides = copy.deepcopy(bundle.answer_overrides)
    next_mapping = {} if replace else dict(overrides.get(benchmark_id, {}))
    next_mapping.update({key: str(value) for key, value in answers.items()})
    if next_mapping:
        overrides[benchmark_id] = dict(sorted(next_mapping.items()))
    else:
        overrides.pop(benchmark_id, None)
    payload = {"version": 1, "overrides": dict(sorted(overrides.items()))}

    raw_records, replacements, _ = _load_raw_sources(samples_dir)
    effective = _assemble_records(raw_records, replacements)
    # Validate the candidate override set before publishing it.
    assemble_inputs(
        raw_records,
        replacements,
        payload["overrides"],
        shard_files=[],
        shards_digest=bundle.shards_digest,
        replacements_digest=bundle.replacements_digest,
    )
    atomic_write_json(answers_path, payload)
    affected_models = sorted(
        {
            record["model_id"]
            for record in effective
            if record["benchmark_id"] == benchmark_id
            and record["sample_id"] in answers
        }
    )
    return {
        "benchmark_id": benchmark_id,
        "updated_samples": sorted(answers),
        "affected_models": affected_models,
        "override_count": sum(len(value) for value in payload["overrides"].values()),
    }


def _validate_replacement_candidate(
    samples_dir: Path,
    answers_path: Path,
    replacements: dict[str, list[dict[str, Any]]],
    batch_id: str,
    new_records: list[dict[str, Any]],
) -> None:
    raw_records, _, _ = _load_raw_sources(samples_dir)
    replacements_candidate = copy.deepcopy(replacements)
    replacements_candidate[batch_id] = new_records
    raw_batches = {record["source_batch"] for record in raw_records}
    if batch_id not in raw_batches:
        raise _error("sample batch does not exist", batch_id=batch_id)
    bundle = load_inputs(samples_dir, answers_path)
    assemble_inputs(
        raw_records,
        replacements_candidate,
        bundle.answer_overrides,
        shard_files=[],
        shards_digest=bundle.shards_digest,
        replacements_digest=digest_json(
            {"version": 1, "batches": replacements_candidate}
        ),
    )


def replace_sample_batch(
    samples_dir: Path,
    answers_path: Path,
    batch_id: str,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(batch_id, str) or not batch_id:
        raise _error("batch id must be a non-empty string")
    if not isinstance(records, list):
        raise _error("replacement records must be a list")
    raw_records, existing_replacements, _ = _load_raw_sources(samples_dir)
    raw_batches = {record["source_batch"] for record in raw_records}
    if batch_id not in raw_batches:
        raise _error("sample batch does not exist", batch_id=batch_id)
    normalized: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise _error(
                "each replacement record must be an object",
                batch_id=batch_id,
                index=index,
            )
        candidate = dict(record)
        candidate.setdefault("source_batch", batch_id)
        item = normalize_record(candidate, source=f"replacement:{batch_id}:{index}")
        if item["source_batch"] != batch_id:
            raise _error(
                "replacement record source_batch must match the batch being replaced",
                batch_id=batch_id,
                sample_id=item["sample_id"],
            )
        normalized.append(item)
    _validate_replacement_candidate(
        samples_dir, answers_path, existing_replacements, batch_id, normalized
    )
    next_replacements = copy.deepcopy(existing_replacements)
    next_replacements[batch_id] = normalized
    payload = {
        "version": 1,
        "batches": {
            key: value for key, value in sorted(next_replacements.items())
        },
    }
    atomic_write_json(samples_dir / "replacements.json", payload)
    old_models = {
        record["model_id"]
        for record in _assemble_records(raw_records, existing_replacements)
        if record["source_batch"] == batch_id
    }
    new_models = {record["model_id"] for record in normalized}
    return {
        "batch_id": batch_id,
        "old_records": sum(
            1
            for record in _assemble_records(raw_records, existing_replacements)
            if record["source_batch"] == batch_id
        ),
        "new_records": len(normalized),
        "affected_models": sorted(old_models | new_models),
    }

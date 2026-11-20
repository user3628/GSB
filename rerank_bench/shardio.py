"""JSONL shard loading."""

from __future__ import annotations

import json

from .errors import E_MISSING_FEATURE, PipelineError


def load_shard(path):
    """Load a JSONL shard into (query_groups, candidates_by_query).

    query_groups: list of group dicts in file order.
    candidates_by_query: query_id -> list of candidate dicts in file order.
    """
    groups = []
    candidates = {}
    with open(path, "r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            rtype = rec.get("type")
            if rtype == "query_group":
                groups.append(rec)
                candidates.setdefault(rec["query_id"], [])
            elif rtype == "candidate":
                qid = rec.get("query_id")
                if qid not in candidates and all(
                    g["query_id"] != qid for g in groups
                ):
                    raise PipelineError(
                        E_MISSING_FEATURE,
                        f"candidate references unknown query group {qid}",
                        query_id=qid, field="query_id", line=line_no,
                    )
                candidates.setdefault(qid, []).append(rec)
    return groups, candidates


def count_candidates(path) -> int:
    n = 0
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue  # corrupt lines fail later in the worker, not here
            if rec.get("type") == "candidate":
                n += 1
    return n

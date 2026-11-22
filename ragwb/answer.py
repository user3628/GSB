"""Stage 4: answer assembly, evidence-chain export and deterministic rebuild.

Adopted paragraphs are rendered through the configured answer template with
dense, one-based citation numbers.  The evidence chain records the original
text offset, version, channel and adjudication reason behind every citation.
Re-importing that chain rebuilds the exact same answer text and citation map.
Missing required template slots or non-continuous citations raise
``E_ANSWER_INVALID`` while preserving the full diagnostic context.
"""
from __future__ import annotations

from typing import Any, Dict, List

from .errors import E_ANSWER_INVALID, WorkbenchError
from .io_utils import digest_json
from .text import find_span

STAGE_NAME = "answer_assembly"


def build_evidence_chain(alignment_product: Dict[str, Any]) -> List[Dict[str, Any]]:
    chain: List[Dict[str, Any]] = []
    for citation_number, adopted in enumerate(
            alignment_product["adopted"], start=1):
        paragraph = adopted["paragraph"]
        span = find_span(paragraph["text"], paragraph["text"]) or (0, 0)
        chain.append({
            "citation_number": citation_number,
            "doc_id": paragraph["doc_id"],
            "text": paragraph["text"],
            "text_offset": {"start": span[0], "end": span[1]},
            "version": paragraph["version"],
            "channel": paragraph["channel"],
            "lang": paragraph["lang"],
            "cluster_index": adopted["cluster_index"],
            "adoption_reason": adopted["adoption_reason"],
            "fully_conflicted_cluster": adopted["fully_conflicted_cluster"],
        })
    return chain


def _validate_chain(chain: List[Dict[str, Any]], *, session_id: str,
                    turn_index: int) -> None:
    expected = 1
    for entry in chain:
        if entry["citation_number"] != expected:
            raise WorkbenchError(
                E_ANSWER_INVALID,
                f"citation numbers must be continuous; expected {expected}, "
                f"got {entry['citation_number']}",
                stage=STAGE_NAME, session_id=session_id,
                turn_index=turn_index,
                context={"chain": chain})
        for required in ("doc_id", "text", "version", "channel",
                         "adoption_reason"):
            if entry.get(required) in (None, ""):
                raise WorkbenchError(
                    E_ANSWER_INVALID,
                    f"evidence-chain entry missing required field: {required}",
                    stage=STAGE_NAME, session_id=session_id,
                    turn_index=turn_index,
                    context={"entry": entry, "chain": chain})
        expected += 1


def render_answer(chain: List[Dict[str, Any]], config: Dict[str, Any],
                  *, session_id: str, turn_index: int,
                  query: str) -> Dict[str, Any]:
    answer_cfg = config["answer"]
    template = answer_cfg["template"]
    required_slots = list(answer_cfg["required_slots"])
    for slot in required_slots:
        if "{" + slot + "}" not in template:
            raise WorkbenchError(
                E_ANSWER_INVALID,
                f"answer template is missing required slot: {slot}",
                stage=STAGE_NAME, session_id=session_id,
                turn_index=turn_index,
                context={"template": template, "required_slots":
                         required_slots})
    _validate_chain(chain, session_id=session_id, turn_index=turn_index)

    body_lines = []
    reference_lines = []
    for entry in chain:
        body_lines.append(
            f"{entry['text']}[{entry['citation_number']}]"
        )
        reference_lines.append(
            f"[{entry['citation_number']}] {entry['doc_id']}"
            f" ({entry['channel']}, v{entry['version']})"
        )
    slots = {
        "answer_body": " ".join(body_lines) if body_lines else query,
        "references": " ".join(reference_lines) if reference_lines else "-",
    }
    try:
        answer_text = template.format(**slots)
    except KeyError as exc:  # pragma: no cover - guarded above
        raise WorkbenchError(
            E_ANSWER_INVALID, f"unknown template slot: {exc}",
            stage=STAGE_NAME, session_id=session_id,
            turn_index=turn_index, context={"template": template})

    citation_map = {
        str(entry["citation_number"]): entry["doc_id"]
        for entry in chain
    }
    return {"answer_text": answer_text, "citation_map": citation_map,
            "slots": slots}


def assemble(alignment_product: Dict[str, Any],
             retrieval_product: Dict[str, Any],
             config: Dict[str, Any]) -> Dict[str, Any]:
    session_id = alignment_product["session_id"]
    turn_index = alignment_product["turn_index"]
    chain = build_evidence_chain(alignment_product)
    rendered = render_answer(
        chain, config, session_id=session_id, turn_index=turn_index,
        query=retrieval_product["rewritten_query"])
    product = {
        "session_id": session_id,
        "turn_index": turn_index,
        "stage": STAGE_NAME,
        "answer_text": rendered["answer_text"],
        "citation_map": rendered["citation_map"],
        "evidence_chain": chain,
        "retained_conflicts": [
            entry for entry in chain
            if entry["fully_conflicted_cluster"]
        ],
    }
    product["fingerprint"] = digest_json({
        "answer_text": product["answer_text"],
        "citation_map": product["citation_map"],
        "evidence_chain": chain,
    })
    return product


def rebuild_from_chain(product: Dict[str, Any], config: Dict[str, Any],
                       query: str) -> Dict[str, Any]:
    """Rebuild answer text and citation mapping from an imported chain."""
    rendered = render_answer(
        product["evidence_chain"], config,
        session_id=product["session_id"],
        turn_index=product["turn_index"], query=query)
    return rendered

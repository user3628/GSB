"""Stage 1: deterministic query rewriting and coreference resolution.

The dialogue focus is maintained across turns as an explicit entity string
plus the doc_id referenced by the previous answer.  Pronoun-like spans
(``它``, ``该条``, ``这条``, ``其``, ``it`` ...) are replaced by the current
focus text and the character offsets of every replacement are reported.  When
no focus exists or no anaphor is found, the original question is returned with
an explicit fallback reason.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .io_utils import digest_json
from .text import find_span

STAGE_NAME = "query_rewrite"

PRONOUNS = ["它", "该条", "这条", "它们", "其", "it", "this"]
_NOUN_TAILS = ("电池", "芯片", "手机", "无人机", "路由器", "显示器",
               "键盘", "充电器", "传感器")
_EN_TAIL_RE = re.compile(
    r"\b(?:the\s+)?([a-z][a-z0-9]*(?:\s+[a-z][a-z0-9]*){0,3}?)"
    r"\s+(?:battery|chip|phone|drone|router|display|keyboard|charger|sensor)\b"
)


@dataclass
class Focus:
    entity: Optional[str] = None
    doc_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {"entity": self.entity, "doc_id": self.doc_id}


def detect_pronouns(question: str,
                    marked_spans: Optional[List[Dict[str, Any]]] = None
                    ) -> List[Dict[str, Any]]:
    """Locate anaphor spans; annotated shard spans take precedence."""
    spans: List[Dict[str, Any]] = []
    if marked_spans:
        for span in marked_spans:
            start = int(span["start"])
            end = int(span["end"])
            if 0 <= start < end <= len(question) and \
                    question[start:end] == span["text"]:
                spans.append({"start": start, "end": end,
                              "text": span["text"], "kind": "pronoun"})
        if spans:
            return spans
    lowered = question.lower()
    for pronoun in sorted(PRONOUNS, key=len, reverse=True):
        index = lowered.find(pronoun)
        if index >= 0:
            return [{"start": index, "end": index + len(pronoun),
                     "text": question[index:index + len(pronoun)],
                     "kind": "pronoun"}]
    return spans


def extract_initial_entity(question: str) -> Optional[str]:
    """Best-effort deterministic head-entity extraction from a fresh question."""
    for tail in _NOUN_TAILS:
        index = question.find(tail)
        if index > 0:
            prefix = question[:index]
            head = re.split(r"[，。？?！!、\s]", prefix)[-1]
            if head:
                return head + tail
    match = _EN_TAIL_RE.search(question.lower())
    if match:
        return match.group(0).replace("the ", "", 1).strip()
    return None


def _resolve_focus(turn: Dict[str, Any], prior: Focus) -> Focus:
    hint = turn.get("focus_hint")
    if hint:
        return Focus(entity=hint, doc_id=prior.doc_id)
    return prior


def rewrite_turn(turn: Dict[str, Any], prior: Focus) -> Dict[str, Any]:
    """Rewrite one turn. ``turn`` is the raw shard record for the turn."""
    question = turn["question"]
    focus = _resolve_focus(turn, prior)
    spans = detect_pronouns(question, turn.get("coref_spans"))

    fallback_reason = ""
    if not spans:
        rewritten = question
        fallback_reason = "no_anaphor_detected"
    elif not focus.entity:
        rewritten = question
        fallback_reason = "empty_dialogue_focus"
    else:
        span = spans[0]
        rewritten = (question[:span["start"]] + focus.entity
                     + question[span["end"]:])

    replacements = []
    if not fallback_reason:
        replacement_span = find_span(rewritten, focus.entity)
        replacements.append({
            "original": spans[0]["text"],
            "replacement": focus.entity,
            "start": replacement_span[0] if replacement_span else 0,
            "end": replacement_span[1] if replacement_span else
            len(focus.entity),
        })

    product = {
        "session_id": turn["session_id"],
        "turn_index": turn["turn_index"],
        "stage": STAGE_NAME,
        "original_question": question,
        "rewritten_query": rewritten if not fallback_reason else question,
        "replacements": replacements,
        "focus_before": prior.to_dict(),
        "focus_after": focus.to_dict(),
        "fallback": bool(fallback_reason),
        "fallback_reason": fallback_reason,
        "lang": turn.get("lang", ""),
    }
    product["fingerprint"] = digest_json(
        {k: product[k] for k in
         ("original_question", "rewritten_query", "replacements",
          "focus_before", "focus_after", "fallback_reason", "lang")}
    )
    return product


def rewrite_session(session_turns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Run stage 1 for every turn, carrying the evolving focus deterministically."""
    ordered = sorted(session_turns, key=lambda row: row["turn_index"])
    focus = Focus()
    products: List[Dict[str, Any]] = []
    for turn in ordered:
        product = rewrite_turn(turn, focus)
        # A fresh explicit hint becomes the persistent entity focus.
        if turn.get("focus_hint"):
            focus = Focus(entity=turn["focus_hint"], doc_id=focus.doc_id)
        else:
            focus = Focus(entity=product["focus_after"]["entity"],
                          doc_id=focus.doc_id)
        products.append(product)
    return products

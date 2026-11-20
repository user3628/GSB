"""Local deterministic arc-standard transition dependency parser.

No pretrained weights, no GPU and no network access are involved. Action
scores are dot products between fixed-hash feature vectors and the weights
declared in the YAML config; ties are broken by a fixed action priority so
that identical (input, config) always yields byte-identical output.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from .config import ACTIONS, N_FEATURES, ModelConfig
from .errors import E_NUMERIC, E_TREE_INVALID, WorkbenchError
from .hashing import hash_parts
from .tokenizer import TaggedToken, tag, tokenize

ROOT_TEXT = "ROOT"
ROOT_POS = "ROOT"
ACTION_PRIORITY = {"left": 0, "right": 1, "shift": 2}
POS_LABELS = {
    "VERB": "verb", "NOUN": "nmod", "PROPN": "name", "DET": "det",
    "ADP": "case", "ADV": "advmod", "ADJ": "amod", "AUX": "aux",
    "CCONJ": "cc", "NUM": "nummod", "CJK": "cmod",
}


@dataclass
class Arc:
    head: int          # 0 means the artificial ROOT
    dependent: int     # 1-based token index
    relation: str
    distance: int


@dataclass
class SentenceParse:
    sentence_id: str
    text: str
    tokens: list[str]
    tags: list[str]
    arcs: list[Arc]
    heads: list[int]
    relations: list[str]
    distances: list[int]
    total_score: float
    actions: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "sentence_id": self.sentence_id,
            "text": self.text,
            "tokens": self.tokens,
            "tags": self.tags,
            "arcs": [arc.__dict__ for arc in self.arcs],
            "heads": self.heads,
            "relations": self.relations,
            "distances": self.distances,
            "total_score": self.total_score,
            "actions": self.actions,
        }


def _relation_for(pos: str, direction: str) -> str:
    if pos == ROOT_POS:
        return "root"
    base = POS_LABELS.get(pos, "dep")
    return f"{base}:{direction}"


def _feature_vector(features: dict[str, Any], action: str) -> list[float]:
    s0 = features.get("s0")
    s1 = features.get("s1")
    b0 = features.get("b0")
    stack_size = features.get("stack_size", 0)
    buffer_size = features.get("buffer_size", 0)

    def sign_for(token: tuple[str, str] | None, salt: str) -> float:
        if token is None:
            return 0.0
        text, pos = token
        return 1.0 if hash_parts("feat", salt, action, text, pos) % 2 == 0 else -1.0

    def float_for(token: tuple[str, str] | None, salt: str) -> float:
        if token is None:
            return 0.0
        text, pos = token
        return (hash_parts("fval", salt, action, text, pos) % 1009) / 1009.0

    return [
        sign_for(s1, "s1"),
        sign_for(s0, "s0"),
        sign_for(b0, "b0"),
        float_for(s0, "fs0"),
        (stack_size % 7) / 7.0,
        (buffer_size % 11) / 11.0,
    ]


def _action_score(vector: list[float], weights: tuple[float, ...]) -> float:
    # numpy float64 dot product; overflow still surfaces as +/-inf and is
    # caught by the finite-value check below.
    with np.errstate(over="ignore", invalid="ignore"):
        return float(np.dot(np.asarray(vector, dtype=np.float64),
                            np.asarray(weights, dtype=np.float64)))


def _numeric_failure(sentence_id: str, token_index: int, action: str,
                     value: float) -> WorkbenchError:
    return WorkbenchError(
        E_NUMERIC,
        f"non-finite action score for '{action}' at token index {token_index}",
        {"sentence_id": sentence_id, "token_index": token_index,
         "action": action, "value": str(value)},
    )


def _token_of(stack_item: int, tagged: list[TaggedToken]) -> tuple[str, str] | None:
    if stack_item == 0:
        return (ROOT_TEXT, ROOT_POS)
    tok = tagged[stack_item - 1]
    return (tok.text, tok.pos)


def validate_tree(parse: SentenceParse, max_distance: int,
                  unique_root: bool) -> None:
    n = len(parse.tokens)
    heads = parse.heads
    if len(heads) != n or any(h < 0 or h > n for h in heads):
        raise WorkbenchError(
            E_TREE_INVALID, "every token must have exactly one in-range head",
            {"sentence_id": parse.sentence_id, "token_index": -1},
        )
    roots = [i + 1 for i, h in enumerate(heads) if h == 0]
    if unique_root and len(roots) != 1:
        raise WorkbenchError(
            E_TREE_INVALID,
            f"expected exactly one root, found {len(roots)}",
            {"sentence_id": parse.sentence_id, "token_index": roots[0] if roots else -1,
             "roots": roots},
        )
    if not roots:
        raise WorkbenchError(
            E_TREE_INVALID, "dependency tree has no root",
            {"sentence_id": parse.sentence_id, "token_index": -1},
        )
    for start in range(1, n + 1):
        seen: set[int] = set()
        node = start
        while node != 0:
            if node in seen:
                raise WorkbenchError(
                    E_TREE_INVALID, "dependency graph contains a cycle",
                    {"sentence_id": parse.sentence_id, "token_index": node},
                )
            seen.add(node)
            node = heads[node - 1]
    for index, distance in enumerate(parse.distances, start=1):
        if distance > max_distance:
            raise WorkbenchError(
                E_TREE_INVALID,
                f"dependency distance {distance} exceeds limit {max_distance}",
                {"sentence_id": parse.sentence_id, "token_index": index,
                 "distance": distance, "limit": max_distance},
            )


def parse_sentence(text: str, model: ModelConfig, sentence_id: str,
                   force_numeric_at: int | None = None) -> SentenceParse:
    tokens = tokenize(text)
    if not tokens:
        raise WorkbenchError(
            E_TREE_INVALID, "sentence contains no tokens",
            {"sentence_id": sentence_id, "token_index": -1},
        )
    tagged = tag(tokens)
    n = len(tokens)

    stack: list[int] = [0]
    buffer: list[int] = list(range(1, n + 1))
    heads: list[int] = [0] * n
    relations: list[str] = [""] * n
    distances: list[int] = [0] * n
    arcs: list[Arc] = []
    action_log: list[str] = []
    total_score = 0.0

    def current_features() -> dict[str, Any]:
        s0_item = stack[-1] if stack else None
        s1_item = stack[-2] if len(stack) >= 2 else None
        b0_item = buffer[0] if buffer else None
        return {
            "s0": _token_of(s0_item, tagged) if s0_item is not None else None,
            "s1": _token_of(s1_item, tagged) if s1_item is not None else None,
            "b0": _token_of(b0_item, tagged) if b0_item is not None else None,
            "stack_size": len(stack),
            "buffer_size": len(buffer),
        }

    while not (len(stack) == 1 and stack[0] == 0 and not buffer):
        legal: list[str] = []
        if buffer:
            legal.append("shift")
        if len(stack) >= 3:
            legal.append("left")
        # A right-arc onto ROOT is reserved for the final transition; allowing
        # it early would attach a partial subtree to the root while more tokens
        # remain in the buffer, producing multiple roots.
        if len(stack) >= 2 and stack[-1] != 0 and stack[-2] != 0:
            legal.append("right")
        if not buffer and len(stack) == 2 and stack == [0, stack[-1]]:
            legal.append("right")
        features = current_features()
        scored: list[tuple[float, str]] = []
        for action in legal:
            vector = _feature_vector(features, action)
            score = _action_score(vector, model.weights[action])
            focus_index = buffer[0] if buffer else stack[-1]
            if force_numeric_at is not None and focus_index == force_numeric_at and \
                    action == legal[0]:
                score = math.inf
            if not math.isfinite(score):
                raise _numeric_failure(sentence_id, focus_index, action, score)
            scored.append((score, action))
        scored.sort(key=lambda pair: (-pair[0], ACTION_PRIORITY[pair[1]]))
        best_score, action = scored[0]
        total_score += best_score
        action_log.append(action)

        if action == "shift":
            stack.append(buffer.pop(0))
        elif action == "left":
            right_node = stack.pop()
            left_node = stack.pop()
            heads[left_node - 1] = right_node
            relations[left_node - 1] = _relation_for(tagged[left_node - 1].pos, "left")
            distances[left_node - 1] = abs(right_node - left_node)
            arcs.append(Arc(head=right_node, dependent=left_node,
                            relation=relations[left_node - 1],
                            distance=distances[left_node - 1]))
            stack.append(right_node)
        else:  # right
            right_node = stack.pop()
            left_node = stack[-1]
            heads[right_node - 1] = left_node
            child_pos = ROOT_POS if left_node == 0 else tagged[right_node - 1].pos
            relations[right_node - 1] = _relation_for(child_pos, "right")
            distances[right_node - 1] = abs(right_node - left_node)
            arcs.append(Arc(head=left_node, dependent=right_node,
                            relation=relations[right_node - 1],
                            distance=distances[right_node - 1]))

    arcs.sort(key=lambda arc: arc.dependent)
    parse = SentenceParse(
        sentence_id=sentence_id, text=text, tokens=tokens,
        tags=[tok.pos for tok in tagged], arcs=arcs, heads=heads,
        relations=relations, distances=distances,
        total_score=total_score, actions=action_log,
    )
    validate_tree(parse, model.max_dependency_distance, model.unique_root)
    return parse

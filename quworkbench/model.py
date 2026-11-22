"""Stage 2a: log-domain multinomial Naive Bayes intent classification.

Priors and token conditionals are estimated from the local labeled samples
with a configurable smoothing strength. Class feature weights are combined
in the log domain. Scores are softmax-normalized into confidences. Unknown
tokens fall back to a configurable <UNK> token.
"""

import math
from collections import Counter

import numpy as np

from .canon import digest
from .errors import E_INVALID_CONFIG, WorkbenchError

INTENTS = ("informational", "local_service", "navigational", "transactional")
_STAGE = "classify"
PROB_PRECISION = 12


class NBModel:
    def __init__(self, alpha, unk_token, class_weights, log_prior, log_cond, vocab, labeled_digest):
        self.alpha = alpha
        self.unk_token = unk_token
        self.class_weights = dict(class_weights)
        self.log_prior = log_prior
        self.log_cond = log_cond
        self.vocab = frozenset(vocab)
        self.labeled_digest = labeled_digest
        self.model_digest = digest(
            {
                "alpha": alpha,
                "unk_token": unk_token,
                "class_weights": self.class_weights,
                "log_prior": log_prior,
                "log_cond": log_cond,
                "labeled_digest": labeled_digest,
            }
        )


def train_nb(labeled_token_samples, alpha, unk_token, class_weights, labeled_digest=""):
    """Train the model. labeled_token_samples: list of (token_texts, intent)."""
    if alpha is None or float(alpha) <= 0.0:
        raise WorkbenchError(
            E_INVALID_CONFIG,
            "smoothing_alpha_must_be_positive",
            stage=_STAGE,
            details={"smoothing_alpha": alpha},
        )
    alpha = float(alpha)
    if not labeled_token_samples:
        raise WorkbenchError(
            E_INVALID_CONFIG,
            "insufficient_labeled_samples_for_prior",
            stage=_STAGE,
            details={"num_samples": 0},
        )
    weights = {c: float(class_weights.get(c, 1.0)) for c in INTENTS}
    for c, w in sorted(weights.items()):
        if w <= 0.0:
            raise WorkbenchError(
                E_INVALID_CONFIG,
                "class_weight_must_be_positive",
                stage=_STAGE,
                details={"intent": c, "weight": w},
            )
    doc_counts = Counter()
    token_counts = {c: Counter() for c in INTENTS}
    vocab = set()
    for tokens, label in labeled_token_samples:
        if label not in INTENTS:
            raise WorkbenchError(
                E_INVALID_CONFIG,
                "unknown_intent_label",
                stage=_STAGE,
                details={"intent": label},
            )
        doc_counts[label] += 1
        token_counts[label].update(tokens)
        vocab.update(tokens)
    missing = [c for c in INTENTS if doc_counts[c] == 0]
    if missing:
        raise WorkbenchError(
            E_INVALID_CONFIG,
            "insufficient_labeled_samples_for_prior",
            stage=_STAGE,
            details={"missing_intents": missing},
        )
    vocab.add(unk_token)
    vocab_size = len(vocab)
    total_docs = sum(doc_counts.values())
    log_prior = {
        c: math.log((doc_counts[c] + alpha) / (total_docs + alpha * len(INTENTS)))
        for c in INTENTS
    }
    log_cond = {}
    for c in INTENTS:
        total_tokens = sum(token_counts[c].values())
        denom = total_tokens + alpha * vocab_size
        log_cond[c] = {
            t: math.log((token_counts[c][t] + alpha) / denom) for t in sorted(vocab)
        }
    return NBModel(alpha, unk_token, weights, log_prior, log_cond, vocab, labeled_digest)


def score_intent(model, token_texts):
    """Return (intent, confidence, {intent: probability}) for one token list."""
    mapped = [t if t in model.vocab else model.unk_token for t in token_texts]
    log_scores = []
    for c in INTENTS:
        s = model.log_prior[c] + math.log(model.class_weights[c])
        cond = model.log_cond[c]
        for t in mapped:
            s += cond[t]
        log_scores.append(s)
    arr = np.asarray(log_scores, dtype=np.float64)
    arr = arr - arr.max()
    exp = np.exp(arr)
    probs = exp / exp.sum()
    scores = {c: round(float(probs[i]), PROB_PRECISION) for i, c in enumerate(INTENTS)}
    best = None
    for c in INTENTS:  # INTENTS is sorted: deterministic tie-break
        if best is None or scores[c] > scores[best]:
            best = c
    return best, scores[best], scores

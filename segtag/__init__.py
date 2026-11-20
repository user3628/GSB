"""segtag：本地确定性中文分词与词性联合标注组件。

核心用法：
    from segtag import Segmenter
    seg = Segmenter.train("data/train.txt", smoothing=0.1)
    result = seg.tag_sentence("我爱北京。")
    report = seg.tag_files(["data/input1.txt"])
"""
from .api import Segmenter, SentenceResult
from .batch import BATCH_TOLERANCE, BatchReport, SentenceRecord, batch_decode
from .decoder import DecodeResult, Decoder
from .errors import (
    ALL_CODES,
    E_CONFIG,
    E_EMPTY,
    E_FORMAT,
    E_INVALID_TAG,
    E_NUMERIC,
    E_SMOOTHING,
    SegError,
    SegTagError,
)
from .model import (
    DEFAULT_UNKNOWN_TOKEN,
    Corpus,
    ProbabilityTables,
    WordTag,
    check_smoothing,
    estimate,
    safe_log,
)
from .validate import Word, validate_and_segment

__version__ = "1.0.0"

__all__ = [
    "Segmenter",
    "SentenceResult",
    "BatchReport",
    "SentenceRecord",
    "batch_decode",
    "BATCH_TOLERANCE",
    "Decoder",
    "DecodeResult",
    "Corpus",
    "ProbabilityTables",
    "WordTag",
    "estimate",
    "safe_log",
    "check_smoothing",
    "DEFAULT_UNKNOWN_TOKEN",
    "Word",
    "validate_and_segment",
    "SegError",
    "SegTagError",
    "ALL_CODES",
    "E_NUMERIC",
    "E_INVALID_TAG",
    "E_EMPTY",
    "E_SMOOTHING",
    "E_FORMAT",
    "E_CONFIG",
    "__version__",
]


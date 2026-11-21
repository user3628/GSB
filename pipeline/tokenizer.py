"""文本规范化与确定性分词。

规则：
  1. Unicode NFKC（同时完成全角/半角统一）、空白折叠、小写化；
  2. 中文（CJK 统一表意文字）按单字切分，并生成相邻二字组合；
  3. 拉丁字母/数字按连续串切分；
  4. 其它字符作为独立 token，保证信息不丢失。
"""

import unicodedata


def normalize_text(text, normalize_cfg):
    if text is None:
        text = ""
    text = str(text)
    if normalize_cfg.get("nfkc", True):
        text = unicodedata.normalize("NFKC", text)
    if normalize_cfg.get("lowercase", True):
        text = text.lower()
    if normalize_cfg.get("fold_whitespace", True):
        text = " ".join(text.split())
    else:
        text = text.strip()
    # halfwidth_fullwidth_unify 已被 NFKC 覆盖；保留开关以便显式表达意图。
    if normalize_cfg.get("halfwidth_fullwidth_unify", True):
        text = unicodedata.normalize("NFKC", text)
    return text


def _is_han(char):
    code = ord(char)
    return (
        0x4E00 <= code <= 0x9FFF
        or 0x3400 <= code <= 0x4DBF
        or 0x20000 <= code <= 0x2A6DF
        or 0xF900 <= code <= 0xFAFF
    )


def _is_alnum(char):
    return char.isascii() and char.isalnum()


def tokenize(normalized, han_bigrams=True):
    """对已规范化文本切词，返回确定性 token 列表（保留重复，用于词频统计）。"""
    tokens = []
    han_runs = []
    index = 0
    length = len(normalized)
    while index < length:
        char = normalized[index]
        if char.isspace():
            index += 1
            continue
        if _is_han(char):
            run = []
            while index < length and _is_han(normalized[index]):
                run.append(normalized[index])
                index += 1
            han_runs.append(run)
            for han_char in run:
                tokens.append("h:" + han_char)
            if han_bigrams and len(run) >= 2:
                for pos in range(len(run) - 1):
                    tokens.append("hb:" + run[pos] + run[pos + 1])
            continue
        if _is_alnum(char):
            start = index
            while index < length and _is_alnum(normalized[index]):
                index += 1
            tokens.append("w:" + normalized[start:index])
            continue
        tokens.append("p:" + char)
        index += 1
    return tokens


def text_to_tokens(text, config):
    normalized = normalize_text(text, config["normalize"])
    if not normalized:
        return normalized, []
    return normalized, tokenize(normalized, config["features"].get("han_bigrams", True))

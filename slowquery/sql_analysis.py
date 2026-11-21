from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable

from .errors import invalid_sample


READ_ONLY_FIRST_WORDS = {"SELECT", "WITH"}
WRITE_KEYWORDS = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "REPLACE",
    "CREATE",
    "DROP",
    "ALTER",
    "ATTACH",
    "DETACH",
    "PRAGMA",
    "VACUUM",
    "REINDEX",
    "ANALYZE",
}
CLAUSE_WORDS = {"WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION", "INTERSECT", "EXCEPT"}
TABLE_STOP_WORDS = CLAUSE_WORDS | {"JOIN", "ON", "USING", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS"}


@dataclass(frozen=True, slots=True)
class Token:
    kind: str
    value: str
    upper: str
    start: int
    end: int
    depth: int


@dataclass(frozen=True, slots=True)
class Comparison:
    column: str
    table: str | None
    operator: str
    literal_count: int = 1
    raw: str = ""


@dataclass(slots=True)
class ParsedQuery:
    sql: str
    tables: list[str]
    table_aliases: dict[str, str]
    where_text: str | None
    where_start: int | None
    where_end: int | None
    has_top_level_or: bool
    comparisons: list[Comparison]
    and_segments: list[str] = field(default_factory=list)
    or_segments: list[str] = field(default_factory=list)
    has_limit: bool = False
    has_order: bool = False
    has_group: bool = False
    has_union: bool = False

    @property
    def primary_table(self) -> str | None:
        return self.tables[0] if self.tables else None


def _unquote_identifier(raw: str) -> str:
    if len(raw) >= 2 and raw[0] in {'"', "`", "["}:
        if raw[0] == "[":
            return raw[1:-1].replace("]]", "]")
        quote = raw[0]
        return raw[1:-1].replace(quote * 2, quote)
    return raw


def tokenize(sql: str) -> list[Token]:
    tokens: list[Token] = []
    depth = 0
    index = 0
    length = len(sql)
    while index < length:
        char = sql[index]
        if char.isspace():
            index += 1
            continue
        if char == "-" and index + 1 < length and sql[index + 1] == "-":
            newline = sql.find("\n", index + 2)
            index = length if newline < 0 else newline + 1
            continue
        if char == "/" and index + 1 < length and sql[index + 1] == "*":
            end_comment = sql.find("*/", index + 2)
            index = length if end_comment < 0 else end_comment + 2
            continue
        if char == "'":
            start = index
            index += 1
            while index < length:
                if sql[index] == "'":
                    if index + 1 < length and sql[index + 1] == "'":
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            tokens.append(Token("string", sql[start:index], sql[start:index], start, index, depth))
            continue
        if char in {'"', "`", "["}:
            start = index
            closing = "]" if char == "[" else char
            index += 1
            while index < length:
                if sql[index] == closing:
                    if closing != "]" and index + 1 < length and sql[index + 1] == closing:
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            raw = sql[start:index]
            tokens.append(Token("identifier", _unquote_identifier(raw), raw.upper(), start, index, depth))
            continue
        if char == "(":
            tokens.append(Token("punct", char, char, index, index + 1, depth))
            depth += 1
            index += 1
            continue
        if char == ")":
            depth = max(0, depth - 1)
            tokens.append(Token("punct", char, char, index, index + 1, depth))
            index += 1
            continue
        two = sql[index : index + 2]
        if two in {"<=", ">=", "<>", "!=", "||"}:
            tokens.append(Token("operator", two, two, index, index + 2, depth))
            index += 2
            continue
        if char in "=<>+-*/%":
            tokens.append(Token("operator", char, char, index, index + 1, depth))
            index += 1
            continue
        if char in ",.;":
            tokens.append(Token("punct", char, char, index, index + 1, depth))
            index += 1
            continue
        if char.isdigit():
            start = index
            index += 1
            while index < length and (sql[index].isdigit() or sql[index] in ".eE+-"):
                if sql[index] in "+-" and sql[index - 1] not in "eE":
                    break
                index += 1
            tokens.append(Token("number", sql[start:index], sql[start:index], start, index, depth))
            continue
        if char.isalpha() or char == "_":
            start = index
            index += 1
            while index < length and (sql[index].isalnum() or sql[index] in "_$"):
                index += 1
            value = sql[start:index]
            tokens.append(Token("word", value, value.upper(), start, index, depth))
            continue
        tokens.append(Token("unknown", char, char.upper(), index, index + 1, depth))
        index += 1
    return tokens


def normalize_sql(sql: Any) -> str:
    if not isinstance(sql, str) or not sql.strip():
        raise invalid_sample("sql must be a non-empty string")
    normalized = sql.strip()
    if normalized.endswith(";"):
        normalized = normalized[:-1].rstrip()
    return normalized


def validate_readonly_sql(sql: Any) -> str:
    normalized = normalize_sql(sql)
    tokens = tokenize(normalized)
    if not tokens:
        raise invalid_sample("sql is empty")
    first_word = next((token.upper for token in tokens if token.kind == "word"), "")
    if first_word not in READ_ONLY_FIRST_WORDS:
        raise invalid_sample("only a single read-only SELECT statement is accepted")
    for token in tokens:
        if token.kind == "punct" and token.value == ";" and token.depth == 0:
            raise invalid_sample("multiple SQL statements are not accepted")
        if token.kind == "word" and token.depth == 0 and token.upper in WRITE_KEYWORDS:
            raise invalid_sample("write or administrative SQL is not accepted as a sample query")
    return normalized


def _keyword_indices(tokens: list[Token], words: set[str]) -> list[tuple[int, Token]]:
    return [
        (index, token)
        for index, token in enumerate(tokens)
        if token.depth == 0 and token.kind == "word" and token.upper in words
    ]


def _next_clause_index(tokens: list[Token], start: int) -> int | None:
    for index in range(start, len(tokens)):
        token = tokens[index]
        if token.depth == 0 and token.kind == "word" and token.upper in CLAUSE_WORDS:
            return index
    return None


def _previous_identifier(tokens: list[Token], index: int) -> tuple[str, str | None] | None:
    if index <= 0:
        return None
    previous = tokens[index - 1]
    if previous.kind not in {"word", "identifier"}:
        return None
    if index >= 3 and tokens[index - 2].value == "." and tokens[index - 3].kind in {"word", "identifier"}:
        qualifier = tokens[index - 3].value
        return previous.value, qualifier
    return previous.value, None


def _extract_comparisons(
    tokens: list[Token],
    table_aliases: dict[str, str],
    default_table: str | None,
) -> list[Comparison]:
    comparisons: list[Comparison] = []
    for index, token in enumerate(tokens):
        operator: str | None = None
        literal_count = 1
        if token.kind == "operator" and token.value in {"=", "<", ">", "<=", ">=", "!=", "<>"}:
            operator = token.value
        elif token.kind == "word" and token.upper in {"IN", "LIKE", "BETWEEN", "IS"}:
            operator = token.upper
            if token.upper == "IN" and index + 1 < len(tokens) and tokens[index + 1].value == "(":
                literal_count = 0
                nested_depth = tokens[index + 1].depth + 1
                for inner in tokens[index + 2 :]:
                    if inner.value == ")" and inner.depth < nested_depth:
                        break
                    if inner.value == "," and inner.depth == nested_depth:
                        literal_count += 1
                literal_count = max(1, literal_count + 1)
        if operator is None:
            continue
        previous = _previous_identifier(tokens, index)
        if previous is None:
            continue
        column, qualifier = previous
        if column.upper() in {
            "SELECT",
            "FROM",
            "WHERE",
            "AND",
            "OR",
            "NOT",
            "ON",
            "BY",
            "AS",
            "IN",
            "IS",
            "NULL",
        }:
            continue
        table = None
        if qualifier:
            table = table_aliases.get(qualifier.lower(), qualifier.lower())
        else:
            table = default_table
        comparisons.append(
            Comparison(
                column=column,
                table=table,
                operator=operator,
                literal_count=literal_count,
                raw=" ".join(item.value for item in tokens),
            )
        )
    return comparisons


def _split_where_on_keyword(
    sql: str, tokens: list[Token], depth: int, keyword: str
) -> list[str]:
    if not tokens:
        return []
    boundaries: list[tuple[int, int]] = []
    for token in tokens:
        if token.depth == depth and token.kind == "word" and token.upper == keyword:
            boundaries.append((token.start, token.end))
    if not boundaries:
        return [sql[tokens[0].start : tokens[-1].end].strip()]
    result: list[str] = []
    cursor = tokens[0].start
    for start, end in boundaries:
        result.append(sql[cursor:start].strip())
        cursor = end
    result.append(sql[cursor : tokens[-1].end].strip())
    return [item for item in result if item]


def parse_query(sql: str) -> ParsedQuery:
    sql = normalize_sql(sql)
    tokens = tokenize(sql)
    if not tokens:
        return ParsedQuery(sql, [], {}, None, None, None, False, [])
    from_positions = _keyword_indices(tokens, {"FROM"})
    if not from_positions:
        return ParsedQuery(sql, [], {}, None, None, None, False, [])

    from_index, _ = from_positions[0]
    from_end_index = _next_clause_index(tokens, from_index + 1)
    if from_end_index is None:
        from_end_index = len(tokens)

    tables: list[str] = []
    table_aliases: dict[str, str] = {}
    expect_table = True
    index = from_index + 1
    while index < from_end_index:
        token = tokens[index]
        if token.depth != 0:
            index += 1
            continue
        if token.kind == "word" and token.upper == "JOIN":
            expect_table = True
            index += 1
            continue
        if token.value == ",":
            expect_table = True
            index += 1
            continue
        if expect_table and token.kind in {"word", "identifier"} and token.upper not in TABLE_STOP_WORDS:
            parts = [token.value]
            lookahead = index + 1
            while (
                lookahead + 1 < from_end_index
                and tokens[lookahead].value == "."
                and tokens[lookahead + 1].kind in {"word", "identifier"}
            ):
                parts.append(tokens[lookahead + 1].value)
                lookahead += 2
            table = parts[-1]
            normalized_table = table.lower()
            tables.append(normalized_table)
            table_aliases[normalized_table] = normalized_table
            alias_cursor = lookahead
            if alias_cursor < from_end_index and tokens[alias_cursor].upper == "AS":
                alias_cursor += 1
            if alias_cursor < from_end_index and tokens[alias_cursor].kind in {"word", "identifier"}:
                candidate = tokens[alias_cursor].value.lower()
                if candidate not in TABLE_STOP_WORDS:
                    table_aliases[candidate] = normalized_table
                    lookahead = alias_cursor + 1
            expect_table = False
            index = max(index + 1, lookahead)
            continue
        index += 1

    where_positions = _keyword_indices(tokens, {"WHERE"})
    if not where_positions:
        comparison_tokens = tokens[from_index + 1 : from_end_index]
        comparisons = _extract_comparisons(
            comparison_tokens, table_aliases, tables[0] if len(tables) == 1 else None
        )
        return ParsedQuery(
            sql=sql,
            tables=tables,
            table_aliases=table_aliases,
            where_text=None,
            where_start=None,
            where_end=None,
            has_top_level_or=False,
            comparisons=comparisons,
            has_limit=bool(_keyword_indices(tokens, {"LIMIT"})),
            has_order=bool(_keyword_indices(tokens, {"ORDER"})),
            has_group=bool(_keyword_indices(tokens, {"GROUP"})),
            has_union=bool(_keyword_indices(tokens, {"UNION", "INTERSECT", "EXCEPT"})),
        )

    where_index, where_token = where_positions[0]
    trailing_index = _next_clause_index(tokens, where_index + 1)
    where_tokens_end = trailing_index if trailing_index is not None else len(tokens)
    where_tokens = tokens[where_index + 1 : where_tokens_end]
    if trailing_index is not None:
        where_end = tokens[trailing_index].start
    else:
        where_end = len(sql)
    where_start = where_token.end
    where_text = sql[where_start:where_end].strip()
    has_or = any(
        token.depth == 0 and token.kind == "word" and token.upper == "OR" for token in where_tokens
    )
    default_table = tables[0] if len(tables) == 1 else None
    comparisons = _extract_comparisons(where_tokens, table_aliases, default_table)
    and_segments = (
        _split_where_on_keyword(sql, where_tokens, depth=0, keyword="AND")
        if not has_or
        else []
    )
    or_segments = (
        _split_where_on_keyword(sql, where_tokens, depth=0, keyword="OR")
        if has_or
        else []
    )

    return ParsedQuery(
        sql=sql,
        tables=tables,
        table_aliases=table_aliases,
        where_text=where_text,
        where_start=where_start,
        where_end=where_end,
        has_top_level_or=has_or,
        comparisons=comparisons,
        and_segments=and_segments,
        or_segments=or_segments,
        has_limit=bool(_keyword_indices(tokens, {"LIMIT"})),
        has_order=bool(_keyword_indices(tokens, {"ORDER"})),
        has_group=bool(_keyword_indices(tokens, {"GROUP"})),
        has_union=bool(_keyword_indices(tokens, {"UNION", "INTERSECT", "EXCEPT"})),
    )


def _table_snapshot(snapshot: dict[str, Any], table: str | None) -> dict[str, Any] | None:
    if not table:
        return None
    return snapshot.get("tables", {}).get(table)


def _ndv_for(table_snapshot: dict[str, Any] | None, column: str) -> int | None:
    if not table_snapshot:
        return None
    columns = table_snapshot.get("columns", {})
    data = columns.get(column.lower()) or columns.get(column)
    if not data:
        return None
    value = data.get("ndv")
    return int(value) if isinstance(value, int) else None


def comparison_selectivity(
    comparison: Comparison, snapshot: dict[str, Any], default_table: str | None
) -> float:
    table = comparison.table or default_table
    table_snapshot = _table_snapshot(snapshot, table)
    ndv = _ndv_for(table_snapshot, comparison.column)
    if comparison.operator == "=":
        return 1.0 / max(ndv or 1, 1)
    if comparison.operator == "IN":
        return min(1.0, comparison.literal_count / max(ndv or 1, 1))
    if comparison.operator in {"<", ">", "<=", ">=", "BETWEEN"}:
        return 0.25
    if comparison.operator == "LIKE":
        return 0.10
    if comparison.operator == "IS":
        return 0.05
    if comparison.operator in {"!=", "<>"}:
        return 0.90
    return 0.50


def estimate_result_rows(parsed: ParsedQuery, snapshot: dict[str, Any]) -> float:
    primary_table = parsed.primary_table
    table_snapshot = _table_snapshot(snapshot, primary_table)
    if not primary_table or not table_snapshot:
        return 0.0
    row_count = float(table_snapshot.get("row_count", 0))
    if row_count <= 0:
        return 0.0
    relevant = [
        comparison
        for comparison in parsed.comparisons
        if (comparison.table or primary_table) == primary_table
    ]
    if not relevant:
        return row_count
    if parsed.has_top_level_or:
        branch_probabilities: list[float] = []
        for branch in parsed.or_segments:
            branch_tokens = tokenize(branch)
            branch_comparisons = _extract_comparisons(branch_tokens, parsed.table_aliases, primary_table)
            if not branch_comparisons:
                branch_probability = 1.0
            else:
                product = 1.0
                for comparison in branch_comparisons:
                    product *= comparison_selectivity(comparison, snapshot, primary_table)
                branch_probability = product
            branch_probabilities.append(min(1.0, max(0.0, branch_probability)))
        if branch_probabilities:
            complement = 1.0
            for probability in branch_probabilities:
                complement *= 1.0 - probability
            return max(1.0, min(row_count, row_count * (1.0 - complement)))
    probability = 1.0
    for comparison in relevant:
        probability *= comparison_selectivity(comparison, snapshot, primary_table)
    return max(1.0, min(row_count, row_count * max(probability, 0.0)))


@dataclass(frozen=True, slots=True)
class MissingIndex:
    table: str
    columns: tuple[str, ...]
    ndv_estimates: tuple[int, ...]


def detect_missing_indexes(parsed: ParsedQuery, snapshot: dict[str, Any]) -> list[MissingIndex]:
    if parsed.has_top_level_or:
        return []
    results: list[MissingIndex] = []
    for table in parsed.tables:
        table_snapshot = _table_snapshot(snapshot, table)
        if not table_snapshot:
            continue
        columns: dict[str, int] = {}
        for comparison in parsed.comparisons:
            if (comparison.table or parsed.primary_table) != table:
                continue
            if comparison.operator not in {"=", "IN"}:
                continue
            ndv = _ndv_for(table_snapshot, comparison.column) or 1
            columns[comparison.column.lower()] = ndv
        if not columns:
            continue
        covered_by_an_index = any(
            set(columns).issubset({str(column).lower() for column in index.get("columns", [])})
            for index in table_snapshot.get("indexes", [])
            if index.get("columns")
        )
        if covered_by_an_index:
            continue
        ordered = sorted(columns.items(), key=lambda item: (-item[1], item[0]))
        results.append(
            MissingIndex(
                table=table,
                columns=tuple(column for column, _ in ordered),
                ndv_estimates=tuple(ndv for _, ndv in ordered),
            )
        )
    return results


def low_ndv_predicate_columns(parsed: ParsedQuery, snapshot: dict[str, Any]) -> list[tuple[str, str, int]]:
    result: list[tuple[str, str, int]] = []
    for comparison in parsed.comparisons:
        table = comparison.table or parsed.primary_table
        table_snapshot = _table_snapshot(snapshot, table)
        ndv = _ndv_for(table_snapshot, comparison.column)
        if table and ndv is not None and ndv <= 10:
            result.append((table, comparison.column.lower(), ndv))
    return result


def append_limit(sql: str, limit: int) -> str:
    if limit < 1:
        raise ValueError("limit must be positive")
    return sql.rstrip().rstrip(";").rstrip() + f" LIMIT {int(limit)}"


def build_split_or(parsed: ParsedQuery) -> str | None:
    if not parsed.has_top_level_or or parsed.where_start is None or parsed.where_end is None:
        return None
    if parsed.has_group or parsed.has_order or parsed.has_limit or parsed.has_union:
        return None
    if not parsed.or_segments or len(parsed.or_segments) < 2:
        return None
    prefix = parsed.sql[: parsed.where_start].rstrip()
    suffix = parsed.sql[parsed.where_end :].strip()
    if suffix:
        return None
    branches = [f"{prefix} {segment.strip()}" for segment in parsed.or_segments]
    return " UNION ".join(branches)


def build_reordered_predicates(parsed: ParsedQuery, snapshot: dict[str, Any]) -> str | None:
    if parsed.has_top_level_or or parsed.where_start is None or parsed.where_end is None:
        return None
    if len(parsed.and_segments) < 2:
        return None
    scored: list[tuple[int, int, str]] = []
    for position, segment in enumerate(parsed.and_segments):
        segment_tokens = tokenize(segment)
        comparisons = _extract_comparisons(
            segment_tokens, parsed.table_aliases, parsed.primary_table
        )
        best_ndv = 1
        for comparison in comparisons:
            table = comparison.table or parsed.primary_table
            ndv = _ndv_for(_table_snapshot(snapshot, table), comparison.column)
            if ndv is not None:
                best_ndv = max(best_ndv, ndv)
        scored.append((-best_ndv, position, segment.strip()))
    reordered = [item[2] for item in sorted(scored)]
    if reordered == [segment.strip() for segment in parsed.and_segments]:
        return None
    replacement = " AND ".join(reordered)
    prefix = parsed.sql[: parsed.where_start].rstrip()
    suffix = parsed.sql[parsed.where_end :].strip()
    if suffix:
        return f"{prefix} {replacement} {suffix}"
    return f"{prefix} {replacement}"


def referenced_tables_and_columns(sql: str) -> tuple[list[str], list[str]]:
    parsed = parse_query(sql)
    return parsed.tables, [comparison.column.lower() for comparison in parsed.comparisons]


def merge_error_details(errors: Iterable[str]) -> str:
    return "; ".join(error for error in errors if error)


_SIMPLE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def safe_identifier(value: str) -> bool:
    return bool(_SIMPLE_IDENTIFIER.match(value))

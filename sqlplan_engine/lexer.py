"""Deterministic SQL lexer with source positions."""

from __future__ import annotations

from dataclasses import dataclass

from .business import ParseError


@dataclass(frozen=True)
class Token:
    kind: str
    value: str
    raw: str
    start: int
    end: int
    line: int
    column: int

    @property
    def upper(self) -> str:
        return self.value.upper()


_TWO_CHAR_OPERATORS = {"<=", ">=", "<>", "!=", "||"}
_ONE_CHAR_TOKENS = set("(),.;+-*/%=<>")


class Lexer:
    def __init__(self, sql: str) -> None:
        self.sql = sql
        self.length = len(sql)
        self.index = 0
        self.line = 1
        self.column = 1

    def tokenize(self) -> list[Token]:
        tokens: list[Token] = []
        while True:
            self._skip_ignored()
            if self.index >= self.length:
                tokens.append(
                    Token("EOF", "", "", self.index, self.index, self.line, self.column)
                )
                return tokens
            start = self.index
            line = self.line
            column = self.column
            char = self.sql[self.index]

            if char in ("'", '"'):
                tokens.append(self._quoted(char, start, line, column))
                continue
            if char == "[":
                tokens.append(self._bracket_identifier(start, line, column))
                continue
            if char.isdigit():
                tokens.append(self._number(start, line, column))
                continue
            if char.isalpha() or char == "_":
                tokens.append(self._word(start, line, column))
                continue

            pair = self.sql[self.index : self.index + 2]
            if pair in _TWO_CHAR_OPERATORS:
                self._advance(pair[0])
                self._advance(pair[1])
                tokens.append(Token("OP", pair, pair, start, self.index, line, column))
                continue
            if char in _ONE_CHAR_TOKENS:
                self._advance(char)
                kind = "OP" if char in "=<>+-*/%" else "PUNC"
                tokens.append(Token(kind, char, char, start, self.index, line, column))
                continue
            raise ParseError(
                f"unexpected character {char!r}",
                line=line,
                column=column,
                offset=start,
                sql=self.sql,
            )

    def _skip_ignored(self) -> None:
        while self.index < self.length:
            char = self.sql[self.index]
            if char.isspace():
                self._advance(char)
                continue
            if self.sql.startswith("--", self.index):
                while self.index < self.length and self.sql[self.index] != "\n":
                    self._advance(self.sql[self.index])
                continue
            if self.sql.startswith("/*", self.index):
                start_line = self.line
                start_column = self.column
                start = self.index
                self._advance("/")
                self._advance("*")
                while self.index < self.length and not self.sql.startswith("*/", self.index):
                    self._advance(self.sql[self.index])
                if self.index >= self.length:
                    raise ParseError(
                        "unterminated block comment",
                        line=start_line,
                        column=start_column,
                        offset=start,
                        sql=self.sql,
                    )
                self._advance("*")
                self._advance("/")
                continue
            return

    def _quoted(self, quote: str, start: int, line: int, column: int) -> Token:
        self._advance(quote)
        chars: list[str] = []
        while self.index < self.length:
            char = self.sql[self.index]
            if char == quote:
                if self.index + 1 < self.length and self.sql[self.index + 1] == quote:
                    chars.append(quote)
                    self._advance(quote)
                    self._advance(quote)
                    continue
                self._advance(quote)
                raw = self.sql[start : self.index]
                kind = "STRING" if quote == "'" else "QIDENT"
                return Token(kind, "".join(chars), raw, start, self.index, line, column)
            chars.append(char)
            self._advance(char)
        label = "string literal" if quote == "'" else "quoted identifier"
        raise ParseError(
            f"unterminated {label}",
            line=line,
            column=column,
            offset=start,
            sql=self.sql,
        )

    def _bracket_identifier(self, start: int, line: int, column: int) -> Token:
        self._advance("[")
        chars: list[str] = []
        while self.index < self.length:
            char = self.sql[self.index]
            if char == "]":
                self._advance("]")
                raw = self.sql[start : self.index]
                return Token("QIDENT", "".join(chars), raw, start, self.index, line, column)
            chars.append(char)
            self._advance(char)
        raise ParseError(
            "unterminated bracket identifier",
            line=line,
            column=column,
            offset=start,
            sql=self.sql,
        )

    def _number(self, start: int, line: int, column: int) -> Token:
        seen_dot = False
        seen_exp = False
        while self.index < self.length:
            char = self.sql[self.index]
            if char.isdigit() or char == "_":
                self._advance(char)
                continue
            if char == "." and not seen_dot and not seen_exp:
                seen_dot = True
                self._advance(char)
                continue
            if char in "eE" and not seen_exp:
                seen_exp = True
                self._advance(char)
                if self.index < self.length and self.sql[self.index] in "+-":
                    self._advance(self.sql[self.index])
                continue
            break
        raw = self.sql[start : self.index]
        return Token("NUMBER", raw.replace("_", ""), raw, start, self.index, line, column)

    def _word(self, start: int, line: int, column: int) -> Token:
        while self.index < self.length:
            char = self.sql[self.index]
            if char.isalnum() or char in "_$":
                self._advance(char)
            else:
                break
        raw = self.sql[start : self.index]
        return Token("WORD", raw, raw, start, self.index, line, column)

    def _advance(self, char: str) -> None:
        self.index += 1
        if char == "\n":
            self.line += 1
            self.column = 1
        else:
            self.column += 1


def tokenize(sql: str) -> list[Token]:
    return Lexer(sql).tokenize()

